/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.bloomfilter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteStreams;
import io.airlift.log.Logger;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.HashProvider;
import org.apache.commons.io.IOUtils;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.util.InputStreamResponseListener;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

// Layout is <hash>:<size>:<size_pre>:<bf_pre>:<bf>, where
//   hash: is a sha256 hash of the bloom filter
//   size: is an int describing the length of the bf bytes
//   size_pre: is an int describing the length of the pre bf bytes
//   expectedInsertions: is an int describing the amount of expected elements
//   falsePositivePercentage: is a double describing the desired false positive percentage
//   bf_pre: is the serialized bloom filter used for pre-filtering
//   bf: is the serialized bloom filter
public class BloomFilter
{
    private static final HashCode HASH_CODE_NOT_FOUND = HashCode.fromInt(0);
    private orestes.bloomfilter.BloomFilter instancePreFilter;
    private orestes.bloomfilter.BloomFilter instance;
    private int expectedInsertions;
    private double falsePositivePercentage;
    private long preMiss;

    private static final boolean USE_PRE_FILTER = true;

    private static final Logger log = Logger.get(BloomFilter.class);

    public static final int DEFAULT_BLOOM_FILTER_EXPECTED_INSERTIONS = 10_000_000;
    public static final double DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_PERCENTAGE = 0.01;

    public static final double BF_MEM_CONSTANT = Math.log(1.0 / (Math.pow(2.0, Math.log(2.0))));

    static
    {
        try {
            if (!BloomFilterScalarFunctions.HTTP_CLIENT.isStarted()) {
                log.warn("Http client was not started, trying to start");
                BloomFilterScalarFunctions.HTTP_CLIENT.start();
            }
        }
        catch (Exception ex) {
            log.error("Error starting http client: " + ex.getMessage());
        }
    }

    public int getExpectedInsertions()
    {
        return expectedInsertions;
    }

    public double getFalsePositivePercentage()
    {
        return falsePositivePercentage;
    }

    public static BloomFilter newInstance()
    {
        return new BloomFilter(DEFAULT_BLOOM_FILTER_EXPECTED_INSERTIONS, DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_PERCENTAGE);
    }

    public static BloomFilter newInstance(int expectedInsertions, double falsePositivePercentage)
    {
        return new BloomFilter(expectedInsertions, falsePositivePercentage);
    }

    public static BloomFilter newInstance(int expectedInsertions)
    {
        return new BloomFilter(expectedInsertions, DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_PERCENTAGE);
    }

    // Construct from serialized string
    public static BloomFilter newInstance(byte[] fromBytes)
    {
        BloomFilter bf = newInstance();
        byte[] serializedBytes = java.util.Base64.getDecoder().decode(fromBytes);
        bf.load(Slices.wrappedBuffer(serializedBytes));
        return bf;
    }

    public static BloomFilter fromUrl(String url) throws Exception
    {
        log.info("Loading bloom filter from " + url);

        Request request = BloomFilterScalarFunctions.HTTP_CLIENT.newRequest(url);
        request.method("GET");
        InputStreamResponseListener listener = new InputStreamResponseListener();
        request.send(listener);

        // Wait for the response headers to arrive
        Response response = listener.get(10, TimeUnit.SECONDS);

        // Look at the response
        if (response.getStatus() == 200) {
            // Use try-with-resources to close input stream.
            try (InputStream responseContent = listener.getInputStream()) {
                byte[] bytes = ByteStreams.toByteArray(responseContent);
                return newInstance(bytes);
            }
        }
        log.warn("Non-200 response status " + response.getStatus());
        return null;
    }

    public static BloomFilter newInstance(Slice serialized)
    {
        BloomFilter bf = newInstance();
        bf.load(serialized);
        return bf;
    }

    private BloomFilter(int expectedInsertions, double falsePositivePercentage)
    {
        this.expectedInsertions = expectedInsertions;
        this.falsePositivePercentage = falsePositivePercentage;
        initbloomFilters();
    }

    public byte[] toBase64()
    {
        return java.util.Base64.getEncoder().encode(serialize().getBytes());
    }

    public BloomFilter put(Slice s)
    {
        if (s == null) {
            return this;
        }
        byte[] b = s.getBytes();
        if (b.length < 1) {
            return this;
        }
        instance.add(b);
        if (USE_PRE_FILTER) {
            instancePreFilter.add(b);
        }
        return this;
    }

    public BloomFilter putAll(BloomFilter other)
    {
        instance.union(other.instance);
        if (USE_PRE_FILTER) {
            instancePreFilter.union(other.instancePreFilter);
        }
        return this;
    }

    public boolean mightContain(Slice s)
    {
        byte[] b = s.getBytes();
        if (USE_PRE_FILTER) {
            if (instancePreFilter.contains(b)) {
                return instance.contains(b);
            }
            else {
                preMiss++;
                return false;
            }
        }
        else {
            return instance.contains(b);
        }
    }

    @VisibleForTesting
    public long getPreMiss()
    {
        return preMiss;
    }

    private void load(Slice serialized)
    {
        BasicSliceInput input = serialized.getInput();

        // Read hash
        byte[] bfHash = new byte[32];
        input.readBytes(bfHash, 0, 32);

        // Get the size of the bloom filter
        int bfSize = input.readInt();

        // Get the size of the bloom filter
        int bfSizePre = input.readInt();

        // Params
        expectedInsertions = input.readInt();
        falsePositivePercentage = input.readDouble();

        // Read the buffer
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            input.readBytes(out, bfSize);
        }
        catch (IOException ix) {
            log.error(ix);
        }

        // Uncompress
        byte[] uncompressed;
        try {
            uncompressed = decompress(out.toByteArray());
        }
        catch (IOException ix) {
            log.error(ix);
            uncompressed = new byte[0];
        }

        // Input stream
        ByteArrayInputStream in = new ByteArrayInputStream(uncompressed);

        // Setup bloom filter
        try {
            ObjectInputStream ois = new ObjectInputStream(in);
            instance = (orestes.bloomfilter.BloomFilter) ois.readObject();
            input.close();
        }
        catch (Exception ix) {
            log.error(ix);
            initbloomFilters();
        }

        // Read the buffer
        ByteArrayOutputStream outPre = new ByteArrayOutputStream();
        try {
            input.readBytes(outPre, bfSizePre);
        }
        catch (IOException ix) {
            log.error(ix);
        }

        // Uncompress
        byte[] uncompressedPre;
        try {
            uncompressedPre = decompress(outPre.toByteArray());
        }
        catch (IOException ix) {
            log.error(ix);
            uncompressedPre = new byte[0];
        }

        // Input stream
        ByteArrayInputStream inPre = new ByteArrayInputStream(uncompressedPre);

        // Setup bloom filter
        try {
            ObjectInputStream ois = new ObjectInputStream(inPre);
            instancePreFilter = (orestes.bloomfilter.BloomFilter) ois.readObject();
            input.close();
        }
        catch (Exception ix) {
            log.error(ix);
            initbloomFilters();
        }
    }

    private void initbloomFilters()
    {
        instance = newBloomFilter();
        instancePreFilter = newPreBloomFilter();
    }

    private orestes.bloomfilter.BloomFilter newBloomFilter()
    {
        return
                new FilterBuilder(expectedInsertions, falsePositivePercentage)
                        .hashFunction(HashProvider.HashMethod.Murmur3KirschMitzenmacher)
                        .buildBloomFilter();
    }

    private orestes.bloomfilter.BloomFilter newPreBloomFilter()
    {
        return
                new FilterBuilder(Math.max(expectedInsertions / 10, 10), Math.min(falsePositivePercentage * 10, 0.5))
                        .hashFunction(HashProvider.HashMethod.FNVWithLCG)
                        .hashes(1)
                        .buildBloomFilter();
    }

    public Slice serialize()
    {
        byte[] bytes = new byte[0];
        byte[] bytesPre = new byte[0];
        try {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            ObjectOutput output = new ObjectOutputStream(buffer);
            output.writeObject(instance);
            bytes = buffer.toByteArray();

            ByteArrayOutputStream bufferPre = new ByteArrayOutputStream();
            ObjectOutput outputPre = new ObjectOutputStream(bufferPre);
            outputPre.writeObject(instancePreFilter);
            bytesPre = bufferPre.toByteArray();
        }
        catch (Exception ix) {
            log.error(ix);
        }

        // Create hash
        byte[] bfHash = Hashing.sha256().hashBytes(bytes).asBytes();

        // Compress
        byte[] compressed;
        try {
            compressed = compress(bytes);
        }
        catch (IOException ix) {
            log.error(ix);
            compressed = new byte[0];
        }
        int size = compressed.length;

        // Compress
        byte[] compressedPre;
        try {
            compressedPre = compress(bytesPre);
        }
        catch (IOException ix) {
            log.error(ix);
            compressedPre = new byte[0];
        }
        int sizePre = compressedPre.length;

        // To slice
        DynamicSliceOutput output = new DynamicSliceOutput(size);

        // Write hash
        output.writeBytes(bfHash); // 32 bytes

        // Write the length of the bloom filter
        output.appendInt(size);

        // Write the length of the pre bloom filter
        output.appendInt(sizePre);

        // Params
        output.appendInt(expectedInsertions);
        output.appendDouble(falsePositivePercentage);

        // Write the bloom filter
        output.appendBytes(compressed);

        // Write the bloom filter
        output.appendBytes(compressedPre);

        return output.slice();
    }

    public static byte[] compress(byte[] b) throws IOException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GZIPOutputStream gzip = new GZIPOutputStream(out);
        gzip.write(b);
        gzip.close();
        return out.toByteArray();
    }

    public static byte[] decompress(byte[] b) throws IOException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOUtils.copy(new GZIPInputStream(new ByteArrayInputStream(b)), out);
        return out.toByteArray();
    }

    public int estimatedInMemorySize()
    {
        // m = ceil((n * log(p)) / log(1.0 / (pow(2.0, log(2.0)))));
        // k = round(log(2.0) * m / n);
        // Source: http://hur.st/bloomfilter
        double m = Math.ceil(((double) expectedInsertions * Math.log(falsePositivePercentage)) / BF_MEM_CONSTANT) / 8.0D;
        return (int) Math.round(m);
    }

    public static HashCode readHash(Slice s)
    {
        if (s == null) {
            return HASH_CODE_NOT_FOUND;
        }
        return HashCode.fromBytes(s.getBytes(0, 32));
    }
}
