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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import io.airlift.log.Logger;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import org.apache.commons.io.IOUtils;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

// Layout is <hash>:<size>:<bf>, where
//   hash: is a sha256 hash of the bloom filter
//   size: is an int describing the length of the bf bytes
//   expectedInsertions: is an int describing the amount of expected elements
//   falsePositivePercentage: is a double describing the desired false positive percentage
//   bf: is the serialized bloom filter
public class BloomFilter
{
    private com.google.common.hash.BloomFilter<byte[]> instance;
    private int expectedInsertions;
    private double falsePositivePercentage;
    private static Funnel<byte[]> funnel;
    private Kryo kryo;
    private static final boolean KRYO_ENABLED = true;

    private static final Logger log = Logger.get(BloomFilter.class);

    static {
        BloomFilter.funnel = Funnels.byteArrayFunnel();
    }

    public static final int DEFAULT_BLOOM_FILTER_EXPECTED_INSERTIONS = 10_000_000;
    public static final double DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_PERCENTAGE = 0.01;

    public static final double BF_MEM_CONSTANT = Math.log(1.0 / (Math.pow(2.0, Math.log(2.0))));

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
        instance = newBloomFilter();

        if (KRYO_ENABLED) {
            kryo = new Kryo();
            kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
            kryo.register(com.google.common.hash.BloomFilter.class);
            kryo.register(Slice.class);
        }
    }

    public void put(Slice s)
    {
        if (s == null || s.length() < 1) {
            return;
        }
        instance.put(s.getBytes());
    }

    public BloomFilter putAll(BloomFilter other)
    {
        instance.putAll(other.instance);
        return this;
    }

    public Funnel<byte[]> getFunnel()
    {
        return funnel;
    }

    public boolean mightContain(Slice s)
    {
        return instance.mightContain(s.getBytes());
    }

    private void load(Slice serialized)
    {
        BasicSliceInput input = serialized.getInput();

        // Read hash
        byte[] bfHash = new byte[32];
        input.readBytes(bfHash, 0, 32);

        // Get the size of the bloom filter
        int bfSize = input.readInt();

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
            if (KRYO_ENABLED) {
                Input i = new Input(in);
                instance = getKryo().readObject(i, com.google.common.hash.BloomFilter.class);
                input.close();
            }
            else {
                instance = com.google.common.hash.BloomFilter.readFrom(in, getFunnel());
            }
        }
        catch (Exception ix) {
            log.error(ix);
            instance = newBloomFilter();
        }
    }

    private com.google.common.hash.BloomFilter<byte[]> newBloomFilter()
    {
        return com.google.common.hash.BloomFilter.create(getFunnel(), expectedInsertions, falsePositivePercentage);
    }

    public Kryo getKryo()
    {
        return kryo;
    }

    public Slice serialize()
    {
        int size;
        byte[] bytes = new byte[0];
        try {
            if (KRYO_ENABLED) {
                // Kryo
                ByteArrayOutputStream buffer2 = new ByteArrayOutputStream();
                Output o = new  Output(buffer2);
                getKryo().writeObject(o, instance);
                o.flush();
                bytes = buffer2.toByteArray();
                o.close();
                buffer2.close();
            }
            else {
                // Write bloom filter to bytes by using the bloom filter method
                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                instance.writeTo(buffer);
                bytes = buffer.toByteArray();
            }
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
        size = compressed.length;

        // To slice
        DynamicSliceOutput output = new DynamicSliceOutput(size);

        // Write hash
        output.writeBytes(bfHash); // 32 bytes

        // Write the length of the bloom filter
        output.appendInt(size);

        // Params
        output.appendInt(expectedInsertions);
        output.appendDouble(falsePositivePercentage);

        // Write the bloom filter
        output.appendBytes(compressed);

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
        return HashCode.fromBytes(s.getBytes(0, 32));
    }
}
