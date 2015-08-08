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

import com.google.common.hash.Funnel;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.hash.PrimitiveSink;
import io.airlift.log.Logger;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

// Layout is <hash>:<size>:<bf>, where
//   hash: is a sha256 hash of the bloom filter
//   size: is an int describing the length of the bf bytes
//   bf: is the serialized bloom filter
public class BloomFilter
{
    private com.google.common.hash.BloomFilter<Slice> instance;
    private int expectedInsertions;
    private double falsePositivePercentage;
    private static Funnel<Slice> funnel;

    private static final Logger log = Logger.get(BloomFilter.class);

    static {
        BloomFilter.funnel = new Funnel<Slice>()
        {
            @Override
            public void funnel(Slice s, PrimitiveSink into)
            {
                into.putBytes(s.getBytes());
            }
        };
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
    }

    public void put(Slice s)
    {
        if (s == null || s.length() < 1) {
            return;
        }
        instance.put(s);
    }

    public BloomFilter putAll(BloomFilter other)
    {
        instance.putAll(other.instance);
        return this;
    }

    public Funnel<Slice> getFunnel()
    {
        return funnel;
    }

    public boolean mightContain(Slice s)
    {
        return instance.mightContain(s);
    }

    private void load(Slice serialized)
    {
        BasicSliceInput input = serialized.getInput();

        // Debug
        log.warn("Deser");

        // Read hash
        byte[] bfHash = new byte[32];
        input.readBytes(bfHash, 0, 32);

        // Get the size of the bloom filter
        int bfSize = input.readInt();

        // Read the buffer
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            input.readBytes(out, bfSize);
        }
        catch (IOException ix) {
            // @todo Log
        }

        // Input stream
        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());

        // Setup bloom filter
        try {
            instance = com.google.common.hash.BloomFilter.readFrom(in, getFunnel());
        }
        catch (IOException ix) {
            // @todo Log
            instance = newBloomFilter();
        }
    }

    private com.google.common.hash.BloomFilter<Slice> newBloomFilter()
    {
        return com.google.common.hash.BloomFilter.create(getFunnel(), expectedInsertions, falsePositivePercentage);
    }

    public Slice serialize()
    {
        // Debug
        log.warn("Ser");

        int size;
        byte[] bytes = new byte[0];
        try {
            // Write bloom filter to bytes
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            instance.writeTo(buffer);
            bytes = buffer.toByteArray();

            // Size "estimation" is actual
            size = bytes.length;
        }
        catch (IOException ix) {
            // @todo Log
            size = 0;
        }

        // Create hash
        byte[] bfHash = Hashing.sha256().hashBytes(bytes).asBytes();

        // To slice
        DynamicSliceOutput output = new DynamicSliceOutput(size);

        // Write hash
        output.writeBytes(bfHash); // 32 bytes

        // Write the length of the bloom filter
        output.appendInt(size);

        // Write the bloom filter
        output.appendBytes(bytes);

        return output.slice();
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
