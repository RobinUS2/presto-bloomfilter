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
import com.google.common.hash.PrimitiveSink;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class BloomFilter
{
    private com.google.common.hash.BloomFilter<Slice> instance;
    private int expectedInsertions;
    private double falsePositivePercentage;

    public static final int DEFAULT_BLOOM_FILTER_EXPECTED_INSERTIONS = 10_000_000;
    public static final double DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_PERCENTAGE = 0.01;

    public static BloomFilter newInstance()
    {
        return new BloomFilter();
    }

    public static BloomFilter newInstance(Slice serialized)
    {
        BloomFilter bf = new BloomFilter();
        bf.load(serialized);
        return bf;
    }

    private BloomFilter()
    {
        expectedInsertions = DEFAULT_BLOOM_FILTER_EXPECTED_INSERTIONS;
        falsePositivePercentage = DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_PERCENTAGE;
        instance = newBloomFilter();
    }

    public void put(Slice s)
    {
        instance.put(s);
    }

    public Funnel<Slice> getFunnel() {
        return new Funnel<Slice>()
        {
            @Override
            public void funnel(Slice s, PrimitiveSink into)
            {
                into.putBytes(s.getBytes());
            }
        };
    }

    public boolean mightContain(Slice s)
    {
        return instance.mightContain(s);
    }

    private void load(Slice serialized)
    {
        BasicSliceInput input = serialized.getInput();

        // Get the size of the bloom filter
        int bfSize = input.readInt();

        // Read the buffer
        // @todo Don't read byte by byte, but read directly full thing into stream
        byte[] bfBuf = new byte[bfSize];
        for (int i = 0; i < bfSize; i++) {
            bfBuf[i] = input.readByte();
        }

        // Input stream
        ByteArrayInputStream in = new ByteArrayInputStream(bfBuf);

        // Setup bloom filter
        try {
            instance = com.google.common.hash.BloomFilter.readFrom(in, getFunnel());
        } catch (IOException ix) {
            // @todo Log
            instance = newBloomFilter();
        }
    }

    private com.google.common.hash.BloomFilter<Slice> newBloomFilter() {
        return com.google.common.hash.BloomFilter.create(getFunnel(), expectedInsertions, falsePositivePercentage);
    }

    public Slice serialize()
    {
        int size;
        byte[] bytes = new byte[0];
        try {
            // Write bloom filter to bytes
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            instance.writeTo(buffer);
            bytes = buffer.toByteArray();

            // Size "estimation" is actual
            size = bytes.length;
        } catch (IOException ix) {
            // @todo Log
            size = 0;
        }

        // To slice
        DynamicSliceOutput output = new DynamicSliceOutput(size);

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
        double n = (double)expectedInsertions;
        double p = falsePositivePercentage;
        // @todo Optimize the constants
        double m = Math.ceil((n * Math.log(p)) / Math.log(1.0 / (Math.pow(2.0, Math.log(2.0)))) );
        //double k = Math.round(Math.log(2.0) * (m / n));
        return (int)Math.round(m);
    }
}
