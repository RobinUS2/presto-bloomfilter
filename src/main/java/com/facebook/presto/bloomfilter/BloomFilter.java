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

public class BloomFilter
{
    private com.google.common.hash.BloomFilter<Slice> instance;

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
        Funnel<Slice> objFunnel = new Funnel<Slice>()
        {
            @Override
            public void funnel(Slice s, PrimitiveSink into)
            {
                into.putBytes(s.getBytes());
            }
        };
        instance = com.google.common.hash.BloomFilter.create(objFunnel, 10_000_000, 0.01);
    }

    public void put(Slice s)
    {
        instance.put(s);
    }

    public boolean mightContain(Slice s)
    {
        return instance.mightContain(s);
    }

    private void load(Slice serialized)
    {
        BasicSliceInput input = serialized.getInput();
        // @todo read input and convert to bloom filter instance
    }

    public Slice serialize()
    {
        int size = estimatedSerializedSize();

        DynamicSliceOutput output = new DynamicSliceOutput(size);
        // @todo write bytes here

        return output.slice();
    }

    public int estimatedSerializedSize()
    {
        return 123; // @todo real estimate in bytes of the serialized length of the bloom filter
    }
}
