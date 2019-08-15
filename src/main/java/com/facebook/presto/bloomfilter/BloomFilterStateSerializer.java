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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.type.Type;

public class BloomFilterStateSerializer
        implements AccumulatorStateSerializer<BloomFilterState>
{
    @Override
    public Type getSerializedType()
    {
        return BloomFilterType.BLOOM_FILTER;
    }

    @Override
    public void serialize(BloomFilterState state, BlockBuilder out)
    {
        if (state.getBloomFilter() == null) {
            out.appendNull();
        }
        else {
            BloomFilterType.BLOOM_FILTER.writeSlice(out, state.getBloomFilter().serialize());
        }
    }

    @Override
    public void deserialize(Block block, int index, BloomFilterState state)
    {
        if (!block.isNull(index)) {
            state.setBloomFilter(BloomFilter.newInstance(BloomFilterType.BLOOM_FILTER.getSlice(block, index)));
        }
    }
}
