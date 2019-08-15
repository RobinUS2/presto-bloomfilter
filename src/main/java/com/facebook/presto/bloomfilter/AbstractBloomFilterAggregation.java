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
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.OutputFunction;

public class AbstractBloomFilterAggregation
{
    protected AbstractBloomFilterAggregation()
    {
    }

    public static BloomFilter getOrCreateBloomFilter(BloomFilterState state, int expectedInsertions, double falsePositivePercentage)
    {
        BloomFilter bf = state.getBloomFilter();
        if (bf == null) {
            bf = BloomFilter.newInstance(expectedInsertions, falsePositivePercentage);
            state.setBloomFilter(bf);
            state.addMemoryUsage(bf.estimatedInMemorySize());
        }
        return bf;
    }

    @CombineFunction
    public static void combine(BloomFilterState state, BloomFilterState otherState)
    {
        int ei = BloomFilter.DEFAULT_BLOOM_FILTER_EXPECTED_INSERTIONS;
        double fpp = BloomFilter.DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_PERCENTAGE;
        if (state.getBloomFilter() == null && otherState.getBloomFilter() != null) {
            ei = otherState.getBloomFilter().getExpectedInsertions();
            fpp = otherState.getBloomFilter().getFalsePositivePercentage();
        }
        else if (otherState.getBloomFilter() == null && state.getBloomFilter() != null) {
            ei = state.getBloomFilter().getExpectedInsertions();
            fpp = state.getBloomFilter().getFalsePositivePercentage();
        }
        BloomFilter bfState = getOrCreateBloomFilter(state, ei, fpp);
        BloomFilter bfOther = getOrCreateBloomFilter(otherState, ei, fpp);
        state.setBloomFilter(bfState.putAll(bfOther));
    }

    @OutputFunction(BloomFilterType.TYPE)
    public static void output(BloomFilterState state, BlockBuilder out)
    {
        BloomFilter bf = getOrCreateBloomFilter(state, BloomFilter.DEFAULT_BLOOM_FILTER_EXPECTED_INSERTIONS, BloomFilter.DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_PERCENTAGE);
        BloomFilterType.BLOOM_FILTER.writeSlice(out, bf.serialize());
    }
}
