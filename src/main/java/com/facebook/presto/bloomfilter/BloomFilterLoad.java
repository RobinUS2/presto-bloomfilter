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

import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;

import static com.facebook.presto.spi.type.StandardTypes.VARCHAR;

@AggregationFunction(value = "bloom_filter_load")
public class BloomFilterLoad
        extends AbstractBloomFilterAggregation
{
    private BloomFilterLoad()
    {
    }

    @InputFunction
    public static void input(
            BloomFilterState state,
            @SqlType(VARCHAR) Slice slice) throws Exception
    {
        BloomFilter bf = getOrCreateBloomFilter(state, BloomFilter.DEFAULT_BLOOM_FILTER_EXPECTED_INSERTIONS, BloomFilter.DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_PERCENTAGE);
        // Do not try catch because we want to have visibility for client errors
        bf.putAll(BloomFilter.fromUrl(new String(slice.getBytes())));
        state.setBloomFilter(bf);
    }
}
