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

import com.facebook.presto.operator.Description;
import com.facebook.presto.operator.scalar.annotations.ScalarFunction;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import io.airlift.slice.Slice;

import javax.annotation.Nullable;

@Description("Determines if this element is in the bloom filter")
@Nullable
@ScalarFunction("bloom_filter_contains") // For now I think the name Bloom Filter indicates the fact it is probabilistic. bloom_filter_might_contain would be an alternative but I think it's too verbose.
public final class BloomFilterContainsScalarFunction extends BloomFilterScalarFunctions
{
    private BloomFilterContainsScalarFunction()
    {
        log.warn("New " + getClass().getSimpleName() + " should never be run");
    }

    @SqlType(StandardTypes.BOOLEAN)
    @Nullable
    public static Boolean varcharBloomFilterContains(@Nullable @SqlType(BloomFilterType.TYPE) Slice bloomFilterSlice, @Nullable @SqlType(StandardTypes.VARCHAR) Slice slice)
    {
        BloomFilter bf = getOrLoadBloomFilter(bloomFilterSlice);
        if (slice == null) {
            return false;
        }
        return bf.mightContain(slice);
    }
}
