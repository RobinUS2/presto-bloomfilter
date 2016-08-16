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

@Description("Display expected insertions from the bloom filter")
@Nullable
@ScalarFunction("get_false_positive_percentage")
public final class BloomFilterGetFalsePositivePercentageScalarFunction
        extends BloomFilterScalarFunctions
{
    private BloomFilterGetFalsePositivePercentageScalarFunction()
    {
        log.warn("New " + getClass().getSimpleName() + " should never be run");
    }

    @Description("Display expected insertions from the bloom filter")
    @Nullable
    @SqlType(StandardTypes.DOUBLE)
    public static Double bloomFilterFalsePositivePercentage(@Nullable @SqlType(BloomFilterType.TYPE) Slice bloomFilterSlice)
    {
        BloomFilter bf = getOrLoadBloomFilter(bloomFilterSlice);
        return bf.getFalsePositivePercentage();
    }
}
