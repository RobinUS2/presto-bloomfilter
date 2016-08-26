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

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;

import javax.annotation.Nullable;

@Description("Display expected insertions from the bloom filter")
@Nullable
@ScalarFunction("get_expected_insertions")
public final class BloomFilterGetExpectedInsertionsScalarFunction
        extends BloomFilterScalarFunctions
{
    private BloomFilterGetExpectedInsertionsScalarFunction()
    {
        log.warn("New " + getClass().getSimpleName() + " should never be run");
    }

    @Nullable
    @SqlType(StandardTypes.BIGINT)
    public static Long bloomFilterExpectedInsertions(@Nullable @SqlType(BloomFilterType.TYPE) Slice bloomFilterSlice)
    {
        BloomFilter bf = getOrLoadBloomFilter(bloomFilterSlice);
        return (long) bf.getExpectedInsertions();
    }
}
