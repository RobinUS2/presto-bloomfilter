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
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import javax.annotation.Nullable;

@Description(value = "Converts a bloom filter to a string representation")
@Nullable
@ScalarFunction(value = "to_string")
public final class BloomFilterToStringScalarFunction
        extends BloomFilterScalarFunctions
{
    private BloomFilterToStringScalarFunction()
    {
        log.warn("New " + getClass().getSimpleName() + " should never be run");
    }

    @Nullable
    @SqlNullable
    @SqlType(StandardTypes.VARCHAR)
    public static Slice bloomFilterToString(@SqlNullable @SqlType(BloomFilterType.TYPE) Slice bloomFilterSlice)
    {
        BloomFilter bf = getOrLoadBloomFilter(bloomFilterSlice);
        return Slices.wrappedBuffer(bf.toBase64());
    }
}
