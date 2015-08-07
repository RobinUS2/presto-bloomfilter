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

import com.facebook.presto.operator.scalar.ScalarFunction;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import io.airlift.slice.Slice;

import javax.annotation.Nullable;

public final class BloomFilterScalarFunctions
{
    private BloomFilterScalarFunctions()
    {
    }

    @Nullable
    @ScalarFunction("bloom_filter_contains") // @todo Or should we use bloom_filter_might_contain? For now I think the name Bloom Filter indicates the fact it is probabilistic
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean varcharBloomFilterContains(@SqlType(BloomFilterType.TYPE) Slice bloomFilterSlice, @SqlType(StandardTypes.VARCHAR) Slice slice)
    {
        // @todo Implement cache
        BloomFilter bf = BloomFilter.newInstance(bloomFilterSlice);
        return bf.mightContain(slice);
    }
}
