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
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.hash.HashCode;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import javax.annotation.Nullable;

public final class BloomFilterScalarFunctions
{
    private static final Cache<HashCode, BloomFilter> BF_CACHE = CacheBuilder.newBuilder().maximumSize(40).build();

    private BloomFilterScalarFunctions()
    {
    }

    @Nullable
    @ScalarFunction("bloom_filter_contains") // For now I think the name Bloom Filter indicates the fact it is probabilistic. bloom_filter_might_contain would be an alternative but I think it's too verbose.
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean varcharBloomFilterContains(@SqlType(BloomFilterType.TYPE) Slice bloomFilterSlice, @SqlType(StandardTypes.VARCHAR) Slice slice)
    {
        BloomFilter bf = getOrLoadBloomFilter(bloomFilterSlice);
        return bf.mightContain(slice);
    }

    @Nullable
    @ScalarFunction("to_string")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice bloomFilterToString(@SqlType(BloomFilterType.TYPE) Slice bloomFilterSlice)
    {
        BloomFilter bf = getOrLoadBloomFilter(bloomFilterSlice);
        return Slices.wrappedBuffer(java.util.Base64.getEncoder().encode(bf.serialize().getBytes()));
    }

    private static BloomFilter getOrLoadBloomFilter(Slice bloomFilterSlice)
    {
        // Read hash
        HashCode hash = BloomFilter.readHash(bloomFilterSlice);

        // From cache
        BloomFilter bf = BF_CACHE.getIfPresent(hash);
        if (bf == null) {
            bf = BloomFilter.newInstance(bloomFilterSlice);
            BF_CACHE.put(hash, bf);
        }
        return bf;
    }
}
