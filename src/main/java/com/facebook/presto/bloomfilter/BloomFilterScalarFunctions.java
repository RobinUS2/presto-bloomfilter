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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.hash.HashCode;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import org.eclipse.jetty.client.HttpClient;

public abstract class BloomFilterScalarFunctions
{
    protected static final Cache<HashCode, BloomFilter> BF_CACHE = CacheBuilder.newBuilder().maximumSize(40).build();
    protected static final Logger log = Logger.get(BloomFilterScalarFunctions.class);
    public static final HttpClient HTTP_CLIENT = new HttpClient();

    static {
        try {
            log.info("Http client starting");
            HTTP_CLIENT.start();
            log.info("Http client started");
        }
        catch (Exception e) {
            log.warn("Failed to start http client", e);
            e.printStackTrace();
        }
    }

    protected BloomFilterScalarFunctions()
    {
        log.warn("New " + getClass().getSimpleName() + " should never be run");
    }

    protected static BloomFilter getOrLoadBloomFilter(Slice bloomFilterSlice)
    {
        // Read hash
        HashCode hash = BloomFilter.readHash(bloomFilterSlice);

        // From cache
        BloomFilter bf = null;
        if (bloomFilterSlice != null) {
            bf = BloomFilterScalarFunctions.BF_CACHE.getIfPresent(hash);
        }
        if (bf == null) {
            bf = BloomFilter.newInstance(bloomFilterSlice);
            BloomFilterScalarFunctions.BF_CACHE.put(hash, bf);
        }
        return bf;
    }
}
