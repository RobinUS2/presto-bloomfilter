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
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.util.StringContentProvider;

import javax.annotation.Nullable;

@Description("Persist a bloom filter to the persist service over HTTP")
@Nullable
@ScalarFunction("bloom_filter_persist")
public final class BloomFilterPersistScalarFunction
        extends BloomFilterScalarFunctions
{
    private BloomFilterPersistScalarFunction()
    {
        log.warn("New " + getClass().getSimpleName() + " should never be run");
    }

    @SqlType(StandardTypes.BOOLEAN)
    @Nullable
    public static Boolean bloomFilterPersist(@Nullable @SqlType(BloomFilterType.TYPE) Slice bloomFilterSlice, @SqlType(StandardTypes.VARCHAR) Slice urlSlice) throws Exception
    {
        // Nothing todo
        if (urlSlice == null) {
            return true;
        }
        BloomFilter bf = getOrLoadBloomFilter(bloomFilterSlice);

        // Persist
        // we do not try catch here to make sure that errors are communicated clearly to the client
        // and typical retry logic continues to work
        String url = new String(urlSlice.getBytes());
        if (!HTTP_CLIENT.isStarted()) {
            log.warn("Http client was not started, trying to start");
            HTTP_CLIENT.start();
        }
        Request post = HTTP_CLIENT.POST(url);
        post.content(new StringContentProvider(new String(bf.toBase64())));
        post.method("PUT");
        post.send();
        log.info("Persisted " + bf.toString() + " " + url);
        return true;
    }
}
