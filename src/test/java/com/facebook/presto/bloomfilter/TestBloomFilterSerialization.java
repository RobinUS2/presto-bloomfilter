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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.Date;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestBloomFilterSerialization
{
    @Test
    public void testBloomFilterCreate()
    {
        // Fresh instance
        BloomFilter bf = BloomFilter.newInstance();

        // Add a little bit of data
        bf.put(Slices.wrappedBuffer("robin".getBytes()));

        // Serialize
        Slice ser = bf.serialize();

        // Validate not empty
        assertNotNull(ser);
        assertTrue(ser.length() > 0);

        // Deserialize
        BloomFilter bf2 = BloomFilter.newInstance(ser);

        // Validate
        assertTrue(bf2.mightContain(Slices.wrappedBuffer("robin".getBytes())));
        assertFalse(bf2.mightContain(Slices.wrappedBuffer("not-in-here".getBytes())));
    }

    @Test
    public void testBloomFilterPerformanceSerialize()
    {
        BloomFilter bf = BloomFilter.newInstance();
        long start = new Date().getTime();
        bf.put(Slices.wrappedBuffer("robin".getBytes()));
        for (int i = 0; i < 10; i++) {
            // @todo Check optimize, seems to take about 20ms
            bf.serialize();
        }
        long took = new Date().getTime() - start;
        assertTrue(took < 10000L);
    }

    @Test
    public void testBloomFilterPerformanceDeserialize()
    {
        BloomFilter bf = BloomFilter.newInstance();
        long start = new Date().getTime();
        bf.put(Slices.wrappedBuffer("robin".getBytes()));
        Slice ser = bf.serialize();
        for (int i = 0; i < 10; i++) {
            // @todo Check optimize, seems to take about 20ms
            BloomFilter.newInstance(ser);
        }
        long took = new Date().getTime() - start;
        assertTrue(took < 10000L);
    }
}
