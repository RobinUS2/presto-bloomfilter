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

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.Random;
import java.util.UUID;

import static org.testng.Assert.assertTrue;

public class TestBloomFilterPerformancePutThenContains
{
    private static final Logger log = Logger.get(TestBloomFilterPerformancePutThenContains.class);

    @Test
    public void testBloomFilterPerformancePutAndMightContains()
    {
        BloomFilter bf = BloomFilter.newInstance();
        long start = new Date().getTime();
        Random rand = new Random();

        // Load data
        int iterations = 250000;
        ArrayList<UUID> list = new ArrayList<>();
        for (int i = 0; i < iterations; i++) {
            UUID u = UUID.randomUUID();
            list.add(u);
            Slice x = Slices.wrappedBuffer(u.toString().getBytes());
            bf.put(x);
        }

        // Read data
        int matches = 0;
        int listSize = list.size();
        for (int i = 0; i < iterations; i++) {
            UUID u = null;
            if (rand.nextBoolean()) {
                u = list.get(rand.nextInt(listSize - 1));
            }
            else {
                u = UUID.randomUUID();
            }
            Slice x = Slices.wrappedBuffer(u.toString().getBytes());
            if (bf.mightContain(x)) {
                matches++;
            }
        }
        log.warn("matches " + matches);
        log.warn("premiss " + bf.getPreMiss());
        long took = new Date().getTime() - start;
        assertTrue(took < 10000L);
        assertTrue(matches >= 1);
        assertTrue(bf.getPreMiss() >= 0.1 * iterations);
    }
}
