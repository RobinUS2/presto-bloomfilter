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

import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertEquals;

public class TestBloomFilter
{
    @Test
    public void testBloomFilterCreate()
    {
        BloomFilter bf = BloomFilter.newInstance();
        assertNotNull(bf);
    }

    @Test
    public void testBloomFilterElementExists()
    {
        BloomFilter bf = BloomFilter.newInstance();
        bf.put(Slices.wrappedBuffer("robin".getBytes()));
        assertTrue(bf.mightContain(Slices.wrappedBuffer("robin".getBytes())));
        assertFalse(bf.mightContain(Slices.wrappedBuffer("verlangen".getBytes())));
    }

    @Test
    public void testBloomFilterMerge()
    {
        BloomFilter bf = BloomFilter.newInstance();
        bf.put(Slices.wrappedBuffer("robin".getBytes()));

        BloomFilter bf2 = BloomFilter.newInstance();
        bf2.put(Slices.wrappedBuffer("verlangen".getBytes()));

        // Merge
        bf.putAll(bf2);

        // Check whether contents of the second BF can be found
        assertTrue(bf.mightContain(Slices.wrappedBuffer("verlangen".getBytes())));
    }

    @Test
    public void testBloomFilterSizeEstimation()
    {
        // Default options (10MM items with 1% error rate)
        BloomFilter bf = BloomFilter.newInstance();
        assertEquals(bf.estimatedInMemorySize(), 11981323);

        // Smaller
        BloomFilter bf2 = BloomFilter.newInstance(100);
        assertEquals(bf2.estimatedInMemorySize(), 120);

        // Smaller with lower false positive percentage
        BloomFilter bf3 = BloomFilter.newInstance(100, 0.001);
        assertEquals(bf3.estimatedInMemorySize(), 180);
    }
}
