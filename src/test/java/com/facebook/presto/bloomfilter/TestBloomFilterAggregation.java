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

import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.BlockBuilderStatusExtended;
import com.facebook.presto.spi.block.PageBuilderStatus;
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

public class TestBloomFilterAggregation
{
    @Test
    public void testBloomFilterInput()
    {
        BloomFilterStateFactory f = new BloomFilterStateFactory();
        BloomFilterState state = f.createSingleState();
        BloomFilterAggregation.input(state, Slices.utf8Slice("robin"));
        BloomFilterAggregation.input(state, Slices.utf8Slice("verlangen"));
    }

    @Test
    public void testBloomFilterCombine()
    {
        BloomFilterStateFactory f = new BloomFilterStateFactory();
        BloomFilterState state = f.createSingleState();
        BloomFilterAggregation.input(state, Slices.utf8Slice("robin"));

        BloomFilterState stateOther = f.createSingleState();
        BloomFilterAggregation.input(state, Slices.utf8Slice("verlangen"));

        // Merge
        BloomFilterAggregation.combine(state, stateOther);
        assertTrue(state.getBloomFilter().mightContain(Slices.utf8Slice("verlangen")));
    }

    @Test
    public void testBloomFilterCombineEmpty()
    {
        BloomFilterStateFactory f = new BloomFilterStateFactory();
        BloomFilterState state = f.createSingleState();
        BloomFilterAggregation.input(state, Slices.utf8Slice("robin"));

        BloomFilterState stateEmpty = f.createSingleState();

        // Merge
        BloomFilterAggregation.combine(state, stateEmpty);
        assertTrue(state.getBloomFilter().mightContain(Slices.utf8Slice("robin")));
        assertTrue(!state.getBloomFilter().mightContain(Slices.utf8Slice("verlangen")));
    }

    @Test
    public void testBloomFilterCombineCustom()
    {
        BloomFilterStateFactory f = new BloomFilterStateFactory();
        BloomFilterState state = f.createSingleState();
        BloomFilterAggregation.input(state, Slices.utf8Slice("robin"), 100);

        BloomFilterState stateOther = f.createSingleState();
        BloomFilterAggregation.input(stateOther, Slices.utf8Slice("verlangen"), 100);

        BloomFilterState stateEmpty = f.createSingleState();

        // Merge
        BloomFilterAggregation.combine(state, stateOther);
        assertTrue(state.getBloomFilter().mightContain(Slices.utf8Slice("verlangen")));

        // Merge
        BloomFilterAggregation.combine(stateOther, stateEmpty);
        assertTrue(state.getBloomFilter().mightContain(Slices.utf8Slice("verlangen")));

        // Merge
        BloomFilterAggregation.combine(stateEmpty, stateOther);
        assertTrue(state.getBloomFilter().mightContain(Slices.utf8Slice("verlangen")));
    }

    @Test
    public void testBloomFilterOutput()
    {
        BloomFilterStateFactory f = new BloomFilterStateFactory();
        BloomFilterState state = f.createSingleState();
        BloomFilterAggregation.input(state, Slices.utf8Slice("robin"));
        BlockBuilderStatus bbs = new BlockBuilderStatusExtended(new PageBuilderStatus());
        BlockBuilder bb = new VariableWidthBlockBuilder(bbs, 1, 5); // @todo check 1 and 5 params
        BloomFilterAggregation.output(state, bb);

        assertTrue(bb.build().getSizeInBytes() > 1);
    }
}
