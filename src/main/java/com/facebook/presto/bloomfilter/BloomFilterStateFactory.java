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

import com.facebook.presto.array.ObjectBigArray;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.GroupedAccumulatorState;

public class BloomFilterStateFactory
        implements AccumulatorStateFactory<BloomFilterState>
{
    @Override
    public BloomFilterState createSingleState()
    {
        return new SingleBloomFilterState();
    }

    @Override
    public Class<? extends BloomFilterState> getSingleStateClass()
    {
        return SingleBloomFilterState.class;
    }

    @Override
    public BloomFilterState createGroupedState()
    {
        return new GroupedBloomFilterState();
    }

    @Override
    public Class<? extends BloomFilterState> getGroupedStateClass()
    {
        return GroupedBloomFilterState.class;
    }

    public static class GroupedBloomFilterState
            implements GroupedAccumulatorState, BloomFilterState
    {
        private final ObjectBigArray<BloomFilter> bfs = new ObjectBigArray<>();
        private long groupId;
        private long size;

        @Override
        public void ensureCapacity(long size)
        {
            bfs.ensureCapacity(size);
        }

        @Override
        public void setGroupId(long groupId)
        {
            this.groupId = groupId;
        }

        @Override
        public BloomFilter getBloomFilter()
        {
            return bfs.get(groupId);
        }

        @Override
        public void setBloomFilter(BloomFilter value)
        {
            if (value == null) {
                throw new NullPointerException("value is null");
            }
            bfs.set(groupId, value);
        }

        @Override
        public void addMemoryUsage(int value)
        {
            size += value;
        }

        @Override
        public long getEstimatedSize()
        {
            return size + bfs.sizeOf();
        }
    }

    public static class SingleBloomFilterState
            implements BloomFilterState
    {
        private BloomFilter bf;

        @Override
        public BloomFilter getBloomFilter()
        {
            return bf;
        }

        @Override
        public void setBloomFilter(BloomFilter value)
        {
            bf = value;
        }

        @Override
        public void addMemoryUsage(int value)
        {
            // noop
        }

        @Override
        public long getEstimatedSize()
        {
            if (bf == null) {
                return 0;
            }
            return bf.estimatedInMemorySize();
        }
    }
}
