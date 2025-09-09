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
package io.trino.plugin.uint256.aggregation.state;

import io.airlift.slice.Slice;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.GroupedAccumulatorState;

import java.util.HashMap;
import java.util.Map;

import static io.airlift.slice.SizeOf.instanceSize;

public class UInt256CountAndSumStateFactory
        implements AccumulatorStateFactory<UInt256CountAndSumState>
{
    @Override
    public UInt256CountAndSumState createSingleState()
    {
        return new SingleUInt256CountAndSumState();
    }

    @Override
    public UInt256CountAndSumState createGroupedState()
    {
        return new GroupedUInt256CountAndSumState();
    }

    public static class SingleUInt256CountAndSumState
            implements UInt256CountAndSumState
    {
        private static final int INSTANCE_SIZE = instanceSize(SingleUInt256CountAndSumState.class);

        private long count;
        private Slice sum;

        @Override
        public long getEstimatedSize()
        {
            long size = INSTANCE_SIZE;
            if (sum != null) {
                size += sum.getRetainedSize();
            }
            return size;
        }

        @Override
        public long getCount()
        {
            return count;
        }

        @Override
        public void setCount(long count)
        {
            this.count = count;
        }

        @Override
        public Slice getSum()
        {
            return sum;
        }

        @Override
        public void setSum(Slice sum)
        {
            this.sum = sum;
        }
    }

    public static class GroupedUInt256CountAndSumState
            implements GroupedAccumulatorState, UInt256CountAndSumState
    {
        private static final int INSTANCE_SIZE = instanceSize(GroupedUInt256CountAndSumState.class);

        private final Map<Long, Slice> sums = new HashMap<>();
        private final Map<Long, Long> counts = new HashMap<>();
        private long groupId;

        @Override
        public void setGroupId(int groupId)
        {
            this.groupId = groupId;
        }

        @Override
        public void ensureCapacity(int size)
        {
            // HashMap grows automatically; nothing to pre-size explicitly here
        }

        // Older SPI used ensureCapacity(long). Current interface requires only ensureCapacity(int).

        @Override
        public long getEstimatedSize()
        {
            long size = INSTANCE_SIZE;
            for (Slice sum : sums.values()) {
                if (sum != null) {
                    size += sum.getRetainedSize();
                }
            }
            return size + (long) (sums.size() + counts.size()) * Long.BYTES;
        }

        @Override
        public long getCount()
        {
            Long count = counts.get(groupId);
            return count == null ? 0L : count;
        }

        @Override
        public void setCount(long count)
        {
            counts.put(groupId, count);
        }

        @Override
        public Slice getSum()
        {
            return sums.get(groupId);
        }

        @Override
        public void setSum(Slice sum)
        {
            sums.put(groupId, sum);
        }
    }
}
