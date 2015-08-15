/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.util.timebuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class TestTimedBuffer {

    @Test
    public void testAgesOff() throws InterruptedException {
        final LongEntityAccess access = new LongEntityAccess();
        final TimedBuffer<TimestampedLong> buffer = new TimedBuffer<>(TimeUnit.SECONDS, 2, access);

        buffer.add(new TimestampedLong(1000000L));
        TimestampedLong aggregate = buffer.getAggregateValue(System.currentTimeMillis() - 30000L);
        assertEquals(1000000L, aggregate.getValue().longValue());
        Thread.sleep(1000L);
        aggregate = buffer.getAggregateValue(System.currentTimeMillis() - 30000L);
        assertEquals(1000000L, aggregate.getValue().longValue());
        Thread.sleep(1500L);
        aggregate = buffer.getAggregateValue(System.currentTimeMillis() - 30000L);
        assertNull(aggregate);
    }

    @Test
    public void testAggregation() throws InterruptedException {
        final LongEntityAccess access = new LongEntityAccess();
        final TimedBuffer<TimestampedLong> buffer = new TimedBuffer<>(TimeUnit.SECONDS, 2, access);

        buffer.add(new TimestampedLong(1000000L));
        buffer.add(new TimestampedLong(1000000L));
        buffer.add(new TimestampedLong(25000L));

        TimestampedLong aggregate = buffer.getAggregateValue(System.currentTimeMillis() - 30000L);
        assertEquals(2025000L, aggregate.getValue().longValue());
        Thread.sleep(1000L);
        aggregate = buffer.getAggregateValue(System.currentTimeMillis() - 30000L);
        assertEquals(2025000L, aggregate.getValue().longValue());
        Thread.sleep(1500L);
        aggregate = buffer.getAggregateValue(System.currentTimeMillis() - 30000L);
        assertNull(aggregate);
    }

    private static class TimestampedLong {

        private final Long value;
        private final long timestamp = System.currentTimeMillis();

        public TimestampedLong(final Long value) {
            this.value = value;
        }

        public Long getValue() {
            return value;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }

    private static class LongEntityAccess implements EntityAccess<TimestampedLong> {

        @Override
        public TimestampedLong aggregate(TimestampedLong oldValue, TimestampedLong toAdd) {
            if (oldValue == null && toAdd == null) {
                return new TimestampedLong(0L);
            } else if (oldValue == null) {
                return toAdd;
            } else if (toAdd == null) {
                return oldValue;
            }

            return new TimestampedLong(oldValue.getValue().longValue() + toAdd.getValue().longValue());
        }

        @Override
        public TimestampedLong createNew() {
            return new TimestampedLong(0L);
        }

        @Override
        public long getTimestamp(TimestampedLong entity) {
            return entity == null ? 0L : entity.getTimestamp();
        }
    }
}
