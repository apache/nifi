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

public class TimestampedLongAggregation {
    private final Long value;
    private final long timestamp;
    private final TimestampedAggregation cumulation;

    public TimestampedLongAggregation(final long value) {
        this.value = value;
        this.timestamp = System.currentTimeMillis();
        this.cumulation = null;
    }

    public TimestampedLongAggregation(final TimestampedAggregation cumulation) {
        this.value = null;
        this.timestamp = System.currentTimeMillis();
        this.cumulation = cumulation;
    }

    public TimestampedAggregation getAggregation() {
        if (cumulation != null) {
            return cumulation;
        }

        return new TimestampedAggregation(value, value, value, 1L);
    }

    public Long getValue() {
        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public static TimestampedLongAggregation newValue(final long value) {
        return new TimestampedLongAggregation(value);
    }

    public static TimestampedLongAggregation newAggregation(final TimestampedAggregation cumulation) {
        return new TimestampedLongAggregation(cumulation);
    }

    public static class TimestampedAggregation {
        private final long min;
        private final long max;
        private final long sum;
        private final long count;

        public TimestampedAggregation(final long min, final long max, final long sum, final long count) {
            this.min = min;
            this.max = max;
            this.sum = sum;
            this.count = count;
        }

        public long getMin() {
            return min;
        }

        public long getMax() {
            return max;
        }

        public long getSum() {
            return sum;
        }

        public long getCount() {
            return count;
        }

        public TimestampedAggregation add(final TimestampedAggregation aggregation) {
            return new TimestampedAggregation(Math.min(min, aggregation.min), Math.max(max, aggregation.max), sum + aggregation.sum, count + aggregation.count);
        }
    }
}
