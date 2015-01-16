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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class TimedBuffer<T> {

    private final int numBins;
    private final EntitySum<T>[] bins;
    private final EntityAccess<T> entityAccess;
    private final TimeUnit binPrecision;

    @SuppressWarnings("unchecked")
    public TimedBuffer(final TimeUnit binPrecision, final int numBins, final EntityAccess<T> accessor) {
        this.binPrecision = binPrecision;
        this.numBins = numBins + 1;
        this.bins = new EntitySum[this.numBins];
        for (int i = 0; i < this.numBins; i++) {
            this.bins[i] = new EntitySum<>(binPrecision, numBins, accessor);
        }
        this.entityAccess = accessor;
    }

    public T add(final T entity) {
        final int binIdx = (int) (binPrecision.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS) % numBins);
        final EntitySum<T> sum = bins[binIdx];

        return sum.addOrReset(entity);
    }

    public T getAggregateValue(final long sinceEpochMillis) {
        final int startBinIdx = (int) (binPrecision.convert(sinceEpochMillis, TimeUnit.MILLISECONDS) % numBins);

        T total = null;
        for (int i = 0; i < numBins; i++) {
            int binIdx = (startBinIdx + i) % numBins;
            final EntitySum<T> bin = bins[binIdx];

            if (!bin.isExpired()) {
                total = entityAccess.aggregate(total, bin.getValue());
            }
        }

        return total;
    }

    private static class EntitySum<S> {

        private final EntityAccess<S> entityAccess;
        private final AtomicReference<S> ref = new AtomicReference<>();
        private final TimeUnit binPrecision;
        private final int numConfiguredBins;

        public EntitySum(final TimeUnit binPrecision, final int numConfiguredBins, final EntityAccess<S> aggregator) {
            this.binPrecision = binPrecision;
            this.entityAccess = aggregator;
            this.numConfiguredBins = numConfiguredBins;
        }

        private S add(final S event) {
            S newValue;
            S value;
            do {
                value = ref.get();
                newValue = entityAccess.aggregate(value, event);
            } while (!ref.compareAndSet(value, newValue));

            return newValue;
        }

        public S getValue() {
            return ref.get();
        }

        public boolean isExpired() {
            // entityAccess.getTimestamp(curValue) represents the time at which the current value
            // was last updated. If the last value is less than current time - 1 binPrecision, then it
            // means that we've rolled over and need to reset the value.
            final long maxExpectedTimePeriod = System.currentTimeMillis() - TimeUnit.MILLISECONDS.convert(numConfiguredBins, binPrecision);

            final S curValue = ref.get();
            return (entityAccess.getTimestamp(curValue) < maxExpectedTimePeriod);
        }

        public S addOrReset(final S event) {
            // entityAccess.getTimestamp(curValue) represents the time at which the current value
            // was last updated. If the last value is less than current time - 1 binPrecision, then it
            // means that we've rolled over and need to reset the value.
            final long maxExpectedTimePeriod = System.currentTimeMillis() - TimeUnit.MILLISECONDS.convert(1, binPrecision);

            final S curValue = ref.get();
            if (entityAccess.getTimestamp(curValue) < maxExpectedTimePeriod) {
                ref.compareAndSet(curValue, entityAccess.createNew());
            }
            return add(event);
        }
    }
}
