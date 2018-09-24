/*
e * Licensed to the Apache Software Foundation (ASF) under one or more
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

package org.apache.nifi.controller.repository.metrics;

import org.apache.nifi.controller.repository.FlowFileEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class SecondPrecisionEventContainer implements EventContainer {
    private static final Logger logger = LoggerFactory.getLogger(SecondPrecisionEventContainer.class);

    private final int numBins;
    private final EventSum[] sums;
    private final EventSumValue aggregateValue = new EventSumValue(0);
    private final AtomicLong lastUpdateSecond = new AtomicLong(0);

    public SecondPrecisionEventContainer(final int numMinutes) {
        // number of bins is number of seconds in 'numMinutes' plus 1. We add one because
        // we want to have the 'current bin' that we are adding values to, in addition to the
        // previous (X = numMinutes * 60) bins of values that have completed
        numBins = numMinutes * 60 + 1;
        sums = new EventSum[numBins];

        for (int i = 0; i < numBins; i++) {
            sums[i] = new EventSum();
        }
    }

    @Override
    public void addEvent(final FlowFileEvent event) {
        addEvent(event, System.currentTimeMillis());
    }

    protected void addEvent(final FlowFileEvent event, final long timestamp) {
        final long second = timestamp / 1000;
        final int binIdx = (int) (second % numBins);
        final EventSum sum = sums[binIdx];

        final EventSumValue replaced = sum.addOrReset(event, timestamp);

        aggregateValue.add(event);

        if (replaced == null) {
            logger.debug("Updated bin {}. Did NOT replace.", binIdx);
        } else {
            logger.debug("Replaced bin {}", binIdx);
            aggregateValue.subtract(replaced);
        }

        // If there are any buckets that have expired, we need to update our aggregate value to reflect that.
        processExpiredBuckets(second);
    }

    private void processExpiredBuckets(final long currentSecond) {
        final long lastUpdate = lastUpdateSecond.get();
        if (currentSecond > lastUpdate) {
            final boolean updated = lastUpdateSecond.compareAndSet(lastUpdate, currentSecond);
            if (updated) {
                if (lastUpdate == 0L) {
                    // First update, so nothing to expire
                    return;
                }

                final int secondsElapsed = (int) (currentSecond - lastUpdate);

                int index = (int) (currentSecond % numBins);
                final long expirationTimestamp = 1000 * (currentSecond - numBins);

                int expired = 0;
                for (int i=0; i < secondsElapsed; i++) {
                    index--;
                    if (index < 0) {
                        index = sums.length - 1;
                    }

                    final EventSum expiredSum = sums[index];
                    final EventSumValue expiredValue = expiredSum.reset(expirationTimestamp);
                    if (expiredValue != null) {
                        aggregateValue.subtract(expiredValue);
                        expired++;
                    }
                }

                logger.debug("Expired {} bins", expired);
            }
        }
    }

    @Override
    public void purgeEvents(final long cutoffEpochMilliseconds) {
        // no need to do anything
    }

    @Override
    public FlowFileEvent generateReport(final long now) {
        final long second = now / 1000 + 1;
        final long lastUpdate = lastUpdateSecond.get();
        final long secondsSinceUpdate = second - lastUpdate;
        if (secondsSinceUpdate > numBins) {
            logger.debug("EventContainer hasn't been updated in {} seconds so will generate report as Empty FlowFile Event", secondsSinceUpdate);
            return EmptyFlowFileEvent.INSTANCE;
        }

        logger.debug("Will expire up to {} bins", secondsSinceUpdate);
        processExpiredBuckets(second);
        return aggregateValue.toFlowFileEvent();
    }
}
