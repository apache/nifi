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

public class SecondPrecisionEventContainer implements EventContainer {
    private final int numBins;
    private final EventSum[] sums;

    public SecondPrecisionEventContainer(final int numMinutes) {
        numBins = 1 + numMinutes * 60;
        sums = new EventSum[numBins];

        for (int i = 0; i < numBins; i++) {
            sums[i] = new EventSum();
        }
    }

    @Override
    public void addEvent(final FlowFileEvent event) {
        final int second = (int) (System.currentTimeMillis() / 1000);
        final int binIdx = second % numBins;
        final EventSum sum = sums[binIdx];

        sum.addOrReset(event);
    }

    @Override
    public void purgeEvents(final long cutoffEpochMilliseconds) {
        // no need to do anything
    }

    @Override
    public FlowFileEvent generateReport(final String componentId, final long sinceEpochMillis) {
        final EventSumValue eventSumValue = new EventSumValue();
        final long second = sinceEpochMillis / 1000;
        final int startBinIdx = (int) (second % numBins);

        for (int i = 0; i < numBins; i++) {
            int binIdx = (startBinIdx + i) % numBins;
            final EventSum sum = sums[binIdx];

            final EventSumValue sumValue = sum.getValue();
            if (sumValue.getTimestamp() >= sinceEpochMillis) {
                eventSumValue.add(sumValue);
            }
        }

        final FlowFileEvent flowFileEvent = eventSumValue.toFlowFileEvent(componentId);
        return flowFileEvent;
    }

}
