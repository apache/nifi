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
package org.apache.nifi.controller.repository;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

public class RingBufferEventRepository implements FlowFileEventRepository {

    private final int numMinutes;
    private final ConcurrentMap<String, EventContainer> componentEventMap = new ConcurrentHashMap<>();

    public RingBufferEventRepository(final int numMinutes) {
        this.numMinutes = numMinutes;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void updateRepository(final FlowFileEvent event) {
        final String componentId = event.getComponentIdentifier();
        EventContainer eventContainer = componentEventMap.get(componentId);
        if (eventContainer == null) {
            eventContainer = new SecondPrecisionEventContainer(numMinutes);
            final EventContainer oldEventContainer = componentEventMap.putIfAbsent(componentId, eventContainer);
            if (oldEventContainer != null) {
                eventContainer = oldEventContainer;
            }
        }

        eventContainer.addEvent(event);
    }

    @Override
    public StandardRepositoryStatusReport reportTransferEvents(final long sinceEpochMillis) {
        final StandardRepositoryStatusReport report = new StandardRepositoryStatusReport();

        for (final Map.Entry<String, EventContainer> entry : componentEventMap.entrySet()) {
            final String consumerId = entry.getKey();
            final EventContainer container = entry.getValue();

            final FlowFileEvent reportEntry = container.generateReport(consumerId, sinceEpochMillis);
            report.addReportEntry(reportEntry);
        }

        return report;
    }

    @Override
    public void purgeTransferEvents(final long cutoffEpochMilliseconds) {
        // This is done so that if a processor is removed from the graph, its events 
        // will be removed rather than being kept in memory
        for (final EventContainer container : componentEventMap.values()) {
            container.purgeEvents(cutoffEpochMilliseconds);
        }
    }

    private static interface EventContainer {

        public void addEvent(FlowFileEvent event);

        public void purgeEvents(long cutoffEpochMillis);

        public FlowFileEvent generateReport(String consumerId, long sinceEpochMillis);
    }

    private class EventSum {

        private final AtomicReference<EventSumValue> ref = new AtomicReference<>(new EventSumValue());

        private void add(final FlowFileEvent event) {
            EventSumValue newValue;
            EventSumValue value;
            do {
                value = ref.get();
                newValue = new EventSumValue(value,
                        event.getFlowFilesIn(), event.getFlowFilesOut(), event.getFlowFilesRemoved(),
                        event.getContentSizeIn(), event.getContentSizeOut(), event.getContentSizeRemoved(),
                        event.getBytesRead(), event.getBytesWritten(),
                        event.getFlowFilesReceived(), event.getBytesReceived(),
                        event.getFlowFilesSent(), event.getBytesSent(),
                        event.getProcessingNanoseconds(), event.getInvocations(), event.getAggregateLineageMillis());
            } while (!ref.compareAndSet(value, newValue));
        }

        public EventSumValue getValue() {
            return ref.get();
        }

        public void addOrReset(final FlowFileEvent event) {
            final long expectedMinute = System.currentTimeMillis() / 60000;

            final EventSumValue curValue = ref.get();
            if (curValue.getMinuteTimestamp() != expectedMinute) {
                ref.compareAndSet(curValue, new EventSumValue());
            }
            add(event);
        }
    }

    private static class EventSumValue {

        private final int flowFilesIn, flowFilesOut, flowFilesRemoved;
        private final long contentSizeIn, contentSizeOut, contentSizeRemoved;
        private final long bytesRead, bytesWritten;
        private final int flowFilesReceived, flowFilesSent;
        private final long bytesReceived, bytesSent;
        private final long processingNanos;
        private final long aggregateLineageMillis;
        private final int invocations;

        private final long minuteTimestamp;
        private final long millisecondTimestamp;

        public EventSumValue() {
            flowFilesIn = flowFilesOut = flowFilesRemoved = 0;
            contentSizeIn = contentSizeOut = contentSizeRemoved = 0;
            bytesRead = bytesWritten = 0;
            flowFilesReceived = flowFilesSent = 0;
            bytesReceived = bytesSent = 0L;
            processingNanos = invocations = 0;
            aggregateLineageMillis = 0L;
            this.millisecondTimestamp = System.currentTimeMillis();
            this.minuteTimestamp = millisecondTimestamp / 60000;
        }

        public EventSumValue(final EventSumValue base, final int flowFilesIn, final int flowFilesOut, final int flowFilesRemoved,
                final long contentSizeIn, final long contentSizeOut, final long contentSizeRemoved,
                final long bytesRead, final long bytesWritten,
                final int flowFilesReceived, final long bytesReceived,
                final int flowFilesSent, final long bytesSent,
                final long processingNanos, final int invocations, final long aggregateLineageMillis) {
            this.flowFilesIn = base.flowFilesIn + flowFilesIn;
            this.flowFilesOut = base.flowFilesOut + flowFilesOut;
            this.flowFilesRemoved = base.flowFilesRemoved + flowFilesRemoved;
            this.contentSizeIn = base.contentSizeIn + contentSizeIn;
            this.contentSizeOut = base.contentSizeOut + contentSizeOut;
            this.contentSizeRemoved = base.contentSizeRemoved + contentSizeRemoved;
            this.bytesRead = base.bytesRead + bytesRead;
            this.bytesWritten = base.bytesWritten + bytesWritten;
            this.flowFilesReceived = base.flowFilesReceived + flowFilesReceived;
            this.bytesReceived = base.bytesReceived + bytesReceived;
            this.flowFilesSent = base.flowFilesSent + flowFilesSent;
            this.bytesSent = base.bytesSent + bytesSent;
            this.processingNanos = base.processingNanos + processingNanos;
            this.invocations = base.invocations + invocations;
            this.aggregateLineageMillis = base.aggregateLineageMillis + aggregateLineageMillis;
            this.millisecondTimestamp = System.currentTimeMillis();
            this.minuteTimestamp = millisecondTimestamp / 60000;
        }

        public long getTimestamp() {
            return millisecondTimestamp;
        }

        public long getMinuteTimestamp() {
            return minuteTimestamp;
        }

        public long getBytesRead() {
            return bytesRead;
        }

        public long getBytesWritten() {
            return bytesWritten;
        }

        public int getFlowFilesIn() {
            return flowFilesIn;
        }

        public int getFlowFilesOut() {
            return flowFilesOut;
        }

        public long getContentSizeIn() {
            return contentSizeIn;
        }

        public long getContentSizeOut() {
            return contentSizeOut;
        }

        public int getFlowFilesRemoved() {
            return flowFilesRemoved;
        }

        public long getContentSizeRemoved() {
            return contentSizeRemoved;
        }

        public long getProcessingNanoseconds() {
            return processingNanos;
        }

        public int getInvocations() {
            return invocations;
        }

        public long getAggregateLineageMillis() {
            return aggregateLineageMillis;
        }

        public int getFlowFilesReceived() {
            return flowFilesReceived;
        }

        public int getFlowFilesSent() {
            return flowFilesSent;
        }

        public long getBytesReceived() {
            return bytesReceived;
        }

        public long getBytesSent() {
            return bytesSent;
        }
    }

    private class SecondPrecisionEventContainer implements EventContainer {

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
            final int binIdx = (int) (second % numBins);
            final EventSum sum = sums[binIdx];

            sum.addOrReset(event);
        }

        @Override
        public void purgeEvents(final long cutoffEpochMilliseconds) {
            // no need to do anything
        }

        @Override
        public FlowFileEvent generateReport(final String consumerId, final long sinceEpochMillis) {
            int flowFilesIn = 0, flowFilesOut = 0, flowFilesRemoved = 0;
            long contentSizeIn = 0L, contentSizeOut = 0L, contentSizeRemoved = 0L;
            long bytesRead = 0L, bytesWritten = 0L;
            int invocations = 0;
            long processingNanos = 0L;
            long aggregateLineageMillis = 0L;
            int flowFilesReceived = 0, flowFilesSent = 0;
            long bytesReceived = 0L, bytesSent = 0L;

            final long second = sinceEpochMillis / 1000;
            final int startBinIdx = (int) (second % numBins);

            for (int i = 0; i < numBins; i++) {
                int binIdx = (startBinIdx + i) % numBins;
                final EventSum sum = sums[binIdx];

                final EventSumValue sumValue = sum.getValue();
                if (sumValue.getTimestamp() >= sinceEpochMillis) {
                    flowFilesIn += sumValue.getFlowFilesIn();
                    flowFilesOut += sumValue.getFlowFilesOut();
                    flowFilesRemoved += sumValue.getFlowFilesRemoved();
                    contentSizeIn += sumValue.getContentSizeIn();
                    contentSizeOut += sumValue.getContentSizeOut();
                    contentSizeRemoved += sumValue.getContentSizeRemoved();
                    bytesRead += sumValue.getBytesRead();
                    bytesWritten += sumValue.getBytesWritten();
                    flowFilesReceived += sumValue.getFlowFilesReceived();
                    bytesReceived += sumValue.getBytesReceived();
                    flowFilesSent += sumValue.getFlowFilesSent();
                    bytesSent += sumValue.getBytesSent();
                    invocations += sumValue.getInvocations();
                    processingNanos += sumValue.getProcessingNanoseconds();
                    aggregateLineageMillis += sumValue.getAggregateLineageMillis();
                }
            }

            return new StandardFlowFileEvent(consumerId, flowFilesIn, contentSizeIn,
                    flowFilesOut, contentSizeOut, flowFilesRemoved, contentSizeRemoved,
                    bytesRead, bytesWritten, flowFilesReceived, bytesReceived, flowFilesSent, bytesSent,
                    invocations, aggregateLineageMillis, processingNanos);
        }
    }
}
