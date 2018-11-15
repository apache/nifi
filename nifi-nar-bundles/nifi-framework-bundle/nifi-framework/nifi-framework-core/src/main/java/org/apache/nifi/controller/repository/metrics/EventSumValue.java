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

package org.apache.nifi.controller.repository.metrics;

import org.apache.nifi.controller.repository.FlowFileEvent;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class EventSumValue {
    private volatile boolean empty = true;

    private int flowFilesIn = 0;
    private int flowFilesOut = 0;
    private int flowFilesRemoved = 0;
    private int flowFilesReceived = 0;
    private int flowFilesSent = 0;

    private long contentSizeIn = 0;
    private long contentSizeOut = 0;
    private long contentSizeRemoved = 0;
    private long bytesRead = 0;
    private long bytesWritten = 0;

    private long bytesReceived = 0;
    private long bytesSent = 0;
    private long processingNanos = 0;
    private long aggregateLineageMillis = 0;
    private int invocations = 0;
    private Map<String, Long> counters;

    private final long millisecondTimestamp;


    public EventSumValue(final long timestamp) {
        this.millisecondTimestamp = timestamp;
    }

    public synchronized void add(final FlowFileEvent flowFileEvent) {
        empty = false;

        this.aggregateLineageMillis += flowFileEvent.getAggregateLineageMillis();
        this.bytesRead += flowFileEvent.getBytesRead();
        this.bytesReceived += flowFileEvent.getBytesReceived();
        this.bytesSent += flowFileEvent.getBytesSent();
        this.bytesWritten += flowFileEvent.getBytesWritten();
        this.contentSizeIn += flowFileEvent.getContentSizeIn();
        this.contentSizeOut += flowFileEvent.getContentSizeOut();
        this.contentSizeRemoved += flowFileEvent.getContentSizeRemoved();
        this.flowFilesIn += flowFileEvent.getFlowFilesIn();
        this.flowFilesOut += flowFileEvent.getFlowFilesOut();
        this.flowFilesReceived += flowFileEvent.getFlowFilesReceived();
        this.flowFilesRemoved += flowFileEvent.getFlowFilesRemoved();
        this.flowFilesSent += flowFileEvent.getFlowFilesSent();
        this.invocations += flowFileEvent.getInvocations();
        this.processingNanos += flowFileEvent.getProcessingNanoseconds();

        final Map<String, Long> eventCounters = flowFileEvent.getCounters();
        if (eventCounters != null) {
            for (final Map.Entry<String, Long> entry : eventCounters.entrySet()) {
                final String counterName = entry.getKey();
                final Long counterValue = entry.getValue();

                if (counters == null) {
                    counters = new HashMap<>();
                }
                counters.compute(counterName, (key, value) -> value == null ? counterValue : value + counterValue);
            }
        }
    }

    public synchronized FlowFileEvent toFlowFileEvent() {
        if (empty) {
            return EmptyFlowFileEvent.INSTANCE;
        }

        final StandardFlowFileEvent event = new StandardFlowFileEvent();
        event.setAggregateLineageMillis(aggregateLineageMillis);
        event.setBytesRead(bytesRead);
        event.setBytesReceived(bytesReceived);
        event.setBytesSent(bytesSent);
        event.setBytesWritten(bytesWritten);
        event.setContentSizeIn(contentSizeIn);
        event.setContentSizeOut(contentSizeOut);
        event.setContentSizeRemoved(contentSizeRemoved);
        event.setFlowFilesIn(flowFilesIn);
        event.setFlowFilesOut(flowFilesOut);
        event.setFlowFilesReceived(flowFilesReceived);
        event.setFlowFilesRemoved(flowFilesRemoved);
        event.setFlowFilesSent(flowFilesSent);
        event.setInvocations(invocations);
        event.setProcessingNanos(processingNanos);
        event.setCounters(this.counters == null ? Collections.emptyMap() : Collections.unmodifiableMap(this.counters));
        return event;
    }

    public synchronized void add(final EventSumValue other) {
        if (other.empty) {
            return;
        }

        synchronized (other) {
            this.aggregateLineageMillis += other.aggregateLineageMillis;
            this.bytesRead += other.bytesRead;
            this.bytesReceived += other.bytesReceived;
            this.bytesSent += other.bytesSent;
            this.bytesWritten += other.bytesWritten;
            this.contentSizeIn += other.contentSizeIn;
            this.contentSizeOut += other.contentSizeOut;
            this.contentSizeRemoved += other.contentSizeRemoved;
            this.flowFilesIn += other.flowFilesIn;
            this.flowFilesOut += other.flowFilesOut;
            this.flowFilesReceived += other.flowFilesReceived;
            this.flowFilesRemoved += other.flowFilesRemoved;
            this.flowFilesSent += other.flowFilesSent;
            this.invocations += other.invocations;
            this.processingNanos += other.processingNanos;

            final Map<String, Long> eventCounters = other.counters;
            if (eventCounters != null) {
                if (counters == null) {
                    counters = new HashMap<>();
                }

                for (final Map.Entry<String, Long> entry : eventCounters.entrySet()) {
                    final String counterName = entry.getKey();
                    final Long counterValue = entry.getValue();

                    counters.compute(counterName, (key, value) -> value == null ? counterValue : value + counterValue);
                }
            }
        }
    }

    public synchronized void subtract(final EventSumValue other) {
        if (other.empty) {
            return;
        }

        synchronized (other) {
            this.aggregateLineageMillis -= other.aggregateLineageMillis;
            this.bytesRead -= other.bytesRead;
            this.bytesReceived -= other.bytesReceived;
            this.bytesSent -= other.bytesSent;
            this.bytesWritten -= other.bytesWritten;
            this.contentSizeIn -= other.contentSizeIn;
            this.contentSizeOut -= other.contentSizeOut;
            this.contentSizeRemoved -= other.contentSizeRemoved;
            this.flowFilesIn -= other.flowFilesIn;
            this.flowFilesOut -= other.flowFilesOut;
            this.flowFilesReceived -= other.flowFilesReceived;
            this.flowFilesRemoved -= other.flowFilesRemoved;
            this.flowFilesSent -= other.flowFilesSent;
            this.invocations -= other.invocations;
            this.processingNanos -= other.processingNanos;

            final Map<String, Long> eventCounters = other.counters;
            if (eventCounters != null) {
                if (counters == null) {
                    counters = new HashMap<>();
                }

                for (final Map.Entry<String, Long> entry : eventCounters.entrySet()) {
                    final String counterName = entry.getKey();
                    final Long counterValue = entry.getValue();

                    counters.compute(counterName, (key, value) -> value == null ? -counterValue : value - counterValue);
                }
            }
        }
    }

    public long getTimestamp() {
        return millisecondTimestamp;
    }
}
