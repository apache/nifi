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

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.controller.repository.FlowFileEvent;

public class EventSumValue {

    private final AtomicInteger flowFilesIn = new AtomicInteger(0);
    private final AtomicInteger flowFilesOut = new AtomicInteger(0);
    private final AtomicInteger flowFilesRemoved = new AtomicInteger(0);
    private final AtomicInteger flowFilesReceived = new AtomicInteger(0);
    private final AtomicInteger flowFilesSent = new AtomicInteger(0);

    private final AtomicLong contentSizeIn = new AtomicLong(0L);
    private final AtomicLong contentSizeOut = new AtomicLong(0L);
    private final AtomicLong contentSizeRemoved = new AtomicLong(0L);
    private final AtomicLong bytesRead = new AtomicLong(0L);
    private final AtomicLong bytesWritten = new AtomicLong(0L);

    private final AtomicLong bytesReceived = new AtomicLong(0L);
    private final AtomicLong bytesSent = new AtomicLong(0L);
    private final AtomicLong processingNanos = new AtomicLong(0L);
    private final AtomicLong aggregateLineageMillis = new AtomicLong(0L);
    private final AtomicInteger invocations = new AtomicInteger(0);
    private final ConcurrentMap<String, Long> counters = new ConcurrentHashMap<>();

    private final long minuteTimestamp;
    private final long millisecondTimestamp;


    public EventSumValue() {
        this.millisecondTimestamp = System.currentTimeMillis();
        this.minuteTimestamp = millisecondTimestamp / 60000;
    }

    public void add(final FlowFileEvent flowFileEvent) {
        this.aggregateLineageMillis.addAndGet(flowFileEvent.getAggregateLineageMillis());
        this.bytesRead.addAndGet(flowFileEvent.getBytesRead());
        this.bytesReceived.addAndGet(flowFileEvent.getBytesReceived());
        this.bytesSent.addAndGet(flowFileEvent.getBytesSent());
        this.bytesWritten.addAndGet(flowFileEvent.getBytesWritten());
        this.contentSizeIn.addAndGet(flowFileEvent.getContentSizeIn());
        this.contentSizeOut.addAndGet(flowFileEvent.getContentSizeOut());
        this.contentSizeRemoved.addAndGet(flowFileEvent.getContentSizeRemoved());
        this.flowFilesIn.addAndGet(flowFileEvent.getFlowFilesIn());
        this.flowFilesOut.addAndGet(flowFileEvent.getFlowFilesOut());
        this.flowFilesReceived.addAndGet(flowFileEvent.getFlowFilesReceived());
        this.flowFilesRemoved.addAndGet(flowFileEvent.getFlowFilesRemoved());
        this.flowFilesSent.addAndGet(flowFileEvent.getFlowFilesSent());
        this.invocations.addAndGet(flowFileEvent.getInvocations());
        this.processingNanos.addAndGet(flowFileEvent.getProcessingNanoseconds());

        final Map<String, Long> eventCounters = flowFileEvent.getCounters();
        if (eventCounters != null) {
            for (final Map.Entry<String, Long> entry : eventCounters.entrySet()) {
                final String counterName = entry.getKey();
                final Long counterValue = entry.getValue();

                counters.compute(counterName, (key, value) -> value == null ? counterValue : value + counterValue);
            }
        }
    }

    public FlowFileEvent toFlowFileEvent(final String componentId) {
        final StandardFlowFileEvent event = new StandardFlowFileEvent(componentId);
        event.setAggregateLineageMillis(getAggregateLineageMillis());
        event.setBytesRead(getBytesRead());
        event.setBytesReceived(getBytesReceived());
        event.setBytesSent(getBytesSent());
        event.setBytesWritten(getBytesWritten());
        event.setContentSizeIn(getContentSizeIn());
        event.setContentSizeOut(getContentSizeOut());
        event.setContentSizeRemoved(getContentSizeRemoved());
        event.setFlowFilesIn(getFlowFilesIn());
        event.setFlowFilesOut(getFlowFilesOut());
        event.setFlowFilesReceived(getFlowFilesReceived());
        event.setFlowFilesRemoved(getFlowFilesRemoved());
        event.setFlowFilesSent(getFlowFilesSent());
        event.setInvocations(getInvocations());
        event.setProcessingNanos(getProcessingNanoseconds());
        event.setCounters(Collections.unmodifiableMap(this.counters));
        return event;
    }

    public void add(final EventSumValue other) {
        this.aggregateLineageMillis.addAndGet(other.getAggregateLineageMillis());
        this.bytesRead.addAndGet(other.getBytesRead());
        this.bytesReceived.addAndGet(other.getBytesReceived());
        this.bytesSent.addAndGet(other.getBytesSent());
        this.bytesWritten.addAndGet(other.getBytesWritten());
        this.contentSizeIn.addAndGet(other.getContentSizeIn());
        this.contentSizeOut.addAndGet(other.getContentSizeOut());
        this.contentSizeRemoved.addAndGet(other.getContentSizeRemoved());
        this.flowFilesIn.addAndGet(other.getFlowFilesIn());
        this.flowFilesOut.addAndGet(other.getFlowFilesOut());
        this.flowFilesReceived.addAndGet(other.getFlowFilesReceived());
        this.flowFilesRemoved.addAndGet(other.getFlowFilesRemoved());
        this.flowFilesSent.addAndGet(other.getFlowFilesSent());
        this.invocations.addAndGet(other.getInvocations());
        this.processingNanos.addAndGet(other.getProcessingNanoseconds());

        final Map<String, Long> eventCounters = other.getCounters();
        if (eventCounters != null) {
            for (final Map.Entry<String, Long> entry : eventCounters.entrySet()) {
                final String counterName = entry.getKey();
                final Long counterValue = entry.getValue();

                counters.compute(counterName, (key, value) -> value == null ? counterValue : value + counterValue);
            }
        }
    }

    public long getTimestamp() {
        return millisecondTimestamp;
    }

    public long getMinuteTimestamp() {
        return minuteTimestamp;
    }

    public long getBytesRead() {
        return bytesRead.get();
    }

    public long getBytesWritten() {
        return bytesWritten.get();
    }

    public int getFlowFilesIn() {
        return flowFilesIn.get();
    }

    public int getFlowFilesOut() {
        return flowFilesOut.get();
    }

    public long getContentSizeIn() {
        return contentSizeIn.get();
    }

    public long getContentSizeOut() {
        return contentSizeOut.get();
    }

    public int getFlowFilesRemoved() {
        return flowFilesRemoved.get();
    }

    public long getContentSizeRemoved() {
        return contentSizeRemoved.get();
    }

    public long getProcessingNanoseconds() {
        return processingNanos.get();
    }

    public int getInvocations() {
        return invocations.get();
    }

    public long getAggregateLineageMillis() {
        return aggregateLineageMillis.get();
    }

    public int getFlowFilesReceived() {
        return flowFilesReceived.get();
    }

    public int getFlowFilesSent() {
        return flowFilesSent.get();
    }

    public long getBytesReceived() {
        return bytesReceived.get();
    }

    public long getBytesSent() {
        return bytesSent.get();
    }

    public Map<String, Long> getCounters() {
        return counters;
    }
}
