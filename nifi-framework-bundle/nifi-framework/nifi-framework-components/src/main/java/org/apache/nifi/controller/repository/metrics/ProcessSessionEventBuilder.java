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

import org.apache.nifi.controller.metrics.ComponentMetricContext;
import org.apache.nifi.controller.metrics.ProcessSessionEvent;

import java.util.HashMap;
import java.util.Map;

/**
 * Builder for immutable instances of ProcessSessionEvent
 */
public class ProcessSessionEventBuilder {

    private static final ComponentMetricContext EMPTY_CONTEXT = new ComponentMetricContext(
            ProcessSessionEventBuilder.class.getSimpleName(),
            ProcessSessionEventBuilder.class.getSimpleName(),
            ProcessSessionEvent.class.getSimpleName(),
            Map.of()
    );

    private static final ProcessSessionEvent EMPTY_EVENT = forComponent(EMPTY_CONTEXT)
            .counters(Map.of())
            .build();

    private final ComponentMetricContext componentMetricContext;

    private int invocations;
    private int flowFilesIn;
    private int flowFilesOut;
    private int flowFilesRemoved;
    private int flowFilesSent;
    private int flowFilesReceived;
    private long contentSizeIn;
    private long contentSizeOut;
    private long contentSizeRemoved;
    private long bytesRead;
    private long bytesWritten;
    private long bytesSent;
    private long bytesReceived;
    private long processingNanoseconds;
    private long cpuNanoseconds;
    private long contentReadNanoseconds;
    private long contentWriteNanoseconds;
    private long sessionCommitNanoseconds;
    private long garbageCollectionMillis;
    private long aggregateLineageMillis;
    private Map<String, Long> counters;

    private ProcessSessionEventBuilder(final ComponentMetricContext componentMetricContext) {
        this.componentMetricContext = componentMetricContext;
    }

    /**
     * Create builder using Component Metric Context for initialization
     *
     * @param componentMetricContext Metric Context for Component being processed
     * @return New Builder for ProcessSessionEvent
     */
    public static ProcessSessionEventBuilder forComponent(final ComponentMetricContext componentMetricContext) {
        return new ProcessSessionEventBuilder(componentMetricContext);
    }

    /**
     * Get shared empty instance of ProcessSessionEvent with zero metrics
     *
     * @return Shared empty ProcessSessionEvent
     */
    public static ProcessSessionEvent empty() {
        return EMPTY_EVENT;
    }

    public ProcessSessionEventBuilder invocations(final int invocations) {
        this.invocations = invocations;
        return this;
    }

    public ProcessSessionEventBuilder flowFilesIn(final int flowFilesIn) {
        this.flowFilesIn = flowFilesIn;
        return this;
    }

    public ProcessSessionEventBuilder flowFilesOut(final int flowFilesOut) {
        this.flowFilesOut = flowFilesOut;
        return this;
    }

    public ProcessSessionEventBuilder flowFilesRemoved(final int flowFilesRemoved) {
        this.flowFilesRemoved = flowFilesRemoved;
        return this;
    }

    public ProcessSessionEventBuilder flowFilesSent(final int flowFilesSent) {
        this.flowFilesSent = flowFilesSent;
        return this;
    }

    public ProcessSessionEventBuilder flowFilesReceived(final int flowFilesReceived) {
        this.flowFilesReceived = flowFilesReceived;
        return this;
    }

    public ProcessSessionEventBuilder contentSizeIn(final long contentSizeIn) {
        this.contentSizeIn = contentSizeIn;
        return this;
    }

    public ProcessSessionEventBuilder contentSizeOut(final long contentSizeOut) {
        this.contentSizeOut = contentSizeOut;
        return this;
    }

    public ProcessSessionEventBuilder contentSizeRemoved(final long contentSizeRemoved) {
        this.contentSizeRemoved = contentSizeRemoved;
        return this;
    }

    public ProcessSessionEventBuilder bytesRead(final long bytesRead) {
        this.bytesRead = bytesRead;
        return this;
    }

    public ProcessSessionEventBuilder bytesWritten(final long bytesWritten) {
        this.bytesWritten = bytesWritten;
        return this;
    }

    public ProcessSessionEventBuilder bytesSent(final long bytesSent) {
        this.bytesSent = bytesSent;
        return this;
    }

    public ProcessSessionEventBuilder bytesReceived(final long bytesReceived) {
        this.bytesReceived = bytesReceived;
        return this;
    }

    public ProcessSessionEventBuilder processingNanoseconds(final long processingNanoseconds) {
        this.processingNanoseconds = processingNanoseconds;
        return this;
    }

    public ProcessSessionEventBuilder cpuNanoseconds(final long cpuNanoseconds) {
        this.cpuNanoseconds = cpuNanoseconds;
        return this;
    }

    public ProcessSessionEventBuilder contentReadNanoseconds(final long contentReadNanoseconds) {
        this.contentReadNanoseconds = contentReadNanoseconds;
        return this;
    }

    public ProcessSessionEventBuilder contentWriteNanoseconds(final long contentWriteNanoseconds) {
        this.contentWriteNanoseconds = contentWriteNanoseconds;
        return this;
    }

    public ProcessSessionEventBuilder sessionCommitNanoseconds(final long sessionCommitNanoseconds) {
        this.sessionCommitNanoseconds = sessionCommitNanoseconds;
        return this;
    }

    public ProcessSessionEventBuilder garbageCollectionMillis(final long garbageCollectionMillis) {
        this.garbageCollectionMillis = garbageCollectionMillis;
        return this;
    }

    public ProcessSessionEventBuilder aggregateLineageMillis(final long aggregateLineageMillis) {
        this.aggregateLineageMillis = aggregateLineageMillis;
        return this;
    }

    public ProcessSessionEventBuilder counters(final Map<String, Long> counters) {
        this.counters = counters;
        return this;
    }

    public ProcessSessionEventBuilder addFlowFilesIn(final int flowFilesIn) {
        this.flowFilesIn += flowFilesIn;
        return this;
    }

    public ProcessSessionEventBuilder addFlowFilesOut(final int flowFilesOut) {
        this.flowFilesOut += flowFilesOut;
        return this;
    }

    public ProcessSessionEventBuilder addContentSizeIn(final long contentSizeIn) {
        this.contentSizeIn += contentSizeIn;
        return this;
    }

    public ProcessSessionEventBuilder addContentSizeOut(final long contentSizeOut) {
        this.contentSizeOut += contentSizeOut;
        return this;
    }

    /**
     * Accumulates the metrics of another event into this builder. The component context of this builder is retained.
     *
     * @param event ProcessSessionEvent required with values to be merged
     * @return ProcessSessionEvent Builder
     */
    public ProcessSessionEventBuilder merge(final ProcessSessionEvent event) {
        this.invocations += event.getInvocations();
        this.flowFilesIn += event.getFlowFilesIn();
        this.flowFilesOut += event.getFlowFilesOut();
        this.flowFilesRemoved += event.getFlowFilesRemoved();
        this.flowFilesSent += event.getFlowFilesSent();
        this.flowFilesReceived += event.getFlowFilesReceived();
        this.contentSizeIn += event.getContentSizeIn();
        this.contentSizeOut += event.getContentSizeOut();
        this.contentSizeRemoved += event.getContentSizeRemoved();
        this.bytesRead += event.getBytesRead();
        this.bytesWritten += event.getBytesWritten();
        this.bytesSent += event.getBytesSent();
        this.bytesReceived += event.getBytesReceived();
        this.processingNanoseconds += event.getProcessingNanoseconds();
        this.cpuNanoseconds += event.getCpuNanoseconds();
        this.contentReadNanoseconds += event.getContentReadNanoseconds();
        this.contentWriteNanoseconds += event.getContentWriteNanoseconds();
        this.sessionCommitNanoseconds += event.getSessionCommitNanoseconds();
        this.garbageCollectionMillis += event.getGarbageCollectionMillis();
        this.aggregateLineageMillis += event.getAggregateLineageMillis();

        final Map<String, Long> eventCounters = event.getCounters();
        if (eventCounters != null && !eventCounters.isEmpty()) {
            final Map<String, Long> mergedCounters = counters == null ? new HashMap<>() : new HashMap<>(counters);
            eventCounters.forEach((name, value) -> mergedCounters.merge(name, value, Long::sum));
            counters = mergedCounters;
        }

        return this;
    }

    /**
     * Build ProcessSessionEvent using current accumulated values
     *
     * @return ProcessSessionEvent with current values
     */
    public ProcessSessionEvent build() {
        return new StandardProcessSessionEvent(
                componentMetricContext,
                invocations,
                flowFilesIn,
                flowFilesOut,
                flowFilesRemoved,
                flowFilesSent,
                flowFilesReceived,
                contentSizeIn,
                contentSizeOut,
                contentSizeRemoved,
                bytesRead,
                bytesWritten,
                bytesSent,
                bytesReceived,
                processingNanoseconds,
                cpuNanoseconds,
                contentReadNanoseconds,
                contentWriteNanoseconds,
                sessionCommitNanoseconds,
                garbageCollectionMillis,
                aggregateLineageMillis,
                counters
        );
    }
}
