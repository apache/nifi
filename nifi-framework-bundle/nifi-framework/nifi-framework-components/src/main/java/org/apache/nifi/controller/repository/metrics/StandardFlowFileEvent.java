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

import java.util.HashMap;
import java.util.Map;

public final class StandardFlowFileEvent implements FlowFileEvent, Cloneable {

    private int flowFilesIn;
    private int flowFilesOut;
    private int flowFilesRemoved;
    private long contentSizeIn;
    private long contentSizeOut;
    private long contentSizeRemoved;
    private long bytesRead;
    private long bytesWritten;
    private long processingNanos;
    private long cpuNanos;
    private long contentReadNanos;
    private long contentWriteNanos;
    private long sessionCommitNanos;
    private long gcMillis;
    private long aggregateLineageMillis;
    private int flowFilesReceived;
    private long bytesReceived;
    private int flowFilesSent;
    private long bytesSent;
    private int invocations;
    private Map<String, Long> counters;

    public StandardFlowFileEvent() {
    }

    @Override
    public int getFlowFilesIn() {
        return flowFilesIn;
    }

    public void setFlowFilesIn(int flowFilesIn) {
        this.flowFilesIn = flowFilesIn;
    }

    @Override
    public int getFlowFilesOut() {
        return flowFilesOut;
    }

    public void setFlowFilesOut(int flowFilesOut) {
        this.flowFilesOut = flowFilesOut;
    }

    @Override
    public long getContentSizeIn() {
        return contentSizeIn;
    }

    public void setContentSizeIn(long contentSizeIn) {
        this.contentSizeIn = contentSizeIn;
    }

    @Override
    public long getContentSizeOut() {
        return contentSizeOut;
    }

    public void setContentSizeOut(long contentSizeOut) {
        this.contentSizeOut = contentSizeOut;
    }

    @Override
    public long getContentSizeRemoved() {
        return contentSizeRemoved;
    }

    public void setContentSizeRemoved(final long contentSizeRemoved) {
        this.contentSizeRemoved = contentSizeRemoved;
    }

    @Override
    public int getFlowFilesRemoved() {
        return flowFilesRemoved;
    }

    public void setFlowFilesRemoved(final int flowFilesRemoved) {
        this.flowFilesRemoved = flowFilesRemoved;
    }

    @Override
    public long getBytesRead() {
        return bytesRead;
    }

    public void setBytesRead(long bytesRead) {
        this.bytesRead = bytesRead;
    }

    @Override
    public long getBytesWritten() {
        return bytesWritten;
    }

    public void setBytesWritten(long bytesWritten) {
        this.bytesWritten = bytesWritten;
    }

    @Override
    public long getProcessingNanoseconds() {
        return processingNanos;
    }

    @Override
    public long getCpuNanoseconds() {
        return cpuNanos;
    }

    public void setCpuNanoseconds(final long nanos) {
        this.cpuNanos = nanos;
    }

    @Override
    public long getContentReadNanoseconds() {
        return contentReadNanos;
    }

    public void setContentReadNanoseconds(final long nanos) {
        this.contentReadNanos = nanos;
    }

    @Override
    public long getContentWriteNanoseconds() {
        return contentWriteNanos;
    }

    public void setContentWriteNanoseconds(final long nanos) {
        this.contentWriteNanos = nanos;
    }

    @Override
    public long getSessionCommitNanoseconds() {
        return sessionCommitNanos;
    }

    public void setSessionCommitNanos(final long nanos) {
        this.sessionCommitNanos = nanos;
    }

    @Override
    public long getGargeCollectionMillis() {
        return gcMillis;
    }

    public void setGarbageCollectionMillis(final long gcMillis) {
        this.gcMillis = gcMillis;
    }

    public void setProcessingNanos(final long processingNanos) {
        this.processingNanos = processingNanos;
    }

    @Override
    public int getInvocations() {
        return invocations;
    }

    public void setInvocations(final int invocations) {
        this.invocations = invocations;
    }

    @Override
    public int getFlowFilesReceived() {
        return flowFilesReceived;
    }

    public void setFlowFilesReceived(int flowFilesReceived) {
        this.flowFilesReceived = flowFilesReceived;
    }

    @Override
    public long getBytesReceived() {
        return bytesReceived;
    }

    public void setBytesReceived(long bytesReceived) {
        this.bytesReceived = bytesReceived;
    }

    @Override
    public int getFlowFilesSent() {
        return flowFilesSent;
    }

    public void setFlowFilesSent(int flowFilesSent) {
        this.flowFilesSent = flowFilesSent;
    }

    @Override
    public long getBytesSent() {
        return bytesSent;
    }

    public void setBytesSent(long bytesSent) {
        this.bytesSent = bytesSent;
    }

    @Override
    public long getAverageLineageMillis() {
        final int flowFileCount = flowFilesOut + flowFilesRemoved;
        return flowFileCount == 0 ? 0L : aggregateLineageMillis / flowFileCount;
    }

    public void setAggregateLineageMillis(long lineageMilliseconds) {
        this.aggregateLineageMillis = lineageMilliseconds;
    }

    @Override
    public long getAggregateLineageMillis() {
        return aggregateLineageMillis;
    }

    @Override
    public Map<String, Long> getCounters() {
        return counters;
    }

    public void setCounters(final Map<String, Long> counters) {
        this.counters = counters;
    }

    public void add(final FlowFileEvent event) {
        flowFilesIn += event.getFlowFilesIn();
        flowFilesOut += event.getFlowFilesOut();
        flowFilesRemoved += event.getFlowFilesRemoved();
        contentSizeIn += event.getContentSizeIn();
        contentSizeOut += event.getContentSizeOut();
        contentSizeRemoved += event.getContentSizeRemoved();
        bytesRead += event.getBytesRead();
        bytesWritten += event.getBytesWritten();
        processingNanos += event.getProcessingNanoseconds();
        cpuNanos += event.getCpuNanoseconds();
        contentReadNanos += event.getContentReadNanoseconds();
        contentWriteNanos += event.getContentWriteNanoseconds();
        sessionCommitNanos += event.getSessionCommitNanoseconds();
        gcMillis += event.getGargeCollectionMillis();
        aggregateLineageMillis += event.getAggregateLineageMillis();
        flowFilesReceived += event.getFlowFilesReceived();
        bytesReceived += event.getBytesReceived();
        flowFilesSent += event.getFlowFilesSent();
        bytesSent += event.getBytesSent();
        invocations += event.getInvocations();

        final Map<String, Long> eventCounters = event.getCounters();
        if (eventCounters != null) {
            if (counters == null) {
                counters = new HashMap<>();
            }

            eventCounters.forEach((k, v) -> counters.merge(k, v, Long::sum));
        }
    }
}
