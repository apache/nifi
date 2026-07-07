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

import java.util.Map;

record StandardProcessSessionEvent(
        ComponentMetricContext componentMetricContext,
        int invocations,
        int flowFilesIn,
        int flowFilesOut,
        int flowFilesRemoved,
        int flowFilesSent,
        int flowFilesReceived,
        long contentSizeIn,
        long contentSizeOut,
        long contentSizeRemoved,
        long bytesRead,
        long bytesWritten,
        long bytesSent,
        long bytesReceived,
        long processingNanoseconds,
        long cpuNanoseconds,
        long contentReadNanoseconds,
        long contentWriteNanoseconds,
        long sessionCommitNanoseconds,
        long garbageCollectionMillis,
        long aggregateLineageMillis,
        Map<String, Long> counters
) implements ProcessSessionEvent {

    @Override
    public ComponentMetricContext getComponentMetricContext() {
        return componentMetricContext;
    }

    @Override
    public int getInvocations() {
        return invocations;
    }

    @Override
    public int getFlowFilesIn() {
        return flowFilesIn;
    }

    @Override
    public int getFlowFilesOut() {
        return flowFilesOut;
    }

    @Override
    public int getFlowFilesRemoved() {
        return flowFilesRemoved;
    }

    @Override
    public int getFlowFilesSent() {
        return flowFilesSent;
    }

    @Override
    public int getFlowFilesReceived() {
        return flowFilesReceived;
    }

    @Override
    public long getContentSizeIn() {
        return contentSizeIn;
    }

    @Override
    public long getContentSizeOut() {
        return contentSizeOut;
    }

    @Override
    public long getContentSizeRemoved() {
        return contentSizeRemoved;
    }

    @Override
    public long getBytesRead() {
        return bytesRead;
    }

    @Override
    public long getBytesWritten() {
        return bytesWritten;
    }

    @Override
    public long getBytesSent() {
        return bytesSent;
    }

    @Override
    public long getBytesReceived() {
        return bytesReceived;
    }

    @Override
    public long getProcessingNanoseconds() {
        return processingNanoseconds;
    }

    @Override
    public long getCpuNanoseconds() {
        return cpuNanoseconds;
    }

    @Override
    public long getContentReadNanoseconds() {
        return contentReadNanoseconds;
    }

    @Override
    public long getContentWriteNanoseconds() {
        return contentWriteNanoseconds;
    }

    @Override
    public long getSessionCommitNanoseconds() {
        return sessionCommitNanoseconds;
    }

    @Override
    public long getGarbageCollectionMillis() {
        return garbageCollectionMillis;
    }

    @Override
    public long getAggregateLineageMillis() {
        return aggregateLineageMillis;
    }

    @Override
    public Map<String, Long> getCounters() {
        return counters;
    }
}
