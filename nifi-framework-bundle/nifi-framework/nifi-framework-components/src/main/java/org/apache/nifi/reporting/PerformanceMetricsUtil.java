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
package org.apache.nifi.reporting;

import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.repository.FlowFileEvent;
import org.apache.nifi.controller.status.PerformanceMetrics;

public class PerformanceMetricsUtil {

    public static PerformanceMetrics getPerformanceMetrics(final FlowFileEvent aggregatedEvent, final FlowFileEvent fileEvent, final ProcessorNode processorNode) {
        long totalCpuNanos = aggregatedEvent.getCpuNanoseconds();
        long totalReadNanos = aggregatedEvent.getContentReadNanoseconds();
        long totalWriteNanos = aggregatedEvent.getContentWriteNanoseconds();
        long totalSessionCommitNanos = aggregatedEvent.getSessionCommitNanoseconds();
        long totalBytesRead = aggregatedEvent.getBytesRead();
        long totalBytesWritten = aggregatedEvent.getBytesWritten();
        long totalGcNanos = aggregatedEvent.getGargeCollectionMillis();

        final PerformanceMetrics newMetrics = new PerformanceMetrics();

        newMetrics.setIdentifier(processorNode.getProcessGroup().getIdentifier());
        newMetrics.setCpuTime(fileEvent.getCpuNanoseconds());
        newMetrics.setCpuTimePercentage(nanosToPercent(fileEvent.getCpuNanoseconds(), totalCpuNanos));
        newMetrics.setReadTime(fileEvent.getContentReadNanoseconds());
        newMetrics.setReadTimePercentage(nanosToPercent(fileEvent.getContentReadNanoseconds(), totalReadNanos));
        newMetrics.setWriteTime(fileEvent.getContentWriteNanoseconds());
        newMetrics.setWriteTimePercentage(nanosToPercent(fileEvent.getContentWriteNanoseconds(), totalWriteNanos));
        newMetrics.setCommitTime(fileEvent.getSessionCommitNanoseconds());
        newMetrics.setCommitTimePercentage(nanosToPercent(fileEvent.getSessionCommitNanoseconds(), totalSessionCommitNanos));
        newMetrics.setGcTime(fileEvent.getGargeCollectionMillis());
        newMetrics.setGcTimePercentage(nanosToPercent(fileEvent.getGargeCollectionMillis(), totalGcNanos));
        newMetrics.setBytesRead(fileEvent.getBytesRead());
        newMetrics.setBytesReadPercentage(nanosToPercent(fileEvent.getBytesRead(), totalBytesRead));
        newMetrics.setBytesWritten(fileEvent.getBytesWritten());
        newMetrics.setBytesWrittenPercentage(nanosToPercent(fileEvent.getBytesWritten(), totalBytesWritten));

        return newMetrics;
    }

    private static long nanosToPercent(final long a, final long b) {
        if (b < 1) {
            return b;
        } else {
            return Math.min(100L, a * 100 / b);
        }
    }
}
