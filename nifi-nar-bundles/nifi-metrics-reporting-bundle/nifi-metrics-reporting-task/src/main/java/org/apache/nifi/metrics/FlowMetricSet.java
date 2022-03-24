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
package org.apache.nifi.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A metric set of NiFi instance related metrics.
 *
 * @author Omer Hadari
 */
public class FlowMetricSet implements MetricSet {


    /**
     * Reference to the process status that should be reported. Should be updated when the status changes.
     */
    private final AtomicReference<ProcessGroupStatus> currentStatusReference;

    /**
     * Create a metric set that will look at a given process status reference for deciding metrics.
     *
     * @param currentStatusReference a reference to the process status.
     */
    public FlowMetricSet(AtomicReference<ProcessGroupStatus> currentStatusReference) {
        this.currentStatusReference = currentStatusReference;
    }

    /**
     * Create a map of {@link Gauge}s for the {@link #currentStatusReference}. This methods reports the metrics as
     * found in the reference.
     *
     * @return map between the metric name and a {@link Gauge} to it's value.
     */
    @Override
    public Map<String, Metric> getMetrics() {

        Map<String, Metric> metrics = new HashMap<>();

        metrics.put(MetricNames.ACTIVE_THREADS, (Gauge<Integer>) () -> currentStatusReference.get().getActiveThreadCount());
        metrics.put(MetricNames.BYTES_QUEUED, (Gauge<Long>) () -> currentStatusReference.get().getQueuedContentSize());
        metrics.put(MetricNames.BYTES_READ, (Gauge<Long>) () -> currentStatusReference.get().getBytesRead());
        metrics.put(MetricNames.BYTES_RECEIVED, (Gauge<Long>) () -> currentStatusReference.get().getBytesReceived());
        metrics.put(MetricNames.BYTES_SENT, (Gauge<Long>) () -> currentStatusReference.get().getBytesSent());
        metrics.put(MetricNames.BYTES_WRITTEN, (Gauge<Long>) () -> currentStatusReference.get().getBytesWritten());
        metrics.put(MetricNames.FLOW_FILES_RECEIVED, (Gauge<Integer>) () -> currentStatusReference.get().getFlowFilesReceived());
        metrics.put(MetricNames.FLOW_FILES_QUEUED, (Gauge<Integer>) () -> currentStatusReference.get().getQueuedCount());
        metrics.put(MetricNames.FLOW_FILES_SENT, (Gauge<Integer>) () -> currentStatusReference.get().getFlowFilesSent());
        metrics.put(MetricNames.TOTAL_TASK_DURATION_NANOS, (Gauge<Long>) () -> calculateProcessingNanos(currentStatusReference.get()));

        return metrics;
    }

    /**
     * Calculate the total processing time of a process group.
     *
     * @param status the current process group status.
     * @return the total amount of nanoseconds spent in each processor in the process group.
     */
    private long calculateProcessingNanos(final ProcessGroupStatus status) {
        long nanos = 0L;

        for (final ProcessorStatus procStats : status.getProcessorStatus()) {
            nanos += procStats.getProcessingNanos();
        }

        for (final ProcessGroupStatus childGroupStatus : status.getProcessGroupStatus()) {
            nanos += calculateProcessingNanos(childGroupStatus);
        }

        return nanos;
    }
}
