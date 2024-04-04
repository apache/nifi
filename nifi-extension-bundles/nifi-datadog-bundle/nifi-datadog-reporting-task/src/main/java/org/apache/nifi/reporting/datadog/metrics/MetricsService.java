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
package org.apache.nifi.reporting.datadog.metrics;

import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.PortStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.metrics.jvm.JmxJvmMetrics;
import org.apache.nifi.processor.DataUnit;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A service used to produce key/value metrics based on a given input.
 */
public class MetricsService {

    //processor - specific metrics
    public Map<String, Double> getProcessorMetrics(ProcessorStatus status) {
        final Map<String, Double> metrics = new HashMap<>();
        metrics.put(MetricNames.FLOW_FILES_RECEIVED, Double.valueOf(status.getInputCount()));
        metrics.put(MetricNames.FLOW_FILES_SENT, Double.valueOf(status.getOutputCount()));
        metrics.put(MetricNames.BYTES_READ, Double.valueOf(status.getInputBytes()));
        metrics.put(MetricNames.BYTES_WRITTEN, Double.valueOf(status.getOutputBytes()));
        metrics.put(MetricNames.ACTIVE_THREADS, Double.valueOf(status.getActiveThreadCount()));
        metrics.put(MetricNames.TOTAL_TASK_DURATION, Double.valueOf(status.getProcessingNanos()));
        return metrics;
    }

    public Map<String, String> getProcessorTags(ProcessorStatus status) {
        Map<String, String> tags = new HashMap<>();
        tags.put("processor", status.getName());
        return tags;
    }

    public Map<String, Double> getPortStatusMetrics(PortStatus status){
        final Map<String, Double> metrics = new HashMap<>();
        metrics.put(MetricNames.ACTIVE_THREADS, Double.valueOf(status.getActiveThreadCount()));
        metrics.put(MetricNames.INPUT_COUNT, Double.valueOf(status.getInputCount()));
        metrics.put(MetricNames.OUTPUT_COUNT, Double.valueOf(status.getOutputCount()));
        metrics.put(MetricNames.INPUT_BYTES, Double.valueOf(status.getInputBytes()));
        metrics.put(MetricNames.OUTPUT_BYTES, Double.valueOf(status.getOutputBytes()));
        metrics.put(MetricNames.FLOW_FILES_RECEIVED, Double.valueOf(status.getFlowFilesReceived()));
        metrics.put(MetricNames.FLOW_FILES_SENT, Double.valueOf(status.getFlowFilesSent()));
        metrics.put(MetricNames.BYTES_RECEIVED, Double.valueOf(status.getBytesReceived()));
        metrics.put(MetricNames.BYTES_SENT, Double.valueOf(status.getBytesSent()));
        return metrics;
    }

    public Map<String, Double> getConnectionStatusMetrics(ConnectionStatus status) {
        final Map<String, Double> metrics = new HashMap<>();
        metrics.put(MetricNames.INPUT_COUNT, Double.valueOf(status.getInputCount()));
        metrics.put(MetricNames.INPUT_BYTES, Double.valueOf(status.getInputBytes()));
        metrics.put(MetricNames.QUEUED_COUNT, Double.valueOf(status.getQueuedCount()));
        metrics.put(MetricNames.QUEUED_BYTES, Double.valueOf(status.getQueuedBytes()));
        metrics.put(MetricNames.OUTPUT_COUNT, Double.valueOf(status.getOutputCount()));
        metrics.put(MetricNames.OUTPUT_BYTES, Double.valueOf(status.getOutputBytes()));
        return metrics;
    }


    //general metrics for whole dataflow
    public Map<String, Double> getDataFlowMetrics(ProcessGroupStatus status) {
        final Map<String, Double> metrics = new HashMap<>();
        metrics.put(MetricNames.FLOW_FILES_RECEIVED, Double.valueOf(status.getFlowFilesReceived()));
        metrics.put(MetricNames.BYTES_RECEIVED, Double.valueOf(status.getBytesReceived()));
        metrics.put(MetricNames.FLOW_FILES_SENT, Double.valueOf(status.getFlowFilesSent()));
        metrics.put(MetricNames.BYTES_SENT, Double.valueOf(status.getBytesSent()));
        metrics.put(MetricNames.FLOW_FILES_QUEUED, Double.valueOf(status.getQueuedCount()));
        metrics.put(MetricNames.BYTES_QUEUED, Double.valueOf(status.getQueuedContentSize()));
        metrics.put(MetricNames.BYTES_READ, Double.valueOf(status.getBytesRead()));
        metrics.put(MetricNames.BYTES_WRITTEN, Double.valueOf(status.getBytesWritten()));
        metrics.put(MetricNames.ACTIVE_THREADS, Double.valueOf(status.getActiveThreadCount()));
        metrics.put(MetricNames.TOTAL_TASK_DURATION, Double.valueOf(calculateProcessingNanos(status)));
        status.getOutputPortStatus();
        return metrics;
    }

    //virtual machine metrics
    public Map<String, Double> getJVMMetrics(JmxJvmMetrics virtualMachineMetrics) {
        final Map<String, Double> metrics = new HashMap<>();
        metrics.put(MetricNames.JVM_UPTIME, Double.valueOf(virtualMachineMetrics.uptime()));
        metrics.put(MetricNames.JVM_HEAP_USED, Double.valueOf(virtualMachineMetrics.heapUsed(DataUnit.B)));
        metrics.put(MetricNames.JVM_HEAP_USAGE, Double.valueOf(virtualMachineMetrics.heapUsage()));
        metrics.put(MetricNames.JVM_NON_HEAP_USAGE, Double.valueOf(virtualMachineMetrics.nonHeapUsage()));
        metrics.put(MetricNames.JVM_THREAD_COUNT, Double.valueOf(virtualMachineMetrics.threadCount()));
        metrics.put(MetricNames.JVM_DAEMON_THREAD_COUNT, Double.valueOf(virtualMachineMetrics.daemonThreadCount()));
        metrics.put(MetricNames.JVM_FILE_DESCRIPTOR_USAGE, Double.valueOf(virtualMachineMetrics.fileDescriptorUsage()));

        for (Map.Entry<Thread.State, Double> entry : virtualMachineMetrics.threadStatePercentages().entrySet()) {
            final int normalizedValue = (int) (100 * (entry.getValue() == null ? 0 : entry.getValue()));
            switch (entry.getKey()) {
                case BLOCKED:
                    metrics.put(MetricNames.JVM_THREAD_STATES_BLOCKED, Double.valueOf(normalizedValue));
                    break;
                case RUNNABLE:
                    metrics.put(MetricNames.JVM_THREAD_STATES_RUNNABLE, Double.valueOf(normalizedValue));
                    break;
                case TERMINATED:
                    metrics.put(MetricNames.JVM_THREAD_STATES_TERMINATED, Double.valueOf(normalizedValue));
                    break;
                case TIMED_WAITING:
                    metrics.put(MetricNames.JVM_THREAD_STATES_TIMED_WAITING, Double.valueOf(normalizedValue));
                    break;
                default:
                    break;
            }
        }

        for (Map.Entry<String, JmxJvmMetrics.GarbageCollectorStats> entry : virtualMachineMetrics.garbageCollectors().entrySet()) {
            final String gcName = entry.getKey().replace(" ", "");
            final long runs = entry.getValue().getRuns();
            final long timeMS = entry.getValue().getTime(TimeUnit.MILLISECONDS);
            metrics.put(MetricNames.JVM_GC_RUNS + "." + gcName, Double.valueOf(runs));
            metrics.put(MetricNames.JVM_GC_TIME + "." + gcName, Double.valueOf(timeMS));
        }

        return metrics;
    }


    // calculates the total processing time of all processors in nanos
    protected long calculateProcessingNanos(final ProcessGroupStatus status) {
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
