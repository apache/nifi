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

import com.yammer.metrics.core.VirtualMachineMetrics;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.PortStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A service used to produce key/value metrics based on a given input.
 */
public class MetricsService {

    //processor - specific metrics
    public Map<String, Double> getProcessorMetrics(ProcessorStatus status) {
        final Map<String, Double> metrics = new HashMap<>();
        metrics.put(MetricNames.FLOW_FILES_RECEIVED, new Double(status.getInputCount()));
        metrics.put(MetricNames.FLOW_FILES_SENT, new Double(status.getOutputCount()));
        metrics.put(MetricNames.BYTES_READ, new Double(status.getInputBytes()));
        metrics.put(MetricNames.BYTES_WRITTEN, new Double(status.getOutputBytes()));
        metrics.put(MetricNames.ACTIVE_THREADS, new Double(status.getActiveThreadCount()));
        metrics.put(MetricNames.TOTAL_TASK_DURATION, new Double(status.getProcessingNanos()));
        return metrics;
    }

    public Map<String, Double> getPortStatusMetrics(PortStatus status){
        final Map<String, Double> metrics = new HashMap<>();
        metrics.put(MetricNames.ACTIVE_THREADS, new Double(status.getActiveThreadCount()));
        metrics.put(MetricNames.INPUT_COUNT, new Double(status.getInputCount()));
        metrics.put(MetricNames.OUTPUT_COUNT, new Double(status.getOutputCount()));
        metrics.put(MetricNames.INPUT_BYTES, new Double(status.getInputBytes()));
        metrics.put(MetricNames.OUTPUT_BYTES, new Double(status.getOutputBytes()));
        metrics.put(MetricNames.FLOW_FILES_RECEIVED, new Double(status.getFlowFilesReceived()));
        metrics.put(MetricNames.FLOW_FILES_SENT, new Double(status.getFlowFilesSent()));
        metrics.put(MetricNames.BYTES_RECEIVED, new Double(status.getBytesReceived()));
        metrics.put(MetricNames.BYTES_SENT, new Double(status.getBytesSent()));
        return metrics;
    }

    public Map<String,String> getPortStatusTags(PortStatus status) {
        final Map<String, String> portTags = new HashMap<>();
        portTags.put(MetricNames.PORT_ID, status.getId());
        portTags.put(MetricNames.PORT_GROUP_ID, status.getGroupId());
        portTags.put(MetricNames.PORT_NAME, status.getName());
        return portTags;
    }

    public Map<String,String> getConnectionStatusTags(ConnectionStatus status) {
        final Map<String, String> connectionTags = new HashMap<>();
        connectionTags.put(MetricNames.CONNECTION_ID, status.getId());
        connectionTags.put(MetricNames.CONNECTION_NAME, status.getName());
        connectionTags.put(MetricNames.CONNECTION_GROUP_ID, status.getGroupId());
        connectionTags.put(MetricNames.CONNECTION_DESTINATION_ID, status.getDestinationId());
        connectionTags.put(MetricNames.CONNECTTION_DESTINATION_NAME, status.getDestinationName());
        connectionTags.put(MetricNames.CONNECTION_SOURCE_ID, status.getSourceId());
        connectionTags.put(MetricNames.CONNECTION_SOURCE_NAME, status.getSourceName());
        return connectionTags;
    }

    public Map<String, Double> getConnectionStatusMetrics(ConnectionStatus status) {
        final Map<String, Double> metrics = new HashMap<>();
        metrics.put(MetricNames.INPUT_COUNT, new Double(status.getInputCount()));
        metrics.put(MetricNames.INPUT_BYTES, new Double(status.getInputBytes()));
        metrics.put(MetricNames.QUEUED_COUNT, new Double(status.getQueuedCount()));
        metrics.put(MetricNames.QUEUED_BYTES, new Double(status.getQueuedBytes()));
        metrics.put(MetricNames.OUTPUT_COUNT, new Double(status.getOutputCount()));
        metrics.put(MetricNames.OUTPUT_BYTES, new Double(status.getOutputBytes()));
        return metrics;
    }


    //general metrics for whole dataflow
    public Map<String, Double> getDataFlowMetrics(ProcessGroupStatus status) {
        final Map<String, Double> metrics = new HashMap<>();
        metrics.put(MetricNames.FLOW_FILES_RECEIVED, new Double(status.getFlowFilesReceived()));
        metrics.put(MetricNames.BYTES_RECEIVED, new Double(status.getBytesReceived()));
        metrics.put(MetricNames.FLOW_FILES_SENT, new Double(status.getFlowFilesSent()));
        metrics.put(MetricNames.BYTES_SENT, new Double(status.getBytesSent()));
        metrics.put(MetricNames.FLOW_FILES_QUEUED, new Double(status.getQueuedCount()));
        metrics.put(MetricNames.BYTES_QUEUED, new Double(status.getQueuedContentSize()));
        metrics.put(MetricNames.BYTES_READ, new Double(status.getBytesRead()));
        metrics.put(MetricNames.BYTES_WRITTEN, new Double(status.getBytesWritten()));
        metrics.put(MetricNames.ACTIVE_THREADS, new Double(status.getActiveThreadCount()));
        metrics.put(MetricNames.TOTAL_TASK_DURATION, new Double(calculateProcessingNanos(status)));
        status.getOutputPortStatus();
        return metrics;
    }

    public List<String> getAllTagsList() {
        List<String> tagsList = new ArrayList<>();
        tagsList.add("env");
        tagsList.add("dataflow_id");
        tagsList.add(MetricNames.PORT_ID);
        tagsList.add(MetricNames.PORT_NAME);
        tagsList.add(MetricNames.PORT_GROUP_ID);
        tagsList.add(MetricNames.CONNECTION_ID);
        tagsList.add(MetricNames.CONNECTION_NAME);
        tagsList.add(MetricNames.CONNECTION_GROUP_ID);
        tagsList.add(MetricNames.CONNECTION_SOURCE_ID);
        tagsList.add(MetricNames.CONNECTION_SOURCE_NAME);
        tagsList.add(MetricNames.CONNECTION_DESTINATION_ID);
        tagsList.add(MetricNames.CONNECTTION_DESTINATION_NAME);
        return tagsList;
    }

    //virtual machine metrics
    public Map<String, Double> getJVMMetrics(VirtualMachineMetrics virtualMachineMetrics) {
        final Map<String, Double> metrics = new HashMap<>();
        metrics.put(MetricNames.JVM_UPTIME, new Double(virtualMachineMetrics.uptime()));
        metrics.put(MetricNames.JVM_HEAP_USED, new Double(virtualMachineMetrics.heapUsed()));
        metrics.put(MetricNames.JVM_HEAP_USAGE, new Double(virtualMachineMetrics.heapUsage()));
        metrics.put(MetricNames.JVM_NON_HEAP_USAGE, new Double(virtualMachineMetrics.nonHeapUsage()));
        metrics.put(MetricNames.JVM_THREAD_COUNT, new Double(virtualMachineMetrics.threadCount()));
        metrics.put(MetricNames.JVM_DAEMON_THREAD_COUNT, new Double(virtualMachineMetrics.daemonThreadCount()));
        metrics.put(MetricNames.JVM_FILE_DESCRIPTOR_USAGE, new Double(virtualMachineMetrics.fileDescriptorUsage()));

        for (Map.Entry<Thread.State, Double> entry : virtualMachineMetrics.threadStatePercentages().entrySet()) {
            final int normalizedValue = (int) (100 * (entry.getValue() == null ? 0 : entry.getValue()));
            switch (entry.getKey()) {
                case BLOCKED:
                    metrics.put(MetricNames.JVM_THREAD_STATES_BLOCKED, new Double(normalizedValue));
                    break;
                case RUNNABLE:
                    metrics.put(MetricNames.JVM_THREAD_STATES_RUNNABLE, new Double(normalizedValue));
                    break;
                case TERMINATED:
                    metrics.put(MetricNames.JVM_THREAD_STATES_TERMINATED, new Double(normalizedValue));
                    break;
                case TIMED_WAITING:
                    metrics.put(MetricNames.JVM_THREAD_STATES_TIMED_WAITING, new Double(normalizedValue));
                    break;
                default:
                    break;
            }
        }

        for (Map.Entry<String, VirtualMachineMetrics.GarbageCollectorStats> entry : virtualMachineMetrics.garbageCollectors().entrySet()) {
            final String gcName = entry.getKey().replace(" ", "");
            final long runs = entry.getValue().getRuns();
            final long timeMS = entry.getValue().getTime(TimeUnit.MILLISECONDS);
            metrics.put(MetricNames.JVM_GC_RUNS + "." + gcName,new Double(runs));
            metrics.put(MetricNames.JVM_GC_TIME + "." + gcName, new Double(timeMS));
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
