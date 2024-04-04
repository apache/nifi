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
package org.apache.nifi.reporting.azure.loganalytics.api;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.nifi.metrics.jvm.JvmMetrics;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.reporting.azure.loganalytics.MetricNames;
import org.apache.nifi.reporting.azure.loganalytics.Metric;
import org.apache.nifi.reporting.azure.loganalytics.MetricsBuilder;


public class AzureLogAnalyticsMetricsFactory {

    public static List<Metric> getDataFlowMetrics(ProcessGroupStatus status, String instanceId){

        final String groupId = status.getId();
        final String groupName = status.getName();
        MetricsBuilder builder= new MetricsBuilder(Metric.CATEGORY_DATAFLOW,instanceId, groupId, groupName);

        // build dataflow metrics
        builder.metric(MetricNames.FLOW_FILES_RECEIVED, status.getFlowFilesReceived())
            .metric(MetricNames.FLOW_FILES_SENT, status.getFlowFilesSent())
            .metric(MetricNames.FLOW_FILES_QUEUED, status.getQueuedCount())
            .metric(MetricNames.BYTES_RECEIVED,status.getBytesReceived())
            .metric(MetricNames.BYTES_WRITTEN,status.getBytesWritten())
            .metric(MetricNames.BYTES_READ, status.getBytesRead())
            .metric(MetricNames.BYTES_SENT, status.getBytesSent())
            .metric(MetricNames.BYTES_QUEUED,status.getQueuedContentSize())
            .metric(MetricNames.ACTIVE_THREADS,status.getActiveThreadCount())
            .metric(MetricNames.TOTAL_TASK_DURATION_SECONDS,calculateProcessingNanos(status));
        return builder.build();
    }


    public static List<Metric> getConnectionStatusMetrics(ConnectionStatus status, String instanceId, String groupName){

        final String groupId = status.getGroupId();
        final String tags = String.format(
            "[source=%s][destination=%s][cname=%s]", status.getSourceName(), status.getDestinationName(),
            status.getName());
        MetricsBuilder builder= new MetricsBuilder(Metric.CATEGORY_CONNECTIONS,instanceId, groupId, groupName);

        builder.setTags(tags)
            .metric(MetricNames.INPUT_COUNT,status.getInputCount())
            .metric(MetricNames.INPUT_BYTES, status.getInputBytes())
            .metric(MetricNames.QUEUED_COUNT, status.getQueuedCount())
            .metric(MetricNames.QUEUED_BYTES, status.getQueuedBytes())
            .metric(MetricNames.OUTPUT_COUNT, status.getOutputCount())
            .metric(MetricNames.OUTPUT_BYTES, status.getOutputBytes());

        return builder.build();
    }

    public static List<Metric> getProcessorMetrics(ProcessorStatus status, String instanceId, String groupName){

        MetricsBuilder builder= new MetricsBuilder(Metric.CATEGORY_PROCESSOR,instanceId, status.getGroupId(), groupName);

        builder.setProcessorId(status.getId())
            .setProcessorName(status.getName())
            .metric(MetricNames.FLOW_FILES_RECEIVED, status.getInputCount())
            .metric(MetricNames.FLOW_FILES_SENT, status.getOutputCount())
            .metric(MetricNames.BYTES_READ, status.getInputBytes())
            .metric(MetricNames.BYTES_WRITTEN,status.getOutputBytes())
            .metric(MetricNames.ACTIVE_THREADS, status.getActiveThreadCount())
            .metric(MetricNames.TOTAL_TASK_DURATION_SECONDS, status.getProcessingNanos());

        return builder.build();
    }

    //virtual machine metrics
    public static List<Metric> getJvmMetrics(JvmMetrics virtualMachineMetrics, String instanceId, String groupName) {

        MetricsBuilder builder = new MetricsBuilder(Metric.CATEGORY_JVM, instanceId, "", groupName);

        builder.metric(MetricNames.JVM_HEAP_USED, virtualMachineMetrics.heapUsed(DataUnit.B))
            .metric(MetricNames.JVM_HEAP_USAGE, virtualMachineMetrics.heapUsage())
            .metric(MetricNames.JVM_NON_HEAP_USAGE, virtualMachineMetrics.nonHeapUsage())
            .metric(MetricNames.JVM_FILE_DESCRIPTOR_USAGE, virtualMachineMetrics.fileDescriptorUsage())
            .metric(MetricNames.JVM_UPTIME, virtualMachineMetrics.uptime())
            .metric(MetricNames.JVM_THREAD_COUNT, virtualMachineMetrics.threadCount())
            .metric(MetricNames.JVM_DAEMON_THREAD_COUNT, virtualMachineMetrics.daemonThreadCount());

        // Append GC stats
        virtualMachineMetrics.garbageCollectors()
                .forEach((name, stat) -> {
                    name = name.toLowerCase().replaceAll("\\s", "_");
                    builder.metric(MetricNames.JVM_GC_RUNS + "." + name, stat.getRuns())
                        .metric(MetricNames.JVM_GC_TIME + "." + name, stat.getTime(TimeUnit.MILLISECONDS));
                });

        // Append thread states
        virtualMachineMetrics.threadStatePercentages()
                .forEach((state, usage) -> {
                    String name = state.name().toLowerCase().replaceAll("\\s", "_");
                    builder.metric("jvm.thread_states." + name, usage);
                });

        // Append pool stats
        virtualMachineMetrics.memoryPoolUsage()
        .forEach((name, usage) -> {
            name = name.toLowerCase().replaceAll("\\s", "_");
            builder.metric("jvm.mem_pool_" + name, usage);
        });

        return builder.build();

    }

    // calculates the total processing time of all processors in nanos
    static long calculateProcessingNanos(final ProcessGroupStatus status) {
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