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

package org.apache.nifi.reporting.prometheus.api;

import java.util.Collection;
import java.util.Map;

import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;

import com.yammer.metrics.core.VirtualMachineMetrics;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;

public class PrometheusMetricsUtil {
    private static final CollectorRegistry NIFI_REGISTRY = new CollectorRegistry();
    private static final CollectorRegistry JVM_REGISTRY = new CollectorRegistry();

    private static final Gauge AMOUNT_FLOWFILES_SENT = Gauge.build()
            .name("nifi_process_group_amount_flowfiles_sent")
            .help("Total number of FlowFiles in ProcessGroup sent")
            .labelNames("instance", "process_group_name", "process_group_id")
            .register(NIFI_REGISTRY);

    private static final Gauge AMOUNT_FLOWFILES_TRANSFERRED = Gauge.build()
            .name("nifi_process_group_amount_flowfiles_transferred")
            .help("Total number of FlowFiles in ProcessGroup transferred")
            .labelNames("instance", "process_group_name", "process_group_id")
            .register(NIFI_REGISTRY);

    private static final Gauge AMOUNT_FLOWFILES_RECEIVED = Gauge.build()
            .name("nifi_process_group_amount_flowfiles_received")
            .help("Total number of FlowFiles in ProcessGroup received")
            .labelNames("instance", "process_group_name", "process_group_id")
            .register(NIFI_REGISTRY);

    private static final Gauge AMOUNT_BYTES_SENT = Gauge.build()
            .name("nifi_process_group_amount_bytes_sent")
            .help("Total number of Bytes in ProcessGroup sent")
            .labelNames("instance", "process_group_name", "process_group_id")
            .register(NIFI_REGISTRY);

    private static final Gauge AMOUNT_BYTES_READ = Gauge.build()
            .name("nifi_process_group_amount_bytes_read")
            .help("Total number of Bytes in ProcessGroup read")
            .labelNames("instance", "process_group_name", "process_group_id")
            .register(NIFI_REGISTRY);

    private static final Gauge AMOUNT_BYTES_WRITTEN = Gauge.build()
            .name("nifi_process_group_amount_bytes_written")
            .help("Total number of Bytes in ProcessGroup written")
            .labelNames("instance", "process_group_name", "process_group_id")
            .register(NIFI_REGISTRY);

    private static final Gauge AMOUNT_BYTES_RECEIVED = Gauge.build()
            .name("nifi_process_group_amount_bytes_received")
            .help("Total number of Bytes in ProcessGroup received")
            .labelNames("instance", "process_group_name", "process_group_id")
            .register(NIFI_REGISTRY);

    private static final Gauge AMOUNT_BYTES_TRANSFERRED = Gauge.build()
            .name("nifi_process_group_amount_bytes_transferred")
            .help("Total number of Bytes in ProcessGroup transferred")
            .labelNames("instance", "process_group_name", "process_group_id")
            .register(NIFI_REGISTRY);


    private static final Gauge AMOUNT_THREADS_TOTAL_ACTIVE = Gauge.build()
            .name("nifi_process_group_amount_threads_active")
            .help("Total number of threads in ProcessGroup active")
            .labelNames("instance", "process_group_name", "process_group_id")
            .register(NIFI_REGISTRY);

    private static final Gauge SIZE_CONTENT_OUTPUT_TOTAL = Gauge.build()
            .name("nifi_process_group_size_content_output_total")
            .help("Total size of content output in ProcessGroup")
            .labelNames("instance", "process_group_name", "process_group_id")
            .register(NIFI_REGISTRY);

    private static final Gauge SIZE_CONTENT_INPUT_TOTAL = Gauge.build()
            .name("nifi_process_group_size_content_input_total")
            .help("Total size of content input in ProcessGroup")
            .labelNames("instance", "process_group_name", "process_group_id")
            .register(NIFI_REGISTRY);

    private static final Gauge SIZE_CONTENT_QUEUED_TOTAL = Gauge.build()
            .name("nifi_process_group_size_content_queued_total")
            .help("Total size of content queued in ProcessGroup")
            .labelNames("instance", "process_group_name", "process_group_id")
            .register(NIFI_REGISTRY);

    private static final Gauge AMOUNT_ITEMS_OUTPUT = Gauge.build()
            .name("nifi_process_group_amount_items_output")
            .help("Total amount of items in ProcessGroup output")
            .labelNames("instance", "process_group_name", "process_group_id")
            .register(NIFI_REGISTRY);

    private static final Gauge AMOUNT_ITEMS_INPUT = Gauge.build()
            .name("nifi_process_group_amount_items_input")
            .help("Total amount of items in ProcessGroup input")
            .labelNames("instance", "process_group_name", "process_group_id")
            .register(NIFI_REGISTRY);

    private static final Gauge AMOUNT_ITEMS_QUEUED = Gauge.build()
            .name("nifi_process_group_amount_items_queued")
            .help("Total amount of items in ProcessGroup queued")
            .labelNames("instance", "process_group_name", "process_group_id")
            .register(NIFI_REGISTRY);

    private static final Gauge PROCESSOR_COUNTERS = Gauge.build()
            .name("nifi_processor_counters")
            .help("Counters exposed by NiFi Processors")
            .labelNames("processor_name", "counter_name", "processor_id", "instance")
            .register(NIFI_REGISTRY);

    private static final Gauge JVM_HEAP_USED = Gauge.build()
            .name("nifi_jvm_heap_used")
            .help("NiFi JVM heap used")
            .labelNames("instance")
            .register(JVM_REGISTRY);

    private static final Gauge JVM_HEAP_USAGE = Gauge.build()
            .name("nifi_jvm_heap_usage")
            .help("NiFi JVM heap usage")
            .labelNames("instance")
            .register(JVM_REGISTRY);

    private static final Gauge JVM_HEAP_NON_USAGE = Gauge.build()
            .name("nifi_jvm_heap_non_usage")
            .help("NiFi JVM heap non usage")
            .labelNames("instance")
            .register(JVM_REGISTRY);

    private static final Gauge JVM_THREAD_COUNT = Gauge.build()
            .name("nifi_jvm_thread_count")
            .help("NiFi JVM thread count")
            .labelNames("instance")
            .register(JVM_REGISTRY);

    private static final Gauge JVM_DAEMON_THREAD_COUNT = Gauge.build()
            .name("nifi_jvm_daemon_thread_count")
            .help("NiFi JVM daemon thread count")
            .labelNames("instance")
            .register(JVM_REGISTRY);

    private static final Gauge JVM_UPTIME = Gauge.build()
            .name("nifi_jvm_uptime")
            .help("NiFi JVM uptime")
            .labelNames("instance")
            .register(JVM_REGISTRY);

    private static final Gauge JVM_FILE_DESCRIPTOR_USAGE = Gauge.build()
            .name("nifi_jvm_file_descriptor_usage")
            .help("NiFi JVM file descriptor usage")
            .labelNames("instance")
            .register(JVM_REGISTRY);

    public static CollectorRegistry createNifiMetrics(ProcessGroupStatus status, String instanceId) {

        final String processGroupId = status.getId();
        final String processGroupName = status.getName();
        Collection<ProcessorStatus> processorStatus = status.getProcessorStatus();

        AMOUNT_FLOWFILES_SENT.labels(instanceId, processGroupName, processGroupId).set(status.getFlowFilesSent());
        AMOUNT_FLOWFILES_TRANSFERRED.labels(instanceId, processGroupName, processGroupId).set(status.getFlowFilesTransferred());
        AMOUNT_FLOWFILES_RECEIVED.labels(instanceId, processGroupName, processGroupId).set(status.getFlowFilesReceived());

        AMOUNT_BYTES_SENT.labels(instanceId, processGroupName, processGroupId).set(status.getBytesSent());
        AMOUNT_BYTES_READ.labels(instanceId, processGroupName, processGroupId).set(status.getBytesRead());
        AMOUNT_BYTES_WRITTEN.labels(instanceId, processGroupName, processGroupId).set(status.getBytesWritten());
        AMOUNT_BYTES_RECEIVED.labels(instanceId, processGroupName, processGroupId).set(status.getBytesReceived());
        AMOUNT_BYTES_TRANSFERRED.labels(instanceId, processGroupName, processGroupId).set(status.getBytesTransferred());

        SIZE_CONTENT_OUTPUT_TOTAL.labels(instanceId, processGroupName, processGroupId).set(status.getOutputContentSize());
        SIZE_CONTENT_INPUT_TOTAL.labels(instanceId, processGroupName, processGroupId).set(status.getInputContentSize());
        SIZE_CONTENT_QUEUED_TOTAL.labels(instanceId, processGroupName, processGroupId).set(status.getQueuedContentSize());

        AMOUNT_ITEMS_OUTPUT.labels(instanceId, processGroupName, processGroupId).set(status.getOutputCount());
        AMOUNT_ITEMS_INPUT.labels(instanceId, processGroupName, processGroupId).set(status.getInputCount());
        AMOUNT_ITEMS_QUEUED.labels(instanceId, processGroupName, processGroupId).set(status.getQueuedCount());



        AMOUNT_THREADS_TOTAL_ACTIVE.labels(instanceId, processGroupName, processGroupId).set(status.getActiveThreadCount());

        for (ProcessorStatus pstatus : processorStatus) {
            Map<String, Long> counters = pstatus.getCounters();

            if(counters != null) {
                counters.entrySet().stream().forEach(entry -> PROCESSOR_COUNTERS
                        .labels(pstatus.getName(), entry.getKey(), pstatus.getId(), instanceId).set(entry.getValue()));
            }
        }

        return NIFI_REGISTRY;

    }

    public static CollectorRegistry createJvmMetrics(VirtualMachineMetrics jvmMetrics, String instanceId) {
        JVM_HEAP_USED.labels(instanceId).set(jvmMetrics.heapUsed());
        JVM_HEAP_USAGE.labels(instanceId).set(jvmMetrics.heapUsage());
        JVM_HEAP_NON_USAGE.labels(instanceId).set(jvmMetrics.nonHeapUsage());

        JVM_THREAD_COUNT.labels(instanceId).set(jvmMetrics.threadCount());
        JVM_DAEMON_THREAD_COUNT.labels(instanceId).set(jvmMetrics.daemonThreadCount());

        JVM_UPTIME.labels(instanceId).set(jvmMetrics.uptime());
        JVM_FILE_DESCRIPTOR_USAGE.labels(instanceId).set(jvmMetrics.fileDescriptorUsage());

        return JVM_REGISTRY;
    }

}
