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
package org.apache.nifi.reporting.util.metrics;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.reporting.util.metrics.api.MetricFields;

import com.yammer.metrics.core.VirtualMachineMetrics;

/**
 * A service used to produce key/value metrics based on a given input.
 */
public class MetricsService {

    /**
     * Generates a Map of metrics for a ProcessGroupStatus instance.
     *
     * @param status a ProcessGroupStatus to get metrics from
     * @param appendPgId if true, the process group ID will be appended at the end of the metric name
     * @return a map of metrics for the given status
     */
    public Map<String,String> getMetrics(ProcessGroupStatus status, boolean appendPgId) {
        final Map<String,String> metrics = new HashMap<>();

        Map<String,Long> longMetrics = getLongMetrics(status, appendPgId);
        for (String key : longMetrics.keySet()) {
            metrics.put(key, String.valueOf(longMetrics.get(key)));
        }

        Map<String,Integer> integerMetrics = getIntegerMetrics(status, appendPgId);
        for (String key : integerMetrics.keySet()) {
            metrics.put(key, String.valueOf(integerMetrics.get(key)));
        }

        return metrics;
    }

    private Map<String,Integer> getIntegerMetrics(ProcessGroupStatus status, boolean appendPgId) {
        final Map<String,Integer> metrics = new HashMap<>();
        metrics.put(appendPgId(MetricNames.FLOW_FILES_RECEIVED, status, appendPgId), status.getFlowFilesReceived());
        metrics.put(appendPgId(MetricNames.FLOW_FILES_SENT, status, appendPgId), status.getFlowFilesSent());
        metrics.put(appendPgId(MetricNames.FLOW_FILES_QUEUED, status, appendPgId), status.getQueuedCount());
        metrics.put(appendPgId(MetricNames.ACTIVE_THREADS, status, appendPgId), status.getActiveThreadCount());
        return metrics;
    }

    private Map<String,Long> getLongMetrics(ProcessGroupStatus status, boolean appendPgId) {
        final Map<String,Long> metrics = new HashMap<>();
        metrics.put(appendPgId(MetricNames.BYTES_RECEIVED, status, appendPgId), status.getBytesReceived());
        metrics.put(appendPgId(MetricNames.BYTES_SENT, status, appendPgId), status.getBytesSent());
        metrics.put(appendPgId(MetricNames.BYTES_QUEUED, status, appendPgId), status.getQueuedContentSize());
        metrics.put(appendPgId(MetricNames.BYTES_READ, status, appendPgId), status.getBytesRead());
        metrics.put(appendPgId(MetricNames.BYTES_WRITTEN, status, appendPgId), status.getBytesWritten());

        final long durationNanos = calculateProcessingNanos(status);
        metrics.put(appendPgId(MetricNames.TOTAL_TASK_DURATION_NANOS, status, appendPgId), durationNanos);

        final long durationSeconds = TimeUnit.SECONDS.convert(durationNanos, TimeUnit.NANOSECONDS);
        metrics.put(appendPgId(MetricNames.TOTAL_TASK_DURATION_SECONDS, status, appendPgId), durationSeconds);

        return metrics;
    }

    /**
     * Generates a Map of metrics for VirtualMachineMetrics.
     *
     * @param virtualMachineMetrics a VirtualMachineMetrics instance to get metrics from
     * @return a map of metrics from the given VirtualMachineStatus
     */
    public Map<String,String> getMetrics(VirtualMachineMetrics virtualMachineMetrics) {
        final Map<String,String> metrics = new HashMap<>();

        Map<String,Integer> integerMetrics = getIntegerMetrics(virtualMachineMetrics);
        for (String key : integerMetrics.keySet()) {
            metrics.put(key, String.valueOf(integerMetrics.get(key)));
        }

        Map<String,Long> longMetrics = getLongMetrics(virtualMachineMetrics);
        for (String key : longMetrics.keySet()) {
            metrics.put(key, String.valueOf(longMetrics.get(key)));
        }

        Map<String,Double> doubleMetrics = getDoubleMetrics(virtualMachineMetrics);
        for (String key : doubleMetrics.keySet()) {
            metrics.put(key, String.valueOf(doubleMetrics.get(key)));
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

    // append the process group ID if necessary
    private String appendPgId(String name, ProcessGroupStatus status, boolean appendPgId) {
        if(appendPgId) {
            return name + MetricNames.METRIC_NAME_SEPARATOR + status.getId();
        } else {
            return name;
        }
    }

    private Map<String,Double> getDoubleMetrics(VirtualMachineMetrics virtualMachineMetrics) {
        final Map<String,Double> metrics = new HashMap<>();
        metrics.put(MetricNames.JVM_HEAP_USED, virtualMachineMetrics.heapUsed());
        metrics.put(MetricNames.JVM_HEAP_USAGE, virtualMachineMetrics.heapUsage());
        metrics.put(MetricNames.JVM_NON_HEAP_USAGE, virtualMachineMetrics.nonHeapUsage());
        metrics.put(MetricNames.JVM_FILE_DESCRIPTOR_USAGE, virtualMachineMetrics.fileDescriptorUsage());
        return metrics;
    }

    private Map<String,Long> getLongMetrics(VirtualMachineMetrics virtualMachineMetrics) {
        final Map<String,Long> metrics = new HashMap<>();
        metrics.put(MetricNames.JVM_UPTIME, virtualMachineMetrics.uptime());

        for (Map.Entry<String,VirtualMachineMetrics.GarbageCollectorStats> entry : virtualMachineMetrics.garbageCollectors().entrySet()) {
            final String gcName = entry.getKey().replace(" ", "");
            final long runs = entry.getValue().getRuns();
            final long timeMS = entry.getValue().getTime(TimeUnit.MILLISECONDS);
            metrics.put(MetricNames.JVM_GC_RUNS + "." + gcName, runs);
            metrics.put(MetricNames.JVM_GC_TIME + "." + gcName, timeMS);
        }

        return metrics;
    }

    private Map<String,Integer> getIntegerMetrics(VirtualMachineMetrics virtualMachineMetrics) {
        final Map<String,Integer> metrics = new HashMap<>();
        metrics.put(MetricNames.JVM_DAEMON_THREAD_COUNT, virtualMachineMetrics.daemonThreadCount());
        metrics.put(MetricNames.JVM_THREAD_COUNT, virtualMachineMetrics.threadCount());

        for (Map.Entry<Thread.State,Double> entry : virtualMachineMetrics.threadStatePercentages().entrySet()) {
            final int normalizedValue = (int) (100 * (entry.getValue() == null ? 0 : entry.getValue()));
            switch(entry.getKey()) {
                case BLOCKED:
                    metrics.put(MetricNames.JVM_THREAD_STATES_BLOCKED, normalizedValue);
                    break;
                case RUNNABLE:
                    metrics.put(MetricNames.JVM_THREAD_STATES_RUNNABLE, normalizedValue);
                    break;
                case TERMINATED:
                    metrics.put(MetricNames.JVM_THREAD_STATES_TERMINATED, normalizedValue);
                    break;
                case TIMED_WAITING:
                    metrics.put(MetricNames.JVM_THREAD_STATES_TIMED_WAITING, normalizedValue);
                    break;
                default:
                    break;
            }
        }

        return metrics;
    }

    public JsonObject getMetrics(JsonBuilderFactory factory, ProcessGroupStatus status, VirtualMachineMetrics virtualMachineMetrics,
            String applicationId, String id, String hostname, long currentTimeMillis, int availableProcessors, double systemLoad) {
        JsonObjectBuilder objectBuilder = factory.createObjectBuilder()
                .add(MetricFields.APP_ID, applicationId)
                .add(MetricFields.HOSTNAME, hostname)
                .add(MetricFields.INSTANCE_ID, status.getId())
                .add(MetricFields.TIMESTAMP, currentTimeMillis);

        objectBuilder
        .add(MetricNames.CORES, availableProcessors)
        .add(MetricNames.LOAD1MN, systemLoad);

        Map<String,Integer> integerMetrics = getIntegerMetrics(virtualMachineMetrics);
        for (String key : integerMetrics.keySet()) {
            objectBuilder.add(key.replaceAll("\\.", ""), integerMetrics.get(key));
        }

        Map<String,Long> longMetrics = getLongMetrics(virtualMachineMetrics);
        for (String key : longMetrics.keySet()) {
            objectBuilder.add(key.replaceAll("\\.", ""), longMetrics.get(key));
        }

        Map<String,Double> doubleMetrics = getDoubleMetrics(virtualMachineMetrics);
        for (String key : doubleMetrics.keySet()) {
            objectBuilder.add(key.replaceAll("\\.", ""), doubleMetrics.get(key));
        }

        Map<String,Long> longPgMetrics = getLongMetrics(status, false);
        for (String key : longPgMetrics.keySet()) {
            objectBuilder.add(key, longPgMetrics.get(key));
        }

        Map<String,Integer> integerPgMetrics = getIntegerMetrics(status, false);
        for (String key : integerPgMetrics.keySet()) {
            objectBuilder.add(key, integerPgMetrics.get(key));
        }

        return objectBuilder.build();
    }

}
