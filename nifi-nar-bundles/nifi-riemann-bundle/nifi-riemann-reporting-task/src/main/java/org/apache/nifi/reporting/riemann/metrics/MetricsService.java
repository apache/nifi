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
package org.apache.nifi.reporting.riemann.metrics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.riemann.attributes.AttributeNames;

import com.aphyr.riemann.Proto;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yammer.metrics.core.VirtualMachineMetrics;

public class MetricsService {
    public final static Joiner SEP = Joiner.on('.');
    private final String servicePrefix;
    private final Collection<String> serviceTags;
    private final String hostName;
    private final Map<String, String> additionalAttributes;

    public MetricsService(String servicePrefix, String hostName, String[] serviceTags, Map<String, String> additionalAttributes) {
        this.servicePrefix = servicePrefix;
        this.hostName = hostName;
        this.serviceTags = new ArrayList<>(serviceTags.length);
        Collections.addAll(this.serviceTags, serviceTags);
        this.additionalAttributes = additionalAttributes;
    }

    public String getServicePrefix() {
        return servicePrefix;
    }

    /**
     * Create a list of Riemann events using the provided JVM virtual machine metrics
     *
     * @param virtualMachineMetrics
     *            JVM virtual machine metrics
     * @return List of Riemann events
     */
    public List<Proto.Event> createJVMEvents(VirtualMachineMetrics virtualMachineMetrics) {
        List<Proto.Event> events = Lists.newArrayListWithCapacity(MetricNames.JVM_METRICS.size());
        Proto.Event.Builder eventBuilder = Proto.Event.newBuilder();
        eventBuilder = addAdditionalAttributes(eventBuilder);
        eventBuilder.addAllTags(serviceTags);
        eventBuilder.setState("ok");
        eventBuilder.setHost(hostName);

        // JVM Uptime
        eventBuilder.setService(SEP.join(servicePrefix, MetricNames.JVM_UPTIME));
        eventBuilder.setMetricSint64(virtualMachineMetrics.uptime());
        events.add(eventBuilder.build());

        // JVM Heap Used
        eventBuilder.setService(SEP.join(servicePrefix, MetricNames.JVM_HEAP_USED));
        eventBuilder.setMetricD(virtualMachineMetrics.heapUsed());
        events.add(eventBuilder.build());

        // JVM Heap Usage
        eventBuilder.setService(SEP.join(servicePrefix, MetricNames.JVM_HEAP_USAGE));
        eventBuilder.setMetricD(virtualMachineMetrics.heapUsage());
        events.add(eventBuilder.build());

        // JVM Non-Heap Usage
        eventBuilder.setService(SEP.join(servicePrefix, MetricNames.JVM_NON_HEAP_USAGE));
        eventBuilder.setMetricD(virtualMachineMetrics.nonHeapUsage());
        events.add(eventBuilder.build());

        // JVM Thread Count
        eventBuilder.setService(SEP.join(servicePrefix, MetricNames.JVM_THREAD_COUNT));
        eventBuilder.setMetricSint64(virtualMachineMetrics.threadCount());
        events.add(eventBuilder.build());

        // JVM Daemon Thread Count
        eventBuilder.setService(SEP.join(servicePrefix, MetricNames.JVM_DAEMON_THREAD_COUNT));
        eventBuilder.setMetricSint64(virtualMachineMetrics.daemonThreadCount());
        events.add(eventBuilder.build());

        // JVM File Descriptor Usage
        eventBuilder.setService(SEP.join(servicePrefix, MetricNames.JVM_FILE_DESCRIPTOR_USAGE));
        eventBuilder.setMetricD(virtualMachineMetrics.fileDescriptorUsage());
        events.add(eventBuilder.build());

        for (Map.Entry<Thread.State, Double> entry : virtualMachineMetrics.threadStatePercentages().entrySet()) {
            final int normalizedValue = (int) (100 * (entry.getValue() == null ? 0 : entry.getValue()));
            switch (entry.getKey()) {
            case BLOCKED:
                // JVM Thread States Blocked
                eventBuilder.setService(SEP.join(servicePrefix, MetricNames.JVM_THREAD_STATES_BLOCKED));
                eventBuilder.setMetricSint64(normalizedValue);
                events.add(eventBuilder.build());
                break;
            case RUNNABLE:
                // JVM Thread States Runnable
                eventBuilder.setService(SEP.join(servicePrefix, MetricNames.JVM_THREAD_STATES_RUNNABLE));
                eventBuilder.setMetricSint64(normalizedValue);
                events.add(eventBuilder.build());
                break;
            case TERMINATED:
                // JVM Thread States Terminated
                eventBuilder.setService(SEP.join(servicePrefix, MetricNames.JVM_THREAD_STATES_TERMINATED));
                eventBuilder.setMetricSint64(normalizedValue);
                events.add(eventBuilder.build());
                break;
            case TIMED_WAITING:
                // JVM Thread States Timed Waiting
                eventBuilder.setService(SEP.join(servicePrefix, MetricNames.JVM_THREAD_STATES_TIMED_WAITING));
                eventBuilder.setMetricSint64(normalizedValue);
                events.add(eventBuilder.build());
                break;
            default:
                break;
            }
        }

        for (Map.Entry<String, VirtualMachineMetrics.GarbageCollectorStats> entry : virtualMachineMetrics.garbageCollectors().entrySet()) {
            final long runs = entry.getValue().getRuns();
            final long timeMS = entry.getValue().getTime(TimeUnit.MILLISECONDS);
            // JVM Garbage Collection Run Count
            eventBuilder.setService(SEP.join(servicePrefix, MetricNames.JVM_GC_RUNS));
            eventBuilder.setMetricSint64(runs);
            events.add(eventBuilder.build());

            // JVM Garbage Collection Time Spent (ms)
            eventBuilder.setService(SEP.join(servicePrefix, MetricNames.JVM_GC_TIME));
            eventBuilder.setMetricSint64(timeMS);
            events.add(eventBuilder.build());
        }

        return events;
    }

    /**
     * Create a list of Riemann events for all individual processors within the NiFi data flow
     *
     * @param statuses
     *            Collection of processor statuses
     * @return List of Riemann events
     */
    public List<Proto.Event> createProcessorEvents(Collection<ProcessorStatus> statuses) {
        List<Proto.Event> events = Lists.newArrayListWithCapacity(MetricNames.PROCESSOR_METRICS.size());
        Proto.Event.Builder eventBuilder = Proto.Event.newBuilder();
        eventBuilder = addAdditionalAttributes(eventBuilder);
        Proto.Attribute.Builder attributeBuilder = Proto.Attribute.newBuilder();
        eventBuilder.addAllTags(serviceTags);
        eventBuilder.setState("ok");
        eventBuilder.setHost(hostName);

        String prefix;
        for (ProcessorStatus status : statuses) {
            eventBuilder.addAttributes(attributeBuilder.setKey(AttributeNames.PROCESSOR_NAME).setValue(status.getName()));
            eventBuilder.addAttributes(attributeBuilder.setKey(AttributeNames.PROCESSOR_ID).setValue(status.getId()));
            eventBuilder.addAttributes(attributeBuilder.setKey(AttributeNames.PROCESSOR_GROUP_ID).setValue(status.getGroupId()));
            eventBuilder.addAttributes(attributeBuilder.setKey(AttributeNames.PROCESSOR_TYPE).setValue(status.getType()));
            eventBuilder.addAttributes(attributeBuilder.setKey(AttributeNames.PROCESSOR_STATUS).setValue(status.getRunStatus().name()));

            prefix = SEP.join(servicePrefix, status.getType(), status.getId());

            // Flow Files Received
            eventBuilder.setService(SEP.join(prefix, MetricNames.FLOW_FILES_RECEIVED, MetricNames.SERVICE_SUFFIX));
            eventBuilder.setMetricSint64(status.getInputCount());
            events.add(eventBuilder.build());

            // Flow Files Sent
            eventBuilder.setService(SEP.join(prefix, MetricNames.FLOW_FILES_SENT, MetricNames.SERVICE_SUFFIX));
            eventBuilder.setMetricSint64(status.getOutputCount());
            events.add(eventBuilder.build());

            // Bytes Received
            eventBuilder.setService(SEP.join(prefix, MetricNames.BYTES_RECEIVED, MetricNames.SERVICE_SUFFIX));
            eventBuilder.setMetricSint64(status.getBytesReceived());
            events.add(eventBuilder.build());

            // Bytes Sent
            eventBuilder.setService(SEP.join(prefix, MetricNames.BYTES_SENT, MetricNames.SERVICE_SUFFIX));
            eventBuilder.setMetricSint64(status.getBytesSent());
            events.add(eventBuilder.build());

            // Bytes Queued
            eventBuilder.setService(SEP.join(prefix, MetricNames.BYTES_QUEUED, MetricNames.SERVICE_SUFFIX));
            eventBuilder.setMetricSint64(status.getBytesSent());
            events.add(eventBuilder.build());

            // Bytes Read
            eventBuilder.setService(SEP.join(prefix, MetricNames.BYTES_READ, MetricNames.SERVICE_SUFFIX));
            eventBuilder.setMetricSint64(status.getBytesRead());
            events.add(eventBuilder.build());

            // Bytes Written
            eventBuilder.setService(SEP.join(prefix, MetricNames.BYTES_WRITTEN, MetricNames.SERVICE_SUFFIX));
            eventBuilder.setMetricSint64(status.getBytesWritten());
            events.add(eventBuilder.build());

            // Active Threads
            eventBuilder.setService(SEP.join(prefix, MetricNames.ACTIVE_THREADS, MetricNames.SERVICE_SUFFIX));
            eventBuilder.setMetricSint64(status.getActiveThreadCount());
            events.add(eventBuilder.build());

            // Average Lineage Duration
            eventBuilder.setService(SEP.join(prefix, MetricNames.AVERAGE_LINEAGE_DURATION, MetricNames.SERVICE_SUFFIX));
            eventBuilder.setMetricSint64(status.getAverageLineageDuration(TimeUnit.MILLISECONDS));
            events.add(eventBuilder.build());

            // Flow Files Removed
            eventBuilder.setService(SEP.join(prefix, MetricNames.FLOW_FILES_REMOVED, MetricNames.SERVICE_SUFFIX));
            eventBuilder.setMetricSint64(status.getFlowFilesRemoved());
            events.add(eventBuilder.build());

            // Input Bytes
            eventBuilder.setService(SEP.join(prefix, MetricNames.INPUT_BYTES, MetricNames.SERVICE_SUFFIX));
            eventBuilder.setMetricSint64(status.getInputBytes());
            events.add(eventBuilder.build());

            // Output Bytes
            eventBuilder.setService(SEP.join(prefix, MetricNames.OUTPUT_BYTES, MetricNames.SERVICE_SUFFIX));
            eventBuilder.setMetricSint64(status.getOutputBytes());
            events.add(eventBuilder.build());
        }

        return events;
    }

    /**
     * Create a list of Riemann events for overall aggregated metrics in the NiFi data flow
     * 
     * @param status
     *            Processor group status
     * @return List of Riemann events
     */
    public List<Proto.Event> createProcessGroupEvents(ProcessGroupStatus status) {
        List<Proto.Event> events = Lists.newArrayListWithCapacity(MetricNames.PROCESS_GROUP_METRICS.size());
        Proto.Event.Builder eventBuilder = Proto.Event.newBuilder();
        eventBuilder = addAdditionalAttributes(eventBuilder);
        eventBuilder.addAllTags(serviceTags);
        eventBuilder.setState("ok");
        eventBuilder.setHost(hostName);

        String prefix = SEP.join(servicePrefix, status.getName());

        // Flow Files Received
        eventBuilder.setService(SEP.join(prefix, MetricNames.FLOW_FILES_RECEIVED, MetricNames.SERVICE_SUFFIX));
        eventBuilder.setMetricSint64(status.getFlowFilesReceived());
        events.add(eventBuilder.build());

        // Flow Files Sent
        eventBuilder.setService(SEP.join(prefix, MetricNames.FLOW_FILES_SENT, MetricNames.SERVICE_SUFFIX));
        eventBuilder.setMetricSint64(status.getFlowFilesSent());
        events.add(eventBuilder.build());

        // Flow Files Transferred
        eventBuilder.setService(SEP.join(prefix, MetricNames.FLOW_FILES_TRANSFERRED, MetricNames.SERVICE_SUFFIX));
        eventBuilder.setMetricSint64(status.getFlowFilesTransferred());
        events.add(eventBuilder.build());

        // Flow Files Queued
        eventBuilder.setService(SEP.join(prefix, MetricNames.FLOW_FILES_QUEUED, MetricNames.SERVICE_SUFFIX));
        eventBuilder.setMetricSint64(status.getQueuedCount());
        events.add(eventBuilder.build());

        // Bytes Received
        eventBuilder.setService(SEP.join(prefix, MetricNames.BYTES_RECEIVED, MetricNames.SERVICE_SUFFIX));
        eventBuilder.setMetricSint64(status.getBytesReceived());
        events.add(eventBuilder.build());

        // Bytes Sent
        eventBuilder.setService(SEP.join(prefix, MetricNames.BYTES_SENT, MetricNames.SERVICE_SUFFIX));
        eventBuilder.setMetricSint64(status.getBytesSent());
        events.add(eventBuilder.build());

        // Bytes Queued
        eventBuilder.setService(SEP.join(prefix, MetricNames.BYTES_QUEUED, MetricNames.SERVICE_SUFFIX));
        eventBuilder.setMetricSint64(status.getQueuedContentSize());
        events.add(eventBuilder.build());

        // Bytes Read
        eventBuilder.setService(SEP.join(prefix, MetricNames.BYTES_READ, MetricNames.SERVICE_SUFFIX));
        eventBuilder.setMetricSint64(status.getBytesRead());
        events.add(eventBuilder.build());

        // Bytes Written
        eventBuilder.setService(SEP.join(prefix, MetricNames.BYTES_WRITTEN, MetricNames.SERVICE_SUFFIX));
        eventBuilder.setMetricSint64(status.getBytesWritten());
        events.add(eventBuilder.build());

        // Bytes Transferred
        eventBuilder.setService(SEP.join(prefix, MetricNames.BYTES_TRANSFERRED, MetricNames.SERVICE_SUFFIX));
        eventBuilder.setMetricSint64(status.getBytesTransferred());
        events.add(eventBuilder.build());

        // Active Threads
        eventBuilder.setService(SEP.join(prefix, MetricNames.ACTIVE_THREADS, MetricNames.SERVICE_SUFFIX));
        eventBuilder.setMetricSint64(status.getActiveThreadCount());
        events.add(eventBuilder.build());

        // Total Processing Nanos
        eventBuilder.setService(SEP.join(prefix, MetricNames.TOTAL_TASK_DURATION, MetricNames.SERVICE_SUFFIX));
        eventBuilder.setMetricSint64(calculateProcessingNanos(status));
        events.add(eventBuilder.build());

        // Input Content Size
        eventBuilder.setService(SEP.join(prefix, MetricNames.INPUT_CONTENT_SIZE, MetricNames.SERVICE_SUFFIX));
        eventBuilder.setMetricSint64(status.getInputContentSize());
        events.add(eventBuilder.build());

        // Output Content Size
        eventBuilder.setService(SEP.join(prefix, MetricNames.OUTPUT_CONTENT_SIZE, MetricNames.SERVICE_SUFFIX));
        eventBuilder.setMetricSint64(status.getOutputContentSize());
        events.add(eventBuilder.build());

        return events;
    }

    /**
     * Create list of Riemann events from a list of bulletin messages
     * 
     * @param bulletins
     *            List of bulletin board messages
     * @return List of Riemann events
     */
    public List<Proto.Event> createBulletinEvents(List<Bulletin> bulletins) {
        List<Proto.Event> events = Lists.newArrayListWithCapacity(bulletins.size());
        Proto.Event.Builder eventBuilder = Proto.Event.newBuilder();
        eventBuilder = addAdditionalAttributes(eventBuilder);
        eventBuilder.addAllTags(serviceTags);
        Proto.Attribute.Builder attributeBuilder = Proto.Attribute.newBuilder();

        String prefix = SEP.join(servicePrefix, "bulletin");
        for (Bulletin bulletin : bulletins) {
            eventBuilder.setService(SEP.join(prefix, bulletin.getSourceType().name(), bulletin.getSourceId()));
            eventBuilder.setState(bulletin.getLevel());
            eventBuilder.setHost(hostName);
            eventBuilder.setDescription(bulletin.getLevel() + " : " + bulletin.getMessage());
            eventBuilder.setTime(bulletin.getTimestamp().getTime());
            eventBuilder.addAttributes(attributeBuilder.setKey(AttributeNames.PROCESSOR_ID).setValue(bulletin.getSourceId()).build());
            eventBuilder.addAttributes(attributeBuilder.setKey(AttributeNames.PROCESSOR_GROUP_ID).setValue(bulletin.getGroupId()).build());
            eventBuilder.addAttributes(attributeBuilder.setKey(AttributeNames.CATEGORY).setValue(bulletin.getCategory()).build());
            events.add(eventBuilder.build());
        }
        return events;
    }

    private Proto.Event.Builder addAdditionalAttributes(Proto.Event.Builder eventBuilder) {
        Proto.Attribute.Builder attributeBuilder = Proto.Attribute.newBuilder();
        for (Map.Entry<String, String> additionalAttribute : additionalAttributes.entrySet()) {
            attributeBuilder.setKey(additionalAttribute.getKey());
            attributeBuilder.setValue(additionalAttribute.getValue());
            eventBuilder.addAttributes(attributeBuilder.build());
        }
        return eventBuilder;
    }

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
