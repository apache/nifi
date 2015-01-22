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
package org.apache.nifi.controller;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.annotation.OnConfigured;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tags({"stats", "log"})
@CapabilityDescription("Logs the 5-minute stats that are shown in the NiFi Summary Page for Processors and Connections, as"
        + " well optionally logging the deltas between the previous iteration and the current iteration. Processors' stats are"
        + " logged using the org.apache.nifi.controller.ControllerStatusReportingTask.Processors logger, while Connections' stats are"
        + " logged using the org.apache.nifi.controller.ControllerStatusReportingTask.Connections logger. These can be configured"
        + " in the NiFi logging configuration to log to different files, if desired.")
public class ControllerStatusReportingTask extends AbstractReportingTask {

    public static final PropertyDescriptor SHOW_DELTAS = new PropertyDescriptor.Builder()
            .name("Show Deltas")
            .description("Specifies whether or not to show the difference in values between the current status and the previous status")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    private static final Logger processorLogger = LoggerFactory.getLogger(ControllerStatusReportingTask.class.getSimpleName() + ".Processors");
    private static final Logger connectionLogger = LoggerFactory.getLogger(ControllerStatusReportingTask.class.getSimpleName() + ".Connections");

    private static final String PROCESSOR_LINE_FORMAT_NO_DELTA = "| %1$-30.30s | %2$-36.36s | %3$-24.24s | %4$10.10s | %5$19.19s | %6$19.19s | %7$12.12s | %8$13.13s | %9$5.5s | %10$12.12s |\n";
    private static final String PROCESSOR_LINE_FORMAT_WITH_DELTA = "| %1$-30.30s | %2$-36.36s | %3$-24.24s | %4$10.10s | %5$43.43s | %6$43.43s | %7$28.28s | %8$30.30s | %9$14.14s | %10$28.28s |\n";

    private static final String CONNECTION_LINE_FORMAT_NO_DELTA = "| %1$-36.36s | %2$-30.30s | %3$-36.36s | %4$-30.30s | %5$19.19s | %6$19.19s | %7$19.19s |\n";
    private static final String CONNECTION_LINE_FORMAT_WITH_DELTA = "| %1$-36.36s | %2$-30.30s | %3$-36.36s | %4$-30.30s | %5$43.43s | %6$43.43s | %7$43.43s |\n";

    private volatile String processorLineFormat;
    private volatile String processorHeader;
    private volatile String processorBorderLine;

    private volatile String connectionLineFormat;
    private volatile String connectionHeader;
    private volatile String connectionBorderLine;

    private volatile Map<String, ProcessorStatus> lastProcessorStatus = new HashMap<>();
    private volatile Map<String, ConnectionStatus> lastConnectionStatus = new HashMap<>();

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(SHOW_DELTAS);
        return descriptors;
    }

    @OnConfigured
    public void onConfigured(final ConfigurationContext context) {
        connectionLineFormat = context.getProperty(SHOW_DELTAS).asBoolean() ? CONNECTION_LINE_FORMAT_WITH_DELTA : CONNECTION_LINE_FORMAT_NO_DELTA;
        connectionHeader = String.format(connectionLineFormat, "Connection ID", "Source", "Connection Name", "Destination", "Flow Files In", "Flow Files Out", "FlowFiles Queued");

        final StringBuilder connectionBorderBuilder = new StringBuilder(connectionHeader.length());
        for (int i = 0; i < connectionHeader.length(); i++) {
            connectionBorderBuilder.append('-');
        }
        connectionBorderLine = connectionBorderBuilder.toString();

        processorLineFormat = context.getProperty(SHOW_DELTAS).asBoolean() ? PROCESSOR_LINE_FORMAT_WITH_DELTA : PROCESSOR_LINE_FORMAT_NO_DELTA;
        processorHeader = String.format(processorLineFormat, "Processor Name", "Processor ID", "Processor Type", "Run Status", "Flow Files In", "Flow Files Out", "Bytes Read", "Bytes Written", "Tasks", "Proc Time");

        final StringBuilder processorBorderBuilder = new StringBuilder(processorHeader.length());
        for (int i = 0; i < processorHeader.length(); i++) {
            processorBorderBuilder.append('-');
        }
        processorBorderLine = processorBorderBuilder.toString();
    }

    @Override
    public void onTrigger(final ReportingContext context) {
        final ProcessGroupStatus controllerStatus = context.getEventAccess().getControllerStatus();
        controllerStatus.clone();

        final boolean showDeltas = context.getProperty(SHOW_DELTAS).asBoolean();

        final StringBuilder builder = new StringBuilder();

        builder.append("Processor Statuses:\n");
        builder.append(processorBorderLine);
        builder.append("\n");
        builder.append(processorHeader);
        builder.append(processorBorderLine);
        builder.append("\n");

        printProcessorStatus(controllerStatus, builder, showDeltas);

        builder.append(processorBorderLine);
        processorLogger.info(builder.toString());

        builder.setLength(0);
        builder.append("Connection Statuses:\n");
        builder.append(connectionBorderLine);
        builder.append("\n");
        builder.append(connectionHeader);
        builder.append(connectionBorderLine);
        builder.append("\n");

        printConnectionStatus(controllerStatus, builder, showDeltas);

        builder.append(connectionBorderLine);
        connectionLogger.info(builder.toString());
    }

    private void populateConnectionStatuses(final ProcessGroupStatus groupStatus, final List<ConnectionStatus> statuses) {
        statuses.addAll(groupStatus.getConnectionStatus());
        for (final ProcessGroupStatus childGroupStatus : groupStatus.getProcessGroupStatus()) {
            populateConnectionStatuses(childGroupStatus, statuses);
        }
    }

    private void populateProcessorStatuses(final ProcessGroupStatus groupStatus, final List<ProcessorStatus> statuses) {
        statuses.addAll(groupStatus.getProcessorStatus());
        for (final ProcessGroupStatus childGroupStatus : groupStatus.getProcessGroupStatus()) {
            populateProcessorStatuses(childGroupStatus, statuses);
        }
    }

    /**
     * Recursively prints the status of all connections in this group.
     *
     * @param groupStatus
     * @param group
     * @param builder
     */
    private void printConnectionStatus(final ProcessGroupStatus groupStatus, final StringBuilder builder, final boolean showDeltas) {
        final List<ConnectionStatus> connectionStatuses = new ArrayList<>();
        populateConnectionStatuses(groupStatus, connectionStatuses);
        Collections.sort(connectionStatuses, new Comparator<ConnectionStatus>() {
            @Override
            public int compare(final ConnectionStatus o1, final ConnectionStatus o2) {
                if (o1 == null && o2 == null) {
                    return 0;
                }
                if (o1 == null) {
                    return 1;
                }
                if (o2 == null) {
                    return -1;
                }

                return -Long.compare(o1.getQueuedBytes(), o2.getQueuedBytes());
            }
        });

        for (final ConnectionStatus connectionStatus : connectionStatuses) {
            final String input = connectionStatus.getInputCount() + " / " + FormatUtils.formatDataSize(connectionStatus.getInputBytes());
            final String output = connectionStatus.getOutputCount() + " / " + FormatUtils.formatDataSize(connectionStatus.getOutputBytes());
            final String queued = connectionStatus.getQueuedCount() + " / " + FormatUtils.formatDataSize(connectionStatus.getQueuedBytes());

            final String inputDiff;
            final String outputDiff;
            final String queuedDiff;

            final ConnectionStatus lastStatus = lastConnectionStatus.get(connectionStatus.getId());
            if (showDeltas && lastStatus != null) {
                inputDiff = toDiff(lastStatus.getInputCount(), lastStatus.getInputBytes(), connectionStatus.getInputCount(), connectionStatus.getInputBytes());
                outputDiff = toDiff(lastStatus.getOutputCount(), lastStatus.getOutputBytes(), connectionStatus.getOutputCount(), connectionStatus.getOutputBytes());
                queuedDiff = toDiff(lastStatus.getQueuedCount(), lastStatus.getQueuedBytes(), connectionStatus.getQueuedCount(), connectionStatus.getQueuedBytes());
            } else {
                inputDiff = toDiff(0L, 0L, connectionStatus.getInputCount(), connectionStatus.getInputBytes());
                outputDiff = toDiff(0L, 0L, connectionStatus.getOutputCount(), connectionStatus.getOutputBytes());
                queuedDiff = toDiff(0L, 0L, connectionStatus.getQueuedCount(), connectionStatus.getQueuedBytes());
            }

            if (showDeltas) {
                builder.append(String.format(connectionLineFormat,
                        connectionStatus.getId(),
                        connectionStatus.getSourceName(),
                        connectionStatus.getName(),
                        connectionStatus.getDestinationName(),
                        input + inputDiff,
                        output + outputDiff,
                        queued + queuedDiff));
            } else {
                builder.append(String.format(connectionLineFormat,
                        connectionStatus.getId(),
                        connectionStatus.getSourceName(),
                        connectionStatus.getName(),
                        connectionStatus.getDestinationName(),
                        input,
                        output,
                        queued));
            }

            lastConnectionStatus.put(connectionStatus.getId(), connectionStatus);
        }
    }

    private String toDiff(final long oldValue, final long newValue) {
        return toDiff(oldValue, newValue, false, false);
    }

    private String toDiff(final long oldValue, final long newValue, final boolean formatDataSize, final boolean formatTime) {
        if (formatDataSize && formatTime) {
            throw new IllegalArgumentException("Cannot format units as both data size and time");
        }

        final long diff = Math.abs(newValue - oldValue);

        final String formattedDiff = formatDataSize ? FormatUtils.formatDataSize(diff)
                : (formatTime ? FormatUtils.formatHoursMinutesSeconds(diff, TimeUnit.NANOSECONDS) : String.valueOf(diff));

        if (oldValue > newValue) {
            return " (-" + formattedDiff + ")";
        } else {
            return " (+" + formattedDiff + ")";
        }
    }

    private String toDiff(final long oldCount, final long oldBytes, final long newCount, final long newBytes) {
        final long countDiff = Math.abs(newCount - oldCount);
        final long bytesDiff = Math.abs(newBytes - oldBytes);

        final StringBuilder sb = new StringBuilder();
        sb.append(" (").append(oldCount > newCount ? "-" : "+").append(countDiff).append("/");
        sb.append(oldBytes > newBytes ? "-" : "+");
        sb.append(FormatUtils.formatDataSize(bytesDiff)).append(")");

        return sb.toString();
    }

    /**
     * Recursively the status of all processors in this group.
     *
     * @param groupStatus
     * @param group
     * @param builder
     */
    private void printProcessorStatus(final ProcessGroupStatus groupStatus, final StringBuilder builder, final boolean showDeltas) {
        final List<ProcessorStatus> processorStatuses = new ArrayList<>();
        populateProcessorStatuses(groupStatus, processorStatuses);
        Collections.sort(processorStatuses, new Comparator<ProcessorStatus>() {
            @Override
            public int compare(final ProcessorStatus o1, final ProcessorStatus o2) {
                if (o1 == null && o2 == null) {
                    return 0;
                }
                if (o1 == null) {
                    return 1;
                }
                if (o2 == null) {
                    return -1;
                }

                return -Long.compare(o1.getProcessingNanos(), o2.getProcessingNanos());
            }
        });

        for (final ProcessorStatus processorStatus : processorStatuses) {
            // get the stats
            final String input = processorStatus.getInputCount() + " / " + FormatUtils.formatDataSize(processorStatus.getInputBytes());
            final String output = processorStatus.getOutputCount() + " / " + FormatUtils.formatDataSize(processorStatus.getOutputBytes());
            final String read = FormatUtils.formatDataSize(processorStatus.getBytesRead());
            final String written = FormatUtils.formatDataSize(processorStatus.getBytesWritten());
            final String invocations = String.valueOf(processorStatus.getInvocations());

            final long nanos = processorStatus.getProcessingNanos();
            final String procTime = FormatUtils.formatHoursMinutesSeconds(nanos, TimeUnit.NANOSECONDS);

            String runStatus = "";
            if (processorStatus.getRunStatus() != null) {
                runStatus = processorStatus.getRunStatus().toString();
            }

            final String inputDiff;
            final String outputDiff;
            final String readDiff;
            final String writtenDiff;
            final String invocationsDiff;
            final String procTimeDiff;

            final ProcessorStatus lastStatus = lastProcessorStatus.get(processorStatus.getId());
            if (showDeltas && lastStatus != null) {
                inputDiff = toDiff(lastStatus.getInputCount(), lastStatus.getInputBytes(), processorStatus.getInputCount(), processorStatus.getInputBytes());
                outputDiff = toDiff(lastStatus.getOutputCount(), lastStatus.getOutputBytes(), processorStatus.getOutputCount(), processorStatus.getOutputBytes());
                readDiff = toDiff(lastStatus.getBytesRead(), processorStatus.getBytesRead(), true, false);
                writtenDiff = toDiff(lastStatus.getBytesWritten(), processorStatus.getBytesWritten(), true, false);
                invocationsDiff = toDiff(lastStatus.getInvocations(), processorStatus.getInvocations());
                procTimeDiff = toDiff(lastStatus.getProcessingNanos(), processorStatus.getProcessingNanos(), false, true);
            } else {
                inputDiff = toDiff(0L, 0L, processorStatus.getInputCount(), processorStatus.getInputBytes());
                outputDiff = toDiff(0L, 0L, processorStatus.getOutputCount(), processorStatus.getOutputBytes());
                readDiff = toDiff(0L, processorStatus.getBytesRead(), true, false);
                writtenDiff = toDiff(0L, processorStatus.getBytesWritten(), true, false);
                invocationsDiff = toDiff(0L, processorStatus.getInvocations());
                procTimeDiff = toDiff(0L, processorStatus.getProcessingNanos(), false, true);
            }

            if (showDeltas) {
                builder.append(String.format(processorLineFormat,
                        processorStatus.getName(),
                        processorStatus.getId(),
                        processorStatus.getType(),
                        runStatus,
                        input + inputDiff,
                        output + outputDiff,
                        read + readDiff,
                        written + writtenDiff,
                        invocations + invocationsDiff,
                        procTime + procTimeDiff));
            } else {
                builder.append(String.format(processorLineFormat,
                        processorStatus.getName(),
                        processorStatus.getId(),
                        processorStatus.getType(),
                        runStatus,
                        input,
                        output,
                        read,
                        written,
                        invocations,
                        procTime));
            }

            lastProcessorStatus.put(processorStatus.getId(), processorStatus);
        }
    }

}
