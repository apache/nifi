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

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Tags({"stats", "log"})
@CapabilityDescription("Logs the 5-minute stats that are shown in the NiFi Summary Page for Processors and Connections, as"
        + " well optionally logging the deltas between the previous iteration and the current iteration. Processors' stats are"
        + " logged using the org.apache.nifi.controller.ControllerStatusReportingTask.Processors logger, while Connections' stats are"
        + " logged using the org.apache.nifi.controller.ControllerStatusReportingTask.Connections logger. These can be configured"
        + " in the NiFi logging configuration to log to different files, if desired.")
public class ControllerStatusReportingTask extends AbstractReportingTask {

    static final AllowableValue FIVE_MINUTE_GRANULARITY = new AllowableValue("five-minutes", "Five Minutes", "The stats that are reported will reflect up to the last 5 minutes' worth of processing," +
        " which will coincide with the stats that are shown in the UI.");
    static final AllowableValue ONE_SECOND_GRANULARITY = new AllowableValue("one-second", "One Second", "The stats that are reported will be an average of the value per second, gathered over the " +
        "last 5 minutes. This is essentially obtained by dividing the stats that are shown in the UI by 300 (300 seconds in 5 minutes), with the exception of when NiFi has been running for less " +
        "than 5 minutes. In that case, the stats will be divided by the amount of time NiFi has been running.");

    public static final PropertyDescriptor SHOW_DELTAS = new Builder()
            .name("Show Deltas")
            .description("Specifies whether or not to show the difference in values between the current status and the previous status")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();
    static final PropertyDescriptor REPORTING_GRANULARITY = new Builder()
        .name("reporting-granularity")
        .displayName("Reporting Granularity")
        .description("When reporting information, specifies the granularity of the metrics to report")
        .allowableValues(FIVE_MINUTE_GRANULARITY, ONE_SECOND_GRANULARITY)
        .defaultValue(FIVE_MINUTE_GRANULARITY.getValue())
        .build();

    private static final Logger processorLogger = LoggerFactory.getLogger(ControllerStatusReportingTask.class.getName() + ".Processors");
    private static final Logger connectionLogger = LoggerFactory.getLogger(ControllerStatusReportingTask.class.getName() + ".Connections");
    private static final Logger counterLogger = LoggerFactory.getLogger(ControllerStatusReportingTask.class.getName() + ".Counters");

    private static final String PROCESSOR_LINE_FORMAT_NO_DELTA = "| %1$-30.30s | %2$-36.36s | %3$-24.24s | %4$10.10s | %5$19.19s | %6$19.19s | %7$12.12s | %8$13.13s | %9$5.5s | %10$12.12s |\n";
    private static final String PROCESSOR_LINE_FORMAT_WITH_DELTA = "| %1$-30.30s | %2$-36.36s | %3$-24.24s | %4$10.10s | %5$43.43s | %6$43.43s | %7$28.28s | %8$30.30s | %9$14.14s | %10$28.28s |\n";

    private static final String CONNECTION_LINE_FORMAT_NO_DELTA = "| %1$-36.36s | %2$-30.30s | %3$-36.36s | %4$-30.30s | %5$19.19s | %6$19.19s | %7$19.19s |\n";
    private static final String CONNECTION_LINE_FORMAT_WITH_DELTA = "| %1$-36.36s | %2$-30.30s | %3$-36.36s | %4$-30.30s | %5$43.43s | %6$43.43s | %7$43.43s |\n";

    private static final String COUNTER_LINE_FORMAT = "| %1$-36.36s | %2$-36.36s | %3$-36.36s |\n";

    private volatile String processorLineFormat;
    private volatile String processorHeader;
    private volatile String processorBorderLine;

    private volatile String connectionLineFormat;
    private volatile String connectionHeader;
    private volatile String connectionBorderLine;

    private volatile String counterHeader;
    private volatile String counterBorderLine;

    private volatile Map<String, ProcessorStatus> lastProcessorStatus = new HashMap<>();
    private volatile Map<String, ConnectionStatus> lastConnectionStatus = new HashMap<>();
    private volatile Map<String, Long> lastCounterValues = new HashMap<>();

    private final long startTimestamp = System.currentTimeMillis();

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(SHOW_DELTAS);
        descriptors.add(REPORTING_GRANULARITY);
        return descriptors;
    }

    @OnScheduled
    public void onConfigured(final ConfigurationContext context) {
        final boolean showDeltas = context.getProperty(SHOW_DELTAS).asBoolean();

        connectionLineFormat = showDeltas ? CONNECTION_LINE_FORMAT_WITH_DELTA : CONNECTION_LINE_FORMAT_NO_DELTA;
        connectionHeader = String.format(connectionLineFormat, "Connection ID", "Source", "Connection Name", "Destination", "Flow Files In", "Flow Files Out", "FlowFiles Queued");
        connectionBorderLine = createLine(connectionHeader);

        processorLineFormat = showDeltas ? PROCESSOR_LINE_FORMAT_WITH_DELTA : PROCESSOR_LINE_FORMAT_NO_DELTA;
        processorHeader = String.format(processorLineFormat, "Processor Name", "Processor ID", "Processor Type", "Run Status", "Flow Files In",
                "Flow Files Out", "Bytes Read", "Bytes Written", "Tasks", "Proc Time");
        processorBorderLine = createLine(processorHeader);

        counterHeader = String.format(COUNTER_LINE_FORMAT, "Counter Context", "Counter Name", "Counter Value");
        counterBorderLine = createLine(counterHeader);
    }

    private String createLine(final String valueToUnderscore) {
        final StringBuilder processorBorderBuilder = new StringBuilder(valueToUnderscore.length());
        for (int i = 0; i < valueToUnderscore.length(); i++) {
            processorBorderBuilder.append('-');
        }
        return processorBorderBuilder.toString();
    }

    @Override
    public void onTrigger(final ReportingContext context) {
        final ProcessGroupStatus controllerStatus = context.getEventAccess().getControllerStatus();
        final boolean showDeltas = context.getProperty(SHOW_DELTAS).asBoolean();

        final String reportingGranularity = context.getProperty(REPORTING_GRANULARITY).getValue();
        final long divisor;
        if (ONE_SECOND_GRANULARITY.getValue().equalsIgnoreCase(reportingGranularity)) {
            final long timestamp = System.currentTimeMillis();
            final long secondsRunning = TimeUnit.MILLISECONDS.toSeconds(timestamp - startTimestamp);
            divisor = Math.min(secondsRunning, 300);
        } else {
            divisor = 1;
        }

        printProcessorStatuses(controllerStatus, showDeltas, divisor);
        printConnectionStatuses(controllerStatus, showDeltas, divisor);
        printCounters(controllerStatus, showDeltas, divisor);
    }

    private void printProcessorStatuses(final ProcessGroupStatus controllerStatus, final boolean showDeltas, final long divisor) {
        final StringBuilder builder = new StringBuilder();

        builder.append("Processor Statuses:\n");
        builder.append(processorBorderLine);
        builder.append("\n");
        builder.append(processorHeader);
        builder.append(processorBorderLine);
        builder.append("\n");

        printProcessorStatus(controllerStatus, builder, showDeltas, divisor);

        builder.append(processorBorderLine);
        processorLogger.info(builder.toString());
    }

    private void printConnectionStatuses(final ProcessGroupStatus controllerStatus, final boolean showDeltas, final long divisor) {
        final StringBuilder builder = new StringBuilder();

        builder.append("Connection Statuses:\n");
        builder.append(connectionBorderLine);
        builder.append("\n");
        builder.append(connectionHeader);
        builder.append(connectionBorderLine);
        builder.append("\n");

        printConnectionStatus(controllerStatus, builder, showDeltas, divisor);

        builder.append(connectionBorderLine);
        connectionLogger.info(builder.toString());
    }

    private void printCounters(final ProcessGroupStatus controllerStatus, final boolean showDeltas, final long divisor) {
        final StringBuilder builder = new StringBuilder();

        builder.append("Counters:\n");
        builder.append(counterBorderLine);
        builder.append("\n");
        builder.append(counterHeader);
        builder.append(counterBorderLine);
        builder.append("\n");

        printCounterStatus(controllerStatus, builder, showDeltas, divisor);

        builder.append(counterBorderLine);
        counterLogger.info(builder.toString());
    }

    private void printCounterStatus(final ProcessGroupStatus status, final StringBuilder builder, final boolean showDeltas, final long divisor) {
        final Collection<ProcessorStatus> processorStatuses = status.getProcessorStatus();
        for (final ProcessorStatus processorStatus : processorStatuses) {
            final Map<String, Long> counters = processorStatus.getCounters();

            for (final Map.Entry<String, Long> entry : counters.entrySet()) {
                final String counterName = entry.getKey();
                final Long counterValue = entry.getValue() / divisor;

                final String counterId = processorStatus.getId() + "_" + counterName;
                final Long lastValue = lastCounterValues.getOrDefault(counterId, 0L);

                lastCounterValues.put(counterId, counterValue);

                if (showDeltas) {
                    final String diff = toDiff(lastValue, counterValue);

                    builder.append(String.format(COUNTER_LINE_FORMAT,
                        processorStatus.getName() + "(" + processorStatus.getId() + ")",
                        counterName,
                        counterValue + diff));
                } else {
                    builder.append(String.format(COUNTER_LINE_FORMAT,
                        processorStatus.getName() + "(" + processorStatus.getId() + ")",
                        counterName,
                        counterValue));
                }
            }
        }

        for (final ProcessGroupStatus childGroupStatus : status.getProcessGroupStatus()) {
            printCounterStatus(childGroupStatus, builder, showDeltas, divisor);
        }
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

    // Recursively prints the status of all connections in this group.
    private void printConnectionStatus(final ProcessGroupStatus groupStatus, final StringBuilder builder, final boolean showDeltas, final long divisor) {
        final List<ConnectionStatus> connectionStatuses = new ArrayList<>();
        populateConnectionStatuses(groupStatus, connectionStatuses);
        connectionStatuses.sort(new Comparator<ConnectionStatus>() {
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
            final long inputCount = connectionStatus.getInputCount() / divisor;
            final long outputCount = connectionStatus.getOutputCount() / divisor;
            final long queuedCount = connectionStatus.getQueuedCount() / divisor;

            final long inputBytes = connectionStatus.getInputBytes() / divisor;
            final long outputBytes = connectionStatus.getOutputBytes() / divisor;
            final long queuedBytes = connectionStatus.getQueuedBytes() / divisor;

            final String input = inputCount + " / " + FormatUtils.formatDataSize(inputBytes);
            final String output = outputCount + " / " + FormatUtils.formatDataSize(outputBytes);
            final String queued = queuedCount + " / " + FormatUtils.formatDataSize(queuedBytes);

            if (showDeltas) {
                final ConnectionStatus lastStatus = lastConnectionStatus.get(connectionStatus.getId());
                final long lastInputCount = lastStatus == null ? 0L : lastStatus.getInputCount() / divisor;
                final long lastOutputCount = lastStatus == null ? 0L :lastStatus.getOutputCount() / divisor;
                final long lastQueuedCount = lastStatus == null ? 0L :lastStatus.getQueuedCount() / divisor;

                final long lastInputBytes = lastStatus == null ? 0L :lastStatus.getInputBytes() / divisor;
                final long lastOutputBytes = lastStatus == null ? 0L :lastStatus.getOutputBytes() / divisor;
                final long lastQueuedBytes = lastStatus == null ? 0L :lastStatus.getQueuedBytes() / divisor;

                final String inputDiff = toDiff(lastInputCount, lastInputBytes, inputCount, inputBytes);
                final String outputDiff = toDiff(lastOutputCount, lastOutputBytes, outputCount, outputBytes);
                final String queuedDiff = toDiff(lastQueuedCount, lastQueuedBytes, queuedCount, queuedBytes);

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

    // Recursively the status of all processors in this group.
    private void printProcessorStatus(final ProcessGroupStatus groupStatus, final StringBuilder builder, final boolean showDeltas, final long divisor) {
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
            final long inputCount = processorStatus.getInputCount() / divisor;
            final long inputBytes = processorStatus.getInputBytes() / divisor;
            final long outputCount = processorStatus.getOutputCount() / divisor;
            final long outputBytes = processorStatus.getOutputBytes() / divisor;
            final long bytesRead = processorStatus.getBytesRead() / divisor;
            final long bytesWritten = processorStatus.getBytesWritten() / divisor;
            final long invocationCount = processorStatus.getInvocations() / divisor;

            final String input = inputCount + " / " + FormatUtils.formatDataSize(inputBytes);
            final String output = outputCount + " / " + FormatUtils.formatDataSize(outputBytes);
            final String read = FormatUtils.formatDataSize(bytesRead);
            final String written = FormatUtils.formatDataSize(bytesWritten);
            final String invocations = String.valueOf(invocationCount);

            final long nanos = processorStatus.getProcessingNanos() / divisor;
            final String procTime = FormatUtils.formatHoursMinutesSeconds(nanos, TimeUnit.NANOSECONDS);

            String runStatus = "";
            if (processorStatus.getRunStatus() != null) {
                runStatus = processorStatus.getRunStatus().toString();
            }

            if (showDeltas) {
                final ProcessorStatus lastStatus = lastProcessorStatus.get(processorStatus.getId());
                final long lastInputCount = lastStatus == null ? 0L : lastStatus.getInputCount() / divisor;
                final long lastInputBytes = lastStatus == null ? 0L : lastStatus.getInputBytes() / divisor;
                final long lastOutputCount = lastStatus == null ? 0L : lastStatus.getOutputCount() / divisor;
                final long lastOutputBytes = lastStatus == null ? 0L : lastStatus.getOutputBytes() / divisor;
                final long lastBytesRead = lastStatus == null ? 0L : lastStatus.getBytesRead() / divisor;
                final long lastBytesWritten = lastStatus == null ? 0L : lastStatus.getBytesWritten() / divisor;
                final long lastInvocationCount = lastStatus == null ? 0L : lastStatus.getInvocations() / divisor;
                final long lastProcessingNanos = lastStatus == null ? 0L : lastStatus.getProcessingNanos() / divisor;

                final String inputDiff = toDiff(lastInputCount, lastInputBytes, inputCount, inputBytes);
                final String outputDiff = toDiff(lastOutputCount, lastOutputBytes, outputCount, outputBytes);
                final String readDiff = toDiff(lastBytesRead, bytesRead, true, false);
                final String writtenDiff = toDiff(lastBytesWritten, bytesWritten, true, false);
                final String invocationsDiff = toDiff(lastInvocationCount, invocationCount);
                final String procTimeDiff = toDiff(lastProcessingNanos, nanos, false, true);

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
