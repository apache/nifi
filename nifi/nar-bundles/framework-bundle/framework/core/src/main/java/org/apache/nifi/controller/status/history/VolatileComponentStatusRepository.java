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
package org.apache.nifi.controller.status.history;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.RemoteProcessGroupStatus;
import org.apache.nifi.controller.status.history.MetricDescriptor.Formatter;
import org.apache.nifi.util.ComponentStatusReport;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.ComponentStatusReport.ComponentType;
import org.apache.nifi.util.RingBuffer;
import org.apache.nifi.util.RingBuffer.ForEachEvaluator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VolatileComponentStatusRepository implements ComponentStatusRepository {

    public static final String NUM_DATA_POINTS_PROPERTY = "nifi.components.status.repository.buffer.size";
    public static final int DEFAULT_NUM_DATA_POINTS = 288;   // 1 day worth of 5-minute snapshots

    private final RingBuffer<Capture> captures;
    private final Logger logger = LoggerFactory.getLogger(VolatileComponentStatusRepository.class);

    private volatile long lastCaptureTime = 0L;

    private static final List<MetricDescriptor<ProcessorStatus>> PROCESSOR_METRIC_DESCRIPTORS;
    private static final List<MetricDescriptor<ConnectionStatus>> CONNECTION_METRIC_DESCRIPTORS;
    private static final List<MetricDescriptor<ProcessGroupStatus>> PROCESS_GROUP_METRIC_DESCRIPTORS;
    private static final List<MetricDescriptor<RemoteProcessGroupStatus>> REMOTE_PROCESS_GROUP_METRIC_DESCRIPTORS;

    static {
        final List<MetricDescriptor<ProcessorStatus>> procFields = new ArrayList<>();
        for (final ProcessorStatusDescriptor descriptor : ProcessorStatusDescriptor.values()) {
            procFields.add(descriptor.getDescriptor());
        }
        PROCESSOR_METRIC_DESCRIPTORS = Collections.unmodifiableList(procFields);

        final List<MetricDescriptor<ConnectionStatus>> connFields = new ArrayList<>();
        for (final ConnectionStatusDescriptor descriptor : ConnectionStatusDescriptor.values()) {
            connFields.add(descriptor.getDescriptor());
        }
        CONNECTION_METRIC_DESCRIPTORS = Collections.unmodifiableList(connFields);

        final List<MetricDescriptor<ProcessGroupStatus>> groupFields = new ArrayList<>();
        for (final ProcessGroupStatusDescriptor descriptor : ProcessGroupStatusDescriptor.values()) {
            groupFields.add(descriptor.getDescriptor());
        }
        PROCESS_GROUP_METRIC_DESCRIPTORS = Collections.unmodifiableList(groupFields);

        final List<MetricDescriptor<RemoteProcessGroupStatus>> remoteGroupFields = new ArrayList<>();
        for (final RemoteProcessGroupStatusDescriptor descriptor : RemoteProcessGroupStatusDescriptor.values()) {
            remoteGroupFields.add(descriptor.getDescriptor());
        }
        REMOTE_PROCESS_GROUP_METRIC_DESCRIPTORS = Collections.unmodifiableList(remoteGroupFields);
    }

    public VolatileComponentStatusRepository() {
        final NiFiProperties properties = NiFiProperties.getInstance();
        final int numDataPoints = properties.getIntegerProperty(NUM_DATA_POINTS_PROPERTY, DEFAULT_NUM_DATA_POINTS);

        captures = new RingBuffer<>(numDataPoints);
    }

    @Override
    public void capture(final ProcessGroupStatus rootGroupStatus) {
        capture(rootGroupStatus, new Date());
    }

    @Override
    public synchronized void capture(final ProcessGroupStatus rootGroupStatus, final Date timestamp) {
        captures.add(new Capture(timestamp, ComponentStatusReport.fromProcessGroupStatus(rootGroupStatus, ComponentType.PROCESSOR, ComponentType.CONNECTION, ComponentType.PROCESS_GROUP, ComponentType.REMOTE_PROCESS_GROUP)));
        logger.debug("Captured metrics for {}", this);
        lastCaptureTime = Math.max(lastCaptureTime, timestamp.getTime());
    }

    @Override
    public Date getLastCaptureDate() {
        return new Date(lastCaptureTime);
    }

    @Override
    public StatusHistory getProcessorStatusHistory(final String processorId, final Date start, final Date end, final int preferredDataPoints) {
        final StandardStatusHistory history = new StandardStatusHistory();
        history.setComponentDetail("Id", processorId);

        captures.forEach(new ForEachEvaluator<Capture>() {
            @Override
            public boolean evaluate(final Capture capture) {
                final ComponentStatusReport statusReport = capture.getStatusReport();
                final ProcessorStatus status = statusReport.getProcessorStatus(processorId);
                if (status == null) {
                    return true;
                }

                history.setComponentDetail("Group Id", status.getGroupId());
                history.setComponentDetail("Name", status.getName());
                history.setComponentDetail("Type", status.getType());

                final StandardStatusSnapshot snapshot = new StandardStatusSnapshot();
                snapshot.setTimestamp(capture.getCaptureDate());

                for (final ProcessorStatusDescriptor descriptor : ProcessorStatusDescriptor.values()) {
                    snapshot.addStatusMetric(descriptor.getDescriptor(), descriptor.getDescriptor().getValueFunction().getValue(status));
                }

                history.addStatusSnapshot(snapshot);
                return true;
            }
        });

        return history;
    }

    @Override
    public StatusHistory getConnectionStatusHistory(final String connectionId, final Date start, final Date end, final int preferredDataPoints) {
        final StandardStatusHistory history = new StandardStatusHistory();
        history.setComponentDetail("Id", connectionId);

        captures.forEach(new ForEachEvaluator<Capture>() {
            @Override
            public boolean evaluate(final Capture capture) {
                final ComponentStatusReport statusReport = capture.getStatusReport();
                final ConnectionStatus status = statusReport.getConnectionStatus(connectionId);
                if (status == null) {
                    return true;
                }

                history.setComponentDetail("Group Id", status.getGroupId());
                history.setComponentDetail("Name", status.getName());
                history.setComponentDetail("Source Id", status.getSourceId());
                history.setComponentDetail("Source Name", status.getSourceName());
                history.setComponentDetail("Destination Id", status.getDestinationId());
                history.setComponentDetail("Destination Name", status.getDestinationName());

                final StandardStatusSnapshot snapshot = new StandardStatusSnapshot();
                snapshot.setTimestamp(capture.getCaptureDate());

                for (final ConnectionStatusDescriptor descriptor : ConnectionStatusDescriptor.values()) {
                    snapshot.addStatusMetric(descriptor.getDescriptor(), descriptor.getDescriptor().getValueFunction().getValue(status));
                }

                history.addStatusSnapshot(snapshot);
                return true;
            }
        });

        return history;
    }

    @Override
    public StatusHistory getProcessGroupStatusHistory(final String processGroupId, final Date start, final Date end, final int preferredDataPoints) {
        final StandardStatusHistory history = new StandardStatusHistory();
        history.setComponentDetail("Id", processGroupId);

        captures.forEach(new ForEachEvaluator<Capture>() {
            @Override
            public boolean evaluate(final Capture capture) {
                final ComponentStatusReport statusReport = capture.getStatusReport();
                final ProcessGroupStatus status = statusReport.getProcessGroupStatus(processGroupId);
                if (status == null) {
                    return true;
                }

                history.setComponentDetail("Name", status.getName());

                final StandardStatusSnapshot snapshot = new StandardStatusSnapshot();
                snapshot.setTimestamp(capture.getCaptureDate());

                for (final ProcessGroupStatusDescriptor descriptor : ProcessGroupStatusDescriptor.values()) {
                    snapshot.addStatusMetric(descriptor.getDescriptor(), descriptor.getDescriptor().getValueFunction().getValue(status));
                }

                history.addStatusSnapshot(snapshot);
                return true;
            }
        });

        return history;
    }

    @Override
    public StatusHistory getRemoteProcessGroupStatusHistory(final String remoteGroupId, final Date start, final Date end, final int preferredDataPoints) {
        final StandardStatusHistory history = new StandardStatusHistory();
        history.setComponentDetail("Id", remoteGroupId);

        captures.forEach(new ForEachEvaluator<Capture>() {
            @Override
            public boolean evaluate(final Capture capture) {
                final ComponentStatusReport statusReport = capture.getStatusReport();
                final RemoteProcessGroupStatus status = statusReport.getRemoteProcessGroupStatus(remoteGroupId);
                if (status == null) {
                    return true;
                }

                history.setComponentDetail("Group Id", status.getGroupId());
                history.setComponentDetail("Name", status.getName());
                history.setComponentDetail("Uri", status.getTargetUri());

                final StandardStatusSnapshot snapshot = new StandardStatusSnapshot();
                snapshot.setTimestamp(capture.getCaptureDate());

                for (final RemoteProcessGroupStatusDescriptor descriptor : RemoteProcessGroupStatusDescriptor.values()) {
                    snapshot.addStatusMetric(descriptor.getDescriptor(), descriptor.getDescriptor().getValueFunction().getValue(status));
                }

                history.addStatusSnapshot(snapshot);
                return true;
            }
        });

        return history;
    }

    private static long calculateTaskMillis(final ProcessGroupStatus status) {
        long nanos = 0L;

        for (final ProcessorStatus procStatus : status.getProcessorStatus()) {
            nanos += procStatus.getProcessingNanos();
        }

        for (final ProcessGroupStatus childStatus : status.getProcessGroupStatus()) {
            nanos += calculateTaskMillis(childStatus);
        }

        return TimeUnit.MILLISECONDS.convert(nanos, TimeUnit.NANOSECONDS);
    }

    private static class Capture {

        private final Date captureDate;
        private final ComponentStatusReport statusReport;

        public Capture(final Date date, final ComponentStatusReport statusReport) {
            this.captureDate = date;
            this.statusReport = statusReport;
        }

        public Date getCaptureDate() {
            return captureDate;
        }

        public ComponentStatusReport getStatusReport() {
            return statusReport;
        }
    }

    public static enum RemoteProcessGroupStatusDescriptor {

        SENT_BYTES(new StandardMetricDescriptor<RemoteProcessGroupStatus>("sentBytes", "Bytes Sent (5 mins)", "The cumulative size of all FlowFiles that have been successfully sent to the remote system in the past 5 minutes", Formatter.DATA_SIZE, new ValueMapper<RemoteProcessGroupStatus>() {
            @Override
            public Long getValue(final RemoteProcessGroupStatus status) {
                return status.getSentContentSize();
            }
        })),
        SENT_COUNT(new StandardMetricDescriptor<RemoteProcessGroupStatus>("sentCount", "FlowFiles Sent (5 mins)", "The number of FlowFiles that have been successfully sent to the remote system in the past 5 minutes", Formatter.COUNT, new ValueMapper<RemoteProcessGroupStatus>() {
            @Override
            public Long getValue(final RemoteProcessGroupStatus status) {
                return Long.valueOf(status.getSentCount().longValue());
            }
        })),
        RECEIVED_BYTES(new StandardMetricDescriptor<RemoteProcessGroupStatus>("receivedBytes", "Bytes Received (5 mins)", "The cumulative size of all FlowFiles that have been received from the remote system in the past 5 minutes", Formatter.DATA_SIZE, new ValueMapper<RemoteProcessGroupStatus>() {
            @Override
            public Long getValue(final RemoteProcessGroupStatus status) {
                return status.getReceivedContentSize();
            }
        })),
        RECEIVED_COUNT(new StandardMetricDescriptor<RemoteProcessGroupStatus>("receivedCount", "FlowFiles Received (5 mins)", "The number of FlowFiles that have been received from the remote system in the past 5 minutes", Formatter.COUNT, new ValueMapper<RemoteProcessGroupStatus>() {
            @Override
            public Long getValue(final RemoteProcessGroupStatus status) {
                return Long.valueOf(status.getReceivedCount().longValue());
            }
        })),
        RECEIVED_BYTES_PER_SECOND(new StandardMetricDescriptor<RemoteProcessGroupStatus>("receivedBytesPerSecond", "Received Bytes Per Second", "The data rate at which data was received from the remote system in the past 5 minutes in terms of Bytes Per Second", Formatter.DATA_SIZE, new ValueMapper<RemoteProcessGroupStatus>() {
            @Override
            public Long getValue(final RemoteProcessGroupStatus status) {
                return Long.valueOf(status.getReceivedContentSize().longValue() / 300L);
            }
        })),
        SENT_BYTES_PER_SECOND(new StandardMetricDescriptor<RemoteProcessGroupStatus>("sentBytesPerSecond", "Sent Bytes Per Second", "The data rate at which data was received from the remote system in the past 5 minutes in terms of Bytes Per Second", Formatter.DATA_SIZE, new ValueMapper<RemoteProcessGroupStatus>() {
            @Override
            public Long getValue(final RemoteProcessGroupStatus status) {
                return Long.valueOf(status.getSentContentSize().longValue() / 300L);
            }
        })),
        TOTAL_BYTES_PER_SECOND(new StandardMetricDescriptor<RemoteProcessGroupStatus>("totalBytesPerSecond", "Total Bytes Per Second", "The sum of the send and receive data rate from the remote system in the past 5 minutes in terms of Bytes Per Second", Formatter.DATA_SIZE, new ValueMapper<RemoteProcessGroupStatus>() {
            @Override
            public Long getValue(final RemoteProcessGroupStatus status) {
                return Long.valueOf((status.getReceivedContentSize().longValue() + status.getSentContentSize().longValue()) / 300L);
            }
        })),
        AVERAGE_LINEAGE_DURATION(new StandardMetricDescriptor<RemoteProcessGroupStatus>(
                "averageLineageDuration",
                "Average Lineage Duration (5 mins)",
                "The average amount of time that a FlowFile took to process from receipt to drop in the past 5 minutes. For Processors that do not terminate FlowFiles, this value will be 0.",
                Formatter.DURATION,
                new ValueMapper<RemoteProcessGroupStatus>() {
                    @Override
                    public Long getValue(final RemoteProcessGroupStatus status) {
                        return status.getAverageLineageDuration(TimeUnit.MILLISECONDS);
                    }
                }, new ValueReducer<StatusSnapshot, Long>() {
                    @Override
                    public Long reduce(final List<StatusSnapshot> values) {
                        long millis = 0L;
                        int count = 0;

                        for (final StatusSnapshot snapshot : values) {
                            final long sent = snapshot.getStatusMetrics().get(SENT_COUNT.getDescriptor()).longValue();
                            count += sent;

                            final long avgMillis = snapshot.getStatusMetrics().get(AVERAGE_LINEAGE_DURATION.getDescriptor()).longValue();
                            final long totalMillis = avgMillis * sent;
                            millis += totalMillis;
                        }

                        return count == 0 ? 0 : millis / count;
                    }
                }
        ));

        private final MetricDescriptor<RemoteProcessGroupStatus> descriptor;

        private RemoteProcessGroupStatusDescriptor(final MetricDescriptor<RemoteProcessGroupStatus> descriptor) {
            this.descriptor = descriptor;
        }

        public String getField() {
            return descriptor.getField();
        }

        public MetricDescriptor<RemoteProcessGroupStatus> getDescriptor() {
            return descriptor;
        }
    }

    public static enum ProcessGroupStatusDescriptor {

        BYTES_READ(new StandardMetricDescriptor<ProcessGroupStatus>("bytesRead", "Bytes Read (5 mins)", "The total number of bytes read from Content Repository by Processors in this Process Group in the past 5 minutes", Formatter.DATA_SIZE, new ValueMapper<ProcessGroupStatus>() {
            @Override
            public Long getValue(final ProcessGroupStatus status) {
                return status.getBytesRead();
            }
        })),
        BYTES_WRITTEN(new StandardMetricDescriptor<ProcessGroupStatus>("bytesWritten", "Bytes Written (5 mins)", "The total number of bytes written to Content Repository by Processors in this Process Group in the past 5 minutes", Formatter.DATA_SIZE, new ValueMapper<ProcessGroupStatus>() {
            @Override
            public Long getValue(final ProcessGroupStatus status) {
                return status.getBytesWritten();
            }
        })),
        BYTES_TRANSFERRED(new StandardMetricDescriptor<ProcessGroupStatus>("bytesTransferred", "Bytes Transferred (5 mins)", "The total number of bytes read from or written to Content Repository by Processors in this Process Group in the past 5 minutes", Formatter.DATA_SIZE, new ValueMapper<ProcessGroupStatus>() {
            @Override
            public Long getValue(final ProcessGroupStatus status) {
                return status.getBytesRead() + status.getBytesWritten();
            }
        })),
        INPUT_BYTES(new StandardMetricDescriptor<ProcessGroupStatus>("inputBytes", "Bytes In (5 mins)", "The cumulative size of all FlowFiles that have entered this Process Group via its Input Ports in the past 5 minutes", Formatter.DATA_SIZE, new ValueMapper<ProcessGroupStatus>() {
            @Override
            public Long getValue(final ProcessGroupStatus status) {
                return status.getInputContentSize();
            }
        })),
        INPUT_COUNT(new StandardMetricDescriptor<ProcessGroupStatus>("inputCount", "FlowFiles In (5 mins)", "The number of FlowFiles that have entered this Process Group via its Input Ports in the past 5 minutes", Formatter.COUNT, new ValueMapper<ProcessGroupStatus>() {
            @Override
            public Long getValue(final ProcessGroupStatus status) {
                return status.getInputCount().longValue();
            }
        })),
        OUTPUT_BYTES(new StandardMetricDescriptor<ProcessGroupStatus>("outputBytes", "Bytes Out (5 mins)", "The cumulative size of all FlowFiles that have exited this Process Group via its Output Ports in the past 5 minutes", Formatter.DATA_SIZE, new ValueMapper<ProcessGroupStatus>() {
            @Override
            public Long getValue(final ProcessGroupStatus status) {
                return status.getOutputContentSize();
            }
        })),
        OUTPUT_COUNT(new StandardMetricDescriptor<ProcessGroupStatus>("outputCount", "FlowFiles Out (5 mins)", "The number of FlowFiles that have exited this Process Group via its Output Ports in the past 5 minutes", Formatter.COUNT, new ValueMapper<ProcessGroupStatus>() {
            @Override
            public Long getValue(final ProcessGroupStatus status) {
                return status.getOutputCount().longValue();
            }
        })),
        QUEUED_BYTES(new StandardMetricDescriptor<ProcessGroupStatus>("queuedBytes", "Queued Bytes", "The cumulative size of all FlowFiles queued in all Connections of this Process Group", Formatter.DATA_SIZE, new ValueMapper<ProcessGroupStatus>() {
            @Override
            public Long getValue(final ProcessGroupStatus status) {
                return status.getQueuedContentSize();
            }
        })),
        QUEUED_COUNT(new StandardMetricDescriptor<ProcessGroupStatus>("queuedCount", "Queued Count", "The number of FlowFiles queued in all Connections of this Process Group", Formatter.COUNT, new ValueMapper<ProcessGroupStatus>() {
            @Override
            public Long getValue(final ProcessGroupStatus status) {
                return status.getQueuedCount().longValue();
            }
        })),
        TASK_MILLIS(new StandardMetricDescriptor<ProcessGroupStatus>("taskMillis", "Total Task Duration (5 mins)", "The total number of thread-milliseconds that the Processors within this ProcessGroup have used to complete their tasks in the past 5 minutes", Formatter.DURATION, new ValueMapper<ProcessGroupStatus>() {
            @Override
            public Long getValue(final ProcessGroupStatus status) {
                return calculateTaskMillis(status);
            }
        }));

        private MetricDescriptor<ProcessGroupStatus> descriptor;

        private ProcessGroupStatusDescriptor(final MetricDescriptor<ProcessGroupStatus> descriptor) {
            this.descriptor = descriptor;
        }

        public String getField() {
            return descriptor.getField();
        }

        public MetricDescriptor<ProcessGroupStatus> getDescriptor() {
            return descriptor;
        }
    }

    public static enum ConnectionStatusDescriptor {

        INPUT_BYTES(new StandardMetricDescriptor<ConnectionStatus>("inputBytes", "Bytes In (5 mins)", "The cumulative size of all FlowFiles that were transferred to this Connection in the past 5 minutes", Formatter.DATA_SIZE, new ValueMapper<ConnectionStatus>() {
            @Override
            public Long getValue(final ConnectionStatus status) {
                return status.getInputBytes();
            }
        })),
        INPUT_COUNT(new StandardMetricDescriptor<ConnectionStatus>("inputCount", "FlowFiles In (5 mins)", "The number of FlowFiles that were transferred to this Connection in the past 5 minutes", Formatter.COUNT, new ValueMapper<ConnectionStatus>() {
            @Override
            public Long getValue(final ConnectionStatus status) {
                return Long.valueOf(status.getInputCount());
            }
        })),
        OUTPUT_BYTES(new StandardMetricDescriptor<ConnectionStatus>("outputBytes", "Bytes Out (5 mins)", "The cumulative size of all FlowFiles that were pulled from this Connection in the past 5 minutes", Formatter.DATA_SIZE, new ValueMapper<ConnectionStatus>() {
            @Override
            public Long getValue(final ConnectionStatus status) {
                return status.getOutputBytes();
            }
        })),
        OUTPUT_COUNT(new StandardMetricDescriptor<ConnectionStatus>("outputCount", "FlowFiles Out (5 mins)", "The number of FlowFiles that were pulled from this Connection in the past 5 minutes", Formatter.COUNT, new ValueMapper<ConnectionStatus>() {
            @Override
            public Long getValue(final ConnectionStatus status) {
                return Long.valueOf(status.getOutputCount());
            }
        })),
        QUEUED_BYTES(new StandardMetricDescriptor<ConnectionStatus>("queuedBytes", "Queued Bytes", "The number of Bytes queued in this Connection", Formatter.DATA_SIZE, new ValueMapper<ConnectionStatus>() {
            @Override
            public Long getValue(final ConnectionStatus status) {
                return status.getQueuedBytes();
            }
        })),
        QUEUED_COUNT(new StandardMetricDescriptor<ConnectionStatus>("queuedCount", "Queued Count", "The number of FlowFiles queued in this Connection", Formatter.COUNT, new ValueMapper<ConnectionStatus>() {
            @Override
            public Long getValue(final ConnectionStatus status) {
                return Long.valueOf(status.getQueuedCount());
            }
        }));

        private MetricDescriptor<ConnectionStatus> descriptor;

        private ConnectionStatusDescriptor(final MetricDescriptor<ConnectionStatus> descriptor) {
            this.descriptor = descriptor;
        }

        public String getField() {
            return descriptor.getField();
        }

        public MetricDescriptor<ConnectionStatus> getDescriptor() {
            return descriptor;
        }
    }

    public static enum ProcessorStatusDescriptor {

        BYTES_READ(new StandardMetricDescriptor<ProcessorStatus>("bytesRead", "Bytes Read (5 mins)", "The total number of bytes read from the Content Repository by this Processor in the past 5 minutes", Formatter.DATA_SIZE, new ValueMapper<ProcessorStatus>() {
            @Override
            public Long getValue(final ProcessorStatus status) {
                return status.getBytesRead();
            }
        })),
        BYTES_WRITTEN(new StandardMetricDescriptor<ProcessorStatus>("bytesWritten", "Bytes Written (5 mins)", "The total number of bytes written to the Content Repository by this Processor in the past 5 minutes", Formatter.DATA_SIZE, new ValueMapper<ProcessorStatus>() {
            @Override
            public Long getValue(final ProcessorStatus status) {
                return status.getBytesWritten();
            }
        })),
        BYTES_TRANSFERRED(new StandardMetricDescriptor<ProcessorStatus>("bytesTransferred", "Bytes Transferred (5 mins)", "The total number of bytes read from or written to the Content Repository by this Processor in the past 5 minutes", Formatter.DATA_SIZE, new ValueMapper<ProcessorStatus>() {
            @Override
            public Long getValue(final ProcessorStatus status) {
                return status.getBytesRead() + status.getBytesWritten();
            }
        })),
        INPUT_BYTES(new StandardMetricDescriptor<ProcessorStatus>("inputBytes", "Bytes In (5 mins)", "The cumulative size of all FlowFiles that this Processor has pulled from its queues in the past 5 minutes", Formatter.DATA_SIZE, new ValueMapper<ProcessorStatus>() {
            @Override
            public Long getValue(final ProcessorStatus status) {
                return status.getInputBytes();
            }
        })),
        INPUT_COUNT(new StandardMetricDescriptor<ProcessorStatus>("inputCount", "FlowFiles In (5 mins)", "The number of FlowFiles that this Processor has pulled from its queues in the past 5 minutes", Formatter.COUNT, new ValueMapper<ProcessorStatus>() {
            @Override
            public Long getValue(final ProcessorStatus status) {
                return Long.valueOf(status.getInputCount());
            }
        })),
        OUTPUT_BYTES(new StandardMetricDescriptor<ProcessorStatus>("outputBytes", "Bytes Out (5 mins)", "The cumulative size of all FlowFiles that this Processor has transferred to downstream queues in the past 5 minutes", Formatter.DATA_SIZE, new ValueMapper<ProcessorStatus>() {
            @Override
            public Long getValue(final ProcessorStatus status) {
                return status.getOutputBytes();
            }
        })),
        OUTPUT_COUNT(new StandardMetricDescriptor<ProcessorStatus>("outputCount", "FlowFiles Out (5 mins)", "The number of FlowFiles that this Processor has transferred to downstream queues in the past 5 minutes", Formatter.COUNT, new ValueMapper<ProcessorStatus>() {
            @Override
            public Long getValue(final ProcessorStatus status) {
                return Long.valueOf(status.getOutputCount());
            }
        })),
        TASK_COUNT(new StandardMetricDescriptor<ProcessorStatus>("taskCount", "Tasks (5 mins)", "The number of tasks that this Processor has completed in the past 5 minutes", Formatter.COUNT, new ValueMapper<ProcessorStatus>() {
            @Override
            public Long getValue(final ProcessorStatus status) {
                return Long.valueOf(status.getInvocations());
            }
        })),
        TASK_MILLIS(new StandardMetricDescriptor<ProcessorStatus>("taskMillis", "Total Task Duration (5 mins)", "The total number of thread-milliseconds that the Processor has used to complete its tasks in the past 5 minutes", Formatter.DURATION, new ValueMapper<ProcessorStatus>() {
            @Override
            public Long getValue(final ProcessorStatus status) {
                return TimeUnit.MILLISECONDS.convert(status.getProcessingNanos(), TimeUnit.NANOSECONDS);
            }
        })),
        FLOWFILES_REMOVED(new StandardMetricDescriptor<ProcessorStatus>("flowFilesRemoved", "FlowFiles Removed (5 mins)", "The total number of FlowFiles removed by this Processor in the last 5 minutes", Formatter.COUNT, new ValueMapper<ProcessorStatus>() {
            @Override
            public Long getValue(final ProcessorStatus status) {
                return Long.valueOf(status.getFlowFilesRemoved());
            }
        })),
        AVERAGE_LINEAGE_DURATION(new StandardMetricDescriptor<ProcessorStatus>(
                "averageLineageDuration",
                "Average Lineage Duration (5 mins)",
                "The average amount of time that a FlowFile took to process (from receipt until this Processor finished processing it) in the past 5 minutes.",
                Formatter.DURATION,
                new ValueMapper<ProcessorStatus>() {
                    @Override
                    public Long getValue(final ProcessorStatus status) {
                        return status.getAverageLineageDuration(TimeUnit.MILLISECONDS);
                    }
                }, new ValueReducer<StatusSnapshot, Long>() {
                    @Override
                    public Long reduce(final List<StatusSnapshot> values) {
                        long millis = 0L;
                        int count = 0;

                        for (final StatusSnapshot snapshot : values) {
                            final long removed = snapshot.getStatusMetrics().get(FLOWFILES_REMOVED.getDescriptor()).longValue();
                            count += removed;

                            count += snapshot.getStatusMetrics().get(OUTPUT_COUNT.getDescriptor()).longValue();

                            final long avgMillis = snapshot.getStatusMetrics().get(AVERAGE_LINEAGE_DURATION.getDescriptor()).longValue();
                            final long totalMillis = avgMillis * removed;
                            millis += totalMillis;
                        }

                        return count == 0 ? 0 : millis / count;
                    }
                }
        )),
        AVERAGE_TASK_MILLIS(new StandardMetricDescriptor<ProcessorStatus>(
                "averageTaskMillis",
                "Average Task Duration",
                "The average duration it took this Processor to complete a task, as averaged over the past 5 minutes",
                Formatter.DURATION,
                new ValueMapper<ProcessorStatus>() {
                    @Override
                    public Long getValue(final ProcessorStatus status) {
                        return status.getInvocations() == 0 ? 0 : TimeUnit.MILLISECONDS.convert(status.getProcessingNanos(), TimeUnit.NANOSECONDS) / status.getInvocations();
                    }
                },
                new ValueReducer<StatusSnapshot, Long>() {
                    @Override
                    public Long reduce(final List<StatusSnapshot> values) {
                        long procMillis = 0L;
                        int invocations = 0;

                        for (final StatusSnapshot snapshot : values) {
                            procMillis += snapshot.getStatusMetrics().get(TASK_MILLIS.getDescriptor()).longValue();
                            invocations += snapshot.getStatusMetrics().get(TASK_COUNT.getDescriptor()).intValue();
                        }

                        if (invocations == 0) {
                            return 0L;
                        }

                        return procMillis / invocations;
                    }
                }
        ));

        private MetricDescriptor<ProcessorStatus> descriptor;

        private ProcessorStatusDescriptor(final MetricDescriptor<ProcessorStatus> descriptor) {
            this.descriptor = descriptor;
        }

        public String getField() {
            return descriptor.getField();
        }

        public MetricDescriptor<ProcessorStatus> getDescriptor() {
            return descriptor;
        }
    }

    @Override
    public List<MetricDescriptor<ConnectionStatus>> getConnectionMetricDescriptors() {
        return CONNECTION_METRIC_DESCRIPTORS;
    }

    @Override
    public List<MetricDescriptor<ProcessGroupStatus>> getProcessGroupMetricDescriptors() {
        return PROCESS_GROUP_METRIC_DESCRIPTORS;
    }

    @Override
    public List<MetricDescriptor<RemoteProcessGroupStatus>> getRemoteProcessGroupMetricDescriptors() {
        return REMOTE_PROCESS_GROUP_METRIC_DESCRIPTORS;
    }

    @Override
    public List<MetricDescriptor<ProcessorStatus>> getProcessorMetricDescriptors() {
        return PROCESSOR_METRIC_DESCRIPTORS;
    }
}
