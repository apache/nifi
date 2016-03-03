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

import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.RemoteProcessGroupStatus;
import org.apache.nifi.util.ComponentStatusReport;
import org.apache.nifi.util.ComponentStatusReport.ComponentType;
import org.apache.nifi.util.NiFiProperties;
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
        captures.add(new Capture(timestamp, ComponentStatusReport.fromProcessGroupStatus(rootGroupStatus, ComponentType.PROCESSOR,
                ComponentType.CONNECTION, ComponentType.PROCESS_GROUP, ComponentType.REMOTE_PROCESS_GROUP)));
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
                history.setComponentDetail("Source Name", status.getSourceName());
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
