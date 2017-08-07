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

import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.RemoteProcessGroupStatus;
import org.apache.nifi.controller.status.history.MetricDescriptor.Formatter;
import org.apache.nifi.util.ComponentStatusReport;
import org.apache.nifi.util.ComponentStatusReport.ComponentType;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.RingBuffer;
import org.apache.nifi.util.RingBuffer.ForEachEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;

public class VolatileComponentStatusRepository implements ComponentStatusRepository {

    public static final String NUM_DATA_POINTS_PROPERTY = "nifi.components.status.repository.buffer.size";
    public static final int DEFAULT_NUM_DATA_POINTS = 288;   // 1 day worth of 5-minute snapshots

    private final RingBuffer<Capture> captures;
    private final Logger logger = LoggerFactory.getLogger(VolatileComponentStatusRepository.class);

    private volatile long lastCaptureTime = 0L;

    /**
     * Default no args constructor for service loading only
     */
    public VolatileComponentStatusRepository(){
        captures = null;
    }

    public VolatileComponentStatusRepository(final NiFiProperties nifiProperties) {
        final int numDataPoints = nifiProperties.getIntegerProperty(NUM_DATA_POINTS_PROPERTY, DEFAULT_NUM_DATA_POINTS);

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
    public StatusHistory getProcessorStatusHistory(final String processorId, final Date start, final Date end, final int preferredDataPoints, final boolean includeCounters) {
        final StandardStatusHistory history = new StandardStatusHistory();
        history.setComponentDetail(COMPONENT_DETAIL_ID, processorId);

        captures.forEach(new ForEachEvaluator<Capture>() {
            @Override
            public boolean evaluate(final Capture capture) {
                final ComponentStatusReport statusReport = capture.getStatusReport();
                final ProcessorStatus status = statusReport.getProcessorStatus(processorId);
                if (status == null) {
                    return true;
                }

                history.setComponentDetail(COMPONENT_DETAIL_GROUP_ID, status.getGroupId());
                history.setComponentDetail(COMPONENT_DETAIL_NAME, status.getName());
                history.setComponentDetail(COMPONENT_DETAIL_TYPE, status.getType());

                final StandardStatusSnapshot snapshot = new StandardStatusSnapshot();
                snapshot.setTimestamp(capture.getCaptureDate());

                for (final ProcessorStatusDescriptor descriptor : ProcessorStatusDescriptor.values()) {
                    if (descriptor.isVisible()) {
                        snapshot.addStatusMetric(descriptor.getDescriptor(), descriptor.getDescriptor().getValueFunction().getValue(status));
                    }
                }

                if (includeCounters) {
                    final Map<String, Long> counters = status.getCounters();
                    if (counters != null) {
                        for (final Map.Entry<String, Long> entry : counters.entrySet()) {
                            final String counterName = entry.getKey();

                            final String label = entry.getKey() + " (5 mins)";
                            final MetricDescriptor<ProcessorStatus> metricDescriptor = new StandardMetricDescriptor<>(entry.getKey(), label, label, Formatter.COUNT,
                                s -> s.getCounters() == null ? null : s.getCounters().get(counterName));

                            snapshot.addStatusMetric(metricDescriptor, entry.getValue());
                        }
                    }
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
        history.setComponentDetail(COMPONENT_DETAIL_ID, connectionId);

        captures.forEach(new ForEachEvaluator<Capture>() {
            @Override
            public boolean evaluate(final Capture capture) {
                final ComponentStatusReport statusReport = capture.getStatusReport();
                final ConnectionStatus status = statusReport.getConnectionStatus(connectionId);
                if (status == null) {
                    return true;
                }

                history.setComponentDetail(COMPONENT_DETAIL_GROUP_ID, status.getGroupId());
                history.setComponentDetail(COMPONENT_DETAIL_NAME, status.getName());
                history.setComponentDetail(COMPONENT_DETAIL_SOURCE_NAME, status.getSourceName());
                history.setComponentDetail(COMPONENT_DETAIL_DESTINATION_NAME, status.getDestinationName());

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
        history.setComponentDetail(COMPONENT_DETAIL_ID, processGroupId);

        captures.forEach(new ForEachEvaluator<Capture>() {
            @Override
            public boolean evaluate(final Capture capture) {
                final ComponentStatusReport statusReport = capture.getStatusReport();
                final ProcessGroupStatus status = statusReport.getProcessGroupStatus(processGroupId);
                if (status == null) {
                    return true;
                }

                history.setComponentDetail(COMPONENT_DETAIL_NAME, status.getName());

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
        history.setComponentDetail(COMPONENT_DETAIL_ID, remoteGroupId);

        captures.forEach(new ForEachEvaluator<Capture>() {
            @Override
            public boolean evaluate(final Capture capture) {
                final ComponentStatusReport statusReport = capture.getStatusReport();
                final RemoteProcessGroupStatus status = statusReport.getRemoteProcessGroupStatus(remoteGroupId);
                if (status == null) {
                    return true;
                }

                history.setComponentDetail(COMPONENT_DETAIL_GROUP_ID, status.getGroupId());
                history.setComponentDetail(COMPONENT_DETAIL_NAME, status.getName());
                history.setComponentDetail(COMPONENT_DETAIL_URI, status.getTargetUri());

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
}
