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
package org.apache.nifi.controller.status.analytics;

import java.util.Date;
import java.util.List;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.status.history.ComponentStatusRepository;
import org.apache.nifi.controller.status.history.ConnectionStatusDescriptor;
import org.apache.nifi.controller.status.history.StatusHistoryUtil;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.web.api.dto.status.StatusHistoryDTO;
import org.apache.nifi.web.api.dto.status.StatusSnapshotDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatusAnalyticEngine implements StatusAnalytics {
    private ComponentStatusRepository statusRepository;
    private FlowController controller;

    private static final Logger LOG = LoggerFactory.getLogger(StatusAnalyticEngine.class);

    public StatusAnalyticEngine(FlowController controller, ComponentStatusRepository statusRepository) {
        this.controller = controller;
        this.statusRepository = statusRepository;
    }

    @Override
    public ConnectionStatusAnalytics getConnectionStatusAnalytics(String connectionId) {
        ProcessGroup rootGroup = controller.getFlowManager().getRootGroup();
        return getConnectionStatusAnalytics(rootGroup.findConnection(connectionId));
    }

    /**
     * Finds the number of millis until the given connection will experience backpressure.
     * @param conn the connection to run the analytic on
     * @return
     */
    public ConnectionStatusAnalytics getConnectionStatusAnalytics(Connection conn) {
        LOG.info("Getting connection history for: " + conn.getIdentifier());
        long connTimeToBackpressure;
        Date minDate = new Date(System.currentTimeMillis() - (5 * 60 * 1000));
        StatusHistoryDTO connHistory = StatusHistoryUtil.createStatusHistoryDTO(
                statusRepository.getConnectionStatusHistory(conn.getIdentifier(), minDate, null, Integer.MAX_VALUE));
        List<StatusSnapshotDTO> aggregateSnapshots = connHistory.getAggregateSnapshots();

        if (aggregateSnapshots.size() < 2) {
            LOG.info("Not enough data to model time to backpressure.");
            connTimeToBackpressure = Long.MAX_VALUE;
        } else {

            long backPressureObjectThreshold = conn.getFlowFileQueue().getBackPressureObjectThreshold();
            LOG.info("Connection " + conn.getIdentifier() + " backpressure object threshold is "
                    + Long.toString(backPressureObjectThreshold));

            ConnectionStatusDescriptor.QUEUED_COUNT.getField();

            SimpleRegression regression = new SimpleRegression();

            for (StatusSnapshotDTO snap : aggregateSnapshots) {
                Long snapQueuedCount = snap.getStatusMetrics().get(ConnectionStatusDescriptor.QUEUED_COUNT.getField());
                long snapTime = snap.getTimestamp().getTime();
                regression.addData(snapTime, snapQueuedCount);
            }

            // Skip this connection if its queue is declining.
            if (regression.getSlope() <= 0) {
                LOG.info("Connection " + conn.getIdentifier() + " is not experiencing backpressure.");
                connTimeToBackpressure = Long.MAX_VALUE;
            } else {

                // Compute time-to backpressure for this connection; Reduce total result iff
                // this connection is lower.
                connTimeToBackpressure = Math
                        .round((backPressureObjectThreshold - regression.getIntercept()) / regression.getSlope())
                        - System.currentTimeMillis();
                LOG.info("Connection " + conn.getIdentifier() + " time to backpressure is " + connTimeToBackpressure);
            }
        }

        return new ConnectionStatusAnalytics() {

            @Override
            public String getSourceName() {
                return conn.getSource().getName();
            }

            @Override
            public String getSourceId() {
                return conn.getSource().getIdentifier();
            }

            @Override
            public String getName() {
                return conn.getName();
            }

            @Override
            public long getMinTimeToBackpressureMillis() {
                return connTimeToBackpressure;
            }

            @Override
            public String getId() {
                return conn.getIdentifier();
            }

            @Override
            public String getGroupId() {
                return conn.getProcessGroup().getIdentifier();
            }

            @Override
            public String getDestinationName() {
                return conn.getDestination().getName();
            }

            @Override
            public String getDestinationId() {
                return conn.getDestination().getIdentifier();
            }
        };
    }

    @Override
    public long getMinTimeToBackpressureMillis() {
        ProcessGroup rootGroup = controller.getFlowManager().getRootGroup();
        List<Connection> allConnections = rootGroup.findAllConnections();
        rootGroup.findConnection("asdf");
        long minTimeToBackpressure = Long.MAX_VALUE;

        for (Connection conn : allConnections) {
            ConnectionStatusAnalytics connAnalytics = getConnectionStatusAnalytics(conn);
            minTimeToBackpressure = Math.min(minTimeToBackpressure, connAnalytics.getMinTimeToBackpressureMillis());
        }

        LOG.info("Min time to backpressure is: " + Long.toString(minTimeToBackpressure));
        return minTimeToBackpressure;
    }
}
