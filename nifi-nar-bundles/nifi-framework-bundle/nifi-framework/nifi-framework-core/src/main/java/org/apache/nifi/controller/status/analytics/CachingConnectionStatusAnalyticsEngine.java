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
import java.util.concurrent.TimeUnit;

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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

public class CachingConnectionStatusAnalyticsEngine implements StatusAnalyticsEngine {
    private ComponentStatusRepository statusRepository;
    private FlowController controller;
    private volatile Cache<String, SimpleRegression> cache;
    private static final Logger LOG = LoggerFactory.getLogger(ConnectionStatusAnalyticsEngine.class);

    public CachingConnectionStatusAnalyticsEngine(FlowController controller, ComponentStatusRepository statusRepository) {
        this.controller = controller;
        this.statusRepository = statusRepository;
        this.cache = Caffeine.newBuilder()
                .expireAfterWrite(1, TimeUnit.MINUTES)
                .build();
    }

    @Override
    public ConnectionStatusAnalytics getConnectionStatusAnalytics(String connectionId) {

        ProcessGroup rootGroup = controller.getFlowManager().getRootGroup();
        Connection connection = rootGroup.findConnection(connectionId);
        SimpleRegression cachedRegression = cache.getIfPresent(connection.getIdentifier());

        if (cachedRegression == null) {
            cachedRegression = getBackPressureRegressionModel(connection);
            if (cachedRegression != null)
                cache.put(connection.getIdentifier(), cachedRegression);
        }

        ConnectionStatusAnalytics cachedResult = calculate(cachedRegression, connection);
        LOG.info("Connection: " + connectionId + " Cached backpressure Time: " + cachedResult.getTimeToCountBackpressureMillis());
        return cachedResult;
    }

    protected ConnectionStatusAnalytics calculate(SimpleRegression regression, Connection conn) {
        long backPressureObjectThreshold = conn.getFlowFileQueue().getBackPressureObjectThreshold();

        final long connTimeToBackpressure;

        if (regression == null) {
            connTimeToBackpressure = Long.MAX_VALUE;
        } else {
            //If calculation returns as negative only 0 will return
            connTimeToBackpressure = Math.max(0, Math.round((backPressureObjectThreshold - regression.getIntercept()) / regression.getSlope())
                    - System.currentTimeMillis());
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
            public String getId() {
                return conn.getIdentifier();
            }

            @Override
            public long getTimeToBytesBackpressureMillis() {
                return 0;
            }

            @Override
            public long getTimeToCountBackpressureMillis() {
                return connTimeToBackpressure;
            }

            @Override
            public long getNextIntervalBytes() {
                return 0;
            }

            @Override
            public int getNextIntervalCount() {
                return 0;
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

    /**
     * Get backpressure model based on current data
     *
     * @param conn the connection to run the analytic on
     * @return
     */
    protected SimpleRegression getBackPressureRegressionModel(Connection conn) {
        Date minDate = new Date(System.currentTimeMillis() - (60 * 1000));
        StatusHistoryDTO connHistory = StatusHistoryUtil.createStatusHistoryDTO(
                statusRepository.getConnectionStatusHistory(conn.getIdentifier(), minDate, null, Integer.MAX_VALUE));
        List<StatusSnapshotDTO> aggregateSnapshots = connHistory.getAggregateSnapshots();

        if (aggregateSnapshots.size() < 2) {
            LOG.info("Not enough data to model time to backpressure.");
            return null;
        } else {

            ConnectionStatusDescriptor.QUEUED_COUNT.getField();
            SimpleRegression regression = new SimpleRegression();

            for (StatusSnapshotDTO snap : aggregateSnapshots) {
                Long snapQueuedCount = snap.getStatusMetrics().get(ConnectionStatusDescriptor.QUEUED_COUNT.getField());
                long snapTime = snap.getTimestamp().getTime();
                regression.addData(snapTime, snapQueuedCount);
            }

            if (regression.getSlope() <= 0 && !conn.getFlowFileQueue().isFull()) {
                LOG.info("Connection " + conn.getIdentifier() + " is not experiencing backpressure.");
                return null;
            } else {
                return regression;
            }
        }
    }
}
