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

import java.util.Map;

import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.status.history.StatusHistoryRepository;
import org.apache.nifi.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * An implementation of {@link StatusAnalyticsEngine} that supports creation of ConnectionStatusAnalytics objects
 * </p>
 */
public class ConnectionStatusAnalyticsEngine implements StatusAnalyticsEngine {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectionStatusAnalyticsEngine.class);
    protected final StatusHistoryRepository statusRepository;
    protected final FlowManager flowManager;
    protected final StatusAnalyticsModelMapFactory statusAnalyticsModelMapFactory;
    protected final long predictionIntervalMillis;
    protected final long queryIntervalMillis;
    protected final String scoreName;
    protected final double scoreThreshold;

    public ConnectionStatusAnalyticsEngine(FlowManager flowManager, StatusHistoryRepository statusRepository,
                                           StatusAnalyticsModelMapFactory statusAnalyticsModelMapFactory, long predictionIntervalMillis,
                                           long queryIntervalMillis, String scoreName, double scoreThreshold) {
        this.flowManager = flowManager;
        this.statusRepository = statusRepository;
        this.predictionIntervalMillis = predictionIntervalMillis;
        this.statusAnalyticsModelMapFactory = statusAnalyticsModelMapFactory;
        this.queryIntervalMillis = queryIntervalMillis;
        this.scoreName = scoreName;
        this.scoreThreshold = scoreThreshold;
    }

    /**
     * Return a connection status analytics instance
     * @param identifier connection identifier
     * @return status analytics object
     */
    @Override
    public StatusAnalytics getStatusAnalytics(String identifier) {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = statusAnalyticsModelMapFactory.getConnectionStatusModelMap();
        ConnectionStatusAnalytics connectionStatusAnalytics = new ConnectionStatusAnalytics(statusRepository, flowManager, modelMap, identifier, false);
        connectionStatusAnalytics.setIntervalTimeMillis(predictionIntervalMillis);
        connectionStatusAnalytics.setQueryIntervalMillis(queryIntervalMillis);
        connectionStatusAnalytics.setScoreName(scoreName);
        connectionStatusAnalytics.setScoreThreshold(scoreThreshold);
        return connectionStatusAnalytics;
    }

}
