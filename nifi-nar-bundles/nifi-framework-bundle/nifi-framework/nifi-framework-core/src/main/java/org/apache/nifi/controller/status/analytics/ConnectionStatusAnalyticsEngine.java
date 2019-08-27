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
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.status.history.ComponentStatusRepository;
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
    protected final ComponentStatusRepository statusRepository;
    protected final FlowManager flowManager;
    protected final FlowFileEventRepository flowFileEventRepository;
    protected final Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap;
    protected final long predictionIntervalMillis;
    protected final String scoreName;
    protected final double scoreThreshold;

    public ConnectionStatusAnalyticsEngine(FlowManager flowManager, ComponentStatusRepository statusRepository, FlowFileEventRepository flowFileEventRepository,
            Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap, long predictionIntervalMillis, String scoreName, double scoreThreshold) {
        this.flowManager = flowManager;
        this.statusRepository = statusRepository;
        this.flowFileEventRepository = flowFileEventRepository;
        this.predictionIntervalMillis = predictionIntervalMillis;
        this.modelMap = modelMap;
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
        ConnectionStatusAnalytics connectionStatusAnalytics = new ConnectionStatusAnalytics(statusRepository, flowManager, flowFileEventRepository, modelMap, identifier, false);
        connectionStatusAnalytics.setIntervalTimeMillis(predictionIntervalMillis);
        connectionStatusAnalytics.setScoreName(scoreName);
        connectionStatusAnalytics.setScoreThreshold(scoreThreshold);
        connectionStatusAnalytics.refresh();
        return connectionStatusAnalytics;
    }

}
