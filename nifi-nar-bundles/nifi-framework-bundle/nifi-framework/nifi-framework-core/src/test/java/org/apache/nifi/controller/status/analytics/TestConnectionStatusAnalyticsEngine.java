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

public class TestConnectionStatusAnalyticsEngine extends TestStatusAnalyticsEngine {

    @Override
    public StatusAnalyticsEngine getStatusAnalyticsEngine(FlowManager flowManager, FlowFileEventRepository flowFileEventRepository,
                                                          ComponentStatusRepository statusRepository, Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap,
                                                            long predictIntervalMillis, long queryIntervalMillis, String scoreName, double scoreThreshold) {
        return new ConnectionStatusAnalyticsEngine(flowManager, statusRepository, flowFileEventRepository,modelMap,
                                                   DEFAULT_PREDICT_INTERVAL_MILLIS, DEFAULT_QUERY_INTERVAL_MILLIS, scoreName, scoreThreshold);
    }

}
