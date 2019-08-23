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

import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.status.history.ComponentStatusRepository;
import org.apache.nifi.controller.status.history.StatusHistory;
import org.apache.nifi.controller.status.history.StatusSnapshot;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.util.Tuple;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public abstract class TestStatusAnalyticsEngine {

    static final long DEFAULT_PREDICT_INTERVAL_MILLIS = 3L * 60 * 1000;
    static final String DEFAULT_SCORE_NAME = "rSquared";
    static final double DEFAULT_SCORE_THRESHOLD = .9;

    protected ComponentStatusRepository statusRepository;
    protected FlowManager flowManager;
    protected FlowFileEventRepository flowFileEventRepository;
    protected Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap;

    @Before
    public void setup() {

        statusRepository = Mockito.mock(ComponentStatusRepository.class);
        flowManager = Mockito.mock(FlowManager.class);
        modelMap = new HashMap<>();

        StatusAnalyticsModel countModel = Mockito.mock(StatusAnalyticsModel.class);
        StatusAnalyticsModel byteModel = Mockito.mock(StatusAnalyticsModel.class);
        StatusMetricExtractFunction extractFunction = Mockito.mock(StatusMetricExtractFunction.class);
        Tuple<StatusAnalyticsModel,StatusMetricExtractFunction> countTuple = new Tuple<>(countModel,extractFunction);
        Tuple<StatusAnalyticsModel,StatusMetricExtractFunction> byteTuple = new Tuple<>(byteModel,extractFunction);
        modelMap.put("queuedCount",countTuple);
        modelMap.put("queuedBytes",byteTuple);

        Double[][] features = new Double[1][1];
        Double[] target = new Double[1];


        ProcessGroup processGroup = Mockito.mock(ProcessGroup.class);
        StatusHistory statusHistory = Mockito.mock(StatusHistory.class);
        StatusSnapshot statusSnapshot = Mockito.mock(StatusSnapshot.class);

        when(extractFunction.extractMetric(anyString(),any(StatusHistory.class))).then(new Answer<Tuple<Stream<Double[]>,Stream<Double>>>() {
            @Override
            public Tuple<Stream<Double[]>, Stream<Double>> answer(InvocationOnMock invocationOnMock) throws Throwable {
                return new Tuple<>(Stream.of(features), Stream.of(target));
            }
        });

        when(statusSnapshot.getMetricDescriptors()).thenReturn(Collections.emptySet());
        when(flowManager.getRootGroup()).thenReturn(processGroup);
        when(statusRepository.getConnectionStatusHistory(anyString(), any(), any(), anyInt())).thenReturn(statusHistory);
    }

    @Test
    public void testGetStatusAnalytics() {
        StatusAnalyticsEngine statusAnalyticsEngine = getStatusAnalyticsEngine(flowManager,flowFileEventRepository, statusRepository, modelMap, DEFAULT_PREDICT_INTERVAL_MILLIS,
                                                                                DEFAULT_SCORE_NAME, DEFAULT_SCORE_THRESHOLD);
        StatusAnalytics statusAnalytics = statusAnalyticsEngine.getStatusAnalytics("1");
        assertNotNull(statusAnalytics);
    }

    public abstract StatusAnalyticsEngine getStatusAnalyticsEngine(FlowManager flowManager, FlowFileEventRepository flowFileEventRepository,
                                                                   ComponentStatusRepository componentStatusRepository, Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>>  modelMap,
                                                                    long predictIntervalMillis, String scoreName, double scoreThreshold);

}
