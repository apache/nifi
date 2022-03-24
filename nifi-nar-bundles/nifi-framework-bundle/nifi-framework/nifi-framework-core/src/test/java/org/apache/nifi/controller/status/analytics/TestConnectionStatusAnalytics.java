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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.repository.FlowFileEvent;
import org.apache.nifi.controller.repository.RepositoryStatusReport;
import org.apache.nifi.controller.status.history.ConnectionStatusDescriptor;
import org.apache.nifi.controller.status.history.MetricDescriptor;
import org.apache.nifi.controller.status.history.StatusHistory;
import org.apache.nifi.controller.status.history.StatusHistoryRepository;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.nar.StandardExtensionDiscoveringManager;
import org.apache.nifi.nar.SystemBundle;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.Tuple;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestConnectionStatusAnalytics {

    private static final Set<MetricDescriptor<?>> CONNECTION_METRICS = Arrays.stream(ConnectionStatusDescriptor.values())
            .map(ConnectionStatusDescriptor::getDescriptor)
            .collect(Collectors.toSet());

    final Connection connection = Mockito.mock(Connection.class);
    final FlowFileEvent flowFileEvent = Mockito.mock(FlowFileEvent.class);
    final RepositoryStatusReport repositoryStatusReport = Mockito.mock(RepositoryStatusReport.class);

    protected ConnectionStatusAnalytics getConnectionStatusAnalytics(Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap) {

        StatusHistoryRepository statusRepository = Mockito.mock(StatusHistoryRepository.class);
        FlowManager flowManager;
        flowManager = Mockito.mock(FlowManager.class);
        final Map<String, String> otherProps = new HashMap<>();
        final String propsFile = "src/test/resources/conf/nifi.properties";
        NiFiProperties nifiProperties = NiFiProperties.createBasicNiFiProperties(propsFile, otherProps);

        // use the system bundle
        Bundle systemBundle = SystemBundle.create(nifiProperties);
        StandardExtensionDiscoveringManager extensionManager = new StandardExtensionDiscoveringManager();
        extensionManager.discoverExtensions(systemBundle, Collections.emptySet());

        final ProcessGroup processGroup = Mockito.mock(ProcessGroup.class);
        final StatusHistory statusHistory = Mockito.mock(StatusHistory.class);
        final FlowFileQueue flowFileQueue = Mockito.mock(FlowFileQueue.class);


        final Set<Connection> connections = new HashSet<>();
        final String connectionIdentifier = "1";
        connections.add(connection);

        when(flowFileQueue.getBackPressureDataSizeThreshold()).thenReturn("100MB");
        when(flowFileQueue.getBackPressureObjectThreshold()).thenReturn(100L);
        when(connection.getIdentifier()).thenReturn(connectionIdentifier);
        when(connection.getFlowFileQueue()).thenReturn(flowFileQueue);
        when(flowManager.getConnection(anyString())).thenReturn(connection);
        when(flowFileEvent.getContentSizeIn()).thenReturn(10L);
        when(flowFileEvent.getContentSizeOut()).thenReturn(10L);
        when(flowFileEvent.getFlowFilesIn()).thenReturn(10);
        when(flowFileEvent.getFlowFilesOut()).thenReturn(10);
        when(repositoryStatusReport.getReportEntry(anyString())).thenReturn(flowFileEvent);
        when(statusRepository.getConnectionStatusHistory(anyString(), any(), any(), anyInt())).thenReturn(statusHistory);

        ConnectionStatusAnalytics connectionStatusAnalytics = new ConnectionStatusAnalytics(statusRepository, flowManager,
                                                                                            modelMap, connectionIdentifier, false);
        connectionStatusAnalytics.refresh();
        return connectionStatusAnalytics;
    }

    public Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> getModelMap( String predictionType, Double score,
                                                                                Double targetPrediction, Double variablePrediction){
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = new HashMap<>();
        StatusAnalyticsModel model = Mockito.mock(StatusAnalyticsModel.class);
        StatusMetricExtractFunction extractFunction = Mockito.mock(StatusMetricExtractFunction.class);
        Tuple<StatusAnalyticsModel,StatusMetricExtractFunction> modelTuple = new Tuple<>(model,extractFunction);
        modelMap.put(predictionType,modelTuple);
        Map<String,Double> scores = new HashMap<>();
        scores.put("rSquared",score);

        Double[][] features = new Double[1][1];
        Double[] target = new Double[1];

        when(extractFunction.extractMetric(anyString(),any(StatusHistory.class))).then(new Answer<Tuple<Stream<Double[]>,Stream<Double>>>() {
            @Override
            public Tuple<Stream<Double[]>, Stream<Double>> answer(InvocationOnMock invocationOnMock) throws Throwable {
                return new Tuple<>(Stream.of(features), Stream.of(target));
            }
        });

        when(model.getScores()).thenReturn(scores);
        when(model.predict(any(Double[].class))).thenReturn(targetPrediction);
        when(model.predictVariable(anyInt(),any(),any())).thenReturn(variablePrediction);
        return modelMap;

    }

    @Test
    public void testInvalidModelLowScore() {
        Date now = new Date();
        Long tomorrowMillis = DateUtils.addDays(now,1).toInstant().toEpochMilli();
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedCount",.5,100.0,tomorrowMillis.doubleValue());
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long countTime = connectionStatusAnalytics.getTimeToCountBackpressureMillis(connection, flowFileEvent);
        assertNotNull(countTime);
        assert (countTime == -1);
    }

      @Test
    public void testInvalidModelNaNScore() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedCount",Double.NaN,Double.NaN,Double.NaN);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long countTime = connectionStatusAnalytics.getTimeToCountBackpressureMillis(connection, flowFileEvent);
        assertNotNull(countTime);
        assert (countTime == -1);
    }

    @Test
    public void testInvalidModelInfiniteScore() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedCount",Double.POSITIVE_INFINITY,Double.POSITIVE_INFINITY,Double.POSITIVE_INFINITY);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long countTime = connectionStatusAnalytics.getTimeToCountBackpressureMillis(connection, flowFileEvent);
        assertNotNull(countTime);
        assert (countTime == -1);
    }

    @Test
    public void testGetIntervalTimeMillis() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedCount",.9,100.0,100.0);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long interval = connectionStatusAnalytics.getIntervalTimeMillis();
        assertNotNull(interval);
        assert (interval == 180000);
    }

    @Test
    public void testGetTimeToCountBackpressureMillis() {
        Date now = new Date();
        Long tomorrowMillis = DateUtils.addDays(now,1).toInstant().toEpochMilli();
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedCount",.9,100.0,tomorrowMillis.doubleValue());
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long countTime = connectionStatusAnalytics.getTimeToCountBackpressureMillis(connection, flowFileEvent);
        assertNotNull(countTime);
        assert (countTime > 0);
    }

    @Test
    public void testCannotPredictTimeToCountNaN() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedCount",.9,Double.NaN,Double.NaN);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long countTime = connectionStatusAnalytics.getTimeToCountBackpressureMillis(connection, flowFileEvent);
        assertNotNull(countTime);
        assert (countTime == -1);
    }

    @Test
    public void testCannotPredictTimeToCountInfinite() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedCount",.9,Double.POSITIVE_INFINITY,Double.POSITIVE_INFINITY);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long countTime = connectionStatusAnalytics.getTimeToCountBackpressureMillis(connection, flowFileEvent);
        assertNotNull(countTime);
        assert (countTime == -1);
    }

    @Test
    public void testCannotPredictTimeToCountNegative() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedCount",.9,-1.0,-1.0);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long countTime = connectionStatusAnalytics.getTimeToCountBackpressureMillis(connection, flowFileEvent);
        assertNotNull(countTime);
        assert (countTime == -1);
    }

    @Test
    public void testMissingModelGetTimeToCount() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("fakeModel",Double.POSITIVE_INFINITY,Double.POSITIVE_INFINITY,Double.POSITIVE_INFINITY);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        try {
            connectionStatusAnalytics.getTimeToCountBackpressureMillis(connection, flowFileEvent);
            fail();
        }catch(IllegalArgumentException iae){
            assertTrue(true);
        }
    }

    @Test
    public void testGetTimeToBytesBackpressureMillis() {
        Date now = new Date();
        Long tomorrowMillis = DateUtils.addDays(now,1).toInstant().toEpochMilli();
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedBytes",.9,100.0,tomorrowMillis.doubleValue());
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long countTime = connectionStatusAnalytics.getTimeToBytesBackpressureMillis(connection, flowFileEvent);
        assertNotNull(countTime);
        assert (countTime > 0);
    }

    @Test
    public void testCannotPredictTimeToBytesNaN() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedBytes",.9,Double.NaN,Double.NaN);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long countTime = connectionStatusAnalytics.getTimeToBytesBackpressureMillis(connection, flowFileEvent);
        assertNotNull(countTime);
        assert (countTime == -1);
    }

    @Test
    public void testCannotPredictTimeToBytesInfinite() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedBytes",.9,Double.POSITIVE_INFINITY,Double.POSITIVE_INFINITY);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long countTime = connectionStatusAnalytics.getTimeToBytesBackpressureMillis(connection, flowFileEvent);
        assertNotNull(countTime);
        assert (countTime == -1);
    }

    @Test
    public void testCannotPredictTimeToBytesNegative() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedBytes",.9,-1.0,-1.0);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long countTime = connectionStatusAnalytics.getTimeToBytesBackpressureMillis(connection, flowFileEvent);
        assertNotNull(countTime);
        assert (countTime == -1);
    }

    @Test
    public void testMissingModelGetTimeToBytes() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("fakeModel",Double.POSITIVE_INFINITY,Double.POSITIVE_INFINITY,Double.POSITIVE_INFINITY);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        try {
            connectionStatusAnalytics.getTimeToBytesBackpressureMillis(connection, flowFileEvent);
            fail();
        }catch(IllegalArgumentException iae){
            assertTrue(true);
        }
    }

    @Test
    public void testGetNextIntervalBytes() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedBytes",.9,1.0,1.0);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long nextIntervalBytes = connectionStatusAnalytics.getNextIntervalBytes(flowFileEvent);
        assertNotNull(nextIntervalBytes);
        assert (nextIntervalBytes > 0);
    }

    @Test
    public void testNextIntervalBytesZero() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedBytes",.9,-1.0,-1.0);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long nextIntervalBytes = connectionStatusAnalytics.getNextIntervalBytes(flowFileEvent);
        assertNotNull(nextIntervalBytes);
        assert (nextIntervalBytes == 0);
    }

    @Test
    public void testCannotPredictNextIntervalBytesNaN() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedBytes",.9,Double.NaN,Double.NaN);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long nextIntervalBytes = connectionStatusAnalytics.getNextIntervalBytes(flowFileEvent);
        assertNotNull(nextIntervalBytes);
        assert (nextIntervalBytes == -1);
    }

    @Test
    public void testCannotPredictNextIntervalBytesInfinity() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedBytes",.9,Double.POSITIVE_INFINITY,Double.POSITIVE_INFINITY);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long nextIntervalBytes = connectionStatusAnalytics.getNextIntervalBytes(flowFileEvent);
        assertNotNull(nextIntervalBytes);
        assert (nextIntervalBytes == -1);
    }

    @Test
    public void testMissingModelNextIntervalBytes() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("fakeModel",Double.POSITIVE_INFINITY,Double.POSITIVE_INFINITY,Double.POSITIVE_INFINITY);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        try {
            connectionStatusAnalytics.getNextIntervalBytes(flowFileEvent);
            fail();
        }catch(IllegalArgumentException iae){
            assertTrue(true);
        }
    }

    @Test
    public void testGetNextIntervalCount() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedCount",.9,1.0,1.0);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long nextIntervalBytes = connectionStatusAnalytics.getNextIntervalCount(flowFileEvent);
        assertNotNull(nextIntervalBytes);
        assert (nextIntervalBytes > 0);
    }

    @Test
    public void testGetNextIntervalCountZero() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedCount",.9,-1.0,-1.0);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long nextIntervalCount = connectionStatusAnalytics.getNextIntervalCount(flowFileEvent);
        assertNotNull(nextIntervalCount);
        assert (nextIntervalCount == 0);
    }

    @Test
    public void testCannotPredictNextIntervalCountNaN() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedCount",.9,Double.NaN,Double.NaN);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long nextIntervalCount = connectionStatusAnalytics.getNextIntervalCount(flowFileEvent);
        assertNotNull(nextIntervalCount);
        assert (nextIntervalCount == -1);
    }

    @Test
    public void testCannotPredictNextIntervalCountInfinity() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedCount",.9,Double.POSITIVE_INFINITY,Double.POSITIVE_INFINITY);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long nextIntervalCount = connectionStatusAnalytics.getNextIntervalCount(flowFileEvent);
        assertNotNull(nextIntervalCount);
        assert (nextIntervalCount == -1);
    }

    @Test
    public void testMissingModelGetNextIntervalCount() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("fakeModel",Double.POSITIVE_INFINITY,Double.POSITIVE_INFINITY,Double.POSITIVE_INFINITY);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        try {
            connectionStatusAnalytics.getNextIntervalCount(flowFileEvent);
            fail();
        }catch(IllegalArgumentException iae){
            assertTrue(true);
        }
    }

    @Test
    public void testGetNextIntervalPercentageUseCount() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedCount",.9,50.0,1.0);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long percentage = connectionStatusAnalytics.getNextIntervalPercentageUseCount(connection, flowFileEvent);
        assertNotNull(percentage);
        assert (percentage == 50);
    }

    @Test
    public void testGetNextIntervalPercentageUseBytes() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedBytes",.9,10000000.0,1.0);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long percentage = connectionStatusAnalytics.getNextIntervalPercentageUseBytes(connection, flowFileEvent);
        assertNotNull(percentage);
        assert (percentage == 10);
    }

    @Test
    public void testGetScores() {
        Date now = new Date();
        Long tomorrowMillis = DateUtils.addDays(now,1).toInstant().toEpochMilli();
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> bytesModelMap = getModelMap("queuedBytes",.9,10000000.0,tomorrowMillis.doubleValue());
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> countModelMap = getModelMap("queuedCount",.9,50.0,tomorrowMillis.doubleValue());
        countModelMap.putAll(bytesModelMap);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(countModelMap);
        connectionStatusAnalytics.loadPredictions(repositoryStatusReport);
        Map<String,Long> scores = connectionStatusAnalytics.getPredictions();
        assertNotNull(scores);
        assertFalse(scores.isEmpty());
        assertTrue(scores.get("nextIntervalPercentageUseCount").equals(50L));
        assertTrue(scores.get("nextIntervalBytes").equals(10000000L));
        assertTrue(scores.get("timeToBytesBackpressureMillis") > 0);
        assertTrue(scores.get("nextIntervalCount").equals(50L));
        assertTrue(scores.get("nextIntervalPercentageUseBytes").equals(10L));
        assertTrue(scores.get("intervalTimeMillis").equals(180000L));
        assertTrue(scores.get("timeToCountBackpressureMillis") > 0);
    }

    @Test
    public void testGetScoresWithBadModel() {
        Date now = new Date();
        Long tomorrowMillis = DateUtils.addDays(now,1).toInstant().toEpochMilli();
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> bytesModelMap = getModelMap("queuedBytes",.9,10000000.0,tomorrowMillis.doubleValue());
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> countModelMap = getModelMap("queuedCount",.1,50.0,tomorrowMillis.doubleValue());
        countModelMap.putAll(bytesModelMap);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(countModelMap);
        connectionStatusAnalytics.loadPredictions(repositoryStatusReport);
        Map<String,Long> scores = connectionStatusAnalytics.getPredictions();
        assertNotNull(scores);
        assertFalse(scores.isEmpty());
        assertTrue(scores.get("nextIntervalPercentageUseCount").equals(-1L));
        assertTrue(scores.get("nextIntervalBytes").equals(10000000L));
        assertTrue(scores.get("timeToBytesBackpressureMillis") > 0);
        assertTrue(scores.get("nextIntervalCount").equals(-1L));
        assertTrue(scores.get("nextIntervalPercentageUseBytes").equals(10L));
        assertTrue(scores.get("intervalTimeMillis").equals(180000L));
        assertTrue(scores.get("timeToCountBackpressureMillis") == -1);
    }
}
