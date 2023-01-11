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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.repository.FlowFileEvent;
import org.apache.nifi.controller.repository.RepositoryStatusReport;
import org.apache.nifi.controller.status.history.StatusHistory;
import org.apache.nifi.controller.status.history.StatusHistoryRepository;
import org.apache.nifi.nar.StandardExtensionDiscoveringManager;
import org.apache.nifi.nar.SystemBundle;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.Tuple;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

public class TestConnectionStatusAnalytics {

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
                                                                                Double targetPrediction, Number variablePrediction){
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = new HashMap<>();
        StatusAnalyticsModel model = Mockito.mock(StatusAnalyticsModel.class);
        StatusMetricExtractFunction extractFunction = Mockito.mock(StatusMetricExtractFunction.class);
        Tuple<StatusAnalyticsModel,StatusMetricExtractFunction> modelTuple = new Tuple<>(model,extractFunction);
        modelMap.put(predictionType,modelTuple);
        Map<String,Double> scores = new HashMap<>();
        scores.put("rSquared",score);

        Double[][] features = new Double[1][1];
        Double[] target = new Double[1];

        when(extractFunction.extractMetric(anyString(),any(StatusHistory.class))).then(
                (Answer<Tuple<Stream<Double[]>, Stream<Double>>>) invocationOnMock -> new Tuple<>(Stream.of(features), Stream.of(target))
        );

        when(model.getScores()).thenReturn(scores);
        when(model.predict(any(Double[].class))).thenReturn(targetPrediction);
        when(model.predictVariable(anyInt(),any(),any())).thenReturn(variablePrediction.doubleValue());
        return modelMap;

    }

    @Test
    public void testInvalidModelLowScore() {
        final Date now = new Date();
        final long tomorrowMillis = DateUtils.addDays(now,1).toInstant().toEpochMilli();
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedCount",.5,100.0, tomorrowMillis);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long countTime = connectionStatusAnalytics.getTimeToCountBackpressureMillis(connection, flowFileEvent);
        assertNotNull(countTime);
        assertEquals(-1, countTime);
    }

    @Test
    public void testInvalidModelNaNScore() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedCount",Double.NaN,Double.NaN,Double.NaN);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long countTime = connectionStatusAnalytics.getTimeToCountBackpressureMillis(connection, flowFileEvent);
        assertNotNull(countTime);
        assertEquals(-1, countTime);
    }

    @Test
    public void testInvalidModelInfiniteScore() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedCount",Double.POSITIVE_INFINITY,Double.POSITIVE_INFINITY,Double.POSITIVE_INFINITY);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long countTime = connectionStatusAnalytics.getTimeToCountBackpressureMillis(connection, flowFileEvent);
        assertNotNull(countTime);
        assertEquals(-1, countTime);
    }

    @Test
    public void testGetIntervalTimeMillis() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedCount",.9,100.0,100.0);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long interval = connectionStatusAnalytics.getIntervalTimeMillis();
        assertNotNull(interval);
        assertEquals(180000, interval);
    }

    @Test
    public void testGetTimeToCountBackpressureMillis() {
        final Date now = new Date();
        final long tomorrowMillis = DateUtils.addDays(now,1).toInstant().toEpochMilli();
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedCount",.9,100.0, tomorrowMillis);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long countTime = connectionStatusAnalytics.getTimeToCountBackpressureMillis(connection, flowFileEvent);
        assertNotNull(countTime);
        assertTrue(countTime > 0);
    }

    @Test
    public void testCannotPredictTimeToCountNaN() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedCount",.9,Double.NaN,Double.NaN);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long countTime = connectionStatusAnalytics.getTimeToCountBackpressureMillis(connection, flowFileEvent);
        assertNotNull(countTime);
        assertEquals(-1, countTime);
    }

    @Test
    public void testCannotPredictTimeToCountInfinite() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedCount",.9,Double.POSITIVE_INFINITY,Double.POSITIVE_INFINITY);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long countTime = connectionStatusAnalytics.getTimeToCountBackpressureMillis(connection, flowFileEvent);
        assertNotNull(countTime);
        assertEquals(-1, countTime);
    }

    @Test
    public void testCannotPredictTimeToCountNegative() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedCount",.9,-1.0,-1.0);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long countTime = connectionStatusAnalytics.getTimeToCountBackpressureMillis(connection, flowFileEvent);
        assertNotNull(countTime);
        assertEquals(-1, countTime);
    }

    @Test
    public void testMissingModelGetTimeToCount() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("fakeModel",Double.POSITIVE_INFINITY,Double.POSITIVE_INFINITY,Double.POSITIVE_INFINITY);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        assertThrows(IllegalArgumentException.class, () -> connectionStatusAnalytics.getTimeToCountBackpressureMillis(connection, flowFileEvent));
    }

    @Test
    public void testGetTimeToBytesBackpressureMillis() {
        final Date now = new Date();
        final long tomorrowMillis = DateUtils.addDays(now,1).toInstant().toEpochMilli();
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedBytes",.9,100.0, tomorrowMillis);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long countTime = connectionStatusAnalytics.getTimeToBytesBackpressureMillis(connection, flowFileEvent);
        assertNotNull(countTime);
        assertTrue(countTime > 0);
    }

    @Test
    public void testCannotPredictTimeToBytesNaN() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedBytes",.9,Double.NaN,Double.NaN);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long countTime = connectionStatusAnalytics.getTimeToBytesBackpressureMillis(connection, flowFileEvent);
        assertNotNull(countTime);
        assertEquals(-1, countTime);
    }

    @Test
    public void testCannotPredictTimeToBytesInfinite() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedBytes",.9,Double.POSITIVE_INFINITY,Double.POSITIVE_INFINITY);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long countTime = connectionStatusAnalytics.getTimeToBytesBackpressureMillis(connection, flowFileEvent);
        assertNotNull(countTime);
        assertEquals(-1, countTime);
    }

    @Test
    public void testCannotPredictTimeToBytesNegative() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedBytes",.9,-1.0,-1.0);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long countTime = connectionStatusAnalytics.getTimeToBytesBackpressureMillis(connection, flowFileEvent);
        assertNotNull(countTime);
        assertEquals(-1, countTime);
    }

    @Test
    public void testMissingModelGetTimeToBytes() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("fakeModel",Double.POSITIVE_INFINITY,Double.POSITIVE_INFINITY,Double.POSITIVE_INFINITY);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        assertThrows(IllegalArgumentException.class, () -> connectionStatusAnalytics.getTimeToBytesBackpressureMillis(connection, flowFileEvent));
    }

    @Test
    public void testGetNextIntervalBytes() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedBytes",.9,1.0,1.0);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long nextIntervalBytes = connectionStatusAnalytics.getNextIntervalBytes(flowFileEvent);
        assertNotNull(nextIntervalBytes);
        assertTrue(nextIntervalBytes > 0);
    }

    @Test
    public void testNextIntervalBytesZero() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedBytes",.9,-1.0,-1.0);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long nextIntervalBytes = connectionStatusAnalytics.getNextIntervalBytes(flowFileEvent);
        assertNotNull(nextIntervalBytes);
        assertEquals(0, nextIntervalBytes);
    }

    @Test
    public void testCannotPredictNextIntervalBytesNaN() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedBytes",.9,Double.NaN,Double.NaN);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long nextIntervalBytes = connectionStatusAnalytics.getNextIntervalBytes(flowFileEvent);
        assertNotNull(nextIntervalBytes);
        assertEquals(-1, nextIntervalBytes);
    }

    @Test
    public void testCannotPredictNextIntervalBytesInfinity() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedBytes",.9,Double.POSITIVE_INFINITY,Double.POSITIVE_INFINITY);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long nextIntervalBytes = connectionStatusAnalytics.getNextIntervalBytes(flowFileEvent);
        assertNotNull(nextIntervalBytes);
        assertEquals(-1, nextIntervalBytes);
    }

    @Test
    public void testMissingModelNextIntervalBytes() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("fakeModel",Double.POSITIVE_INFINITY,Double.POSITIVE_INFINITY,Double.POSITIVE_INFINITY);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        assertThrows(IllegalArgumentException.class, () -> connectionStatusAnalytics.getNextIntervalBytes(flowFileEvent));
    }

    @Test
    public void testGetNextIntervalCount() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedCount",.9,1.0,1.0);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long nextIntervalBytes = connectionStatusAnalytics.getNextIntervalCount(flowFileEvent);
        assertNotNull(nextIntervalBytes);
        assertTrue(nextIntervalBytes > 0);
    }

    @Test
    public void testGetNextIntervalCountZero() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedCount",.9,-1.0,-1.0);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long nextIntervalCount = connectionStatusAnalytics.getNextIntervalCount(flowFileEvent);
        assertNotNull(nextIntervalCount);
        assertEquals(0, nextIntervalCount);
    }

    @Test
    public void testCannotPredictNextIntervalCountNaN() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedCount",.9,Double.NaN,Double.NaN);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long nextIntervalCount = connectionStatusAnalytics.getNextIntervalCount(flowFileEvent);
        assertNotNull(nextIntervalCount);
        assertEquals(-1, nextIntervalCount);
    }

    @Test
    public void testCannotPredictNextIntervalCountInfinity() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedCount",.9,Double.POSITIVE_INFINITY,Double.POSITIVE_INFINITY);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long nextIntervalCount = connectionStatusAnalytics.getNextIntervalCount(flowFileEvent);
        assertNotNull(nextIntervalCount);
        assertEquals(-1, nextIntervalCount);
    }

    @Test
    public void testMissingModelGetNextIntervalCount() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("fakeModel",Double.POSITIVE_INFINITY,Double.POSITIVE_INFINITY,Double.POSITIVE_INFINITY);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        assertThrows(IllegalArgumentException.class, () -> connectionStatusAnalytics.getNextIntervalCount(flowFileEvent));
    }

    @Test
    public void testGetNextIntervalPercentageUseCount() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedCount",.9,50.0,1.0);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long percentage = connectionStatusAnalytics.getNextIntervalPercentageUseCount(connection, flowFileEvent);
        assertNotNull(percentage);
        assertEquals(50, percentage);
    }

    @Test
    public void testGetNextIntervalPercentageUseBytes() {
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = getModelMap("queuedBytes",.9,10000000.0,1.0);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(modelMap);
        Long percentage = connectionStatusAnalytics.getNextIntervalPercentageUseBytes(connection, flowFileEvent);
        assertNotNull(percentage);
        assertEquals(10, percentage);
    }

    @Test
    public void testGetScores() {
        final Date now = new Date();
        final long tomorrowMillis = DateUtils.addDays(now,1).toInstant().toEpochMilli();
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> bytesModelMap = getModelMap("queuedBytes",.9,10000000.0, tomorrowMillis);
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> countModelMap = getModelMap("queuedCount",.9,50.0, tomorrowMillis);
        countModelMap.putAll(bytesModelMap);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(countModelMap);
        connectionStatusAnalytics.loadPredictions(repositoryStatusReport);
        Map<String,Long> scores = connectionStatusAnalytics.getPredictions();
        assertNotNull(scores);
        assertFalse(scores.isEmpty());
        assertEquals(50L, scores.get("nextIntervalPercentageUseCount"));
        assertEquals(10000000L, scores.get("nextIntervalBytes"));
        assertTrue(scores.get("timeToBytesBackpressureMillis") > 0);
        assertEquals(50L, scores.get("nextIntervalCount"));
        assertEquals(10L, scores.get("nextIntervalPercentageUseBytes"));
        assertEquals(180000L, scores.get("intervalTimeMillis"));
        assertTrue(scores.get("timeToCountBackpressureMillis") > 0);
    }

    @Test
    public void testGetScoresWithBadModel() {
        final Date now = new Date();
        final long tomorrowMillis = DateUtils.addDays(now,1).toInstant().toEpochMilli();
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> bytesModelMap = getModelMap("queuedBytes",.9,10000000.0, tomorrowMillis);
        Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> countModelMap = getModelMap("queuedCount",.1,50.0, tomorrowMillis);
        countModelMap.putAll(bytesModelMap);
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(countModelMap);
        connectionStatusAnalytics.loadPredictions(repositoryStatusReport);
        Map<String,Long> scores = connectionStatusAnalytics.getPredictions();
        assertNotNull(scores);
        assertFalse(scores.isEmpty());
        assertEquals(-1L, scores.get("nextIntervalPercentageUseCount"));
        assertEquals(10000000L, scores.get("nextIntervalBytes"));
        assertTrue(scores.get("timeToBytesBackpressureMillis") > 0);
        assertEquals(-1L, scores.get("nextIntervalCount"));
        assertEquals(10L, scores.get("nextIntervalPercentageUseBytes"));
        assertEquals(180000L, scores.get("intervalTimeMillis"));
        assertEquals(-1, scores.get("timeToCountBackpressureMillis"));
    }
}
