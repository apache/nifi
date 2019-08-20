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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.repository.FlowFileEvent;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.repository.RepositoryStatusReport;
import org.apache.nifi.controller.status.history.ComponentStatusRepository;
import org.apache.nifi.controller.status.history.ConnectionStatusDescriptor;
import org.apache.nifi.controller.status.history.MetricDescriptor;
import org.apache.nifi.controller.status.history.StandardStatusSnapshot;
import org.apache.nifi.controller.status.history.StatusHistory;
import org.apache.nifi.controller.status.history.StatusSnapshot;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.nar.StandardExtensionDiscoveringManager;
import org.apache.nifi.nar.SystemBundle;
import org.apache.nifi.util.NiFiProperties;
import org.junit.Test;
import org.mockito.Mockito;

public class TestConnectionStatusAnalytics {

    private static final Set<MetricDescriptor<?>> CONNECTION_METRICS = Arrays.stream(ConnectionStatusDescriptor.values())
            .map(ConnectionStatusDescriptor::getDescriptor)
            .collect(Collectors.toSet());

    protected ConnectionStatusAnalytics getConnectionStatusAnalytics(Long queuedBytes, Long queuedCount, String backPressureDataSizeThreshhold,
                                                                     Long backPressureObjectThreshold, Boolean isConstantStatus) {

        ComponentStatusRepository statusRepository = Mockito.mock(ComponentStatusRepository.class);
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
        final Connection connection = Mockito.mock(Connection.class);
        final FlowFileQueue flowFileQueue = Mockito.mock(FlowFileQueue.class);
        final FlowFileEventRepository flowFileEventRepository = Mockito.mock(FlowFileEventRepository.class);
        final RepositoryStatusReport repositoryStatusReport = Mockito.mock(RepositoryStatusReport.class);
        final FlowFileEvent flowFileEvent = Mockito.mock(FlowFileEvent.class);


        final List<Connection> connections = new ArrayList<>();
        final String connectionIdentifier = "1";
        connections.add(connection);

        List<StatusSnapshot> snapshotList = new ArrayList<>();
        final long startTime = System.currentTimeMillis();
        int iterations = 10;

        Long inputBytes = queuedBytes * 2;
        Long outputBytes = inputBytes - queuedBytes;
        Long inputCount = queuedCount * 2;
        Long outputCount = inputCount - queuedCount;

        for (int i = 0; i < iterations; i++) {
            final StandardStatusSnapshot snapshot = new StandardStatusSnapshot(CONNECTION_METRICS);
            snapshot.setTimestamp(new Date(startTime + i * 1000));
            snapshot.addStatusMetric(ConnectionStatusDescriptor.QUEUED_BYTES.getDescriptor(), (isConstantStatus || i < 5) ? queuedBytes : queuedBytes * 2);
            snapshot.addStatusMetric(ConnectionStatusDescriptor.QUEUED_COUNT.getDescriptor(), (isConstantStatus || i < 5) ? queuedCount : queuedCount * 2);
            snapshot.addStatusMetric(ConnectionStatusDescriptor.INPUT_BYTES.getDescriptor(), (isConstantStatus || i < 5) ? inputBytes : inputBytes * 2);
            snapshot.addStatusMetric(ConnectionStatusDescriptor.INPUT_COUNT.getDescriptor(), (isConstantStatus || i < 5) ? inputCount : inputCount * 2);
            snapshot.addStatusMetric(ConnectionStatusDescriptor.OUTPUT_BYTES.getDescriptor(), (isConstantStatus || i < 5) ? outputBytes : outputBytes * 2);
            snapshot.addStatusMetric(ConnectionStatusDescriptor.OUTPUT_COUNT.getDescriptor(), (isConstantStatus || i < 5) ? outputCount : outputCount * 2);
            snapshotList.add(snapshot);
        }

        when(flowFileQueue.getBackPressureDataSizeThreshold()).thenReturn(backPressureDataSizeThreshhold);
        when(flowFileQueue.getBackPressureObjectThreshold()).thenReturn(backPressureObjectThreshold);
        when(connection.getIdentifier()).thenReturn(connectionIdentifier);
        when(connection.getFlowFileQueue()).thenReturn(flowFileQueue);
        when(processGroup.findAllConnections()).thenReturn(connections);
        when(statusHistory.getStatusSnapshots()).thenReturn(snapshotList);
        when(flowManager.getRootGroup()).thenReturn(processGroup);
        when(flowFileEvent.getContentSizeIn()).thenReturn(inputBytes);
        when(flowFileEvent.getContentSizeOut()).thenReturn(outputBytes);
        when(flowFileEvent.getFlowFilesIn()).thenReturn(inputCount.intValue());
        when(flowFileEvent.getFlowFilesOut()).thenReturn(outputCount.intValue());
        when(flowFileEventRepository.reportTransferEvents(anyLong())).thenReturn(repositoryStatusReport);
        when(repositoryStatusReport.getReportEntry(anyString())).thenReturn(flowFileEvent);
        when(statusRepository.getConnectionStatusHistory(anyString(), any(), any(), anyInt())).thenReturn(statusHistory);

        ConnectionStatusAnalytics connectionStatusAnalytics = new ConnectionStatusAnalytics(statusRepository, flowManager,flowFileEventRepository, connectionIdentifier, false);
        connectionStatusAnalytics.init(StatusAnalyticsModelMapFactory.getConnectionStatusModelMap(extensionManager,nifiProperties));
        connectionStatusAnalytics.setScoreThreshold(.9);
        connectionStatusAnalytics.setScoreName("rSquared");
        return connectionStatusAnalytics;
    }

    @Test
    public void testGetIntervalTimeMillis() {
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(5000L, 50L, "100MB", 100L, true);
        Long interval = connectionStatusAnalytics.getIntervalTimeMillis();
        assertNotNull(interval);
        assert (interval == 180000);
    }

    @Test
    public void testGetTimeToCountBackpressureMillisConstantStatus() {
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(5000L, 50L, "100MB", 100L, true);
        Long countTime = connectionStatusAnalytics.getTimeToCountBackpressureMillis();
        assertNotNull(countTime);
        assert (countTime > 0);
    }

    @Test
    public void testGetTimeToCountBackpressureMillisVaryingStatus() {
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(5000L, 50L, "100MB", 100L, false);
        Long countTime = connectionStatusAnalytics.getTimeToCountBackpressureMillis();
        assertNotNull(countTime);
        assert (countTime == -1L);
    }

    @Test
    public void testGetTimeToBytesBackpressureMillisConstantStatus() {
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(5000L, 50L, "100MB", 100L, true);
        Long bytesTime = connectionStatusAnalytics.getTimeToBytesBackpressureMillis();
        assertNotNull(bytesTime);
        assert (bytesTime == -1L || bytesTime == 0);
    }

    @Test
    public void testGetTimeToBytesBackpressureMillisVaryingStatus() {
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(5000L, 50L, "100MB", 100L, false);
        Long bytesTime = connectionStatusAnalytics.getTimeToBytesBackpressureMillis();
        assertNotNull(bytesTime);
        assert (bytesTime == -1L);
    }

    @Test
    public void testGetNextIntervalBytesConstantStatus() {
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(5000L, 50L, "100MB", 100L, true);
        Long nextBytes = connectionStatusAnalytics.getNextIntervalBytes();
        assertNotNull(nextBytes);
        assert (nextBytes == 5000L);
    }

    @Test
    public void testGetNextIntervalBytesVaryingStatus() {
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(5000L, 50L, "100MB", 100L, false);
        Long nextBytes = connectionStatusAnalytics.getNextIntervalBytes();
        assertNotNull(nextBytes);
        assert (nextBytes == -1L);
    }

    @Test
    public void testGetNextIntervalCountConstantStatus() {
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(5000L, 50L, "100MB", 100L, true);
        Long nextCount = connectionStatusAnalytics.getNextIntervalCount();
        assertNotNull(nextCount);
        assert (nextCount == 50L);
    }

    @Test
    public void testGetNextIntervalCountVaryingStatus() {

        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(5000L, 50L, "100MB", 100L, false);
        Long nextCount = connectionStatusAnalytics.getNextIntervalCount();
        assertNotNull(nextCount);
        assert (nextCount == -1L);
    }

    @Test
    public void testGetNextIntervalPercentageUseBytesConstantStatus() {
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(50000L, 50L, "1MB", 100L, true);
        Long nextBytesPercentage = connectionStatusAnalytics.getNextIntervalPercentageUseBytes();
        assertNotNull(nextBytesPercentage);
        assert (nextBytesPercentage == 5);
    }

    @Test
    public void testGetNextIntervalPercentageUseCountConstantStatus() {
        ConnectionStatusAnalytics connectionStatusAnalytics = getConnectionStatusAnalytics(5000L, 50L, "100MB", 100L, true);
        Long nextCountPercentage = connectionStatusAnalytics.getNextIntervalPercentageUseCount();
        assertNotNull(nextCountPercentage);
        assert (nextCountPercentage == 50);
    }

}
