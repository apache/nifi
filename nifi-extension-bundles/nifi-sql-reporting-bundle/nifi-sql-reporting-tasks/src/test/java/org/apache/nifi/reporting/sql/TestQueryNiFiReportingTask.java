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
package org.apache.nifi.reporting.sql;


import org.apache.nifi.action.Action;
import org.apache.nifi.action.Component;
import org.apache.nifi.action.FlowChangeAction;
import org.apache.nifi.action.Operation;
import org.apache.nifi.action.details.FlowChangeConfigureDetails;
import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.analytics.ConnectionStatusPredictions;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.provenance.MockProvenanceEvent;
import org.apache.nifi.provenance.ProvenanceEventBuilder;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.record.sink.MockRecordSinkService;
import org.apache.nifi.record.sink.RecordSinkService;
import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.BulletinFactory;
import org.apache.nifi.reporting.BulletinQuery;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.reporting.ComponentType;
import org.apache.nifi.reporting.EventAccess;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.ReportingInitializationContext;
import org.apache.nifi.reporting.sql.util.QueryMetricsUtil;
import org.apache.nifi.reporting.sql.util.TrackedQueryTime;
import org.apache.nifi.reporting.util.metrics.MetricNames;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.util.MockBulletinRepository;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.MockPropertyValue;
import org.apache.nifi.util.SharedSessionState;
import org.apache.nifi.util.db.JdbcProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;

class TestQueryNiFiReportingTask {

    private ReportingContext context;
    private MockQueryNiFiReportingTask reportingTask;
    private MockRecordSinkService mockRecordSinkService;
    private ProcessGroupStatus status;
    private BulletinRepository mockBulletinRepository;
    private MockProvenanceRepository mockProvenanceRepository;
    private AtomicLong currentTime;
    private MockStateManager mockStateManager;
    private List<Action> flowConfigHistory;

    @BeforeEach
    public void setup() {
        currentTime = new AtomicLong();
        status = new ProcessGroupStatus();
        status.setId("1234");
        status.setFlowFilesReceived(5);
        status.setBytesReceived(10000);
        status.setFlowFilesSent(10);
        status.setBytesRead(20000L);
        status.setBytesSent(20000);
        status.setQueuedCount(100);
        status.setQueuedContentSize(1024L);
        status.setBytesWritten(80000L);
        status.setActiveThreadCount(5);

        // create a processor status with processing time
        ProcessorStatus procStatus = new ProcessorStatus();
        procStatus.setId("proc");
        procStatus.setName("Processor 1");
        procStatus.setProcessingNanos(123456789);

        Collection<ProcessorStatus> processorStatuses = new ArrayList<>();
        processorStatuses.add(procStatus);
        status.setProcessorStatus(processorStatuses);

        ConnectionStatus root1ConnectionStatus = new ConnectionStatus();
        root1ConnectionStatus.setId("root1");
        root1ConnectionStatus.setQueuedCount(1000);
        root1ConnectionStatus.setBackPressureObjectThreshold(1000);
        // Set backpressure predictions
        ConnectionStatusPredictions connectionStatusPredictions1 = new ConnectionStatusPredictions();
        connectionStatusPredictions1.setPredictedTimeToCountBackpressureMillis(2000);
        connectionStatusPredictions1.setPredictedTimeToBytesBackpressureMillis(2000);
        connectionStatusPredictions1.setNextPredictedQueuedBytes(1024);
        connectionStatusPredictions1.setNextPredictedQueuedCount(1);
        connectionStatusPredictions1.setPredictedPercentBytes(55);
        connectionStatusPredictions1.setPredictedPercentCount(30);
        root1ConnectionStatus.setPredictions(connectionStatusPredictions1);

        ConnectionStatus root2ConnectionStatus = new ConnectionStatus();
        root2ConnectionStatus.setId("root2");
        root2ConnectionStatus.setQueuedCount(500);
        root2ConnectionStatus.setBackPressureObjectThreshold(1000);
        // Set backpressure predictions
        ConnectionStatusPredictions connectionStatusPredictions2 = new ConnectionStatusPredictions();
        connectionStatusPredictions2.setPredictedTimeToBytesBackpressureMillis(2000);
        connectionStatusPredictions2.setPredictedTimeToBytesBackpressureMillis(2000);
        connectionStatusPredictions2.setNextPredictedQueuedBytes(1024);
        connectionStatusPredictions2.setNextPredictedQueuedCount(1);
        connectionStatusPredictions2.setPredictedPercentBytes(55);
        connectionStatusPredictions2.setPredictedPercentCount(30);
        root2ConnectionStatus.setPredictions(connectionStatusPredictions2);

        Collection<ConnectionStatus> rootConnectionStatuses = new ArrayList<>();
        rootConnectionStatuses.add(root1ConnectionStatus);
        rootConnectionStatuses.add(root2ConnectionStatus);
        status.setConnectionStatus(rootConnectionStatuses);

        // create a group status with processing time
        ProcessGroupStatus groupStatus1 = new ProcessGroupStatus();
        groupStatus1.setProcessorStatus(processorStatuses);
        groupStatus1.setBytesRead(1234L);

        // Create a nested group status with a connection
        ProcessGroupStatus groupStatus2 = new ProcessGroupStatus();
        groupStatus2.setProcessorStatus(processorStatuses);
        groupStatus2.setBytesRead(12345L);
        ConnectionStatus nestedConnectionStatus = new ConnectionStatus();
        nestedConnectionStatus.setId("nested");
        nestedConnectionStatus.setQueuedCount(1001);
        // Set backpressure predictions
        ConnectionStatusPredictions connectionStatusPredictions3 = new ConnectionStatusPredictions();
        connectionStatusPredictions3.setPredictedTimeToBytesBackpressureMillis(2000);
        connectionStatusPredictions3.setPredictedTimeToBytesBackpressureMillis(2000);
        connectionStatusPredictions3.setNextPredictedQueuedBytes(1024);
        connectionStatusPredictions3.setNextPredictedQueuedCount(1);
        connectionStatusPredictions3.setPredictedPercentBytes(55);
        connectionStatusPredictions3.setPredictedPercentCount(30);
        nestedConnectionStatus.setPredictions(connectionStatusPredictions3);
        Collection<ConnectionStatus> nestedConnectionStatuses = new ArrayList<>();
        nestedConnectionStatuses.add(nestedConnectionStatus);
        groupStatus2.setConnectionStatus(nestedConnectionStatuses);
        Collection<ProcessGroupStatus> nestedGroupStatuses = new ArrayList<>();
        nestedGroupStatuses.add(groupStatus2);
        groupStatus1.setProcessGroupStatus(nestedGroupStatuses);

        ProcessGroupStatus groupStatus3 = new ProcessGroupStatus();
        groupStatus3.setBytesRead(1L);
        ConnectionStatus nestedConnectionStatus2 = new ConnectionStatus();
        nestedConnectionStatus2.setId("nested2");
        nestedConnectionStatus2.setQueuedCount(3);
        // Set backpressure predictions
        ConnectionStatusPredictions connectionStatusPredictions4 = new ConnectionStatusPredictions();
        connectionStatusPredictions4.setPredictedTimeToBytesBackpressureMillis(2000);
        connectionStatusPredictions4.setPredictedTimeToBytesBackpressureMillis(2000);
        connectionStatusPredictions4.setNextPredictedQueuedBytes(1024);
        connectionStatusPredictions4.setNextPredictedQueuedCount(1);
        connectionStatusPredictions4.setPredictedPercentBytes(55);
        connectionStatusPredictions4.setPredictedPercentCount(30);
        nestedConnectionStatus2.setPredictions(connectionStatusPredictions4);
        Collection<ConnectionStatus> nestedConnectionStatuses2 = new ArrayList<>();
        nestedConnectionStatuses2.add(nestedConnectionStatus2);
        groupStatus3.setConnectionStatus(nestedConnectionStatuses2);

        Collection<ProcessGroupStatus> groupStatuses = new ArrayList<>();
        groupStatuses.add(groupStatus1);
        groupStatuses.add(groupStatus3);
        status.setProcessGroupStatus(groupStatuses);

        // Populate flow config history
        FlowChangeAction action1 = new FlowChangeAction();
        action1.setId(123);
        action1.setTimestamp(new Date());
        action1.setUserIdentity("test");
        action1.setSourceId("proc");
        action1.setSourceName("Processor 1");
        action1.setSourceType(Component.Processor);
        action1.setOperation(Operation.Configure);
        FlowChangeConfigureDetails configureDetails1 = new FlowChangeConfigureDetails();
        configureDetails1.setName("property1");
        configureDetails1.setPreviousValue("1");
        configureDetails1.setValue("2");
        action1.setActionDetails(configureDetails1);
        flowConfigHistory = Collections.singletonList(action1);
    }

    @Test
    void testConnectionStatusTable() throws InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(QueryMetricsUtil.RECORD_SINK, "mock-record-sink");
        properties.put(QueryMetricsUtil.QUERY, "select id,queuedCount,isBackPressureEnabled from CONNECTION_STATUS order by queuedCount desc");
        reportingTask = initTask(properties);
        reportingTask.onTrigger(context);

        List<Map<String, Object>> rows = mockRecordSinkService.getRows();
        assertEquals(4, rows.size());
        // Validate the first row
        Map<String, Object> row = rows.getFirst();
        assertEquals(3, row.size()); // Only projected 2 columns
        Object id = row.get("id");

        assertInstanceOf(String.class, id);
        assertEquals("nested", id);
        assertEquals(1001, row.get("queuedCount"));
        // Validate the second row
        row = rows.get(1);
        id = row.get("id");
        assertEquals("root1", id);
        assertEquals(1000, row.get("queuedCount"));
        assertEquals(true, row.get("isBackPressureEnabled"));
        // Validate the third row
        row = rows.get(2);
        id = row.get("id");
        assertEquals("root2", id);
        assertEquals(500, row.get("queuedCount"));
        assertEquals(false, row.get("isBackPressureEnabled"));
        // Validate the fourth row
        row = rows.get(3);
        id = row.get("id");
        assertEquals("nested2", id);
        assertEquals(3, row.get("queuedCount"));
    }

    @Test
    void testConnectionStatusTableJoin() throws InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(QueryMetricsUtil.RECORD_SINK, "mock-record-sink");
        properties.put(QueryMetricsUtil.QUERY, "SELECT id "
                + "FROM CONNECTION_STATUS "
                + "JOIN CONNECTION_STATUS_PREDICTIONS ON CONNECTION_STATUS_PREDICTIONS.connectionId = CONNECTION_STATUS.id");
        reportingTask = initTask(properties);
        reportingTask.onTrigger(context);

        List<Map<String, Object>> rows = mockRecordSinkService.getRows();
        assertEquals(4, rows.size());
    }

    @Test
    void testBulletinIsInTimeWindow() throws InitializationException {
        String query = "select * from BULLETINS where bulletinTimestamp > $bulletinStartTime and bulletinTimestamp <= $bulletinEndTime";

        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(QueryMetricsUtil.QUERY, query);
        reportingTask = initTask(properties);
        currentTime.set(Instant.now().toEpochMilli());
        reportingTask.onTrigger(context);

        List<Map<String, Object>> rows = mockRecordSinkService.getRows();
        assertEquals(3, rows.size());

        final Bulletin bulletin = BulletinFactory.createBulletin(ComponentType.INPUT_PORT.name().toLowerCase(), "ERROR", "test bulletin 3", "testFlowFileUuid");
        mockBulletinRepository.addBulletin(bulletin);
        currentTime.set(bulletin.getTimestamp().getTime());

        reportingTask.onTrigger(context);


        List<Map<String, Object>> sameRows = mockRecordSinkService.getRows();
        assertEquals(1, sameRows.size());
    }

    @Test
    void testBulletinIsOutOfTimeWindow() throws InitializationException {
        String query = "select * from BULLETINS where bulletinTimestamp > $bulletinStartTime and bulletinTimestamp <= $bulletinEndTime";

        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(QueryMetricsUtil.QUERY, query);
        reportingTask = initTask(properties);
        currentTime.set(Instant.now().toEpochMilli());
        reportingTask.onTrigger(context);

        List<Map<String, Object>> rows = mockRecordSinkService.getRows();
        assertEquals(3, rows.size());

        final Bulletin bulletin = BulletinFactory.createBulletin("input port", "ERROR", "test bulletin 3", "testFlowFileUuid");
        mockBulletinRepository.addBulletin(bulletin);
        currentTime.set(bulletin.getTimestamp().getTime() - 1);

        reportingTask.onTrigger(context);

        List<Map<String, Object>> sameRows = mockRecordSinkService.getRows();
        assertEquals(0, sameRows.size());
    }

    @Test
    void testProvenanceEventIsInTimeWindow() throws InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(QueryMetricsUtil.QUERY, "select * from PROVENANCE where timestampMillis > $provenanceStartTime and timestampMillis <= $provenanceEndTime");
        reportingTask = initTask(properties);
        currentTime.set(Instant.now().toEpochMilli());
        reportingTask.onTrigger(context);

        List<Map<String, Object>> rows = mockRecordSinkService.getRows();
        assertEquals(1001, rows.size());

        MockFlowFile mockFlowFile = new MockFlowFile(1002L);
        ProvenanceEventRecord prov1002 = mockProvenanceRepository.eventBuilder()
                .setEventType(ProvenanceEventType.CREATE)
                .fromFlowFile(mockFlowFile)
                .setComponentId("12345")
                .setComponentType("ReportingTask")
                .setFlowFileUUID("I am FlowFile 1")
                .setEventTime(Instant.now().toEpochMilli())
                .setEventDuration(100)
                .setTransitUri("test://")
                .setSourceSystemFlowFileIdentifier("I am FlowFile 1")
                .setAlternateIdentifierUri("remote://test")
                .build();

        mockProvenanceRepository.registerEvent(prov1002);

        currentTime.set(prov1002.getEventTime());
        reportingTask.onTrigger(context);

        List<Map<String, Object>> sameRows = mockRecordSinkService.getRows();
        assertEquals(1, sameRows.size());
    }

    @Test
    void testProvenanceEventIsOutOfTimeWindow() throws InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(QueryMetricsUtil.RECORD_SINK, "mock-record-sink");
        properties.put(QueryMetricsUtil.QUERY, "select * from PROVENANCE where timestampMillis > $provenanceStartTime and timestampMillis <= $provenanceEndTime");
        reportingTask = initTask(properties);
        currentTime.set(Instant.now().toEpochMilli());
        reportingTask.onTrigger(context);

        List<Map<String, Object>> rows = mockRecordSinkService.getRows();
        assertEquals(1001, rows.size());

        MockFlowFile mockFlowFile = new MockFlowFile(1002L);
        ProvenanceEventRecord prov1002 = mockProvenanceRepository.eventBuilder()
                .setEventType(ProvenanceEventType.CREATE)
                .fromFlowFile(mockFlowFile)
                .setComponentId("12345")
                .setComponentType("ReportingTask")
                .setFlowFileUUID("I am FlowFile 1")
                .setEventTime(Instant.now().toEpochMilli())
                .setEventDuration(100)
                .setTransitUri("test://")
                .setSourceSystemFlowFileIdentifier("I am FlowFile 1")
                .setAlternateIdentifierUri("remote://test")
                .build();

        mockProvenanceRepository.registerEvent(prov1002);

        currentTime.set(prov1002.getEventTime() - 1);
        reportingTask.onTrigger(context);

        List<Map<String, Object>> sameRows = mockRecordSinkService.getRows();
        assertEquals(0, sameRows.size());
    }

    @Test
    void testUniqueProvenanceAndBulletinQuery() throws InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(QueryMetricsUtil.QUERY, "select * from BULLETINS, PROVENANCE where " +
                "bulletinTimestamp > $bulletinStartTime and bulletinTimestamp <= $bulletinEndTime " +
                "and timestampMillis > $provenanceStartTime and timestampMillis <= $provenanceEndTime LIMIT 10");
        reportingTask = initTask(properties);
        currentTime.set(Instant.now().toEpochMilli());
        reportingTask.onTrigger(context);

        List<Map<String, Object>> rows = mockRecordSinkService.getRows();
        assertEquals(10, rows.size());

        final Bulletin bulletin = BulletinFactory.createBulletin(ComponentType.INPUT_PORT.name().toLowerCase(), "ERROR", "test bulletin 3", "testFlowFileUuid");
        mockBulletinRepository.addBulletin(bulletin);

        MockFlowFile mockFlowFile = new MockFlowFile(1002L);
        ProvenanceEventRecord prov1002 = mockProvenanceRepository.eventBuilder()
                .setEventType(ProvenanceEventType.CREATE)
                .fromFlowFile(mockFlowFile)
                .setComponentId("12345")
                .setComponentType("ReportingTask")
                .setFlowFileUUID("I am FlowFile 1")
                .build();

        mockProvenanceRepository.registerEvent(prov1002);

        currentTime.set(bulletin.getTimestamp().toInstant().plusSeconds(1).toEpochMilli());
        reportingTask.onTrigger(context);

        List<Map<String, Object>> sameRows = mockRecordSinkService.getRows();
        assertEquals(1, sameRows.size());
    }

    @Test
    void testTimeWindowFromStateMap() throws IOException, InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(QueryMetricsUtil.RECORD_SINK, "mock-record-sink");
        properties.put(QueryMetricsUtil.QUERY, "select * from BULLETINS, PROVENANCE where " +
                "bulletinTimestamp > $bulletinStartTime and bulletinTimestamp <= $bulletinEndTime " +
                "and timestampMillis > $provenanceStartTime and timestampMillis <= $provenanceEndTime");
        reportingTask = initTask(properties);

        long testBulletinStartTime = 1609538145L;
        long testProvenanceStartTime = 1641074145L;
        final Map<String, String> stateMap = new HashMap<>();
        stateMap.put(TrackedQueryTime.BULLETIN_START_TIME.name(), String.valueOf(testBulletinStartTime));
        stateMap.put(TrackedQueryTime.PROVENANCE_START_TIME.name(), String.valueOf(testProvenanceStartTime));
        mockStateManager.setState(stateMap, Scope.LOCAL);

        final long bulletinStartTime = Long.parseLong(context.getStateManager().getState(Scope.LOCAL).get(TrackedQueryTime.BULLETIN_START_TIME.name()));
        final long provenanceStartTime = Long.parseLong(context.getStateManager().getState(Scope.LOCAL).get(TrackedQueryTime.PROVENANCE_START_TIME.name()));

        assertEquals(testBulletinStartTime, bulletinStartTime);
        assertEquals(testProvenanceStartTime, provenanceStartTime);

        final long currentTime = Instant.now().toEpochMilli();
        this.currentTime.set(currentTime);

        reportingTask.onTrigger(context);

        final long updatedBulletinStartTime = Long.parseLong(context.getStateManager().getState(Scope.LOCAL).get(TrackedQueryTime.BULLETIN_START_TIME.name()));
        final long updatedProvenanceStartTime = Long.parseLong(context.getStateManager().getState(Scope.LOCAL).get(TrackedQueryTime.PROVENANCE_START_TIME.name()));

        assertEquals(currentTime, updatedBulletinStartTime);
        assertEquals(currentTime, updatedProvenanceStartTime);
    }

    @Test
    void testJvmMetricsTable() throws InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(QueryMetricsUtil.RECORD_SINK, "mock-record-sink");
        properties.put(QueryMetricsUtil.QUERY, "select "
                + Stream.of(MetricNames.JVM_DAEMON_THREAD_COUNT,
                MetricNames.JVM_THREAD_COUNT,
                MetricNames.JVM_THREAD_STATES_BLOCKED,
                MetricNames.JVM_THREAD_STATES_RUNNABLE,
                MetricNames.JVM_THREAD_STATES_TERMINATED,
                MetricNames.JVM_THREAD_STATES_TIMED_WAITING,
                MetricNames.JVM_UPTIME,
                MetricNames.JVM_HEAP_USED,
                MetricNames.JVM_HEAP_USAGE,
                MetricNames.JVM_NON_HEAP_USAGE,
                MetricNames.JVM_FILE_DESCRIPTOR_USAGE).map((s) -> s.replace(".", "_")).collect(Collectors.joining(","))
                + " from JVM_METRICS");
        reportingTask = initTask(properties);
        reportingTask.onTrigger(context);

        List<Map<String, Object>> rows = mockRecordSinkService.getRows();
        assertEquals(1, rows.size());
        Map<String, Object> row = rows.getFirst();
        assertEquals(11, row.size());

        assertInstanceOf(Integer.class, row.get(MetricNames.JVM_DAEMON_THREAD_COUNT.replace(".", "_")));
        assertInstanceOf(Double.class, row.get(MetricNames.JVM_HEAP_USAGE.replace(".", "_")));
    }

    @Test
    void testProcessGroupStatusTable() throws InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(QueryMetricsUtil.RECORD_SINK, "mock-record-sink");
        properties.put(QueryMetricsUtil.QUERY, "select * from PROCESS_GROUP_STATUS order by bytesRead asc");
        reportingTask = initTask(properties);
        reportingTask.onTrigger(context);

        List<Map<String, Object>> rows = mockRecordSinkService.getRows();
        assertEquals(4, rows.size());
        // Validate the first row
        Map<String, Object> row = rows.getFirst();
        assertEquals(26, row.size());
        assertEquals(1L, row.get("bytesRead"));
        // Validate the second row
        row = rows.get(1);
        assertEquals(1234L, row.get("bytesRead"));
        // Validate the third row
        row = rows.get(2);
        assertEquals(12345L, row.get("bytesRead"));
        // Validate the fourth row
        row = rows.get(3);
        assertEquals(20000L, row.get("bytesRead"));
    }

    @Test
    void testNoResults() throws InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(QueryMetricsUtil.RECORD_SINK, "mock-record-sink");
        properties.put(QueryMetricsUtil.QUERY, "select * from CONNECTION_STATUS where queuedCount > 2000");
        reportingTask = initTask(properties);
        reportingTask.onTrigger(context);

        List<Map<String, Object>> rows = mockRecordSinkService.getRows();
        assertEquals(0, rows.size());
    }

    @Test
    void testProvenanceTable() throws InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(QueryMetricsUtil.RECORD_SINK, "mock-record-sink");
        properties.put(QueryMetricsUtil.QUERY, "select * from PROVENANCE order by eventId asc");
        reportingTask = initTask(properties);
        reportingTask.onTrigger(context);

        List<Map<String, Object>> rows = mockRecordSinkService.getRows();
        assertEquals(1001, rows.size());
        // Validate the first row
        Map<String, Object> row = rows.getFirst();
        assertEquals(24, row.size());
        // Verify the first row contents
        final Long firstEventId = (Long) row.get("eventId");
        assertEquals("CREATE", row.get("eventType"));
        assertEquals(12L, row.get("entitySize"));

        assertNull(row.get("contentPath"));
        assertNull(row.get("previousContentPath"));

        Object o = row.get("previousAttributes");
        assertInstanceOf(Map.class, o);
        Map<String, String> previousAttributes = (Map<String, String>) o;
        assertEquals("A", previousAttributes.get("test.value"));
        o = row.get("updatedAttributes");
        assertInstanceOf(Map.class, o);
        Map<String, String> updatedAttributes = (Map<String, String>) o;
        assertEquals("B", updatedAttributes.get("test.value"));

        // Verify some fields in the second row
        row = rows.get(1);
        assertEquals(24, row.size());
        // Verify the second row contents
        assertEquals(firstEventId + 1, row.get("eventId"));
        assertEquals("DROP", row.get("eventType"));

        // Verify some fields in the last row
        row = rows.get(1000);
        assertEquals(24, row.size());
        // Verify the last row contents
        assertEquals(firstEventId + 1000L, row.get("eventId"));
        assertEquals("DROP", row.get("eventType"));
    }

    @Test
    void testBulletinTable() throws InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(QueryMetricsUtil.RECORD_SINK, "mock-record-sink");
        properties.put(QueryMetricsUtil.QUERY, "select * from BULLETINS order by bulletinTimestamp asc");
        reportingTask = initTask(properties);
        reportingTask.onTrigger(context);

        final List<Map<String, Object>> rows = mockRecordSinkService.getRows();
        final String flowFileUuid = "testFlowFileUuid";
        assertEquals(3, rows.size());

        // Ensure that bulletins are properly ordered.
        for (int i = 1; i < 3; i++) {
            final Map<String, Object> values = rows.get(i);
            final long timestamp = (long) values.get("bulletinTimestamp");

            final Map<String, Object> previousValues = rows.get(i - 1);
            final long previousTimestamp = (long) previousValues.get("bulletinTimestamp");
            assertTrue(timestamp >= previousTimestamp);
        }

        // Validate the first row
        Map<String, Object> row = rows.getFirst();
        assertEquals(14, row.size());
        assertNotNull(row.get("bulletinId"));
        assertEquals("controller", row.get("bulletinCategory"));
        assertEquals("WARN", row.get("bulletinLevel"));
        assertEquals(flowFileUuid, row.get("bulletinFlowFileUuid"));

        // Validate the second row
        row = rows.get(1);
        assertEquals("processor", row.get("bulletinCategory"));
        assertEquals("INFO", row.get("bulletinLevel"));

        // Validate the third row
        row = rows.get(2);
        assertEquals("controller_service", row.get("bulletinCategory"));
        assertEquals("ERROR", row.get("bulletinLevel"));
        assertEquals(flowFileUuid, row.get("bulletinFlowFileUuid"));
    }

    @Test
    void testFlowConfigHistoryTable() throws InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(QueryMetricsUtil.RECORD_SINK, "mock-record-sink");
        properties.put(QueryMetricsUtil.QUERY, "select * from FLOW_CONFIG_HISTORY");
        reportingTask = initTask(properties);
        reportingTask.onTrigger(context);

        List<Map<String, Object>> rows = mockRecordSinkService.getRows();
        assertEquals(1, rows.size());
        // Validate the first row
        Map<String, Object> row = rows.getFirst();
        assertEquals(22, row.size());
        // Verify the first row contents
        assertEquals(123, row.get("actionId"));
        assertEquals("Configure", row.get("actionOperation"));
    }

    private MockQueryNiFiReportingTask initTask(final Map<PropertyDescriptor, String> customProperties) throws InitializationException {

        final ComponentLog logger = mock(ComponentLog.class);
        reportingTask = new MockQueryNiFiReportingTask();
        final ReportingInitializationContext initContext = mock(ReportingInitializationContext.class);
        Mockito.when(initContext.getIdentifier()).thenReturn(UUID.randomUUID().toString());
        Mockito.when(initContext.getLogger()).thenReturn(logger);
        reportingTask.initialize(initContext);

        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        for (final PropertyDescriptor descriptor : reportingTask.getSupportedPropertyDescriptors()) {
            properties.put(descriptor, descriptor.getDefaultValue());
        }
        properties.putAll(customProperties);

        context = mock(ReportingContext.class);

        mockStateManager = new MockStateManager(reportingTask);

        Mockito.when(context.getStateManager()).thenReturn(mockStateManager);
        Mockito.doAnswer((Answer<PropertyValue>) invocation -> {
            final PropertyDescriptor descriptor = invocation.getArgument(0, PropertyDescriptor.class);
            return new MockPropertyValue(properties.get(descriptor));
        }).when(context).getProperty(Mockito.any(PropertyDescriptor.class));

        final EventAccess eventAccess = mock(EventAccess.class);
        Mockito.when(context.getEventAccess()).thenReturn(eventAccess);
        Mockito.when(eventAccess.getControllerStatus()).thenReturn(status);
        Mockito.when(eventAccess.getFlowChanges(anyInt(), anyInt())).thenReturn(flowConfigHistory);

        final PropertyValue pValue = mock(StandardPropertyValue.class);
        mockRecordSinkService = new MockRecordSinkService();
        Mockito.when(context.getProperty(QueryMetricsUtil.RECORD_SINK)).thenReturn(pValue);
        Mockito.when(pValue.asControllerService(RecordSinkService.class)).thenReturn(mockRecordSinkService);

        ConfigurationContext configContext = mock(ConfigurationContext.class);
        Mockito.when(configContext.getProperty(QueryMetricsUtil.RECORD_SINK)).thenReturn(pValue);

        Mockito.when(configContext.getProperty(JdbcProperties.VARIABLE_REGISTRY_ONLY_DEFAULT_PRECISION)).thenReturn(new MockPropertyValue("10"));
        Mockito.when(configContext.getProperty(JdbcProperties.VARIABLE_REGISTRY_ONLY_DEFAULT_SCALE)).thenReturn(new MockPropertyValue("0"));
        reportingTask.setup(configContext);

        mockProvenanceRepository = new MockProvenanceRepository();
        long currentTimeMillis = System.currentTimeMillis();
        Map<String, String> previousAttributes = new HashMap<>();
        previousAttributes.put("mime.type", "application/json");
        previousAttributes.put("test.value", "A");
        Map<String, String> updatedAttributes = new HashMap<>(previousAttributes);
        updatedAttributes.put("test.value", "B");

        // Generate provenance events and put them in a repository
        Processor processor = mock(Processor.class);
        SharedSessionState sharedState = new SharedSessionState(processor, new AtomicLong(0));
        MockProcessSession processSession = new MockProcessSession(sharedState, processor);
        MockFlowFile mockFlowFile = processSession.createFlowFile("Test content".getBytes());

        ProvenanceEventRecord prov1 = mockProvenanceRepository.eventBuilder()
                .setEventType(ProvenanceEventType.CREATE)
                .fromFlowFile(mockFlowFile)
                .setComponentId("12345")
                .setComponentType("ReportingTask")
                .setFlowFileUUID("I am FlowFile 1")
                .setEventTime(currentTimeMillis)
                .setEventDuration(100)
                .setTransitUri("test://")
                .setSourceSystemFlowFileIdentifier("I am FlowFile 1")
                .setAlternateIdentifierUri("remote://test")
                .setAttributes(previousAttributes, updatedAttributes)
                .build();

        mockProvenanceRepository.registerEvent(prov1);

        for (int i = 1; i < 1001; i++) {
            String indexString = Integer.toString(i);
            mockFlowFile = processSession.createFlowFile(("Test content " + indexString).getBytes());
            ProvenanceEventRecord prov = mockProvenanceRepository.eventBuilder()
                    .fromFlowFile(mockFlowFile)
                    .setEventType(ProvenanceEventType.DROP)
                    .setComponentId(indexString)
                    .setComponentType("Processor")
                    .setFlowFileUUID("I am FlowFile " + indexString)
                    .setEventTime(currentTimeMillis - i)
                    .build();
            mockProvenanceRepository.registerEvent(prov);
        }

        Mockito.when(eventAccess.getProvenanceRepository()).thenReturn(mockProvenanceRepository);
        try {
            Mockito.when(eventAccess.getProvenanceEvents(anyLong(), anyInt())).thenAnswer((Answer<List<ProvenanceEventRecord>>) invocation -> {
                final long startEventId = invocation.getArgument(0);
                final int max = invocation.getArgument(1);
                return mockProvenanceRepository.getEvents(startEventId, max);
            });
        } catch (final IOException e) {
            // Won't happen
            throw new RuntimeException(e);
        }

        mockBulletinRepository = new MockQueryBulletinRepository();
        mockBulletinRepository.addBulletin(BulletinFactory.createBulletin("controller", "WARN", "test bulletin 2", "testFlowFileUuid"));
        sleep(); // Sleep 2 milliseconds so that bulletins won't have the same timestamp
        mockBulletinRepository.addBulletin(BulletinFactory.createBulletin(ComponentType.PROCESSOR.name().toLowerCase(), "INFO", "test bulletin 1", "testFlowFileUuid"));
        sleep(); // Sleep 2 milliseconds so that bulletins won't have the same timestamp
        mockBulletinRepository.addBulletin(BulletinFactory.createBulletin(ComponentType.CONTROLLER_SERVICE.name().toLowerCase(), "ERROR", "test bulletin 2", "testFlowFileUuid"));

        Mockito.when(context.getBulletinRepository()).thenReturn(mockBulletinRepository);

        return reportingTask;
    }

    private void sleep() {
        try {
            Thread.sleep(2L);
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    private final class MockQueryNiFiReportingTask extends QueryNiFiReportingTask {
        @Override
        public long getCurrentTime() {
            return currentTime.get();
        }
    }

    private static class MockQueryBulletinRepository extends MockBulletinRepository {
        Map<String, List<Bulletin>> bulletins = new HashMap<>();

        @Override
        public void addBulletin(Bulletin bulletin) {
            bulletins.computeIfAbsent(bulletin.getCategory(), __ -> new ArrayList<>())
                    .add(bulletin);
        }

        @Override
        public List<Bulletin> findBulletins(BulletinQuery bulletinQuery) {
            if (bulletinQuery.getSourceType() == null) {
                final List<Bulletin> allBulletins = new ArrayList<>();
                for (final List<Bulletin> bulletins : bulletins.values()) {
                    allBulletins.addAll(bulletins);
                }

                return allBulletins;
            }

            return new ArrayList<>(
                    Optional.ofNullable(bulletins.get(bulletinQuery.getSourceType().name().toLowerCase()))
                            .orElse(Collections.emptyList()));
        }

        @Override
        public List<Bulletin> findBulletinsForController() {
            return Optional.ofNullable(bulletins.get("controller"))
                    .orElse(Collections.emptyList());
        }
    }

    private static class MockProvenanceRepository implements ProvenanceEventRepository {
        private final List<ProvenanceEventRecord> events = new ArrayList<>();

        @Override
        public ProvenanceEventBuilder eventBuilder() {
            return new MockProvenanceEvent.Builder();
        }

        @Override
        public void registerEvent(final ProvenanceEventRecord event) {
            events.add(event);
        }

        @Override
        public void registerEvents(final Iterable<ProvenanceEventRecord> events) {
            events.forEach(this::registerEvent);
        }

        @Override
        public List<ProvenanceEventRecord> getEvents(final long firstRecordId, final int maxRecords) {
            return events.stream()
                    .filter(event -> event.getEventId() >= firstRecordId)
                    .limit(maxRecords)
                    .collect(Collectors.toList());
        }

        @Override
        public Long getMaxEventId() {
            return events.stream()
                    .map(ProvenanceEventRecord::getEventId)
                    .max(Long::compareTo)
                    .orElse(null);
        }

        @Override
        public ProvenanceEventRecord getEvent(final long id) {
            return events.stream()
                    .filter(event -> event.getEventId() == id)
                    .findFirst()
                    .orElse(null);
        }

        @Override
        public void close() {
        }
    }
}