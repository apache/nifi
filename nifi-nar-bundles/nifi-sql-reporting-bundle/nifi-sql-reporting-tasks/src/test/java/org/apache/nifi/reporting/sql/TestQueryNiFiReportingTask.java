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


import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.provenance.MockProvenanceRepository;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.record.sink.MockRecordSinkService;
import org.apache.nifi.record.sink.RecordSinkService;
import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.BulletinFactory;
import org.apache.nifi.reporting.BulletinQuery;
import org.apache.nifi.reporting.ComponentType;
import org.apache.nifi.reporting.EventAccess;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.ReportingInitializationContext;
import org.apache.nifi.reporting.sql.util.QueryMetricsUtil;
import org.apache.nifi.reporting.util.metrics.MetricNames;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.util.MockBulletinRepository;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.MockPropertyValue;
import org.apache.nifi.util.SharedSessionState;
import org.apache.nifi.util.db.JdbcProperties;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class TestQueryNiFiReportingTask {

    private ReportingContext context;
    private MockQueryNiFiReportingTask reportingTask;
    private MockRecordSinkService mockRecordSinkService;
    private ProcessGroupStatus status;

    @Before
    public void setup() {
        mockRecordSinkService = new MockRecordSinkService();
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
        procStatus.setProcessingNanos(123456789);

        Collection<ProcessorStatus> processorStatuses = new ArrayList<>();
        processorStatuses.add(procStatus);
        status.setProcessorStatus(processorStatuses);

        ConnectionStatus root1ConnectionStatus = new ConnectionStatus();
        root1ConnectionStatus.setId("root1");
        root1ConnectionStatus.setQueuedCount(1000);
        root1ConnectionStatus.setBackPressureObjectThreshold(1000);

        ConnectionStatus root2ConnectionStatus = new ConnectionStatus();
        root2ConnectionStatus.setId("root2");
        root2ConnectionStatus.setQueuedCount(500);
        root2ConnectionStatus.setBackPressureObjectThreshold(1000);

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
        Collection<ConnectionStatus> nestedConnectionStatuses2 = new ArrayList<>();
        nestedConnectionStatuses2.add(nestedConnectionStatus2);
        groupStatus3.setConnectionStatus(nestedConnectionStatuses2);
        Collection<ProcessGroupStatus> nestedGroupStatuses2 = new ArrayList<>();
        nestedGroupStatuses2.add(groupStatus3);

        Collection<ProcessGroupStatus> groupStatuses = new ArrayList<>();
        groupStatuses.add(groupStatus1);
        groupStatuses.add(groupStatus3);
        status.setProcessGroupStatus(groupStatuses);

    }

    @Test
    public void testConnectionStatusTable() throws IOException, InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(QueryMetricsUtil.RECORD_SINK, "mock-record-sink");
        properties.put(QueryMetricsUtil.QUERY, "select id,queuedCount,isBackPressureEnabled from CONNECTION_STATUS order by queuedCount desc");
        reportingTask = initTask(properties);
        reportingTask.onTrigger(context);

        List<Map<String, Object>> rows = mockRecordSinkService.getRows();
        assertEquals(4, rows.size());
        // Validate the first row
        Map<String, Object> row = rows.get(0);
        assertEquals(3, row.size()); // Only projected 2 columns
        Object id = row.get("id");
        assertTrue(id instanceof String);
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
    public void testJvmMetricsTable() throws IOException, InitializationException {
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
        Map<String, Object> row = rows.get(0);
        assertEquals(11, row.size());
        assertTrue(row.get(MetricNames.JVM_DAEMON_THREAD_COUNT.replace(".", "_")) instanceof Integer);
        assertTrue(row.get(MetricNames.JVM_HEAP_USAGE.replace(".", "_")) instanceof Double);
    }

    @Test
    public void testProcessGroupStatusTable() throws IOException, InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(QueryMetricsUtil.RECORD_SINK, "mock-record-sink");
        properties.put(QueryMetricsUtil.QUERY, "select * from PROCESS_GROUP_STATUS order by bytesRead asc");
        reportingTask = initTask(properties);
        reportingTask.onTrigger(context);

        List<Map<String, Object>> rows = mockRecordSinkService.getRows();
        assertEquals(4, rows.size());
        // Validate the first row
        Map<String, Object> row = rows.get(0);
        assertEquals(20, row.size());
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
    public void testNoResults() throws IOException, InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(QueryMetricsUtil.RECORD_SINK, "mock-record-sink");
        properties.put(QueryMetricsUtil.QUERY, "select * from CONNECTION_STATUS where queuedCount > 2000");
        reportingTask = initTask(properties);
        reportingTask.onTrigger(context);

        List<Map<String, Object>> rows = mockRecordSinkService.getRows();
        assertEquals(0, rows.size());
    }

    @Test
    public void testProvenanceTable() throws IOException, InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(QueryMetricsUtil.RECORD_SINK, "mock-record-sink");
        properties.put(QueryMetricsUtil.QUERY, "select * from PROVENANCE order by eventId asc");
        reportingTask = initTask(properties);
        reportingTask.onTrigger(context);

        List<Map<String, Object>> rows = mockRecordSinkService.getRows();
        assertEquals(1001, rows.size());
        // Validate the first row
        Map<String, Object> row = rows.get(0);
        assertEquals(24, row.size());
        // Verify the first row contents
        assertEquals(0L, row.get("eventId"));
        assertEquals("CREATE", row.get("eventType"));
        assertEquals(12L, row.get("entitySize"));
        assertNull(row.get("contentPath"));
        assertNull(row.get("previousContentPath"));

        Object o = row.get("previousAttributes");
        assertTrue(o instanceof Map);
        Map<String, String> previousAttributes = (Map<String, String>) o;
        assertEquals("A", previousAttributes.get("test.value"));
        o = row.get("updatedAttributes");
        assertTrue(o instanceof Map);
        Map<String, String> updatedAttributes = (Map<String, String>) o;
        assertEquals("B", updatedAttributes.get("test.value"));

        // Verify some fields in the second row
        row = rows.get(1);
        assertEquals(24, row.size());
        // Verify the second row contents
        assertEquals(1L, row.get("eventId"));
        assertEquals("DROP", row.get("eventType"));

        // Verify some fields in the last row
        row = rows.get(1000);
        assertEquals(24, row.size());
        // Verify the last row contents
        assertEquals(1000L, row.get("eventId"));
        assertEquals("DROP", row.get("eventType"));
    }

    @Test
    public void testBulletinTable() throws IOException, InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(QueryMetricsUtil.RECORD_SINK, "mock-record-sink");
        properties.put(QueryMetricsUtil.QUERY, "select * from BULLETINS order by bulletinTimestamp asc");
        reportingTask = initTask(properties);
        reportingTask.onTrigger(context);

        List<Map<String, Object>> rows = mockRecordSinkService.getRows();
        assertEquals(3, rows.size());
        // Validate the first row
        Map<String, Object> row = rows.get(0);
        assertEquals(13, row.size());
        assertNotNull(row.get("bulletinId"));
        assertEquals("controller", row.get("bulletinCategory"));
        assertEquals("WARN", row.get("bulletinLevel"));
        // Validate the second row
        row = rows.get(1);
        assertEquals("processor", row.get("bulletinCategory"));
        assertEquals("INFO", row.get("bulletinLevel"));
        // Validate the third row
        row = rows.get(2);
        assertEquals("controller service", row.get("bulletinCategory"));
        assertEquals("ERROR", row.get("bulletinLevel"));
    }

    private MockQueryNiFiReportingTask initTask(Map<PropertyDescriptor, String> customProperties) throws InitializationException, IOException {

        final ComponentLog logger = mock(ComponentLog.class);
        reportingTask = new MockQueryNiFiReportingTask();
        final ReportingInitializationContext initContext = mock(ReportingInitializationContext.class);
        Mockito.when(initContext.getIdentifier()).thenReturn(UUID.randomUUID().toString());
        Mockito.when(initContext.getLogger()).thenReturn(logger);
        reportingTask.initialize(initContext);
        Map<PropertyDescriptor, String> properties = new HashMap<>();
        for (final PropertyDescriptor descriptor : reportingTask.getSupportedPropertyDescriptors()) {
            properties.put(descriptor, descriptor.getDefaultValue());
        }
        properties.putAll(customProperties);

        context = mock(ReportingContext.class);
        Mockito.when(context.getStateManager()).thenReturn(new MockStateManager(reportingTask));
        Mockito.doAnswer((Answer<PropertyValue>) invocation -> {
            final PropertyDescriptor descriptor = invocation.getArgument(0, PropertyDescriptor.class);
            return new MockPropertyValue(properties.get(descriptor));
        }).when(context).getProperty(Mockito.any(PropertyDescriptor.class));

        final EventAccess eventAccess = mock(EventAccess.class);
        Mockito.when(context.getEventAccess()).thenReturn(eventAccess);
        Mockito.when(eventAccess.getControllerStatus()).thenReturn(status);

        final PropertyValue pValue = mock(StandardPropertyValue.class);
        mockRecordSinkService = new MockRecordSinkService();
        Mockito.when(context.getProperty(QueryMetricsUtil.RECORD_SINK)).thenReturn(pValue);
        Mockito.when(pValue.asControllerService(RecordSinkService.class)).thenReturn(mockRecordSinkService);

        ConfigurationContext configContext = mock(ConfigurationContext.class);
        Mockito.when(configContext.getProperty(QueryMetricsUtil.RECORD_SINK)).thenReturn(pValue);

        Mockito.when(configContext.getProperty(JdbcProperties.VARIABLE_REGISTRY_ONLY_DEFAULT_PRECISION)).thenReturn(new MockPropertyValue("10"));
        Mockito.when(configContext.getProperty(JdbcProperties.VARIABLE_REGISTRY_ONLY_DEFAULT_SCALE)).thenReturn(new MockPropertyValue("0"));
        reportingTask.setup(configContext);

        MockProvenanceRepository provenanceRepository = new MockProvenanceRepository();
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

        ProvenanceEventRecord prov1 = provenanceRepository.eventBuilder()
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

        provenanceRepository.registerEvent(prov1);

        for (int i = 1; i < 1001; i++) {
            String indexString = Integer.toString(i);
            mockFlowFile = processSession.createFlowFile(("Test content " + indexString).getBytes());
            ProvenanceEventRecord prov = provenanceRepository.eventBuilder()
                    .fromFlowFile(mockFlowFile)
                    .setEventType(ProvenanceEventType.DROP)
                    .setComponentId(indexString)
                    .setComponentType("Processor")
                    .setFlowFileUUID("I am FlowFile " + indexString)
                    .setEventTime(currentTimeMillis - i)
                    .build();
            provenanceRepository.registerEvent(prov);
        }

        Mockito.when(eventAccess.getProvenanceRepository()).thenReturn(provenanceRepository);

        MockBulletinRepository bulletinRepository = new MockQueryBulletinRepository();
        bulletinRepository.addBulletin(BulletinFactory.createBulletin("controller", "WARN", "test bulletin 2"));
        bulletinRepository.addBulletin(BulletinFactory.createBulletin("processor", "INFO", "test bulletin 1"));
        bulletinRepository.addBulletin(BulletinFactory.createBulletin("controller service", "ERROR", "test bulletin 2"));
        Mockito.when(context.getBulletinRepository()).thenReturn(bulletinRepository);

        return reportingTask;
    }

    private static final class MockQueryNiFiReportingTask extends QueryNiFiReportingTask {
    }

    private static class MockQueryBulletinRepository extends MockBulletinRepository {

        List<Bulletin> bulletinList;


        public MockQueryBulletinRepository() {
            bulletinList = new ArrayList<>();
        }

        @Override
        public void addBulletin(Bulletin bulletin) {
            bulletinList.add(bulletin);
        }

        @Override
        public List<Bulletin> findBulletins(BulletinQuery bulletinQuery) {
            if (bulletinQuery.getSourceType().equals(ComponentType.PROCESSOR)) {
                return Collections.singletonList(bulletinList.get(1));
            } else if (bulletinQuery.getSourceType().equals(ComponentType.CONTROLLER_SERVICE)) {
                return Collections.singletonList(bulletinList.get(2));
            } else {
                return Collections.emptyList();
            }
        }

        @Override
        public List<Bulletin> findBulletinsForController() {
            return Collections.singletonList(bulletinList.get(0));
        }

    }
}