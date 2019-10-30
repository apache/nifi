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
import org.apache.nifi.record.sink.MockRecordSinkService;
import org.apache.nifi.record.sink.RecordSinkService;
import org.apache.nifi.reporting.EventAccess;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.ReportingInitializationContext;
import org.apache.nifi.reporting.util.metrics.MetricNames;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.util.MockPropertyValue;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
        properties.put(QueryNiFiReportingTask.RECORD_SINK, "mock-record-sink");
        properties.put(QueryNiFiReportingTask.QUERY, "select id,queuedCount,isBackPressureEnabled from CONNECTION_STATUS order by queuedCount desc");
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
        properties.put(QueryNiFiReportingTask.RECORD_SINK, "mock-record-sink");
        properties.put(QueryNiFiReportingTask.QUERY, "select "
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
        Map<String,Object> row = rows.get(0);
        assertEquals(11, row.size());
        assertTrue(row.get(MetricNames.JVM_DAEMON_THREAD_COUNT.replace(".","_")) instanceof Integer);
        assertTrue(row.get(MetricNames.JVM_HEAP_USAGE.replace(".","_")) instanceof Double);
    }

    @Test
    public void testProcessGroupStatusTable() throws IOException, InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(QueryNiFiReportingTask.RECORD_SINK, "mock-record-sink");
        properties.put(QueryNiFiReportingTask.QUERY, "select * from PROCESS_GROUP_STATUS order by bytesRead asc");
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
        properties.put(QueryNiFiReportingTask.RECORD_SINK, "mock-record-sink");
        properties.put(QueryNiFiReportingTask.QUERY, "select * from CONNECTION_STATUS where queuedCount > 2000");
        reportingTask = initTask(properties);
        reportingTask.onTrigger(context);

        List<Map<String, Object>> rows = mockRecordSinkService.getRows();
        assertEquals(0, rows.size());
    }

    private MockQueryNiFiReportingTask initTask(Map<PropertyDescriptor, String> customProperties) throws InitializationException, IOException {

        final ComponentLog logger = Mockito.mock(ComponentLog.class);
        reportingTask = new MockQueryNiFiReportingTask();
        final ReportingInitializationContext initContext = Mockito.mock(ReportingInitializationContext.class);
        Mockito.when(initContext.getIdentifier()).thenReturn(UUID.randomUUID().toString());
        Mockito.when(initContext.getLogger()).thenReturn(logger);
        reportingTask.initialize(initContext);
        Map<PropertyDescriptor, String> properties = new HashMap<>();
        for (final PropertyDescriptor descriptor : reportingTask.getSupportedPropertyDescriptors()) {
            properties.put(descriptor, descriptor.getDefaultValue());
        }
        properties.putAll(customProperties);

        context = Mockito.mock(ReportingContext.class);
        Mockito.when(context.getStateManager()).thenReturn(new MockStateManager(reportingTask));
        Mockito.doAnswer((Answer<PropertyValue>) invocation -> {
            final PropertyDescriptor descriptor = invocation.getArgument(0, PropertyDescriptor.class);
            return new MockPropertyValue(properties.get(descriptor));
        }).when(context).getProperty(Mockito.any(PropertyDescriptor.class));

        final EventAccess eventAccess = Mockito.mock(EventAccess.class);
        Mockito.when(context.getEventAccess()).thenReturn(eventAccess);
        Mockito.when(eventAccess.getControllerStatus()).thenReturn(status);

        final PropertyValue pValue = Mockito.mock(StandardPropertyValue.class);
        mockRecordSinkService = new MockRecordSinkService();
        Mockito.when(context.getProperty(QueryNiFiReportingTask.RECORD_SINK)).thenReturn(pValue);
        Mockito.when(pValue.asControllerService(RecordSinkService.class)).thenReturn(mockRecordSinkService);

        ConfigurationContext configContext = Mockito.mock(ConfigurationContext.class);
        Mockito.when(configContext.getProperty(QueryNiFiReportingTask.RECORD_SINK)).thenReturn(pValue);
        reportingTask.setup(configContext);

        return reportingTask;
    }

    private static final class MockQueryNiFiReportingTask extends QueryNiFiReportingTask {
    }
}