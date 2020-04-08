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

import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.provenance.MockProvenanceRepository;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.sql.connectionstatus.ConnectionStatusTable;
import org.apache.nifi.reporting.sql.provenance.ProvenanceTable;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.SharedSessionState;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class TestQueryNiFiTables {

    private final ComponentLog logger = mock(ComponentLog.class);
    private ProcessGroupStatus rootGroupStatus;
    private MockProvenanceRepository provenanceRepository;

    private Supplier<ProcessGroupStatus> rootGroupStatusSupplier;

    @Before
    public void setup() {
        rootGroupStatus = new ProcessGroupStatus();
        rootGroupStatus.setId("1234");
        rootGroupStatus.setFlowFilesReceived(5);
        rootGroupStatus.setBytesReceived(10000);
        rootGroupStatus.setFlowFilesSent(10);
        rootGroupStatus.setBytesRead(20000L);
        rootGroupStatus.setBytesSent(20000);
        rootGroupStatus.setQueuedCount(100);
        rootGroupStatus.setQueuedContentSize(1024L);
        rootGroupStatus.setBytesWritten(80000L);
        rootGroupStatus.setActiveThreadCount(5);

        // create a processor status with processing time
        ProcessorStatus procStatus = new ProcessorStatus();
        procStatus.setId("proc");
        procStatus.setProcessingNanos(123456789);

        Collection<ProcessorStatus> processorStatuses = new ArrayList<>();
        processorStatuses.add(procStatus);
        rootGroupStatus.setProcessorStatus(processorStatuses);

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
        rootGroupStatus.setConnectionStatus(rootConnectionStatuses);

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
        rootGroupStatus.setProcessGroupStatus(groupStatuses);

        rootGroupStatusSupplier = () -> rootGroupStatus;

        // Set up provenance repo
        provenanceRepository = new MockProvenanceRepository();
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
    }

    @Test
    public void testQueryConnectionStatusTable() throws Exception {
        final CalciteConnection connection = createConnection();
        final SchemaPlus rootSchema = connection.getRootSchema();

        final ConnectionStatusTable connectionStatusTable = new ConnectionStatusTable(rootGroupStatusSupplier, logger);
        rootSchema.add("CONNECTION_STATUS", connectionStatusTable);
        rootSchema.setCacheEnabled(false);

        final PreparedStatement stmt = connection.prepareStatement("SELECT * FROM CONNECTION_STATUS WHERE id = 'nested'");
        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(1001, rs.getLong(14)); // queuedCount
        assertFalse(rs.next());
    }

    @Test
    public void testQueryProvenanceTable() throws Exception {
        final CalciteConnection connection = createConnection();
        final SchemaPlus rootSchema = connection.getRootSchema();

        final ProvenanceTable provenanceTable = new ProvenanceTable(provenanceRepository,
                rootGroupStatusSupplier.get(),
                false, null,
                logger);
        rootSchema.add("PROVENANCE", provenanceTable);
        rootSchema.setCacheEnabled(false);

        final PreparedStatement stmt = connection.prepareStatement("SELECT * FROM PROVENANCE WHERE eventType = 'CREATE'");
        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals("12345", rs.getString(7)); // componentId
        assertFalse(rs.next());
    }

    private CalciteConnection createConnection() {
        final Properties properties = new Properties();
        properties.put(CalciteConnectionProperty.LEX.camelName(), Lex.MYSQL_ANSI.name());

        try {
            final Connection connection = DriverManager.getConnection("jdbc:calcite:", properties);
            return connection.unwrap(CalciteConnection.class);
        } catch (final Exception e) {
            throw new ProcessException(e);
        }
    }
}
