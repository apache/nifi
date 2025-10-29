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

package org.apache.nifi.reporting;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.provenance.ProvenanceEventBuilder;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.provenance.StandardProvenanceEventRecord;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.reporting.s2s.SiteToSiteUtils;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockPropertyValue;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;
import jakarta.json.JsonValue;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;

public class TestSiteToSiteProvenanceReportingTask {

    private final ReportingContext context = Mockito.mock(ReportingContext.class);
    private final ReportingInitializationContext initContext = Mockito.mock(ReportingInitializationContext.class);
    private final ConfigurationContext confContext = Mockito.mock(ConfigurationContext.class);

    private MockSiteToSiteProvenanceReportingTask setup(ProvenanceEventRecord event, Map<PropertyDescriptor, String> properties) throws IOException {
        return setup(event, properties, 2500);
    }

    private MockSiteToSiteProvenanceReportingTask setup(ProvenanceEventRecord event, Map<PropertyDescriptor, String> properties, long maxEventId) throws IOException {
        final MockSiteToSiteProvenanceReportingTask task = new MockSiteToSiteProvenanceReportingTask();

        when(context.getStateManager())
                .thenReturn(new MockStateManager(task));
        Mockito.doAnswer((Answer<PropertyValue>) invocation -> {
            final PropertyDescriptor descriptor = invocation.getArgument(0, PropertyDescriptor.class);
            return new MockPropertyValue(properties.get(descriptor));
        }).when(context).getProperty(Mockito.any(PropertyDescriptor.class));

        Mockito.doAnswer((Answer<PropertyValue>) invocation -> {
            final PropertyDescriptor descriptor = invocation.getArgument(0, PropertyDescriptor.class);
            return new MockPropertyValue(properties.get(descriptor));
        }).when(confContext).getProperty(Mockito.any(PropertyDescriptor.class));

        final AtomicInteger totalEvents = new AtomicInteger(0);

        final EventAccess eventAccess = Mockito.mock(EventAccess.class);
        Mockito.doAnswer((Answer<List<ProvenanceEventRecord>>) invocation -> {
            final long startId = invocation.getArgument(0, Long.class);
            final int maxRecords = invocation.getArgument(1, Integer.class);

            final List<ProvenanceEventRecord> eventsToReturn = new ArrayList<>();
            for (int i = (int) Math.max(0, startId); i < (int) (startId + maxRecords) && totalEvents.get() < maxEventId; i++) {
                if (event != null) {
                    eventsToReturn.add(event);
                }

                totalEvents.getAndIncrement();
            }
            return eventsToReturn;
        }).when(eventAccess).getProvenanceEvents(Mockito.anyLong(), Mockito.anyInt());
        ProcessGroupStatus pgRoot = new ProcessGroupStatus();
        pgRoot.setId("root");
        when(eventAccess.getControllerStatus()).thenReturn(pgRoot);

        // Add child Process Groups.
        // Root -> (A, B -> (B2 -> (B3)))
        final ProcessGroupStatus pgA = new ProcessGroupStatus();
        pgA.setId("pgA");
        final ProcessGroupStatus pgB = new ProcessGroupStatus();
        pgB.setId("pgB");
        final ProcessGroupStatus pgB2 = new ProcessGroupStatus();
        pgB2.setId("pgB2");
        final ProcessGroupStatus pgB3 = new ProcessGroupStatus();
        pgB3.setId("pgB3");
        final Collection<ProcessGroupStatus> childPGs = pgRoot.getProcessGroupStatus();
        childPGs.add(pgA);
        childPGs.add(pgB);
        pgB.getProcessGroupStatus().add(pgB2);
        pgB2.getProcessGroupStatus().add(pgB3);

        // Add Processors.
        final ProcessorStatus prcRoot = new ProcessorStatus();
        prcRoot.setId("1234");
        pgRoot.getProcessorStatus().add(prcRoot);

        final ProcessorStatus prcA = new ProcessorStatus();
        prcA.setId("A001");
        prcA.setName("Processor in PGA");
        pgA.getProcessorStatus().add(prcA);

        final ProcessorStatus prcB = new ProcessorStatus();
        prcB.setId("B001");
        prcB.setName("Processor in PGB");
        pgB.getProcessorStatus().add(prcB);

        final ProcessorStatus prcB2 = new ProcessorStatus();
        prcB2.setId("B201");
        prcB2.setName("Processor in PGB2");
        pgB2.getProcessorStatus().add(prcB2);

        final ProcessorStatus prcB3 = new ProcessorStatus();
        prcB3.setId("B301");
        prcB3.setName("Processor in PGB3");
        pgB3.getProcessorStatus().add(prcB3);

        // Add connection status to test Remote Input/Output Ports
        final ConnectionStatus b2RemoteInputPort = new ConnectionStatus();
        b2RemoteInputPort.setGroupId("pgB2");
        b2RemoteInputPort.setSourceId("B201");
        b2RemoteInputPort.setDestinationId("riB2");
        b2RemoteInputPort.setDestinationName("Remote Input Port name");
        pgB2.getConnectionStatus().add(b2RemoteInputPort);

        final ConnectionStatus b3RemoteOutputPort = new ConnectionStatus();
        b3RemoteOutputPort.setGroupId("pgB3");
        b3RemoteOutputPort.setSourceId("roB3");
        b3RemoteOutputPort.setSourceName("Remote Output Port name");
        b3RemoteOutputPort.setDestinationId("B301");
        pgB3.getConnectionStatus().add(b3RemoteOutputPort);

        final ProvenanceEventRepository provenanceRepository = Mockito.mock(ProvenanceEventRepository.class);
        Mockito.doAnswer((Answer<Long>) invocation -> maxEventId).when(provenanceRepository).getMaxEventId();

        when(context.getEventAccess()).thenReturn(eventAccess);
        when(eventAccess.getProvenanceRepository()).thenReturn(provenanceRepository);

        final ComponentLog logger = Mockito.mock(ComponentLog.class);
        when(initContext.getIdentifier()).thenReturn(UUID.randomUUID().toString());
        when(initContext.getLogger()).thenReturn(logger);

        return task;
    }

    @Test
    public void testSerializedForm() throws IOException, InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        for (final PropertyDescriptor descriptor : new MockSiteToSiteProvenanceReportingTask().getSupportedPropertyDescriptors()) {
            properties.put(descriptor, descriptor.getDefaultValue());
        }
        properties.put(SiteToSiteUtils.BATCH_SIZE, "1000");

        ProvenanceEventRecord event = createProvenanceEventRecord();

        MockSiteToSiteProvenanceReportingTask task = setup(event, properties);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(3, task.dataSent.size());
        final String msg = new String(task.dataSent.get(0), StandardCharsets.UTF_8);
        JsonReader jsonReader = Json.createReader(new ByteArrayInputStream(msg.getBytes()));
        JsonObject object = jsonReader.readArray().getJsonObject(0);
        JsonValue details = object.get("details");
        JsonObject msgArray = object.getJsonObject("updatedAttributes");
        assertNull(details);
        assertEquals(msgArray.getString("abc"), event.getAttributes().get("abc"));
        assertNull(msgArray.get("emptyVal"));
    }

    @Test
    public void testSerializedFormWithNullValues() throws IOException, InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        for (final PropertyDescriptor descriptor : new MockSiteToSiteProvenanceReportingTask().getSupportedPropertyDescriptors()) {
            properties.put(descriptor, descriptor.getDefaultValue());
        }
        properties.put(SiteToSiteUtils.BATCH_SIZE, "1000");
        properties.put(SiteToSiteStatusReportingTask.ALLOW_NULL_VALUES, "true");

        ProvenanceEventRecord event = createProvenanceEventRecord();

        MockSiteToSiteProvenanceReportingTask task = setup(event, properties);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        final String msg = new String(task.dataSent.get(0), StandardCharsets.UTF_8);
        JsonReader jsonReader = Json.createReader(new ByteArrayInputStream(msg.getBytes()));
        JsonObject object = jsonReader.readArray().getJsonObject(0);
        JsonValue details = object.get("details");
        JsonValue emptyVal = object.getJsonObject("updatedAttributes").get("emptyVal");
        assertEquals(JsonValue.NULL, details);
        assertEquals(JsonValue.NULL, emptyVal);
    }
    @Test
    public void testFilterComponentIdSuccess() throws IOException, InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        for (final PropertyDescriptor descriptor : new MockSiteToSiteProvenanceReportingTask().getSupportedPropertyDescriptors()) {
            properties.put(descriptor, descriptor.getDefaultValue());
        }
        properties.put(SiteToSiteUtils.BATCH_SIZE, "1000");
        properties.put(SiteToSiteProvenanceReportingTask.FILTER_COMPONENT_ID, "2345, 5678,  1234");

        ProvenanceEventRecord event = createProvenanceEventRecord();

        MockSiteToSiteProvenanceReportingTask task = setup(event, properties);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(3, task.dataSent.size());
    }


    @Test
    public void testFilterComponentIdNoResult() throws IOException, InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        for (final PropertyDescriptor descriptor : new MockSiteToSiteProvenanceReportingTask().getSupportedPropertyDescriptors()) {
            properties.put(descriptor, descriptor.getDefaultValue());
        }
        properties.put(SiteToSiteUtils.BATCH_SIZE, "1000");
        properties.put(SiteToSiteProvenanceReportingTask.FILTER_COMPONENT_ID, "9999");

        ProvenanceEventRecord event = createProvenanceEventRecord();

        MockSiteToSiteProvenanceReportingTask task = setup(event, properties);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(0, task.dataSent.size());
    }

    @Test
    public void testFilterComponentTypeSuccess() throws IOException, InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        for (final PropertyDescriptor descriptor : new MockSiteToSiteProvenanceReportingTask().getSupportedPropertyDescriptors()) {
            properties.put(descriptor, descriptor.getDefaultValue());
        }
        properties.put(SiteToSiteUtils.BATCH_SIZE, "1000");
        properties.put(SiteToSiteProvenanceReportingTask.FILTER_COMPONENT_TYPE, "dummy.*");

        ProvenanceEventRecord event = createProvenanceEventRecord();

        MockSiteToSiteProvenanceReportingTask task = setup(event, properties);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(3, task.dataSent.size());
    }

    @Test
    public void testFilterComponentName() throws IOException, InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        for (final PropertyDescriptor descriptor : new MockSiteToSiteProvenanceReportingTask().getSupportedPropertyDescriptors()) {
            properties.put(descriptor, descriptor.getDefaultValue());
        }
        properties.put(SiteToSiteUtils.BATCH_SIZE, "1000");
        properties.put(SiteToSiteProvenanceReportingTask.FILTER_COMPONENT_NAME, "Processor in .*");
        properties.put(SiteToSiteProvenanceReportingTask.FILTER_COMPONENT_NAME_EXCLUDE, ".*PGB");

        // A001 has name "Processor in PGA" and should be picked
        ProvenanceEventRecord event = createProvenanceEventRecord("A001", "dummy");
        MockSiteToSiteProvenanceReportingTask task = setup(event, properties, 1);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(1, task.dataSent.size());
        JsonNode reportedEvent = new ObjectMapper().readTree(task.dataSent.get(0)).get(0);
        assertEquals("A001", reportedEvent.get("componentId").asText());
        assertEquals("Processor in PGA", reportedEvent.get("componentName").asText());

        // B001 has name "Processor in PGB" and should not be picked
        event = createProvenanceEventRecord("B001", "dummy");
        task = setup(event, properties, 1);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(0, task.dataSent.size());
    }

    @Test
    public void testFilterComponentTypeExcludeSuccess() throws IOException, InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        for (final PropertyDescriptor descriptor : new MockSiteToSiteProvenanceReportingTask().getSupportedPropertyDescriptors()) {
            properties.put(descriptor, descriptor.getDefaultValue());
        }
        properties.put(SiteToSiteUtils.BATCH_SIZE, "1000");
        properties.put(SiteToSiteProvenanceReportingTask.FILTER_COMPONENT_TYPE_EXCLUDE, "dummy.*");

        ProvenanceEventRecord event = createProvenanceEventRecord();

        MockSiteToSiteProvenanceReportingTask task = setup(event, properties);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(0, task.dataSent.size());
    }

    @Test
    public void testFilterComponentTypeNoResult() throws IOException, InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        for (final PropertyDescriptor descriptor : new MockSiteToSiteProvenanceReportingTask().getSupportedPropertyDescriptors()) {
            properties.put(descriptor, descriptor.getDefaultValue());
        }
        properties.put(SiteToSiteUtils.BATCH_SIZE, "1000");
        properties.put(SiteToSiteProvenanceReportingTask.FILTER_COMPONENT_TYPE, "proc.*");

        ProvenanceEventRecord event = createProvenanceEventRecord();

        MockSiteToSiteProvenanceReportingTask task = setup(event, properties);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(0, task.dataSent.size());
    }

    @Test
    public void testFilterComponentTypeNoResultExcluded() throws IOException, InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        for (final PropertyDescriptor descriptor : new MockSiteToSiteProvenanceReportingTask().getSupportedPropertyDescriptors()) {
            properties.put(descriptor, descriptor.getDefaultValue());
        }
        properties.put(SiteToSiteUtils.BATCH_SIZE, "1000");
        properties.put(SiteToSiteProvenanceReportingTask.FILTER_COMPONENT_TYPE_EXCLUDE, "proc.*");

        ProvenanceEventRecord event = createProvenanceEventRecord();

        MockSiteToSiteProvenanceReportingTask task = setup(event, properties);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(3, task.dataSent.size());
    }

    @Test
    public void testFilterEventTypeSuccess() throws IOException, InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        for (final PropertyDescriptor descriptor : new MockSiteToSiteProvenanceReportingTask().getSupportedPropertyDescriptors()) {
            properties.put(descriptor, descriptor.getDefaultValue());
        }
        properties.put(SiteToSiteUtils.BATCH_SIZE, "1000");
        properties.put(SiteToSiteProvenanceReportingTask.FILTER_EVENT_TYPE, "RECEIVE, notExistingType, DROP");

        ProvenanceEventRecord event = createProvenanceEventRecord();

        MockSiteToSiteProvenanceReportingTask task = setup(event, properties);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(3, task.dataSent.size());
    }

    @Test
    public void testFilterEventTypeExcludeSuccess() throws IOException, InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        for (final PropertyDescriptor descriptor : new MockSiteToSiteProvenanceReportingTask().getSupportedPropertyDescriptors()) {
            properties.put(descriptor, descriptor.getDefaultValue());
        }
        properties.put(SiteToSiteUtils.BATCH_SIZE, "1000");
        properties.put(SiteToSiteProvenanceReportingTask.FILTER_EVENT_TYPE_EXCLUDE, "RECEIVE, notExistingType, DROP");

        ProvenanceEventRecord event = createProvenanceEventRecord();

        MockSiteToSiteProvenanceReportingTask task = setup(event, properties);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(0, task.dataSent.size());
    }

    @Test
    public void testFilterEventTypeNoResult() throws IOException, InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        for (final PropertyDescriptor descriptor : new MockSiteToSiteProvenanceReportingTask().getSupportedPropertyDescriptors()) {
            properties.put(descriptor, descriptor.getDefaultValue());
        }
        properties.put(SiteToSiteUtils.BATCH_SIZE, "1000");
        properties.put(SiteToSiteProvenanceReportingTask.FILTER_EVENT_TYPE, "DROP");

        ProvenanceEventRecord event = createProvenanceEventRecord();

        MockSiteToSiteProvenanceReportingTask task = setup(event, properties);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(0, task.dataSent.size());
    }

    @Test
    public void testFilterMultiFilterNoResult() throws IOException, InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        for (final PropertyDescriptor descriptor : new MockSiteToSiteProvenanceReportingTask().getSupportedPropertyDescriptors()) {
            properties.put(descriptor, descriptor.getDefaultValue());
        }
        properties.put(SiteToSiteUtils.BATCH_SIZE, "1000");
        properties.put(SiteToSiteProvenanceReportingTask.FILTER_COMPONENT_ID, "2345, 5678,  1234");
        properties.put(SiteToSiteProvenanceReportingTask.FILTER_COMPONENT_TYPE, "dummy.*");
        properties.put(SiteToSiteProvenanceReportingTask.FILTER_EVENT_TYPE, "DROP");

        ProvenanceEventRecord event = createProvenanceEventRecord();

        MockSiteToSiteProvenanceReportingTask task = setup(event, properties);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(0, task.dataSent.size());
    }

    @Test
    public void testFilterMultiFilterSuccess() throws IOException, InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        for (final PropertyDescriptor descriptor : new MockSiteToSiteProvenanceReportingTask().getSupportedPropertyDescriptors()) {
            properties.put(descriptor, descriptor.getDefaultValue());
        }
        properties.put(SiteToSiteUtils.BATCH_SIZE, "1000");
        properties.put(SiteToSiteProvenanceReportingTask.FILTER_COMPONENT_ID, "2345, 5678,  1234");
        properties.put(SiteToSiteProvenanceReportingTask.FILTER_COMPONENT_TYPE, "dummy.*");
        properties.put(SiteToSiteProvenanceReportingTask.FILTER_EVENT_TYPE, "RECEIVE");

        ProvenanceEventRecord event = createProvenanceEventRecord();

        MockSiteToSiteProvenanceReportingTask task = setup(event, properties);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(3, task.dataSent.size());
    }

    @Test
    public void testFilterMultiFilterExcludeTakesPrecedence() throws IOException, InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        for (final PropertyDescriptor descriptor : new MockSiteToSiteProvenanceReportingTask().getSupportedPropertyDescriptors()) {
            properties.put(descriptor, descriptor.getDefaultValue());
        }
        properties.put(SiteToSiteUtils.BATCH_SIZE, "1000");
        properties.put(SiteToSiteProvenanceReportingTask.FILTER_COMPONENT_TYPE_EXCLUDE, "dummy.*");
        properties.put(SiteToSiteProvenanceReportingTask.FILTER_EVENT_TYPE, "RECEIVE");

        ProvenanceEventRecord event = createProvenanceEventRecord();

        MockSiteToSiteProvenanceReportingTask task = setup(event, properties);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(0, task.dataSent.size());
    }

    @Test
    public void testFilterProcessGroupId() throws IOException, InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        for (final PropertyDescriptor descriptor : new MockSiteToSiteProvenanceReportingTask().getSupportedPropertyDescriptors()) {
            properties.put(descriptor, descriptor.getDefaultValue());
        }
        properties.put(SiteToSiteUtils.BATCH_SIZE, "1000");
        properties.put(SiteToSiteProvenanceReportingTask.FILTER_COMPONENT_ID, "pgB2");


        // B201 belongs to ProcessGroup B2, so it should be picked.
        ProvenanceEventRecord event = createProvenanceEventRecord("B201", "dummy");
        MockSiteToSiteProvenanceReportingTask task = setup(event, properties, 1);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(1, task.dataSent.size());
        JsonNode reportedEvent = new ObjectMapper().readTree(task.dataSent.get(0)).get(0);
        assertEquals("B201", reportedEvent.get("componentId").asText());
        assertEquals("Processor in PGB2", reportedEvent.get("componentName").asText());


        // B301 belongs to PG B3, whose parent is PGB2, so it should be picked, too.
        event = createProvenanceEventRecord("B301", "dummy");
        task = setup(event, properties, 1);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(1, task.dataSent.size());
        reportedEvent = new ObjectMapper().readTree(task.dataSent.get(0)).get(0);
        assertEquals("B301", reportedEvent.get("componentId").asText());
        assertEquals("Processor in PGB3", reportedEvent.get("componentName").asText());

        // A001 belongs to PG A, whose parent is the root PG, so it should be filtered out.
        event = createProvenanceEventRecord("A001", "dummy");
        task = setup(event, properties, 1);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(0, task.dataSent.size());
    }

    @Test
    public void testRemotePorts() throws IOException, InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        for (final PropertyDescriptor descriptor : new MockSiteToSiteProvenanceReportingTask().getSupportedPropertyDescriptors()) {
            properties.put(descriptor, descriptor.getDefaultValue());
        }
        properties.put(SiteToSiteUtils.BATCH_SIZE, "1000");
        properties.put(SiteToSiteProvenanceReportingTask.FILTER_COMPONENT_ID, "riB2,roB3");


        // riB2 is a Remote Input Port in Process Group B2.
        ProvenanceEventRecord event = createProvenanceEventRecord("riB2", "Remote Input Port");
        MockSiteToSiteProvenanceReportingTask task = setup(event, properties, 1);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(1, task.dataSent.size());
        JsonNode reportedEvent = new ObjectMapper().readTree(task.dataSent.get(0)).get(0);
        assertEquals("riB2", reportedEvent.get("componentId").asText());
        assertEquals("Remote Input Port name", reportedEvent.get("componentName").asText());
        assertEquals("pgB2", reportedEvent.get("processGroupId").asText());


        // roB3 is a Remote Output Port in Process Group B3.
        event = createProvenanceEventRecord("roB3", "Remote Output Port");
        task = setup(event, properties, 1);
        task.initialize(initContext);
        task.onScheduled(confContext);
        task.onTrigger(context);

        assertEquals(1, task.dataSent.size());
        reportedEvent = new ObjectMapper().readTree(task.dataSent.get(0)).get(0);
        assertEquals("roB3", reportedEvent.get("componentId").asText());
        assertEquals("Remote Output Port name", reportedEvent.get("componentName").asText());
        assertEquals("pgB3", reportedEvent.get("processGroupId").asText());

    }

    @Test
    public void testWhenProvenanceMaxIdEqualToLastEventIdInStateManager() throws IOException, InitializationException {
        final long maxEventId = 2500;

        // create the mock reporting task and mock state manager
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        for (final PropertyDescriptor descriptor : new MockSiteToSiteProvenanceReportingTask().getSupportedPropertyDescriptors()) {
            properties.put(descriptor, descriptor.getDefaultValue());
        }

        final MockSiteToSiteProvenanceReportingTask task = setup(null, properties);
        final MockStateManager stateManager = new MockStateManager(task);

        // create the state map and set the last id to the same value as maxEventId
        final Map<String, String> state = new HashMap<>();
        state.put(SiteToSiteProvenanceReportingTask.LAST_EVENT_ID_KEY, String.valueOf(maxEventId));
        stateManager.setState(state, Scope.LOCAL);

        // setup the mock provenance repository to return maxEventId
        final ProvenanceEventRepository provenanceRepository = Mockito.mock(ProvenanceEventRepository.class);
        Mockito.doAnswer((Answer<Long>) invocation -> maxEventId).when(provenanceRepository).getMaxEventId();

        // setup the mock EventAccess to return the mock provenance repository
        final EventAccess eventAccess = Mockito.mock(EventAccess.class);
        when(eventAccess.getProvenanceRepository()).thenReturn(provenanceRepository);

        task.initialize(initContext);

        // execute the reporting task and should not produce any data b/c max id same as previous id
        task.onScheduled(confContext);
        task.onTrigger(context);
        assertEquals(0, task.dataSent.size());
    }

    public static FlowFile createFlowFile(final long id, final Map<String, String> attributes) {
        MockFlowFile mockFlowFile = new MockFlowFile(id);
        mockFlowFile.putAttributes(attributes);
        return mockFlowFile;
    }

    private ProvenanceEventRecord createProvenanceEventRecord() {
        return createProvenanceEventRecord("1234", "dummy processor");
    }
    private ProvenanceEventRecord createProvenanceEventRecord(final String componentId, final String componentType) {
        final String uuid = "10000000-0000-0000-0000-000000000000";
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "xyz");
        attributes.put("xyz", "abc");
        attributes.put("filename", "file-" + uuid);
        attributes.put("emptyVal", null);

        final Map<String, String> prevAttrs = new HashMap<>();
        attributes.put("filename", "1234.xyz");

        final ProvenanceEventBuilder builder = new StandardProvenanceEventRecord.Builder();
        builder.setEventTime(System.currentTimeMillis());
        builder.setEventType(ProvenanceEventType.RECEIVE);
        builder.setTransitUri("nifi://unit-test");
        attributes.put("uuid", uuid);
        builder.fromFlowFile(createFlowFile(3L, attributes));
        builder.setAttributes(prevAttrs, attributes);
        builder.setComponentId(componentId);
        builder.setComponentType(componentType);
        return builder.build();
    }

    private static final class MockSiteToSiteProvenanceReportingTask extends SiteToSiteProvenanceReportingTask {

        public MockSiteToSiteProvenanceReportingTask() throws IOException {
            super();
        }

        final List<byte[]> dataSent = new ArrayList<>();

        @Override
        public void setup(PropertyContext reportContext) {
            if (siteToSiteClient == null) {
                final SiteToSiteClient client = Mockito.mock(SiteToSiteClient.class);
                final Transaction transaction = Mockito.mock(Transaction.class);

                assertDoesNotThrow(() -> {
                            Mockito.doAnswer((Answer<Object>) invocation -> {
                                final byte[] data = invocation.getArgument(0, byte[].class);
                                dataSent.add(data);
                                return null;
                            }).when(transaction).send(Mockito.any(byte[].class), Mockito.any(Map.class));

                            when(client.createTransaction(Mockito.any(TransferDirection.class))).thenReturn(transaction);

                        });
                siteToSiteClient = client;
            }
        }

        public List<byte[]> getDataSent() {
            return dataSent;
        }
    }

}
