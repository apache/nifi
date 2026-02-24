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

import jakarta.json.Json;
import jakarta.json.JsonNumber;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;
import jakarta.json.JsonString;
import jakarta.json.JsonValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.PortStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.RemoteProcessGroupStatus;
import org.apache.nifi.controller.status.RunStatus;
import org.apache.nifi.controller.status.TransmissionStatus;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.registry.flow.VersionedFlowState;
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

public class TestSiteToSiteStatusReportingTask {
    private ReportingContext context;

    public MockSiteToSiteStatusReportingTask initTask(final Map<PropertyDescriptor, String> customProperties,
            final ProcessGroupStatus pgStatus) throws InitializationException, IOException {
        final MockSiteToSiteStatusReportingTask task = new MockSiteToSiteStatusReportingTask();
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        for (final PropertyDescriptor descriptor : task.getSupportedPropertyDescriptors()) {
            properties.put(descriptor, descriptor.getDefaultValue());
        }
        properties.putAll(customProperties);

        context = Mockito.mock(ReportingContext.class);
        Mockito.when(context.getStateManager())
                .thenReturn(new MockStateManager(task));
        Mockito.doAnswer((Answer<PropertyValue>) invocation -> {
            final PropertyDescriptor descriptor = invocation.getArgument(0, PropertyDescriptor.class);
            return new MockPropertyValue(properties.get(descriptor));
        }).when(context).getProperty(Mockito.any(PropertyDescriptor.class));

        final EventAccess eventAccess = Mockito.mock(EventAccess.class);

        Mockito.when(context.getEventAccess()).thenReturn(eventAccess);
        Mockito.when(eventAccess.getControllerStatus()).thenReturn(pgStatus);

        final ComponentLog logger = Mockito.mock(ComponentLog.class);
        final ReportingInitializationContext initContext = Mockito.mock(ReportingInitializationContext.class);
        Mockito.when(initContext.getIdentifier()).thenReturn(UUID.randomUUID().toString());
        Mockito.when(initContext.getLogger()).thenReturn(logger);
        task.initialize(initContext);

        return task;
    }

    @Test
    public void testSerializedForm() throws IOException, InitializationException {
        final ProcessGroupStatus pgStatus = generateProcessGroupStatus("root", "Awesome", 1, 0);

        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(SiteToSiteUtils.BATCH_SIZE, "4");
        properties.put(SiteToSiteStatusReportingTask.COMPONENT_NAME_FILTER_REGEX, "Awesome.*");
        properties.put(SiteToSiteStatusReportingTask.COMPONENT_TYPE_FILTER_REGEX, ".*");

        final MockSiteToSiteStatusReportingTask task = initTask(properties, pgStatus);
        task.onTrigger(context);

        assertEquals(16, task.dataSent.size());
        final String msg = new String(task.dataSent.getFirst(), StandardCharsets.UTF_8);
        final JsonReader jsonReader = Json.createReader(new ByteArrayInputStream(msg.getBytes()));
        final JsonObject firstElement = jsonReader.readArray().getJsonObject(0);
        final JsonString componentId = firstElement.getJsonString("componentId");
        assertEquals(pgStatus.getId(), componentId.getString());
        final JsonNumber terminatedThreads = firstElement.getJsonNumber("terminatedThreadCount");
        assertEquals(1, terminatedThreads.longValue());
        final JsonString versionedFlowState = firstElement.getJsonString("versionedFlowState");
        assertEquals("UP_TO_DATE", versionedFlowState.getString());
    }

    @Test
    public void testComponentTypeFilter() throws IOException, InitializationException {
        final ProcessGroupStatus pgStatus = generateProcessGroupStatus("root", "Awesome", 1, 0);

        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(SiteToSiteUtils.BATCH_SIZE, "4");
        properties.put(SiteToSiteStatusReportingTask.COMPONENT_NAME_FILTER_REGEX, "Awesome.*");
        properties.put(SiteToSiteStatusReportingTask.COMPONENT_TYPE_FILTER_REGEX, "(ProcessGroup|RootProcessGroup)");

        final MockSiteToSiteStatusReportingTask task = initTask(properties, pgStatus);
        task.onTrigger(context);

        assertEquals(1, task.dataSent.size()); // Only root pg and 3 child pgs
        final String msg = new String(task.dataSent.getFirst(), StandardCharsets.UTF_8);
        final JsonReader jsonReader = Json.createReader(new ByteArrayInputStream(msg.getBytes()));
        final JsonString componentId = jsonReader.readArray().getJsonObject(0).getJsonString("componentId");
        assertEquals(pgStatus.getId(), componentId.getString());
    }

    @Test
    public void testConnectionStatus() throws IOException, InitializationException {
        final ProcessGroupStatus pgStatus = generateProcessGroupStatus("root", "Awesome", 1, 0);

        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(SiteToSiteUtils.BATCH_SIZE, "4");
        properties.put(SiteToSiteStatusReportingTask.COMPONENT_NAME_FILTER_REGEX, "Awesome.*");
        properties.put(SiteToSiteStatusReportingTask.COMPONENT_TYPE_FILTER_REGEX, "(Connection)");

        final MockSiteToSiteStatusReportingTask task = initTask(properties, pgStatus);
        task.onTrigger(context);

        final String msg = new String(task.dataSent.getFirst(), StandardCharsets.UTF_8);
        final JsonReader jsonReader = Json.createReader(new ByteArrayInputStream(msg.getBytes()));
        final JsonObject object = jsonReader.readArray().getJsonObject(0);
        final JsonString backpressure = object.getJsonString("isBackPressureEnabled");
        final JsonString source = object.getJsonString("sourceName");
        assertEquals("true", backpressure.getString());
        assertEquals("source", source.getString());
        final JsonString dataSizeThreshold = object.getJsonString("backPressureDataSizeThreshold");
        final JsonNumber bytesThreshold = object.getJsonNumber("backPressureBytesThreshold");
        assertEquals("1 KB", dataSizeThreshold.getString());
        assertEquals(1024, bytesThreshold.intValue());
        assertNull(object.get("destinationName"));
    }

    @Test
    public void testConnectionStatusWithNullValues() throws IOException, InitializationException {
        final ProcessGroupStatus pgStatus = generateProcessGroupStatus("root", "Awesome", 1, 0);

        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(SiteToSiteUtils.BATCH_SIZE, "4");
        properties.put(SiteToSiteStatusReportingTask.COMPONENT_NAME_FILTER_REGEX, "Awesome.*");
        properties.put(SiteToSiteStatusReportingTask.COMPONENT_TYPE_FILTER_REGEX, "(Connection)");
        properties.put(SiteToSiteStatusReportingTask.ALLOW_NULL_VALUES, "true");

        final MockSiteToSiteStatusReportingTask task = initTask(properties, pgStatus);
        task.onTrigger(context);

        final String msg = new String(task.dataSent.getFirst(), StandardCharsets.UTF_8);
        final JsonReader jsonReader = Json.createReader(new ByteArrayInputStream(msg.getBytes()));
        final JsonObject object = jsonReader.readArray().getJsonObject(0);
        final JsonValue destination = object.get("destinationName");
        assertEquals(destination, JsonValue.NULL);

    }

    @Test
    public void testComponentNameFilter() throws IOException, InitializationException {
        final ProcessGroupStatus pgStatus = generateProcessGroupStatus("root", "Awesome", 1, 0);

        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(SiteToSiteUtils.BATCH_SIZE, "4");
        properties.put(SiteToSiteStatusReportingTask.COMPONENT_NAME_FILTER_REGEX, "Awesome.*processor.*");
        properties.put(SiteToSiteStatusReportingTask.COMPONENT_TYPE_FILTER_REGEX, ".*");

        final MockSiteToSiteStatusReportingTask task = initTask(properties, pgStatus);
        task.onTrigger(context);

        assertEquals(3, task.dataSent.size());  // 3 processors for each of 4 groups
        final String msg = new String(task.dataSent.getFirst(), StandardCharsets.UTF_8);
        final JsonReader jsonReader = Json.createReader(new ByteArrayInputStream(msg.getBytes()));
        final JsonString componentId = jsonReader.readArray().getJsonObject(0).getJsonString("componentId");
        assertEquals("root.1.processor.1", componentId.getString());
    }

    @Test
    public void testComponentNameFilter_nested() throws IOException, InitializationException {
        final ProcessGroupStatus pgStatus = generateProcessGroupStatus("root", "Awesome", 2, 0);

        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(SiteToSiteUtils.BATCH_SIZE, "4");
        properties.put(SiteToSiteStatusReportingTask.COMPONENT_NAME_FILTER_REGEX, "Awesome.*processor.*");
        properties.put(SiteToSiteStatusReportingTask.COMPONENT_TYPE_FILTER_REGEX, ".*");

        final MockSiteToSiteStatusReportingTask task = initTask(properties, pgStatus);
        task.onTrigger(context);

        assertEquals(10, task.dataSent.size());  // 3 + (3 * 3) + (3 * 3 * 3) = 39, or 10 batches of 4
        final String msg = new String(task.dataSent.getFirst(), StandardCharsets.UTF_8);
        final JsonReader jsonReader = Json.createReader(new ByteArrayInputStream(msg.getBytes()));
        final JsonString componentId = jsonReader.readArray().getJsonObject(0).getJsonString("componentId");
        assertEquals("root.1.1.processor.1", componentId.getString());
    }

    @Test
    public void testPortStatus() throws IOException, InitializationException {
        final ProcessGroupStatus pgStatus = generateProcessGroupStatus("root", "Awesome", 1, 0);

        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(SiteToSiteUtils.BATCH_SIZE, "4");
        properties.put(SiteToSiteStatusReportingTask.COMPONENT_NAME_FILTER_REGEX, "Awesome.*");
        properties.put(SiteToSiteStatusReportingTask.COMPONENT_TYPE_FILTER_REGEX, "(InputPort)");
        properties.put(SiteToSiteStatusReportingTask.ALLOW_NULL_VALUES, "false");

        final MockSiteToSiteStatusReportingTask task = initTask(properties, pgStatus);
        task.onTrigger(context);

        final String msg = new String(task.dataSent.getFirst(), StandardCharsets.UTF_8);
        final JsonReader jsonReader = Json.createReader(new ByteArrayInputStream(msg.getBytes()));
        final JsonObject object = jsonReader.readArray().getJsonObject(0);
        final JsonString runStatus = object.getJsonString("runStatus");
        assertEquals(RunStatus.Stopped.name(), runStatus.getString());
        final boolean isTransmitting = object.getBoolean("transmitting");
        assertFalse(isTransmitting);
        final JsonNumber inputBytes = object.getJsonNumber("inputBytes");
        assertEquals(5, inputBytes.intValue());
        assertNull(object.get("activeThreadCount"));
    }

    @Test
    public void testPortStatusWithNullValues() throws IOException, InitializationException {
        final ProcessGroupStatus pgStatus = generateProcessGroupStatus("root", "Awesome", 1, 0);

        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(SiteToSiteUtils.BATCH_SIZE, "4");
        properties.put(SiteToSiteStatusReportingTask.COMPONENT_NAME_FILTER_REGEX, "Awesome.*");
        properties.put(SiteToSiteStatusReportingTask.COMPONENT_TYPE_FILTER_REGEX, "(InputPort)");
        properties.put(SiteToSiteStatusReportingTask.ALLOW_NULL_VALUES, "true");
        final MockSiteToSiteStatusReportingTask task = initTask(properties, pgStatus);
        task.onTrigger(context);

        final String msg = new String(task.dataSent.getFirst(), StandardCharsets.UTF_8);
        final JsonReader jsonReader = Json.createReader(new ByteArrayInputStream(msg.getBytes()));
        final JsonObject object = jsonReader.readArray().getJsonObject(0);
        final JsonValue activeThreadCount = object.get("activeThreadCount");
        assertEquals(activeThreadCount, JsonValue.NULL);
    }

    @Test
    public void testRemoteProcessGroupStatus() throws IOException, InitializationException {
        final ProcessGroupStatus pgStatus = generateProcessGroupStatus("root", "Awesome", 1, 0);

        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(SiteToSiteUtils.BATCH_SIZE, "4");
        properties.put(SiteToSiteStatusReportingTask.COMPONENT_NAME_FILTER_REGEX, "Awesome.*");
        properties.put(SiteToSiteStatusReportingTask.COMPONENT_TYPE_FILTER_REGEX, "(RemoteProcessGroup)");

        final MockSiteToSiteStatusReportingTask task = initTask(properties, pgStatus);
        task.onTrigger(context);

        assertEquals(3, task.dataSent.size());
        final String msg = new String(task.dataSent.getFirst(), StandardCharsets.UTF_8);
        final JsonReader jsonReader = Json.createReader(new ByteArrayInputStream(msg.getBytes()));
        final JsonObject firstElement = jsonReader.readArray().getJsonObject(0);
        final JsonNumber activeThreadCount = firstElement.getJsonNumber("activeThreadCount");
        assertEquals(1L, activeThreadCount.longValue());
        final JsonString transmissionStatus = firstElement.getJsonString("transmissionStatus");
        assertEquals("Transmitting", transmissionStatus.getString());
    }

    @Test
    public void testRemoteProcessGroupStatusWithNullValues() throws IOException, InitializationException {
        final ProcessGroupStatus pgStatus = generateProcessGroupStatus("root", "Awesome", 1, 0);

        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(SiteToSiteUtils.BATCH_SIZE, "4");
        properties.put(SiteToSiteStatusReportingTask.COMPONENT_NAME_FILTER_REGEX, "Awesome.*");
        properties.put(SiteToSiteStatusReportingTask.COMPONENT_TYPE_FILTER_REGEX, "(RemoteProcessGroup)");
        properties.put(SiteToSiteStatusReportingTask.ALLOW_NULL_VALUES, "true");

        final MockSiteToSiteStatusReportingTask task = initTask(properties, pgStatus);
        task.onTrigger(context);

        assertEquals(3, task.dataSent.size());
        final String msg = new String(task.dataSent.getFirst(), StandardCharsets.UTF_8);
        final JsonReader jsonReader = Json.createReader(new ByteArrayInputStream(msg.getBytes()));
        final JsonObject firstElement = jsonReader.readArray().getJsonObject(0);
        final JsonValue targetURI = firstElement.get("targetURI");
        assertEquals(targetURI, JsonValue.NULL);
    }

    @Test
    public void testProcessorStatus() throws IOException, InitializationException {
        final ProcessGroupStatus pgStatus = generateProcessGroupStatus("root", "Awesome", 1, 0);

        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(SiteToSiteUtils.BATCH_SIZE, "4");
        properties.put(SiteToSiteStatusReportingTask.COMPONENT_NAME_FILTER_REGEX, "Awesome.*");
        properties.put(SiteToSiteStatusReportingTask.COMPONENT_TYPE_FILTER_REGEX, "(Processor)");

        final MockSiteToSiteStatusReportingTask task = initTask(properties, pgStatus);
        task.onTrigger(context);

        final String msg = new String(task.dataSent.getFirst(), StandardCharsets.UTF_8);
        final JsonReader jsonReader = Json.createReader(new ByteArrayInputStream(msg.getBytes()));
        final JsonObject object = jsonReader.readArray().getJsonObject(0);
        final JsonString parentName = object.getJsonString("parentName");
        assertTrue(parentName.getString().startsWith("Awesome.1-"));
        final JsonString parentPath = object.getJsonString("parentPath");
        assertTrue(parentPath.getString().startsWith("NiFi Flow / Awesome.1"));
        final JsonString runStatus = object.getJsonString("runStatus");
        assertEquals(RunStatus.Running.name(), runStatus.getString());
        final JsonNumber inputBytes = object.getJsonNumber("inputBytes");
        assertEquals(9, inputBytes.intValue());
        final JsonObject counterMap = object.getJsonObject("counters");
        assertNotNull(counterMap);
        assertEquals(10, counterMap.getInt("counter1"));
        assertEquals(5, counterMap.getInt("counter2"));
        assertNull(object.get("processorType"));
    }

    @Test
    public void testProcessorStatusWithNullValues() throws IOException, InitializationException {
        final ProcessGroupStatus pgStatus = generateProcessGroupStatus("root", "Awesome", 1, 0);

        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(SiteToSiteUtils.BATCH_SIZE, "4");
        properties.put(SiteToSiteStatusReportingTask.COMPONENT_NAME_FILTER_REGEX, "Awesome.*");
        properties.put(SiteToSiteStatusReportingTask.COMPONENT_TYPE_FILTER_REGEX, "(Processor)");
        properties.put(SiteToSiteStatusReportingTask.ALLOW_NULL_VALUES, "true");

        final MockSiteToSiteStatusReportingTask task = initTask(properties, pgStatus);
        task.onTrigger(context);

        final String msg = new String(task.dataSent.getFirst(), StandardCharsets.UTF_8);
        final JsonReader jsonReader = Json.createReader(new ByteArrayInputStream(msg.getBytes()));
        final JsonObject object = jsonReader.readArray().getJsonObject(0);
        final JsonValue type = object.get("processorType");
        assertEquals(type, JsonValue.NULL);
    }

    /***********************************
     * Test component generator methods
     ***********************************/

    public static ProcessGroupStatus generateProcessGroupStatus(final String id, final String namePrefix,
            final int maxRecursion, final int currentDepth) {
        final Collection<ConnectionStatus> cStatus = new ArrayList<>();
        final Collection<PortStatus> ipStatus = new ArrayList<>();
        final Collection<PortStatus> opStatus = new ArrayList<>();
        final Collection<ProcessorStatus> pStatus = new ArrayList<>();
        final Collection<RemoteProcessGroupStatus> rpgStatus = new ArrayList<>();
        final Collection<ProcessGroupStatus> childPgStatus = new ArrayList<>();

        if (currentDepth < maxRecursion) {
            for (int i = 1; i < 4; i++) {
                childPgStatus.add(generateProcessGroupStatus(id + "." + i, namePrefix + "." + i,
                        maxRecursion, currentDepth + 1));
            }
        }
        for (int i = 1; i < 4; i++) {
            pStatus.add(generateProcessorStatus(id + ".processor." + i, namePrefix + ".processor." + i));
        }
        for (int i = 1; i < 4; i++) {
            cStatus.add(generateConnectionStatus(id + ".connection." + i, namePrefix + ".connection." + i));
        }
        for (int i = 1; i < 4; i++) {
            rpgStatus.add(generateRemoteProcessGroupStatus(id + ".rpg." + i, namePrefix + ".rpg." + i));
        }
        for (int i = 1; i < 4; i++) {
            ipStatus.add(generatePortStatus(id + ".ip." + i, namePrefix + ".ip." + i));
        }
        for (int i = 1; i < 4; i++) {
            opStatus.add(generatePortStatus(id + ".op." + i, namePrefix + ".op." + i));
        }

        final ProcessGroupStatus pgStatus = new ProcessGroupStatus();
        pgStatus.setId(id);
        pgStatus.setName(namePrefix + "-" + UUID.randomUUID());
        pgStatus.setInputPortStatus(ipStatus);
        pgStatus.setOutputPortStatus(opStatus);
        pgStatus.setProcessGroupStatus(childPgStatus);
        pgStatus.setRemoteProcessGroupStatus(rpgStatus);
        pgStatus.setProcessorStatus(pStatus);
        pgStatus.setVersionedFlowState(VersionedFlowState.UP_TO_DATE);
        pgStatus.setActiveThreadCount(1);
        pgStatus.setBytesRead(2L);
        pgStatus.setBytesReceived(3L);
        pgStatus.setBytesSent(4L);
        pgStatus.setBytesTransferred(5L);
        pgStatus.setBytesWritten(6L);
        pgStatus.setConnectionStatus(cStatus);
        pgStatus.setFlowFilesReceived(7);
        pgStatus.setFlowFilesSent(8);
        pgStatus.setFlowFilesTransferred(9);
        pgStatus.setInputContentSize(10L);
        pgStatus.setInputCount(11);
        pgStatus.setOutputContentSize(12L);
        pgStatus.setOutputCount(13);
        pgStatus.setQueuedContentSize(14L);
        pgStatus.setQueuedCount(15);
        pgStatus.setTerminatedThreadCount(1);

        return pgStatus;
    }

    public static PortStatus generatePortStatus(final String id, final String namePrefix) {
        final PortStatus pStatus = new PortStatus();
        pStatus.setId(id);
        pStatus.setName(namePrefix + "-" + UUID.randomUUID());
        pStatus.setActiveThreadCount(null);
        pStatus.setBytesReceived(1L);
        pStatus.setBytesSent(2L);
        pStatus.setFlowFilesReceived(3);
        pStatus.setFlowFilesSent(4);
        pStatus.setInputBytes(5L);
        pStatus.setInputCount(6);
        pStatus.setOutputBytes(7L);
        pStatus.setOutputCount(8);
        pStatus.setRunStatus(RunStatus.Stopped);
        pStatus.setTransmitting(false);

        return pStatus;
    }

    public static ProcessorStatus generateProcessorStatus(final String id, final String namePrefix) {
        final ProcessorStatus pStatus = new ProcessorStatus();
        pStatus.setId(id);
        pStatus.setName(namePrefix + "-" + UUID.randomUUID());
        pStatus.setActiveThreadCount(0);
        pStatus.setAverageLineageDuration(1L);
        pStatus.setBytesRead(2L);
        pStatus.setBytesReceived(3L);
        pStatus.setBytesSent(4L);
        pStatus.setBytesWritten(5L);
        pStatus.setFlowFilesReceived(6);
        pStatus.setFlowFilesRemoved(7);
        pStatus.setFlowFilesSent(8);
        pStatus.setInputBytes(9L);
        pStatus.setInputCount(10);
        pStatus.setInvocations(11);
        pStatus.setOutputBytes(12L);
        pStatus.setOutputCount(13);
        pStatus.setProcessingNanos(14L);
        pStatus.setType(null);
        pStatus.setTerminatedThreadCount(1);
        pStatus.setRunStatus(RunStatus.Running);
        pStatus.setCounters(Map.of("counter1", 10L, "counter2", 5L));

        return pStatus;
    }

    public static RemoteProcessGroupStatus generateRemoteProcessGroupStatus(final String id, final String namePrefix) {
        final RemoteProcessGroupStatus rpgStatus = new RemoteProcessGroupStatus();
        rpgStatus.setId(id);
        rpgStatus.setName(namePrefix + "-" + UUID.randomUUID());
        rpgStatus.setActiveRemotePortCount(0);
        rpgStatus.setActiveThreadCount(1);
        rpgStatus.setAverageLineageDuration(2L);
        rpgStatus.setInactiveRemotePortCount(3);
        rpgStatus.setReceivedContentSize(4L);
        rpgStatus.setReceivedCount(5);
        rpgStatus.setSentContentSize(6L);
        rpgStatus.setSentCount(7);
        rpgStatus.setTargetUri(null);
        rpgStatus.setTransmissionStatus(TransmissionStatus.Transmitting);

        return rpgStatus;
    }

    public static ConnectionStatus generateConnectionStatus(final String id, final String namePrefix) {
        final ConnectionStatus cStatus = new ConnectionStatus();
        cStatus.setId(id);
        cStatus.setName(namePrefix + "-" + UUID.randomUUID());
        cStatus.setBackPressureDataSizeThreshold("1 KB"); // sets backPressureBytesThreshold too
        cStatus.setBackPressureObjectThreshold(1L);
        cStatus.setInputBytes(2L);
        cStatus.setInputCount(3);
        cStatus.setMaxQueuedBytes(4L);
        cStatus.setMaxQueuedCount(5);
        cStatus.setOutputBytes(6);
        cStatus.setOutputCount(7);
        cStatus.setQueuedBytes(8L);
        cStatus.setQueuedCount(9);
        cStatus.setSourceId(id);
        cStatus.setSourceName("source");
        cStatus.setDestinationId(id);
        cStatus.setDestinationName(null);

        return cStatus;
    }

    public static FlowFile createFlowFile(final long id, final Map<String, String> attributes) {
        final MockFlowFile mockFlowFile = new MockFlowFile(id);
        mockFlowFile.putAttributes(attributes);
        return mockFlowFile;
    }

    private static final class MockSiteToSiteStatusReportingTask extends SiteToSiteStatusReportingTask {

        public MockSiteToSiteStatusReportingTask() throws IOException {
        }

        final List<byte[]> dataSent = new ArrayList<>();

        @Override
        public void setup(final PropertyContext reportContext) {
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
