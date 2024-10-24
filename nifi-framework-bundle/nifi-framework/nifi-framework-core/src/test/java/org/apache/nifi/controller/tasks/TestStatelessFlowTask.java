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

package org.apache.nifi.controller.tasks;

import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.ConnectionUtils.FlowFileCloneResult;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.MockFlowFileRecord;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.controller.repository.FlowFileEvent;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.RepositoryRecord;
import org.apache.nifi.controller.repository.RepositoryRecordType;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.controller.repository.claim.StandardContentClaim;
import org.apache.nifi.controller.repository.claim.StandardResourceClaim;
import org.apache.nifi.controller.repository.claim.StandardResourceClaimManager;
import org.apache.nifi.controller.repository.metrics.StandardFlowFileEvent;
import org.apache.nifi.controller.service.mock.MockProcessGroup;
import org.apache.nifi.controller.tasks.StatelessFlowTask.Invocation;
import org.apache.nifi.controller.tasks.StatelessFlowTask.PolledFlowFile;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.StatelessGroupNode;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.mock.MockComponentLogger;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.provenance.StandardProvenanceEventRecord;
import org.apache.nifi.stateless.flow.StatelessDataflow;
import org.apache.nifi.stateless.flow.TriggerResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestStatelessFlowTask {

    private StatelessFlowTask task;
    private ProcessGroup rootGroup;
    private Port successPort;
    private Port secondOutputPort;
    private Port inputPort1;
    private ResourceClaimManager resourceClaimManager;
    private List<ProvenanceEventRecord> registeredProvenanceEvents;
    private List<ProvenanceEventRecord> statelessProvenanceEvents;
    private Map<String, StandardFlowFileEvent> flowFileEventsByComponentId;
    private ProvenanceEventRepository statelessProvRepo;

    @BeforeEach
    public void setup() throws IOException {
        rootGroup = new MockProcessGroup(null);

        inputPort1 = mock(Port.class);
        when(inputPort1.getName()).thenReturn("input1");
        when(inputPort1.getIdentifier()).thenReturn("inputPortId");
        rootGroup.addInputPort(inputPort1);

        successPort = mock(Port.class);
        when(successPort.getName()).thenReturn("success");
        when(successPort.getIdentifier()).thenReturn("successPortId");
        rootGroup.addOutputPort(successPort);

        secondOutputPort = mock(Port.class);
        when(secondOutputPort.getName()).thenReturn("port2");
        when(secondOutputPort.getIdentifier()).thenReturn("port2Id");
        rootGroup.addOutputPort(secondOutputPort);

        statelessProvenanceEvents = new ArrayList<>();
        statelessProvRepo = mock(ProvenanceEventRepository.class);
        doAnswer(invocation -> statelessProvenanceEvents.size() - 1).when(statelessProvRepo).getMaxEventId();
        doAnswer(invocation -> {
            final long startEventId = invocation.getArgument(0, Long.class);
            final int numEvents = invocation.getArgument(1, Integer.class);
            if (startEventId >= statelessProvenanceEvents.size()) {
                return Collections.emptyList();
            }
            final long lastEvent = Math.min(startEventId + numEvents, statelessProvenanceEvents.size());
            return statelessProvenanceEvents.subList((int) startEventId, (int) lastEvent);
        }).when(statelessProvRepo).getEvents(anyLong(), anyInt());

        final StatelessDataflow statelessFlow = mock(StatelessDataflow.class);

        final StatelessGroupNode statelessGroupNode = mock(StatelessGroupNode.class);
        when(statelessGroupNode.getProcessGroup()).thenReturn(rootGroup);

        final FlowFileRepository flowFileRepo = mock(FlowFileRepository.class);

        resourceClaimManager = new StandardResourceClaimManager();
        final ContentRepository contentRepo = mock(ContentRepository.class);
        doAnswer(invocation -> {
            final ContentClaim claim = invocation.getArgument(0, ContentClaim.class);
            if (claim == null) {
                return 0;
            }

            return resourceClaimManager.incrementClaimantCount(claim.getResourceClaim());
        }).when(contentRepo).incrementClaimaintCount(any(ContentClaim.class));

        doAnswer(invocation -> {
            final ContentClaim claim = invocation.getArgument(0, ContentClaim.class);
            if (claim == null) {
                return 0;
            }

            return resourceClaimManager.decrementClaimantCount(claim.getResourceClaim());
        }).when(contentRepo).decrementClaimantCount(any(ContentClaim.class));

        flowFileEventsByComponentId = new HashMap<>();
        final FlowFileEventRepository eventRepository = mock(FlowFileEventRepository.class);
        doAnswer(invocation -> {
            final String componentId = invocation.getArgument(1, String.class);
            final FlowFileEvent event = invocation.getArgument(0, FlowFileEvent.class);

            final StandardFlowFileEvent existingEvent = flowFileEventsByComponentId.computeIfAbsent(componentId, key -> new StandardFlowFileEvent());
            existingEvent.add(event);
            return null;
        }).when(eventRepository).updateRepository(any(FlowFileEvent.class), anyString());
        final ComponentLog logger = new MockComponentLogger();

        registeredProvenanceEvents = new ArrayList<>();
        final ProvenanceEventRepository provenanceRepo = mock(ProvenanceEventRepository.class);
        doAnswer(invocation -> {
            final Iterable<ProvenanceEventRecord> events = invocation.getArgument(0, Iterable.class);
            events.forEach(registeredProvenanceEvents::add);
            return null;
        }).when(provenanceRepo).registerEvents(any(Iterable.class));

        task = new StatelessFlowTask.Builder()
            .statelessFlow(statelessFlow)
            .statelessGroupNode(statelessGroupNode)
            .nifiFlowFileRepository(flowFileRepo)
            .nifiContentRepository(contentRepo)
            .nifiProvenanceRepository(provenanceRepo)
            .flowFileEventRepository(eventRepository)
            .logger(logger)
            .build();

        task.resetState();
    }

    @Test
    public void testDropInputFlowFile() {
        // Test method with an Invocation that has no input FlowFile
        final TriggerResult triggerResult = mock(TriggerResult.class);
        final Invocation noInputInvocation = new Invocation();
        noInputInvocation.setTriggerResult(triggerResult);
        task.dropInputFlowFiles(noInputInvocation);
        assertTrue(task.getOutputRepositoryRecords().isEmpty());

        final FlowFileRecord inputFlowFile = new MockFlowFileRecord(1L);
        final FlowFileQueue queue = mock(FlowFileQueue.class);
        final Port port = mock(Port.class);

        // Test method with multiple Invocations each having an input FlowFile
        for (int i = 0; i < 5; i++) {
            final Invocation invocation = new Invocation();
            invocation.setTriggerResult(triggerResult);
            invocation.addPolledFlowFile(new PolledFlowFile(inputFlowFile, queue, port));

            task.dropInputFlowFiles(invocation);
            final List<RepositoryRecord> dropRecords = task.getOutputRepositoryRecords();
            assertEquals(i + 1, dropRecords.size());
            final RepositoryRecord dropRecord = dropRecords.get(i);
            assertEquals(RepositoryRecordType.DELETE, dropRecord.getType());
            assertEquals(inputFlowFile, dropRecord.getCurrent());
        }
    }

    @Test
    public void testCreateOutputRecordsOnSuccessWithOneOutput() {
        final Connection conn1 = mockConnection();
        final Set<Connection> singleConnectionSet = Collections.singleton(conn1);

        when(successPort.getConnections()).thenReturn(singleConnectionSet);

        final Map<String, List<FlowFile>> outputFlowFiles = new HashMap<>();
        final FlowFileRecord flowFileRecord = new MockFlowFileRecord(1L);
        outputFlowFiles.put("success", Collections.singletonList(flowFileRecord));
        task.createOutputRecords(outputFlowFiles);

        final List<RepositoryRecord> outputRecords = task.getOutputRepositoryRecords();
        assertEquals(1, outputRecords.size());

        final List<FlowFileCloneResult> cloneResults = task.getCloneResults();
        assertEquals(1, cloneResults.size());
        final FlowFileCloneResult cloneResult = cloneResults.get(0);
        final Map<FlowFileQueue, List<FlowFileRecord>> outputAfterClone = cloneResult.getFlowFilesToEnqueue();
        assertEquals(1, outputAfterClone.size());
        final List<FlowFileRecord> flowFilesForQueue = outputAfterClone.get(conn1.getFlowFileQueue());
        assertEquals(Collections.singletonList(flowFileRecord), flowFilesForQueue);

        assertTrue(task.getCloneProvenanceEvents().isEmpty());
    }

    @Test
    public void testCreateOutputRecordsOnSuccessWithOnePortTwoConnections() {
        final FlowFileQueue queue1 = mock(FlowFileQueue.class);
        final Connection conn1 = mock(Connection.class);
        when(conn1.getFlowFileQueue()).thenReturn(queue1);

        final FlowFileQueue queue2 = mock(FlowFileQueue.class);
        final Connection conn2 = mock(Connection.class);
        when(conn2.getFlowFileQueue()).thenReturn(queue2);

        final Set<Connection> connectionSet = Set.of(conn1, conn2);
        when(successPort.getConnections()).thenReturn(connectionSet);

        final FlowFileRecord flowFileRecord = new MockFlowFileRecord(1L);
        final Map<String, List<FlowFile>> outputFlowFiles = new HashMap<>();
        outputFlowFiles.put("success", Collections.singletonList(flowFileRecord));
        task.createOutputRecords(outputFlowFiles);

        final List<RepositoryRecord> outputRecords = task.getOutputRepositoryRecords();
        assertEquals(2, outputRecords.size());

        final List<FlowFileCloneResult> cloneResults = task.getCloneResults();
        assertEquals(1, cloneResults.size());
        final FlowFileCloneResult cloneResult = cloneResults.getFirst();
        final Map<FlowFileQueue, List<FlowFileRecord>> outputAfterClone = cloneResult.getFlowFilesToEnqueue();
        assertEquals(2, outputAfterClone.size());

        assertEquals(1, outputAfterClone.get(queue1).size());
        assertEquals(1, outputAfterClone.get(queue2).size());
        assertNotEquals(outputAfterClone.get(queue1), outputAfterClone.get(queue2));

        final List<ProvenanceEventRecord> cloneEvents = task.getCloneProvenanceEvents();
        assertEquals(1, cloneEvents.size());
        final ProvenanceEventRecord cloneEvent = cloneEvents.getFirst();

        assertEquals(Collections.singletonList(flowFileRecord.getAttribute(CoreAttributes.UUID.key())), cloneEvent.getParentUuids());

        // The clone event's child UUIDs should have either the UUID of the FlowFile in queue1 or queue2 but we aren't sure which, as there's no guarantee
        // whether the original will go to queue1 and the clone to queue2 or vice versa.
        assertTrue(cloneEvent.getChildUuids().contains(outputAfterClone.get(queue1).getFirst().getAttribute(CoreAttributes.UUID.key()))
            || cloneEvent.getChildUuids().contains(outputAfterClone.get(queue2).getFirst().getAttribute(CoreAttributes.UUID.key())));
        assertFalse(cloneEvent.getChildUuids().contains(cloneEvent.getParentUuids().getFirst()));
    }

    @Test
    public void testCreateOutputRecordsOnSuccessWithTwoPorts() {
        final Connection successConnection = mockConnection();
        when(successPort.getConnections()).thenReturn(Collections.singleton(successConnection));

        final Connection port2Connection = mockConnection();
        when(secondOutputPort.getConnections()).thenReturn(Collections.singleton(port2Connection));

        final FlowFileRecord flowFileRecord = new MockFlowFileRecord(1L);
        final FlowFileRecord port2FlowFile1 = new MockFlowFileRecord(2L);
        final FlowFileRecord port2FlowFile2 = new MockFlowFileRecord(3L);

        final Map<String, List<FlowFile>> outputFlowFiles = new HashMap<>();
        outputFlowFiles.put("success", Collections.singletonList(flowFileRecord));
        outputFlowFiles.put("port2", Arrays.asList(port2FlowFile1, port2FlowFile2));
        task.createOutputRecords(outputFlowFiles);

        final List<RepositoryRecord> outputRecords = task.getOutputRepositoryRecords();
        assertEquals(3, outputRecords.size());

        // We should have a FlowFileCloneResult for each output FlowFile.
        // The results are a bit messy to check because of the collections that have indeterminate ordering.
        // Because of that, we'll check that the collections have the correct counts, and that each of the FlowFiles is found in one of the collections.
        final List<FlowFileCloneResult> cloneResults = task.getCloneResults();
        assertEquals(3, cloneResults.size());

        int successQueueCount = 0;
        int port2Count = 0;
        boolean port2FlowFile1Found = false;
        boolean port2FlowFile2Found = false;
        for (final FlowFileCloneResult cloneResult : cloneResults) {
            final Map<FlowFileQueue, List<FlowFileRecord>> outputAfterClone = cloneResult.getFlowFilesToEnqueue();
            assertEquals(1, outputAfterClone.size());
            assertEquals(1, outputAfterClone.values().iterator().next().size());

            final FlowFileQueue successQueue = successConnection.getFlowFileQueue();
            final FlowFileQueue port2Queue = port2Connection.getFlowFileQueue();

            if (outputAfterClone.containsKey(successQueue)) {
                assertEquals(Collections.singletonList(flowFileRecord), outputAfterClone.get(successQueue));
                successQueueCount++;
            } else if (outputAfterClone.containsKey(port2Queue)) {
                assertEquals(1, outputAfterClone.get(port2Queue).size());
                final FlowFileRecord outputFlowFile = outputAfterClone.get(port2Queue).get(0);
                if (outputFlowFile.equals(port2FlowFile1)) {
                    port2FlowFile1Found = true;
                } else if (outputFlowFile.equals(port2FlowFile2)) {
                    port2FlowFile2Found = true;
                } else {
                    Assertions.fail("Found FlowFileCloneResult for unknown FlowFile " + outputFlowFile);
                }

                port2Count++;
            } else {
                Assertions.fail("Found FlowFileCloneResult for unknown FlowFileQueue " + outputAfterClone.keySet());
            }
        }

        assertEquals(1, successQueueCount);
        assertEquals(2, port2Count);
        assertTrue(port2FlowFile1Found);
        assertTrue(port2FlowFile2Found);

        assertTrue(task.getCloneProvenanceEvents().isEmpty());
    }

    @Test
    public void testUpdateClaimantCountSingleOutput() {
        final Connection conn1 = mockConnection();
        final Set<Connection> singleConnectionSet = Collections.singleton(conn1);

        when(successPort.getConnections()).thenReturn(singleConnectionSet);

        final FlowFileRecord flowFileRecord = createFlowFile();
        final Map<String, List<FlowFile>> outputFlowFiles = new HashMap<>();
        outputFlowFiles.put("success", Collections.singletonList(flowFileRecord));
        task.createOutputRecords(outputFlowFiles);

        task.updateClaimantCounts();
        assertEquals(1, resourceClaimManager.getClaimantCount(flowFileRecord.getContentClaim().getResourceClaim()));
    }

    @Test
    public void testUpdateClaimantCountWithClone() {
        final Connection conn1 = mockConnection();
        final Connection conn2 = mockConnection();

        final Set<Connection> connectionSet = new HashSet<>(Arrays.asList(conn1, conn2));
        when(successPort.getConnections()).thenReturn(connectionSet);

        final FlowFileRecord flowFileRecord = createFlowFile();
        when(flowFileRecord.getAttribute("uuid")).thenReturn("uuid1");

        assertEquals(0, resourceClaimManager.getClaimantCount(flowFileRecord.getContentClaim().getResourceClaim()));

        final Map<String, List<FlowFile>> outputFlowFiles = new HashMap<>();
        outputFlowFiles.put("success", Collections.singletonList(flowFileRecord));
        task.createOutputRecords(outputFlowFiles);

        task.updateClaimantCounts();
        assertEquals(2, resourceClaimManager.getClaimantCount(flowFileRecord.getContentClaim().getResourceClaim()));
    }

    @Test
    public void testContentClaimCountWhenMultipleFlowFilesTransferredToPort() {
        final Connection conn1 = mockConnection();
        final Set<Connection> singleConnectionSet = Collections.singleton(conn1);

        when(successPort.getConnections()).thenReturn(singleConnectionSet);

        final Map<String, List<FlowFile>> outputFlowFiles = new HashMap<>();
        final FlowFileRecord flowFile1 = createFlowFile();
        final FlowFileRecord flowFile2 = createFlowFile();
        outputFlowFiles.put("success", Arrays.asList(flowFile1, flowFile2));
        task.createOutputRecords(outputFlowFiles);

        task.updateClaimantCounts();
        assertEquals(2, resourceClaimManager.getClaimantCount(flowFile1.getContentClaim().getResourceClaim()));
    }

    private Connection mockConnection() {
        final FlowFileQueue queue = mock(FlowFileQueue.class);
        final Connection connection = mock(Connection.class);
        when(connection.getFlowFileQueue()).thenReturn(queue);
        return connection;
    }

    @Test
    public void testProvenanceEventsCopiedFromStatelessFlow() {
        for (int i = 0; i < 5; i++) {
            final ProvenanceEventRecord event = new StandardProvenanceEventRecord.Builder()
                .setEventId(i)
                .setEventType(ProvenanceEventType.CREATE)
                .setComponentId("component-" + i)
                .setEventTime(System.currentTimeMillis())
                .setFlowFileUUID("uuid-" + i)
                .setComponentType("Unit Test")
                .setCurrentContentClaim(null, null, null, null, 0)
                .build();
            statelessProvenanceEvents.add(event);
        }

        task.updateProvenanceRepository(statelessProvRepo, event -> true);

        assertEquals(statelessProvenanceEvents, registeredProvenanceEvents);
        for (final ProvenanceEventRecord eventRecord : registeredProvenanceEvents) {
            assertEquals(-1, eventRecord.getEventId());
        }
    }

    @Test
    public void testMoreThan1000ProvenanceEventsCopied() {
        for (int i = 0; i < 2003; i++) {
            final ProvenanceEventRecord event = new StandardProvenanceEventRecord.Builder()
                .setEventId(i)
                .setEventType(ProvenanceEventType.CREATE)
                .setComponentId("component-" + i)
                .setEventTime(System.currentTimeMillis())
                .setFlowFileUUID("uuid-" + i)
                .setComponentType("Unit Test")
                .setCurrentContentClaim(null, null, null, null, 0)
                .build();
            statelessProvenanceEvents.add(event);
        }

        task.updateProvenanceRepository(statelessProvRepo, event -> true);

        assertEquals(statelessProvenanceEvents, registeredProvenanceEvents);
        for (final ProvenanceEventRecord eventRecord : registeredProvenanceEvents) {
            assertEquals(-1, eventRecord.getEventId());
        }
    }

    @Test
    public void testUpdateEventRepositoryWithNoInputFlowFile() {
        when(successPort.getConnections()).thenReturn(Collections.singleton(mock(Connection.class)));
        when(secondOutputPort.getConnections()).thenReturn(Collections.singleton(mock(Connection.class)));

        final Map<String, List<FlowFile>> outputFlowFiles = new HashMap<>();
        outputFlowFiles.put(successPort.getName(), Collections.singletonList(createFlowFile()));
        outputFlowFiles.put(secondOutputPort.getName(), Arrays.asList(createFlowFile(), createFlowFile(), createFlowFile()));

        final TriggerResult triggerResult = mock(TriggerResult.class);
        when(triggerResult.getOutputFlowFiles()).thenReturn(outputFlowFiles);

        final Invocation noInputInvocation = new Invocation();
        noInputInvocation.setTriggerResult(triggerResult);

        task.updateEventRepository(Collections.singletonList(noInputInvocation));

        final FlowFileEvent port1Event = flowFileEventsByComponentId.get(successPort.getIdentifier());
        assertNotNull(port1Event);
        assertEquals(1, port1Event.getFlowFilesOut());
        assertEquals(5, port1Event.getContentSizeOut());

        final FlowFileEvent port2Event = flowFileEventsByComponentId.get(secondOutputPort.getIdentifier());
        assertNotNull(port2Event);
        assertEquals(3, port2Event.getFlowFilesOut());
        assertEquals(15, port2Event.getContentSizeOut());
    }

    @Test
    public void testUpdateEventRepositoryWithOutputCloned() {
        when(successPort.getConnections()).thenReturn(new HashSet<>(Arrays.asList(mock(Connection.class), mock(Connection.class), mock(Connection.class))));

        final Map<String, List<FlowFile>> outputFlowFiles = new HashMap<>();
        outputFlowFiles.put(successPort.getName(), Collections.singletonList(createFlowFile()));

        final TriggerResult triggerResult = mock(TriggerResult.class);
        when(triggerResult.getOutputFlowFiles()).thenReturn(outputFlowFiles);

        final Invocation noInputInvocation = new Invocation();
        noInputInvocation.setTriggerResult(triggerResult);

        task.updateEventRepository(Collections.singletonList(noInputInvocation));

        final FlowFileEvent port1Event = flowFileEventsByComponentId.get(successPort.getIdentifier());
        assertNotNull(port1Event);
        assertEquals(3, port1Event.getFlowFilesOut());
        assertEquals(15, port1Event.getContentSizeOut());
        assertEquals(1, port1Event.getFlowFilesIn());
        assertEquals(5, port1Event.getContentSizeIn());
    }

    @Test
    public void testUpdateEventRepositoryWithInputFlowFile() {
        final Map<String, List<FlowFile>> outputFlowFiles = Collections.emptyMap();
        final TriggerResult triggerResult = mock(TriggerResult.class);
        when(triggerResult.getOutputFlowFiles()).thenReturn(outputFlowFiles);

        final FlowFileRecord inputFlowFile = createFlowFile();
        final FlowFileQueue queue = mock(FlowFileQueue.class);

        when(inputPort1.getConnections()).thenReturn(new HashSet<>(Arrays.asList(mock(Connection.class), mock(Connection.class))));

        // Test method with multiple Invocations each having an input FlowFile
        final Invocation invocationWithInput = new Invocation();
        invocationWithInput.setTriggerResult(triggerResult);
        invocationWithInput.addPolledFlowFile(new PolledFlowFile(inputFlowFile, queue, inputPort1));

        task.updateEventRepository(Collections.singletonList(invocationWithInput));
        final FlowFileEvent inputPortEvent = flowFileEventsByComponentId.get(inputPort1.getIdentifier());
        assertNotNull(inputPortEvent);
        assertEquals(1, inputPortEvent.getFlowFilesIn());
        assertEquals(5, inputPortEvent.getContentSizeIn());
        assertEquals(2, inputPortEvent.getFlowFilesOut());
        assertEquals(10, inputPortEvent.getContentSizeOut());
    }


    private FlowFileRecord createFlowFile() {
        final ResourceClaim resourceClaim = new StandardResourceClaim(resourceClaimManager, "container", "section", "1", false);
        final ContentClaim contentClaim = new StandardContentClaim(resourceClaim, 0L);
        final FlowFileRecord flowFileRecord = mock(FlowFileRecord.class);
        when(flowFileRecord.getContentClaim()).thenReturn(contentClaim);
        when(flowFileRecord.getSize()).thenReturn(5L);

        return flowFileRecord;
    }
}
