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

package org.apache.nifi.tests.system.connectors;

import jakarta.ws.rs.WebApplicationException;
import org.apache.nifi.components.connector.ConnectorState;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.AssetReferenceDTO;
import org.apache.nifi.web.api.dto.ConnectorConfigurationDTO;
import org.apache.nifi.web.api.dto.ConnectorValueReferenceDTO;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.flow.FlowDTO;
import org.apache.nifi.web.api.entity.ActivateControllerServicesEntity;
import org.apache.nifi.web.api.entity.AssetEntity;
import org.apache.nifi.web.api.entity.AssetsEntity;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ParameterProviderEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.ScheduleComponentsEntity;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * System tests that validate the Troubleshooting lifecycle of Connectors.
 */
public class ConnectorTroubleshootingIT extends NiFiSystemIT {

    /**
     * Transition a Connector into Troubleshooting, modify a processor inside the managed flow, then transition back
     * out. The Connector's authoritative flow should be restored on exit and the Connector should start smoothly.
     */
    @Test
    public void testEnterAndExitTroubleshootingRestoresFlow() throws NiFiClientException, IOException, InterruptedException {
        final ConnectorEntity connector = getClientUtil().createConnector("ComponentLifecycleConnector");
        final String connectorId = connector.getId();

        getClientUtil().applyConnectorUpdate(connector);
        getClientUtil().waitForValidConnector(connectorId);

        final List<ProcessorEntity> originalProcessors = findAllProcessors(connectorId);
        assertFalse(originalProcessors.isEmpty(), "Managed flow should contain processors");
        final ProcessorEntity originalProcessor = originalProcessors.get(0);
        final String originalSchedulingPeriod = originalProcessor.getComponent().getConfig().getSchedulingPeriod();

        getClientUtil().enterTroubleshooting(connectorId);
        assertConnectorState(connectorId, ConnectorState.TROUBLESHOOTING);

        final ProcessorEntity fetchedProcessor = getNifiClient().getProcessorClient().getProcessor(originalProcessor.getId());
        assertNotNull(fetchedProcessor);

        fetchedProcessor.getComponent().getConfig().setSchedulingPeriod("42 sec");
        final ProcessorEntity updatedProcessor = getNifiClient().getProcessorClient().updateProcessor(fetchedProcessor);
        assertEquals("42 sec", updatedProcessor.getComponent().getConfig().getSchedulingPeriod());

        getClientUtil().endTroubleshooting(connectorId);
        assertConnectorState(connectorId, ConnectorState.STOPPED);

        final ProcessorEntity restoredProcessor = findProcessorByName(connectorId, originalProcessor.getComponent().getName());
        assertNotNull(restoredProcessor, "Expected original processor to be restored by authoritative flow");
        assertEquals(originalSchedulingPeriod, restoredProcessor.getComponent().getConfig().getSchedulingPeriod(),
                "Scheduling period should be restored to authoritative value");

        getClientUtil().startConnector(connectorId);
        assertConnectorState(connectorId, ConnectorState.RUNNING);
    }

    /**
     * Transition into Troubleshooting, add a new Connection, queue up data in that Connection, and verify that ending
     * Troubleshooting fails with a 409. Restart NiFi and verify the data is still queued. Drop all FlowFiles, then end
     * Troubleshooting successfully.
     */
    @Test
    public void testEndTroubleshootingBlockedByQueuedData() throws NiFiClientException, IOException, InterruptedException {
        final ConnectorEntity connector = getClientUtil().createConnector("ComponentLifecycleConnector");
        final String connectorId = connector.getId();

        getClientUtil().applyConnectorUpdate(connector);
        getClientUtil().waitForValidConnector(connectorId);

        getClientUtil().enterTroubleshooting(connectorId);

        final String managedGroupId = getNifiClient().getConnectorClient().getConnector(connectorId).getComponent().getManagedProcessGroupId();
        assertNotNull(managedGroupId);

        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile", managedGroupId);
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile", managedGroupId);
        getClientUtil().updateProcessorProperties(generate, Map.of("File Size", "10 B"));
        getClientUtil().updateProcessorSchedulingPeriod(generate, "10 ms");

        final ConnectionEntity connection = getClientUtil().createConnection(generate, terminate, "success", managedGroupId);
        final String connectionId = connection.getId();

        getClientUtil().startProcessor(generate);
        waitForQueuedFlowFiles(connectorId, connectionId, 1);
        getClientUtil().stopProcessor(generate);

        try {
            getClientUtil().endTroubleshooting(connectorId);
            fail("Expected endTroubleshooting to fail with 409 while connection has queued data");
        } catch (final NiFiClientException e) {
            assertConflict(e);
        }

        assertConnectorState(connectorId, ConnectorState.TROUBLESHOOTING);

        restartNiFi();

        assertConnectorState(connectorId, ConnectorState.TROUBLESHOOTING);
        final OptionalInt queued = getQueuedCount(connectorId, connectionId);
        assertTrue(queued.isPresent(), "Connection must still be present after restart");
        assertTrue(queued.getAsInt() > 0, "Queued data must survive restart");

        getClientUtil().emptyQueue(connectionId);
        waitForQueuedFlowFiles(connectorId, connectionId, 0);

        getClientUtil().endTroubleshooting(connectorId);
        assertConnectorState(connectorId, ConnectorState.STOPPED);

        final List<String> processorIdsAfterRestore = findAllProcessors(connectorId).stream()
                .map(ProcessorEntity::getId)
                .toList();

        assertFalse(processorIdsAfterRestore.contains(generate.getId()), "GenerateFlowFile processor added in Troubleshooting should be removed once authoritative flow is restored");
        assertFalse(processorIdsAfterRestore.contains(terminate.getId()), "TerminateFlowFile processor added in Troubleshooting should be removed once authoritative flow is restored");
        assertFalse(getQueuedCount(connectorId, connectionId).isPresent(), "Connection added in Troubleshooting should be removed once authoritative flow is restored");
    }

    /**
     * Verify that GET/PUT/POST/DELETE on a processor that lives within a Connector's managed flow is denied with a 409
     * when the Connector is not in Troubleshooting mode, and succeeds once it is.
     */
    @Test
    public void testComponentAccessBlockedWhenNotInTroubleshooting() throws NiFiClientException, IOException, InterruptedException {
        final ConnectorEntity connector = getClientUtil().createConnector("ComponentLifecycleConnector");
        final String connectorId = connector.getId();

        getClientUtil().applyConnectorUpdate(connector);
        getClientUtil().waitForValidConnector(connectorId);

        final List<ProcessorEntity> processors = findAllProcessors(connectorId);
        assertFalse(processors.isEmpty());
        final String processorId = processors.get(0).getId();

        try {
            getNifiClient().getProcessorClient().getProcessor(processorId);
            fail("Expected 409 Conflict retrieving processor managed by Connector when not in Troubleshooting");
        } catch (final NiFiClientException e) {
            assertConflict(e);
        }

        getClientUtil().enterTroubleshooting(connectorId);
        final ProcessorEntity processor = getNifiClient().getProcessorClient().getProcessor(processorId);
        assertNotNull(processor);
        assertEquals(processorId, processor.getId());
    }

    /**
     * Stop a connector, enter Troubleshooting, start processors inside the managed flow, restart NiFi, and ensure the
     * processors are still running and the Connector remains in Troubleshooting.
     */
    @Test
    public void testProcessorsInTroubleshootingStillRunningAfterRestart() throws NiFiClientException, IOException, InterruptedException {
        final ConnectorEntity connector = getClientUtil().createConnector("ComponentLifecycleConnector");
        final String connectorId = connector.getId();

        getClientUtil().applyConnectorUpdate(connector);
        getClientUtil().waitForValidConnector(connectorId);

        getClientUtil().enterTroubleshooting(connectorId);

        final List<ProcessorEntity> processors = findAllProcessors(connectorId);
        assertFalse(processors.isEmpty());

        final List<String> startedProcessorIds = new ArrayList<>();
        for (final ProcessorEntity processor : processors) {
            if (ScheduledState.DISABLED.name().equals(processor.getComponent().getState())) {
                continue;
            }

            try {
                getClientUtil().startProcessor(processor);
                startedProcessorIds.add(processor.getId());
            } catch (final Exception ignored) {
            }
        }

        assertFalse(startedProcessorIds.isEmpty(), "Expected at least one processor to be started");

        for (final String id : startedProcessorIds) {
            waitForProcessorState(id, ScheduledState.RUNNING);
        }

        restartNiFi();

        assertConnectorState(connectorId, ConnectorState.TROUBLESHOOTING);
        for (final String id : startedProcessorIds) {
            waitForProcessorState(id, ScheduledState.RUNNING);
        }
    }

    /**
     * A running Connector must be able to transition into
     * Troubleshooting mode without the framework stopping any of the components inside the managed flow. Any component
     * that was RUNNING prior to entering Troubleshooting must remain RUNNING after the transition, and must also
     * survive a restart of NiFi while the Connector stays in Troubleshooting.
     */
    @Test
    public void testEnterTroubleshootingFromRunningKeepsProcessorsRunning() throws NiFiClientException, IOException, InterruptedException {
        final ConnectorEntity connector = getClientUtil().createConnector("ComponentLifecycleConnector");
        final String connectorId = connector.getId();

        getClientUtil().applyConnectorUpdate(connector);
        getClientUtil().waitForValidConnector(connectorId);
        getClientUtil().startConnector(connectorId);
        assertConnectorState(connectorId, ConnectorState.RUNNING);

        final List<ProcessorEntity> runningBeforeTroubleshooting = findProcessorsInState(connectorId, ScheduledState.RUNNING);
        assertFalse(runningBeforeTroubleshooting.isEmpty(), "Expected at least one processor to be RUNNING before entering Troubleshooting");

        getClientUtil().enterTroubleshooting(connectorId);
        assertConnectorState(connectorId, ConnectorState.TROUBLESHOOTING);

        for (final ProcessorEntity processor : runningBeforeTroubleshooting) {
            final ProcessorEntity refreshed = getNifiClient().getProcessorClient().getProcessor(processor.getId());
            assertEquals(ScheduledState.RUNNING.name(), refreshed.getComponent().getState(),
                "Processor " + refreshed.getComponent().getName() + " must remain RUNNING after entering Troubleshooting");
        }

        restartNiFi();

        assertConnectorState(connectorId, ConnectorState.TROUBLESHOOTING);
        for (final ProcessorEntity processor : runningBeforeTroubleshooting) {
            waitForProcessorState(processor.getId(), ScheduledState.RUNNING);
        }
    }

    /**
     * While a Connector is in Troubleshooting mode, lifecycle operations on the Connector itself (start, stop,
     * applyUpdate, drain, purge, and delete) must be blocked with a 409 Conflict. Once Troubleshooting is exited those
     * operations become available again.
     */
    @Test
    public void testConnectorLifecycleBlockedDuringTroubleshooting() throws NiFiClientException, IOException, InterruptedException {
        final ConnectorEntity connector = getClientUtil().createConnector("ComponentLifecycleConnector");
        final String connectorId = connector.getId();

        getClientUtil().applyConnectorUpdate(connector);
        getClientUtil().waitForValidConnector(connectorId);

        getClientUtil().enterTroubleshooting(connectorId);
        assertConnectorState(connectorId, ConnectorState.TROUBLESHOOTING);

        assertConflictExpected("startConnector", () -> {
            final ConnectorEntity entity = getNifiClient().getConnectorClient().getConnector(connectorId);
            getNifiClient().getConnectorClient().startConnector(entity);
        });

        assertConflictExpected("stopConnector", () -> {
            final ConnectorEntity entity = getNifiClient().getConnectorClient().getConnector(connectorId);
            getNifiClient().getConnectorClient().stopConnector(entity);
        });

        assertConflictExpected("applyUpdate", () -> {
            final ConnectorEntity entity = getNifiClient().getConnectorClient().getConnector(connectorId);
            getNifiClient().getConnectorClient().applyUpdate(entity);
        });

        assertConflictExpected("drainConnector", () -> {
            final ConnectorEntity entity = getNifiClient().getConnectorClient().getConnector(connectorId);
            getNifiClient().getConnectorClient().drainConnector(entity);
        });

        assertConflictExpected("createPurgeRequest", () -> getNifiClient().getConnectorClient().createPurgeRequest(connectorId));

        assertConflictExpected("deleteConnector", () -> {
            final ConnectorEntity entity = getNifiClient().getConnectorClient().getConnector(connectorId);
            getNifiClient().getConnectorClient().deleteConnector(entity);
        });

        getClientUtil().endTroubleshooting(connectorId);
        assertConnectorState(connectorId, ConnectorState.STOPPED);

        getClientUtil().startConnector(connectorId);
        assertConnectorState(connectorId, ConnectorState.RUNNING);
    }

    /**
     * Verify that non-Processor component types inside a Connector's managed flow are reachable through the standard
     * component REST endpoints while the Connector is in Troubleshooting mode, and that access is blocked with a 409
     * Conflict when the Connector is not in Troubleshooting mode. Covers Connections, Ports (input and output),
     * ControllerServices, and child ProcessGroups.
     */
    @Test
    public void testNonProcessorComponentsInManagedFlow() throws NiFiClientException, IOException, InterruptedException {
        final ConnectorEntity connector = getClientUtil().createConnector("ComponentLifecycleConnector");
        final String connectorId = connector.getId();

        getClientUtil().applyConnectorUpdate(connector);
        getClientUtil().waitForValidConnector(connectorId);

        // Enter Troubleshooting briefly to discover the IDs of components inside the managed flow. This is the only
        // straightforward way to obtain the ControllerService ID; the rest are discoverable via the Connector's flow
        // endpoint, but using Troubleshooting here keeps the discovery uniform.
        getClientUtil().enterTroubleshooting(connectorId);
        final String connectionId = findFirstConnectionId(connectorId);
        final String inputPortId = findFirstInputPortId(connectorId);
        final String outputPortId = findFirstOutputPortId(connectorId);
        final String controllerServiceId = findFirstControllerServiceId(connectorId);
        final String childGroupId = findFirstChildProcessGroupId(connectorId);

        assertNotNull(connectionId, "Managed flow should contain at least one Connection");
        assertNotNull(inputPortId, "Managed flow should contain at least one InputPort");
        assertNotNull(outputPortId, "Managed flow should contain at least one OutputPort");
        assertNotNull(controllerServiceId, "Managed flow should contain at least one ControllerService");
        assertNotNull(childGroupId, "Managed flow should contain at least one child ProcessGroup");

        final ConnectionEntity connection = getNifiClient().getConnectionClient().getConnection(connectionId);
        assertEquals(connectionId, connection.getId());

        final PortEntity inputPort = getNifiClient().getInputPortClient().getInputPort(inputPortId);
        assertEquals(inputPortId, inputPort.getId());

        final PortEntity outputPort = getNifiClient().getOutputPortClient().getOutputPort(outputPortId);
        assertEquals(outputPortId, outputPort.getId());

        final ControllerServiceEntity controllerService = getNifiClient().getControllerServicesClient().getControllerService(controllerServiceId);
        assertEquals(controllerServiceId, controllerService.getId());

        final ProcessGroupEntity childGroup = getNifiClient().getProcessGroupClient().getProcessGroup(childGroupId);
        assertEquals(childGroupId, childGroup.getId());

        getClientUtil().endTroubleshooting(connectorId);
        assertConnectorState(connectorId, ConnectorState.STOPPED);

        assertConflictExpected("GET connection", () -> getNifiClient().getConnectionClient().getConnection(connectionId));
        assertConflictExpected("GET input port", () -> getNifiClient().getInputPortClient().getInputPort(inputPortId));
        assertConflictExpected("GET output port", () -> getNifiClient().getOutputPortClient().getOutputPort(outputPortId));
        assertConflictExpected("GET controller service", () -> getNifiClient().getControllerServicesClient().getControllerService(controllerServiceId));
        assertConflictExpected("GET process group", () -> getNifiClient().getProcessGroupClient().getProcessGroup(childGroupId));
    }

    /**
     * Verify that the Connector's flow endpoint correctly returns the flow for a non-root child process
     * group within the managed flow while in Troubleshooting mode. The fix that enables this is in
     * StandardProcessGroup.findProcessGroup: child groups within a connector's managed hierarchy do not
     * carry a connectorId field on their own ProcessGroup object, so the old connector-ID-filtered
     * getGroup(id, connectorId) lookup always returned null for them, causing a 404. The unconditional
     * getGroup(id) lookup combined with the existing isOwner hierarchy check is the correct approach.
     */
    @Test
    public void testGetFlowForChildProcessGroupInTroubleshooting() throws NiFiClientException, IOException, InterruptedException {
        final ConnectorEntity connector = getClientUtil().createConnector("ComponentLifecycleConnector");
        final String connectorId = connector.getId();

        getClientUtil().applyConnectorUpdate(connector);
        getClientUtil().waitForValidConnector(connectorId);

        getClientUtil().enterTroubleshooting(connectorId);
        assertConnectorState(connectorId, ConnectorState.TROUBLESHOOTING);

        final ProcessGroupFlowEntity rootFlow = getNifiClient().getConnectorClient().getFlow(connectorId);
        final List<ProcessGroupEntity> childGroups = new ArrayList<>(rootFlow.getProcessGroupFlow().getFlow().getProcessGroups());
        assertFalse(childGroups.isEmpty(), "ComponentLifecycleConnector managed flow must contain at least one child process group");

        final String childGroupId = childGroups.get(0).getId();

        final ProcessGroupFlowEntity childFlow = getNifiClient().getConnectorClient().getFlow(connectorId, childGroupId);
        assertNotNull(childFlow);
        assertEquals(childGroupId, childFlow.getProcessGroupFlow().getId());
    }

    /**
     * While the Connector is in Troubleshooting, the bulk activate/deactivate REST endpoint must accept a child
     * Process Group inside the Connector's managed flow as the target group. Without the
     * {@code includeConnectorManaged=true} lookup in {@code StandardProcessGroupDAO#activateControllerServices}, the
     * call would fail with a 400 response complaining that the Process Group could not be found.
     */
    @Test
    public void testActivateControllerServicesInChildManagedGroupDuringTroubleshooting() throws NiFiClientException, IOException, InterruptedException {
        final ConnectorEntity connector = getClientUtil().createConnector("ComponentLifecycleConnector");
        final String connectorId = connector.getId();

        getClientUtil().applyConnectorUpdate(connector);
        getClientUtil().waitForValidConnector(connectorId);

        getClientUtil().enterTroubleshooting(connectorId);
        assertConnectorState(connectorId, ConnectorState.TROUBLESHOOTING);

        final String childGroupId = findFirstChildProcessGroupId(connectorId);
        assertNotNull(childGroupId, "ComponentLifecycleConnector managed flow must contain at least one child Process Group");

        final String childServiceId = collectFirstControllerServiceId(childGroupId);
        assertNotNull(childServiceId, "Child managed Process Group should contain at least one Controller Service");

        getNifiClient().getFlowClient().activateControllerServices(activateControllerServicesRequest(childGroupId, ActivateControllerServicesEntity.STATE_DISABLED));
        getClientUtil().waitForControllerServiceRunStatus(childServiceId, "DISABLED");

        getNifiClient().getFlowClient().activateControllerServices(activateControllerServicesRequest(childGroupId, ActivateControllerServicesEntity.STATE_ENABLED));
        getClientUtil().waitForControllerServiceRunStatus(childServiceId, "ENABLED");

        // Disable the services again so the Connector can transition out of Troubleshooting; endTroubleshooting
        // requires every Controller Service inside the managed flow to be DISABLED.
        getClientUtil().disableControllerServices(getNifiClient().getConnectorClient().getConnector(connectorId)
                .getComponent().getManagedProcessGroupId(), true);

        getClientUtil().endTroubleshooting(connectorId);
        assertConnectorState(connectorId, ConnectorState.STOPPED);
    }

    /**
     * While the Connector is in Troubleshooting, the schedule-components and enable-components REST endpoints must
     * accept a child Process Group inside the Connector's managed flow as the target group. Without the
     * {@code includeConnectorManaged=true} lookup in {@code StandardProcessGroupDAO#scheduleComponents} and
     * {@code StandardProcessGroupDAO#enableComponents}, the call would fail with a 404 because the Process Group is
     * not registered in the main flow's FlowManager.
     */
    @Test
    public void testScheduleAndEnableComponentsInChildManagedGroupDuringTroubleshooting() throws NiFiClientException, IOException, InterruptedException {
        final ConnectorEntity connector = getClientUtil().createConnector("ComponentLifecycleConnector");
        final String connectorId = connector.getId();

        getClientUtil().applyConnectorUpdate(connector);
        getClientUtil().waitForValidConnector(connectorId);

        getClientUtil().enterTroubleshooting(connectorId);
        assertConnectorState(connectorId, ConnectorState.TROUBLESHOOTING);

        final String childGroupId = findFirstChildProcessGroupId(connectorId);
        assertNotNull(childGroupId, "ComponentLifecycleConnector managed flow must contain at least one child Process Group");

        final ProcessorEntity childProcessor = findFirstStoppedProcessorInGroup(connectorId, childGroupId);
        assertNotNull(childProcessor, "Child managed Process Group should contain at least one STOPPED Processor");
        final String childProcessorId = childProcessor.getId();
        final Map<String, RevisionDTO> componentRevisions = Map.of(childProcessorId, childProcessor.getRevision());

        // scheduleComponents on the child group is a no-op for an already-STOPPED processor, but exercises the
        // locateProcessGroup call path that previously failed with a 404 for connector-managed child groups.
        getNifiClient().getFlowClient().scheduleProcessGroupComponents(childGroupId,
                scheduleComponentsRequest(childGroupId, ScheduledState.STOPPED.name(), componentRevisions));

        final ProcessorEntity afterSchedule = getNifiClient().getProcessorClient().getProcessor(childProcessorId);
        getNifiClient().getFlowClient().scheduleProcessGroupComponents(childGroupId,
                scheduleComponentsRequest(childGroupId, "DISABLED", Map.of(childProcessorId, afterSchedule.getRevision())));
        waitForProcessorState(childProcessorId, ScheduledState.DISABLED);

        final ProcessorEntity afterDisable = getNifiClient().getProcessorClient().getProcessor(childProcessorId);
        getNifiClient().getFlowClient().scheduleProcessGroupComponents(childGroupId,
                scheduleComponentsRequest(childGroupId, "ENABLED", Map.of(childProcessorId, afterDisable.getRevision())));
        waitForProcessorState(childProcessorId, ScheduledState.STOPPED);

        getClientUtil().endTroubleshooting(connectorId);
        assertConnectorState(connectorId, ConnectorState.STOPPED);
    }

    private ActivateControllerServicesEntity activateControllerServicesRequest(final String groupId, final String state) {
        final ActivateControllerServicesEntity entity = new ActivateControllerServicesEntity();
        entity.setId(groupId);
        entity.setState(state);
        entity.setDisconnectedNodeAcknowledged(true);
        return entity;
    }

    private ScheduleComponentsEntity scheduleComponentsRequest(final String groupId, final String state, final Map<String, RevisionDTO> componentRevisions) {
        final ScheduleComponentsEntity entity = new ScheduleComponentsEntity();
        entity.setId(groupId);
        entity.setState(state);
        entity.setComponents(componentRevisions);
        entity.setDisconnectedNodeAcknowledged(true);
        return entity;
    }

    private ProcessorEntity findFirstStoppedProcessorInGroup(final String connectorId, final String groupId) throws NiFiClientException, IOException {
        final ProcessGroupFlowEntity flow = getNifiClient().getConnectorClient().getFlow(connectorId, groupId);
        for (final ProcessorEntity processor : flow.getProcessGroupFlow().getFlow().getProcessors()) {
            if (ScheduledState.STOPPED.name().equals(processor.getComponent().getState())) {
                return processor;
            }
        }
        return null;
    }

    private void assertConnectorState(final String connectorId, final ConnectorState expected) throws NiFiClientException, IOException {
        final ConnectorEntity entity = getNifiClient().getConnectorClient().getConnector(connectorId);
        assertEquals(expected.name(), entity.getComponent().getState());
    }

    /**
     * Stop and restart the NiFi instance, then wait for all nodes to reconnect when running in a clustered
     * environment. Subsequent flow-modifying requests (such as {@code endTroubleshooting}) would otherwise be rejected
     * with HTTP 409 Conflict while one of the nodes is still in the CONNECTING state after the restart.
     */
    private void restartNiFi() {
        getNiFiInstance().stop();
        getNiFiInstance().start();

        if (getNiFiInstance().isClustered()) {
            waitForAllNodesConnected();
        }
    }

    private void assertConflict(final NiFiClientException e) {
        final Throwable cause = e.getCause();
        if (cause instanceof WebApplicationException wae) {
            assertEquals(409, wae.getResponse().getStatus(), "Expected 409 Conflict, got: " + wae.getResponse().getStatus());
            return;
        }

        fail("Expected WebApplicationException 409, got: " + cause);
    }

    private List<ProcessorEntity> findAllProcessors(final String connectorId) throws NiFiClientException, IOException {
        final List<ProcessorEntity> result = new ArrayList<>();
        collectProcessors(connectorId, null, result);
        return result;
    }

    private void collectProcessors(final String connectorId, final String groupId, final List<ProcessorEntity> collected) throws NiFiClientException, IOException {
        final ProcessGroupFlowEntity entity = (groupId == null) ? getNifiClient().getConnectorClient().getFlow(connectorId) : getNifiClient().getConnectorClient().getFlow(connectorId, groupId);
        final FlowDTO flow = entity.getProcessGroupFlow().getFlow();
        collected.addAll(flow.getProcessors());

        for (final ProcessGroupEntity child : flow.getProcessGroups()) {
            collectProcessors(connectorId, child.getId(), collected);
        }
    }

    private ProcessorEntity findProcessorByName(final String connectorId, final String name) throws NiFiClientException, IOException {
        for (final ProcessorEntity entity : findAllProcessors(connectorId)) {
            final ProcessorDTO dto = entity.getComponent();
            if (name.equals(dto.getName())) {
                return entity;
            }
        }

        return null;
    }

    private OptionalInt getQueuedCount(final String connectorId, final String connectionId) throws NiFiClientException, IOException {
        return collectQueuedCount(connectorId, null, connectionId);
    }

    private OptionalInt collectQueuedCount(final String connectorId, final String groupId, final String connectionId) throws NiFiClientException, IOException {
        final ProcessGroupFlowEntity entity = (groupId == null) ? getNifiClient().getConnectorClient().getFlow(connectorId) : getNifiClient().getConnectorClient().getFlow(connectorId, groupId);

        final FlowDTO flow = entity.getProcessGroupFlow().getFlow();
        for (final ConnectionEntity connection : flow.getConnections()) {
            if (connectionId.equals(connection.getId())) {
                final String queued = connection.getStatus().getAggregateSnapshot().getQueued();
                final String count = queued.substring(0, queued.indexOf(' '));
                return OptionalInt.of(Integer.parseInt(count.replace(",", "")));
            }
        }

        for (final ProcessGroupEntity child : flow.getProcessGroups()) {
            final OptionalInt childResult = collectQueuedCount(connectorId, child.getId(), connectionId);
            if (childResult.isPresent()) {
                return childResult;
            }
        }

        return OptionalInt.empty();
    }

    private void waitForQueuedFlowFiles(final String connectorId, final String connectionId, final int minCount) throws InterruptedException {
        waitFor(() -> {
            try {
                final OptionalInt queuedCount = getQueuedCount(connectorId, connectionId);
                if (queuedCount.isEmpty()) {
                    return false;
                }
                final int count = queuedCount.getAsInt();
                if (minCount == 0) {
                    return count == 0;
                }
                return count >= minCount;
            } catch (final Exception e) {
                return false;
            }
        });
    }

    private void waitForProcessorState(final String processorId, final ScheduledState desired) throws InterruptedException {
        waitFor(() -> {
            try {
                final ProcessorEntity entity = getNifiClient().getProcessorClient().getProcessor(processorId);
                return desired.name().equals(entity.getComponent().getState());
            } catch (final Exception e) {
                return false;
            }
        });
    }

    private List<ProcessorEntity> findProcessorsInState(final String connectorId, final ScheduledState state) throws NiFiClientException, IOException {
        final List<ProcessorEntity> matches = new ArrayList<>();
        for (final ProcessorEntity entity : findAllProcessors(connectorId)) {
            if (state.name().equals(entity.getComponent().getState())) {
                matches.add(entity);
            }
        }
        return matches;
    }

    private String findFirstConnectionId(final String connectorId) throws NiFiClientException, IOException {
        final FlowDTO rootFlow = getNifiClient().getConnectorClient().getFlow(connectorId).getProcessGroupFlow().getFlow();
        if (!rootFlow.getConnections().isEmpty()) {
            return rootFlow.getConnections().iterator().next().getId();
        }
        for (final ProcessGroupEntity child : rootFlow.getProcessGroups()) {
            final FlowDTO childFlow = getNifiClient().getConnectorClient().getFlow(connectorId, child.getId()).getProcessGroupFlow().getFlow();
            if (!childFlow.getConnections().isEmpty()) {
                return childFlow.getConnections().iterator().next().getId();
            }
        }
        return null;
    }

    private String findFirstInputPortId(final String connectorId) throws NiFiClientException, IOException {
        return findFirstPortId(connectorId, true);
    }

    private String findFirstOutputPortId(final String connectorId) throws NiFiClientException, IOException {
        return findFirstPortId(connectorId, false);
    }

    private String findFirstPortId(final String connectorId, final boolean input) throws NiFiClientException, IOException {
        final FlowDTO rootFlow = getNifiClient().getConnectorClient().getFlow(connectorId).getProcessGroupFlow().getFlow();
        for (final PortEntity port : input ? rootFlow.getInputPorts() : rootFlow.getOutputPorts()) {
            return port.getId();
        }
        for (final ProcessGroupEntity child : rootFlow.getProcessGroups()) {
            final FlowDTO childFlow = getNifiClient().getConnectorClient().getFlow(connectorId, child.getId()).getProcessGroupFlow().getFlow();
            for (final PortEntity port : input ? childFlow.getInputPorts() : childFlow.getOutputPorts()) {
                return port.getId();
            }
        }
        return null;
    }

    private String findFirstControllerServiceId(final String connectorId) throws NiFiClientException, IOException {
        final String managedGroupId = getNifiClient().getConnectorClient().getConnector(connectorId).getComponent().getManagedProcessGroupId();
        return collectFirstControllerServiceId(managedGroupId);
    }

    private String collectFirstControllerServiceId(final String groupId) throws NiFiClientException, IOException {
        for (final ControllerServiceEntity entity : getNifiClient().getFlowClient().getControllerServices(groupId).getControllerServices()) {
            final ControllerServiceDTO dto = entity.getComponent();
            if (dto != null) {
                return entity.getId();
            }
        }

        final ProcessGroupFlowEntity flow = getNifiClient().getFlowClient().getProcessGroup(groupId);
        for (final ProcessGroupEntity child : flow.getProcessGroupFlow().getFlow().getProcessGroups()) {
            final String childServiceId = collectFirstControllerServiceId(child.getId());
            if (childServiceId != null) {
                return childServiceId;
            }
        }

        return null;
    }

    private String findFirstChildProcessGroupId(final String connectorId) throws NiFiClientException, IOException {
        final FlowDTO flow = getNifiClient().getConnectorClient().getFlow(connectorId).getProcessGroupFlow().getFlow();
        for (final ProcessGroupEntity child : flow.getProcessGroups()) {
            final ProcessGroupDTO dto = child.getComponent();
            if (dto != null) {
                return child.getId();
            }
        }
        return null;
    }

    private void assertConflictExpected(final String description, final ConflictingCall call) {
        try {
            call.run();
            fail("Expected 409 Conflict for " + description + " but request succeeded");
        } catch (final NiFiClientException e) {
            assertConflict(e);
        } catch (final IOException e) {
            fail("Unexpected IOException while invoking " + description + ": " + e.getMessage());
        }
    }

    @FunctionalInterface
    private interface ConflictingCall {
        void run() throws NiFiClientException, IOException;
    }

    /**
     * Validates that a Connector whose managed flow references parameters correctly resolves those parameter values
     * both before and after a NiFi restart while in Troubleshooting mode. Parameter values live inside the managed
     * Process Group's ParameterContext. The Connector's managed Parameter Context is intentionally not registered
     * with the global ParameterContextManager and is therefore not persisted in flow.json, so on every restart the
     * Connector's lifecycle must re-populate it. The restart path for a Connector whose effective state is
     * TROUBLESHOOTING goes through {@code StandardConnectorRepository#syncConnector}, which calls
     * {@code ConnectorNode#inheritConfiguration} so the Connector's applyUpdate re-populates the Parameter Context
     * from the persisted active configuration, and then immediately overlays the persisted Troubleshooting snapshot
     * via {@code StandardFlowContext#restoreTroubleshootingFlow}. The structural overlay preserves the in-memory Parameter
     * Context binding, so the Connector-supplied parameter values remain available while the user's flow
     * modifications are the ones that run. The transient Connector-supplied flow shape is invisible outside this
     * synchronization cycle: flow synchronization completes before the FlowFile Repository attaches FlowFiles to
     * queues, so no FlowFile movement can observe it.
     */
    @Test
    public void testParameterValuesResolvedBeforeAndAfterRestartInTroubleshooting() throws NiFiClientException, IOException, InterruptedException {
        // Use a secret name unique to this test so the underlying SecretsManager cannot return a value cached for the
        // generic name "secret" by another test that ran earlier in the same JVM.
        final String secretName = "parameterResolutionSecret";
        final String sensitiveSecretValue = "my-super-secret-value";
        final String assetFileContent = "Hello, World!";
        final File sensitiveOutputFile = new File("target/troubleshooting-sensitive.txt");
        final File assetOutputFile = new File("target/troubleshooting-asset.txt");
        sensitiveOutputFile.delete();
        assetOutputFile.delete();

        final ParameterProviderEntity paramProvider = getClientUtil().createParameterProvider("PropertiesParameterProvider");
        getClientUtil().updateParameterProviderProperties(paramProvider, Map.of("parameters", secretName + "=" + sensitiveSecretValue));

        final ConnectorEntity connector = getClientUtil().createConnector("ParameterContextConnector");
        final String connectorId = connector.getId();

        final File assetFile = new File("src/test/resources/sample-assets/helloworld.txt");
        final AssetEntity assetEntity = getNifiClient().getConnectorClient().createAsset(connectorId, assetFile.getName(), assetFile);
        final String uploadedAssetId = assetEntity.getAsset().getId();

        final ConnectorValueReferenceDTO secretRef = getClientUtil().createSecretValueReference(
                paramProvider.getId(), secretName, "PropertiesParameterProvider.Parameters." + secretName);
        final ConnectorValueReferenceDTO assetRef = new ConnectorValueReferenceDTO();
        assetRef.setValueType("ASSET_REFERENCE");
        assetRef.setAssetReferences(List.of(new AssetReferenceDTO(uploadedAssetId)));

        final Map<String, ConnectorValueReferenceDTO> propertyValues = new HashMap<>();
        propertyValues.put("Sensitive Value", secretRef);
        propertyValues.put("Asset File", assetRef);
        propertyValues.put("Sensitive Output File", createStringLiteralRef(sensitiveOutputFile.getAbsolutePath()));
        propertyValues.put("Asset Output File", createStringLiteralRef(assetOutputFile.getAbsolutePath()));

        getClientUtil().configureConnectorWithReferences(connectorId, "Parameter Context Configuration", propertyValues);
        getClientUtil().applyConnectorUpdate(connector);
        getClientUtil().waitForValidConnector(connectorId);

        // Transition into Troubleshooting from STOPPED; components inside the managed flow are not yet running.
        getClientUtil().enterTroubleshooting(connectorId);
        assertConnectorState(connectorId, ConnectorState.TROUBLESHOOTING);

        // First verification: parameter values resolve correctly on the initial flow, before any restart.
        runManagedFlowAndAssertParameterValues(connectorId, sensitiveOutputFile, assetOutputFile, sensitiveSecretValue, assetFileContent, "before restart");

        // Stop every component and clear the output files so that the post-restart run can prove the parameter
        // values were re-populated correctly rather than simply finding the files produced by the pre-restart run.
        stopAllManagedComponents(connectorId);
        assertTrue(sensitiveOutputFile.delete() || !sensitiveOutputFile.exists(), "Failed to delete sensitive output file between runs");
        assertTrue(assetOutputFile.delete() || !assetOutputFile.exists(), "Failed to delete asset output file between runs");

        restartNiFi();

        assertConnectorState(connectorId, ConnectorState.TROUBLESHOOTING);

        // Second verification: after the restore path has been exercised, the Parameter Context must still produce
        // the correct resolved values when the processors are started again.
        runManagedFlowAndAssertParameterValues(connectorId, sensitiveOutputFile, assetOutputFile, sensitiveSecretValue, assetFileContent, "after restart");
    }

    /**
     * Validates that a Connector's configured (non-default) Active configuration survives a NiFi restart while the
     * Connector is in Troubleshooting mode, and that flow modifications made during Troubleshooting are discarded
     * when Troubleshooting exits. The end-to-end scenario:
     * <ol>
     *   <li>Create a Connector and configure it with non-default property values so its active configuration and
     *       active managed flow both differ from the unconfigured defaults.</li>
     *   <li>Apply the update so the active flow is the Connector's authoritative non-default flow.</li>
     *   <li>Enter Troubleshooting and modify the managed flow by adding a processor that the Connector's
     *       authoritative flow does not contain.</li>
     *   <li>Restart NiFi while still in Troubleshooting.</li>
     *   <li>Exit Troubleshooting.</li>
     *   <li>Assert the active configuration still contains the configured (non-default) values, the active flow is
     *       the Connector's authoritative non-default flow rather than the user-modified Troubleshooting flow, and
     *       finally that running the Connector produces output at the configured non-default destinations.</li>
     * </ol>
     */
    @Test
    public void testConfigurationAndAuthoritativeFlowRestoredAfterTroubleshootingRestart() throws NiFiClientException, IOException, InterruptedException {
        // Use a secret name unique to this test so the underlying SecretsManager cannot return a value cached for the
        // generic name "secret" by another test that ran earlier in the same JVM.
        final String secretName = "configurationRestoreSecret";
        final String sensitiveSecretValue = "configured-secret-value";
        final String assetFileContent = "Hello, World!";
        final File configuredSensitiveOutput = new File("target/configuration-restore-sensitive.txt");
        final File configuredAssetOutput = new File("target/configuration-restore-asset.txt");
        configuredSensitiveOutput.delete();
        configuredAssetOutput.delete();

        final ParameterProviderEntity paramProvider = getClientUtil().createParameterProvider("PropertiesParameterProvider");
        getClientUtil().updateParameterProviderProperties(paramProvider, Map.of("parameters", secretName + "=" + sensitiveSecretValue));

        final ConnectorEntity connector = getClientUtil().createConnector("ParameterContextConnector");
        final String connectorId = connector.getId();

        final File assetFile = new File("src/test/resources/sample-assets/helloworld.txt");
        final AssetEntity assetEntity = getNifiClient().getConnectorClient().createAsset(connectorId, assetFile.getName(), assetFile);
        final String uploadedAssetId = assetEntity.getAsset().getId();

        final ConnectorValueReferenceDTO secretRef = getClientUtil().createSecretValueReference(
                paramProvider.getId(), secretName, "PropertiesParameterProvider.Parameters." + secretName);
        final ConnectorValueReferenceDTO assetRef = new ConnectorValueReferenceDTO();
        assetRef.setValueType("ASSET_REFERENCE");
        assetRef.setAssetReferences(List.of(new AssetReferenceDTO(uploadedAssetId)));

        // The output file paths differ from the property descriptors' default values, so the active configuration
        // after restart can be checked against these specific paths to prove the configured values were preserved
        // instead of being overwritten by defaults.
        final Map<String, ConnectorValueReferenceDTO> propertyValues = new HashMap<>();
        propertyValues.put("Sensitive Value", secretRef);
        propertyValues.put("Asset File", assetRef);
        propertyValues.put("Sensitive Output File", createStringLiteralRef(configuredSensitiveOutput.getAbsolutePath()));
        propertyValues.put("Asset Output File", createStringLiteralRef(configuredAssetOutput.getAbsolutePath()));

        getClientUtil().configureConnectorWithReferences(connectorId, "Parameter Context Configuration", propertyValues);
        getClientUtil().applyConnectorUpdate(connector);
        getClientUtil().waitForValidConnector(connectorId);

        assertNotNull(findProcessorByName(connectorId, "UpdateContent"),
                "Active flow should contain UpdateContent before Troubleshooting");
        assertNotNull(findProcessorByName(connectorId, "ReplaceWithFile"),
                "Active flow should contain ReplaceWithFile before Troubleshooting");

        getClientUtil().enterTroubleshooting(connectorId);
        assertConnectorState(connectorId, ConnectorState.TROUBLESHOOTING);

        // The Sleep processor is created in STOPPED state and is left disconnected so endTroubleshooting can later
        // succeed without first having to stop or empty any user-introduced components.
        final String managedGroupId = getNifiClient().getConnectorClient().getConnector(connectorId).getComponent().getManagedProcessGroupId();
        final ProcessorEntity troubleshootingProcessor = getClientUtil().createProcessor("Sleep", managedGroupId);
        final String troubleshootingProcessorId = troubleshootingProcessor.getId();
        assertTrue(containsProcessorId(connectorId, troubleshootingProcessorId),
                "User-added Sleep processor should be present in the managed flow after adding it in Troubleshooting");

        restartNiFi();

        assertConnectorState(connectorId, ConnectorState.TROUBLESHOOTING);
        assertTrue(containsProcessorId(connectorId, troubleshootingProcessorId),
                "User-added Sleep processor should survive restart while in Troubleshooting");

        getClientUtil().endTroubleshooting(connectorId);
        assertConnectorState(connectorId, ConnectorState.STOPPED);

        final ConnectorEntity afterExit = getNifiClient().getConnectorClient().getConnector(connectorId);
        final ConnectorConfigurationDTO activeConfig = afterExit.getComponent().getActiveConfiguration();
        final Map<String, ConnectorValueReferenceDTO> activeProperties = activeConfig.getConfigurationStepConfigurations().getFirst()
                .getPropertyGroupConfigurations().getFirst().getPropertyValues();
        assertEquals(configuredSensitiveOutput.getAbsolutePath(), activeProperties.get("Sensitive Output File").getValue(),
                "Active configuration must retain the configured Sensitive Output File path");
        assertEquals(configuredAssetOutput.getAbsolutePath(), activeProperties.get("Asset Output File").getValue(),
                "Active configuration must retain the configured Asset Output File path");
        assertEquals("ASSET_REFERENCE", activeProperties.get("Asset File").getValueType(),
                "Active configuration must retain the Asset reference for Asset File");
        assertEquals(uploadedAssetId, activeProperties.get("Asset File").getAssetReferences().get(0).getId(),
                "Active configuration must retain the uploaded Asset id for Asset File");
        assertEquals("SECRET_REFERENCE", activeProperties.get("Sensitive Value").getValueType(),
                "Active configuration must retain the Secret reference for Sensitive Value");

        assertFalse(containsProcessorId(connectorId, troubleshootingProcessorId),
                "User-added Sleep processor must be removed once the authoritative flow is restored on Troubleshooting exit");
        assertNotNull(findProcessorByName(connectorId, "UpdateContent"),
                "Restored authoritative flow should contain UpdateContent");
        assertNotNull(findProcessorByName(connectorId, "ReplaceWithFile"),
                "Restored authoritative flow should contain ReplaceWithFile");
        assertNotNull(findProcessorByName(connectorId, "GenerateFlowFile"),
                "Restored authoritative flow should contain GenerateFlowFile");

        getClientUtil().startConnector(connectorId);
        assertConnectorState(connectorId, ConnectorState.RUNNING);

        waitFor(() -> configuredSensitiveOutput.exists() && configuredAssetOutput.exists());
        assertEquals(sensitiveSecretValue, Files.readString(configuredSensitiveOutput.toPath()).trim(),
                "Running Connector must write the configured sensitive value to the configured Sensitive Output File");
        assertEquals(assetFileContent, Files.readString(configuredAssetOutput.toPath()).trim(),
                "Running Connector must write the configured asset content to the configured Asset Output File");
    }

    /**
     * Validates that Connector Assets are retained across a NiFi restart while the Connector is in Troubleshooting mode
     * and that, on exit, the Processor properties inside the managed flow are restored to reference the assets that the
     * Connector's authoritative flow expects.
     *
     * <p>The scenario:</p>
     * <ol>
     *   <li>Create a {@code ParameterContextConnector}, upload Asset A, and configure the Connector to use Asset A.</li>
     *   <li>Apply the update so the active flow's {@code ReplaceWithFile} processor's {@code Filename} property is
     *       {@code #{asset_param}}, where {@code asset_param} resolves to Asset A's file path.</li>
     *   <li>Enter Troubleshooting.</li>
     *   <li>Upload a second Asset B, and override the managed flow's {@code ReplaceWithFile.Filename} property to a
     *       literal that does not reference Asset A. This represents a user editing the Processor's asset-bearing
     *       property while the Connector is open for direct edits.</li>
     *   <li>Restart NiFi.</li>
     *   <li>After restart, assert that the Connector is still in Troubleshooting, both Assets A and B are still listed,
     *       and the user's override of {@code Filename} survived the restart.</li>
     *   <li>End Troubleshooting.</li>
     *   <li>Assert that the Connector's authoritative configuration is restored: {@code Asset File} still references
     *       Asset A, and {@code ReplaceWithFile.Filename} is back to {@code #{asset_param}}.</li>
     * </ol>
     */
    @Test
    public void testAssetsRetainedAcrossRestartInTroubleshooting() throws NiFiClientException, IOException, InterruptedException {
        final String secretName = "assetRetentionSecret";
        final String sensitiveSecretValue = "asset-retention-secret-value";
        final File sensitiveOutputFile = new File("target/asset-retention-sensitive.txt");
        final File assetOutputFile = new File("target/asset-retention-asset.txt");
        sensitiveOutputFile.delete();
        assetOutputFile.delete();

        final ParameterProviderEntity paramProvider = getClientUtil().createParameterProvider("PropertiesParameterProvider");
        getClientUtil().updateParameterProviderProperties(paramProvider, Map.of("parameters", secretName + "=" + sensitiveSecretValue));

        final ConnectorEntity connector = getClientUtil().createConnector("ParameterContextConnector");
        final String connectorId = connector.getId();

        final File assetA = new File("src/test/resources/sample-assets/helloworld.txt");
        final File assetB = new File("src/test/resources/sample-assets/helloworld2.txt");
        final AssetEntity assetAEntity = getNifiClient().getConnectorClient().createAsset(connectorId, assetA.getName(), assetA);
        final String assetAId = assetAEntity.getAsset().getId();

        final ConnectorValueReferenceDTO secretRef = getClientUtil().createSecretValueReference(
                paramProvider.getId(), secretName, "PropertiesParameterProvider.Parameters." + secretName);
        final ConnectorValueReferenceDTO assetRef = new ConnectorValueReferenceDTO();
        assetRef.setValueType("ASSET_REFERENCE");
        assetRef.setAssetReferences(List.of(new AssetReferenceDTO(assetAId)));

        final Map<String, ConnectorValueReferenceDTO> propertyValues = new HashMap<>();
        propertyValues.put("Sensitive Value", secretRef);
        propertyValues.put("Asset File", assetRef);
        propertyValues.put("Sensitive Output File", createStringLiteralRef(sensitiveOutputFile.getAbsolutePath()));
        propertyValues.put("Asset Output File", createStringLiteralRef(assetOutputFile.getAbsolutePath()));

        getClientUtil().configureConnectorWithReferences(connectorId, "Parameter Context Configuration", propertyValues);
        getClientUtil().applyConnectorUpdate(connector);
        getClientUtil().waitForValidConnector(connectorId);

        final ProcessorEntity replaceWithFileBeforeTroubleshooting = findProcessorByName(connectorId, "ReplaceWithFile");
        assertNotNull(replaceWithFileBeforeTroubleshooting, "Active flow should contain ReplaceWithFile before Troubleshooting");
        final String authoritativeFilenameValue = replaceWithFileBeforeTroubleshooting.getComponent().getConfig().getProperties().get("Filename");
        assertEquals("#{asset_param}", authoritativeFilenameValue,
                "Authoritative ReplaceWithFile.Filename should be parameterized as #{asset_param} before Troubleshooting");

        getClientUtil().enterTroubleshooting(connectorId);
        assertConnectorState(connectorId, ConnectorState.TROUBLESHOOTING);

        // Upload a second Asset while in Troubleshooting and confirm it joins the existing Asset.
        final AssetEntity assetBEntity = getNifiClient().getConnectorClient().createAsset(connectorId, assetB.getName(), assetB);
        final String assetBId = assetBEntity.getAsset().getId();
        assertAssetIds(connectorId, assetAId, assetBId);

        // Override the Processor's asset-bearing property to a literal that does not reference the Connector's Asset A.
        final ProcessorEntity replaceWithFileInTroubleshooting = findProcessorByName(connectorId, "ReplaceWithFile");
        assertNotNull(replaceWithFileInTroubleshooting, "ReplaceWithFile should remain accessible while in Troubleshooting");
        final String overriddenFilenameValue = "target/asset-retention-override.txt";
        getClientUtil().updateProcessorProperties(replaceWithFileInTroubleshooting, Map.of("Filename", overriddenFilenameValue));

        final ProcessorEntity afterPropertyOverride = findProcessorByName(connectorId, "ReplaceWithFile");
        assertEquals(overriddenFilenameValue, afterPropertyOverride.getComponent().getConfig().getProperties().get("Filename"),
                "ReplaceWithFile.Filename should reflect the user override applied while in Troubleshooting");

        restartNiFi();

        assertConnectorState(connectorId, ConnectorState.TROUBLESHOOTING);
        assertAssetIds(connectorId, assetAId, assetBId);

        final ProcessorEntity afterRestart = findProcessorByName(connectorId, "ReplaceWithFile");
        assertNotNull(afterRestart, "ReplaceWithFile should be present after restart while in Troubleshooting");
        assertEquals(overriddenFilenameValue, afterRestart.getComponent().getConfig().getProperties().get("Filename"),
                "User override of ReplaceWithFile.Filename must survive a restart while in Troubleshooting");

        getClientUtil().endTroubleshooting(connectorId);
        assertConnectorState(connectorId, ConnectorState.STOPPED);

        // After exit, the Connector's authoritative flow is restored. The Processor's Filename must reference the
        // authoritative value (#{asset_param}), not the user override.
        final ProcessorEntity replaceWithFileAfterExit = findProcessorByName(connectorId, "ReplaceWithFile");
        assertNotNull(replaceWithFileAfterExit, "Restored authoritative flow should contain ReplaceWithFile");
        assertEquals(authoritativeFilenameValue, replaceWithFileAfterExit.getComponent().getConfig().getProperties().get("Filename"),
                "Authoritative ReplaceWithFile.Filename must be restored on Troubleshooting exit");

        // Verify the active configuration still references Asset A (the Connector's expected asset).
        final ConnectorEntity afterExit = getNifiClient().getConnectorClient().getConnector(connectorId);
        final ConnectorConfigurationDTO activeConfig = afterExit.getComponent().getActiveConfiguration();
        final Map<String, ConnectorValueReferenceDTO> activeProperties = activeConfig.getConfigurationStepConfigurations().getFirst()
                .getPropertyGroupConfigurations().getFirst().getPropertyValues();
        assertEquals("ASSET_REFERENCE", activeProperties.get("Asset File").getValueType(),
                "Active configuration must retain the Asset reference for Asset File");
        assertEquals(assetAId, activeProperties.get("Asset File").getAssetReferences().get(0).getId(),
                "Active configuration must continue to reference Asset A (the Connector's expected asset) after Troubleshooting exit");
    }

    private void assertAssetIds(final String connectorId, final String... expectedAssetIds) throws NiFiClientException, IOException {
        final List<String> actualAssetIds = new ArrayList<>();
        final AssetsEntity assetsEntity = getNifiClient().getConnectorClient().getAssets(connectorId);
        if (assetsEntity != null && assetsEntity.getAssets() != null) {
            for (final AssetEntity asset : assetsEntity.getAssets()) {
                if (asset.getAsset() != null) {
                    actualAssetIds.add(asset.getAsset().getId());
                }
            }
        }
        for (final String expected : expectedAssetIds) {
            assertTrue(actualAssetIds.contains(expected),
                    "Expected Connector " + connectorId + " to contain Asset " + expected + " but found: " + actualAssetIds);
        }
    }

    private boolean containsProcessorId(final String connectorId, final String processorId) throws NiFiClientException, IOException {
        for (final ProcessorEntity entity : findAllProcessors(connectorId)) {
            if (processorId.equals(entity.getId())) {
                return true;
            }
        }
        return false;
    }

    private void runManagedFlowAndAssertParameterValues(final String connectorId, final File sensitiveOutputFile, final File assetOutputFile,
                                                        final String expectedSensitiveValue, final String expectedAssetValue, final String phase)
                                                        throws NiFiClientException, IOException, InterruptedException {

        // Starting individual components is permitted while in Troubleshooting. The managed Process Group is a
        // standard (non-stateless) group so processors and ports can be scheduled individually. The flow built by
        // ParameterContextConnector routes FlowFiles through child group Input Ports, so every Port inside the
        // managed flow must also be started for the pipeline to actually pass FlowFiles.
        final List<PortEntity> inputPorts = findAllInputPorts(connectorId);
        final List<PortEntity> outputPorts = findAllOutputPorts(connectorId);
        final List<ProcessorEntity> processors = findAllProcessors(connectorId);
        assertFalse(processors.isEmpty(), "Managed flow should contain processors " + phase);

        for (final PortEntity port : inputPorts) {
            getNifiClient().getInputPortClient().startInputPort(port);
        }
        for (final PortEntity port : outputPorts) {
            getNifiClient().getOutputPortClient().startOutputPort(port);
        }
        for (final ProcessorEntity processor : processors) {
            getClientUtil().waitForValidProcessor(processor.getId());
            getClientUtil().startProcessor(processor);
        }

        waitFor(() -> sensitiveOutputFile.exists() && assetOutputFile.exists());

        assertEquals(expectedSensitiveValue, Files.readString(sensitiveOutputFile.toPath()).trim(),
                "Sensitive output file must contain the configured sensitive parameter value " + phase);
        assertEquals(expectedAssetValue, Files.readString(assetOutputFile.toPath()).trim(),
                "Asset output file must contain the asset contents referenced by the asset parameter " + phase);
    }

    private void stopAllManagedComponents(final String connectorId) throws NiFiClientException, IOException, InterruptedException {
        for (final ProcessorEntity processor : findAllProcessors(connectorId)) {
            getClientUtil().stopProcessor(processor);
        }
        for (final PortEntity port : findAllInputPorts(connectorId)) {
            getNifiClient().getInputPortClient().stopInputPort(port);
        }
        for (final PortEntity port : findAllOutputPorts(connectorId)) {
            getNifiClient().getOutputPortClient().stopOutputPort(port);
        }
        for (final ProcessorEntity processor : findAllProcessors(connectorId)) {
            waitForProcessorState(processor.getId(), ScheduledState.STOPPED);
        }
    }

    private List<PortEntity> findAllInputPorts(final String connectorId) throws NiFiClientException, IOException {
        final List<PortEntity> result = new ArrayList<>();
        collectPorts(connectorId, null, true, result);
        return result;
    }

    private List<PortEntity> findAllOutputPorts(final String connectorId) throws NiFiClientException, IOException {
        final List<PortEntity> result = new ArrayList<>();
        collectPorts(connectorId, null, false, result);
        return result;
    }

    private void collectPorts(final String connectorId, final String groupId, final boolean input, final List<PortEntity> collected) throws NiFiClientException, IOException {
        final ProcessGroupFlowEntity entity = (groupId == null) ? getNifiClient().getConnectorClient().getFlow(connectorId) : getNifiClient().getConnectorClient().getFlow(connectorId, groupId);
        final FlowDTO flow = entity.getProcessGroupFlow().getFlow();
        collected.addAll(input ? flow.getInputPorts() : flow.getOutputPorts());

        for (final ProcessGroupEntity child : flow.getProcessGroups()) {
            collectPorts(connectorId, child.getId(), input, collected);
        }
    }

    private ConnectorValueReferenceDTO createStringLiteralRef(final String value) {
        final ConnectorValueReferenceDTO ref = new ConnectorValueReferenceDTO();
        ref.setValueType("STRING_LITERAL");
        ref.setValue(value);
        return ref;
    }
}
