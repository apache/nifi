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
package org.apache.nifi.tests.system.pg;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.flow.VersionedComponentState;
import org.apache.nifi.flow.VersionedNodeState;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.ComponentStateDTO;
import org.apache.nifi.web.api.dto.StateEntryDTO;
import org.apache.nifi.web.api.entity.ComponentStateEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupImportEntity;
import org.apache.nifi.web.api.entity.ProcessGroupReplaceRequestEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FlowDefinitionExportImportStateIT extends NiFiSystemIT {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Test
    public void testExportWithoutStateFlagHasNoComponentState() throws NiFiClientException, IOException, InterruptedException {
        final ProcessGroupEntity pg = getClientUtil().createProcessGroup("TestGroup", "root");
        final ProcessorEntity stateful = getClientUtil().createProcessor("StatefulCountProcessor", pg.getId());
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile", pg.getId());
        getClientUtil().createConnection(stateful, terminate, "success");

        getClientUtil().startProcessor(stateful);
        waitForStatePopulated(stateful.getId(), Scope.LOCAL);
        getClientUtil().stopProcessor(stateful);
        getClientUtil().waitForStoppedProcessor(stateful.getId());

        final File exportFile = new File("target/st1-export.json");
        getNifiClient().getProcessGroupClient().exportProcessGroup(pg.getId(), false, false, exportFile);

        final RegisteredFlowSnapshot snapshot = MAPPER.readValue(exportFile, RegisteredFlowSnapshot.class);
        for (final VersionedProcessor vp : snapshot.getFlowContents().getProcessors()) {
            assertNull(vp.getComponentState(), "componentState should be null when includeComponentState=false");
        }
    }

    @Test
    public void testExportWithStateFlagHasBothScopes() throws NiFiClientException, IOException, InterruptedException {
        final ProcessGroupEntity pg = getClientUtil().createProcessGroup("TestGroup", "root");
        final ProcessorEntity stateful = getClientUtil().createProcessor("StatefulCountProcessor", pg.getId());
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile", pg.getId());
        getClientUtil().createConnection(stateful, terminate, "success");

        getClientUtil().startProcessor(stateful);
        waitForStatePopulated(stateful.getId(), Scope.LOCAL);
        getClientUtil().stopProcessor(stateful);
        getClientUtil().waitForStoppedProcessor(stateful.getId());

        final File exportFile = new File("target/st2-export.json");
        getNifiClient().getProcessGroupClient().exportProcessGroup(pg.getId(), true, true, exportFile);

        final RegisteredFlowSnapshot snapshot = MAPPER.readValue(exportFile, RegisteredFlowSnapshot.class);
        final VersionedProcessor proc = findProcessorByType(snapshot.getFlowContents(), "StatefulCountProcessor");
        assertNotNull(proc);

        final VersionedComponentState state = proc.getComponentState();
        assertNotNull(state, "componentState should be present when includeComponentState=true");
        assertNotNull(state.getLocalNodeStates(), "localNodeStates should be present for StatefulCountProcessor");
        assertEquals(1, state.getLocalNodeStates().size());
        final VersionedNodeState nodeState = state.getLocalNodeStates().get(0);
        assertNotNull(nodeState);
        assertNotNull(nodeState.getState().get("count"));
        assertNull(state.getClusterState(), "clusterState should not be exported on a standalone node since the cluster state provider is not enabled");
    }

    @Test
    public void testExportWithStateWhileRunningReturns409() throws NiFiClientException, IOException, InterruptedException {
        final ProcessGroupEntity pg = getClientUtil().createProcessGroup("TestGroup", "root");
        final ProcessorEntity stateful = getClientUtil().createProcessor("StatefulCountProcessor", pg.getId());
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile", pg.getId());
        getClientUtil().createConnection(stateful, terminate, "success");

        getClientUtil().startProcessor(stateful);
        waitForStatePopulated(stateful.getId(), Scope.LOCAL);

        try {
            final File exportFile = new File("target/st3-export.json");
            getNifiClient().getProcessGroupClient().exportProcessGroup(pg.getId(), false, true, exportFile);
            throw new AssertionError("Expected export to fail when processors are running");
        } catch (final NiFiClientException e) {
            assertNotNull(e.getMessage());
        } finally {
            getClientUtil().stopProcessor(stateful);
            getClientUtil().waitForStoppedProcessor(stateful.getId());
        }
    }

    @Test
    public void testExportWithStateWhileControllerServiceEnabledReturns409() throws NiFiClientException, IOException, InterruptedException {
        final ProcessGroupEntity pg = getClientUtil().createProcessGroup("TestGroup", "root");
        final ControllerServiceEntity service = getClientUtil().createControllerService("StandardCountService", pg.getId());
        getClientUtil().enableControllerService(service);

        try {
            final File exportFile = new File("target/st4-export.json");
            getNifiClient().getProcessGroupClient().exportProcessGroup(pg.getId(), false, true, exportFile);
            throw new AssertionError("Expected export to fail when controller services are enabled");
        } catch (final NiFiClientException e) {
            assertNotNull(e.getMessage());
        } finally {
            getClientUtil().disableControllerService(service);
        }
    }

    @Test
    public void testExportWithStateFlagNonStatefulProcessorHasNoComponentState() throws NiFiClientException, IOException, InterruptedException {
        final ProcessGroupEntity pg = getClientUtil().createProcessGroup("TestGroup", "root");
        final ProcessorEntity stateful = getClientUtil().createProcessor("StatefulCountProcessor", pg.getId());
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile", pg.getId());
        getClientUtil().createConnection(stateful, terminate, "success");

        getClientUtil().startProcessor(stateful);
        waitForStatePopulated(stateful.getId(), Scope.LOCAL);
        getClientUtil().stopProcessor(stateful);
        getClientUtil().waitForStoppedProcessor(stateful.getId());

        final File exportFile = new File("target/st5-export.json");
        getNifiClient().getProcessGroupClient().exportProcessGroup(pg.getId(), true, true, exportFile);

        final RegisteredFlowSnapshot snapshot = MAPPER.readValue(exportFile, RegisteredFlowSnapshot.class);
        final VersionedProcessor statefulVersioned = findProcessorByType(snapshot.getFlowContents(), "StatefulCountProcessor");
        final VersionedProcessor terminateVersioned = findProcessorByType(snapshot.getFlowContents(), "TerminateFlowFile");

        assertNotNull(statefulVersioned.getComponentState(), "Stateful processor should have componentState");
        assertNull(terminateVersioned.getComponentState(), "Non-stateful processor should not have componentState");
    }

    @Test
    public void testExportDefaultNoParamHasNoState() throws NiFiClientException, IOException, InterruptedException {
        final ProcessGroupEntity pg = getClientUtil().createProcessGroup("TestGroup", "root");
        final ProcessorEntity stateful = getClientUtil().createProcessor("StatefulCountProcessor", pg.getId());
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile", pg.getId());
        getClientUtil().createConnection(stateful, terminate, "success");

        getClientUtil().startProcessor(stateful);
        waitForStatePopulated(stateful.getId(), Scope.LOCAL);
        getClientUtil().stopProcessor(stateful);
        getClientUtil().waitForStoppedProcessor(stateful.getId());

        final File exportFile = new File("target/st6-export.json");
        getNifiClient().getProcessGroupClient().exportProcessGroup(pg.getId(), false, exportFile);

        final RegisteredFlowSnapshot snapshot = MAPPER.readValue(exportFile, RegisteredFlowSnapshot.class);
        for (final VersionedProcessor vp : snapshot.getFlowContents().getProcessors()) {
            assertNull(vp.getComponentState(), "componentState should be null by default");
        }
    }

    @Test
    public void testStandaloneRoundTripViaUploadLocalState() throws NiFiClientException, IOException, InterruptedException {
        final ProcessGroupEntity pg = getClientUtil().createProcessGroup("TestGroup", "root");
        final ProcessorEntity stateful = getClientUtil().createProcessor("StatefulCountProcessor", pg.getId());
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile", pg.getId());
        getClientUtil().createConnection(stateful, terminate, "success");

        getClientUtil().startProcessor(stateful);
        waitForStatePopulated(stateful.getId(), Scope.LOCAL);
        getClientUtil().stopProcessor(stateful);
        getClientUtil().waitForStoppedProcessor(stateful.getId());

        final Map<String, String> originalLocalState = getProcessorState(stateful.getId(), Scope.LOCAL);
        assertNotNull(originalLocalState.get("count"));

        final File exportFile = new File("target/st7-export.json");
        getNifiClient().getProcessGroupClient().exportProcessGroup(pg.getId(), true, true, exportFile);

        emptyQueuesAndDeleteProcessGroup(pg);

        final ProcessGroupEntity uploaded = getNifiClient().getProcessGroupClient().upload("root", exportFile, "ImportedGroup", 0.0, 0.0);

        final ProcessorEntity importedProcessor = findProcessorByTypeInGroup(uploaded.getId(), "StatefulCountProcessor");
        assertNotNull(importedProcessor);

        final Map<String, String> importedLocalState = getProcessorState(importedProcessor.getId(), Scope.LOCAL);
        assertEquals(originalLocalState.get("count"), importedLocalState.get("count"), "Local state count should match after round-trip");
    }

    @Test
    public void testReplaceRejectsFlowDefinitionWithComponentState() throws NiFiClientException, IOException, InterruptedException {
        final ProcessGroupEntity pg = getClientUtil().createProcessGroup("TestGroup", "root");
        final ProcessorEntity stateful = getClientUtil().createProcessor("StatefulCountProcessor", pg.getId());
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile", pg.getId());
        getClientUtil().createConnection(stateful, terminate, "success");

        getClientUtil().startProcessor(stateful);
        waitForStatePopulated(stateful.getId(), Scope.LOCAL);
        getClientUtil().stopProcessor(stateful);
        getClientUtil().waitForStoppedProcessor(stateful.getId());

        final File exportFile = new File("target/st8-export.json");
        getNifiClient().getProcessGroupClient().exportProcessGroup(pg.getId(), true, true, exportFile);

        final RegisteredFlowSnapshot snapshot = MAPPER.readValue(exportFile, RegisteredFlowSnapshot.class);
        final NiFiClientException exception = assertThrows(NiFiClientException.class, () -> replaceProcessGroup(pg, snapshot));
        assertTrue(exception.getMessage().contains("component state"),
                "Expected rejection message about component state but got: " + exception.getMessage());
    }

    @Test
    public void testImportFlowWithoutComponentStateHasNoState() throws NiFiClientException, IOException, InterruptedException {
        final ProcessGroupEntity pg = getClientUtil().createProcessGroup("TestGroup", "root");
        final ProcessorEntity stateful = getClientUtil().createProcessor("StatefulCountProcessor", pg.getId());
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile", pg.getId());
        getClientUtil().createConnection(stateful, terminate, "success");

        getClientUtil().startProcessor(stateful);
        waitForStatePopulated(stateful.getId(), Scope.LOCAL);
        getClientUtil().stopProcessor(stateful);
        getClientUtil().waitForStoppedProcessor(stateful.getId());

        final File exportFile = new File("target/st9-export.json");
        getNifiClient().getProcessGroupClient().exportProcessGroup(pg.getId(), true, false, exportFile);

        emptyQueuesAndDeleteProcessGroup(pg);

        final ProcessGroupEntity uploaded = getNifiClient().getProcessGroupClient().upload("root", exportFile, "ImportedGroup", 0.0, 0.0);
        final ProcessorEntity importedProcessor = findProcessorByTypeInGroup(uploaded.getId(), "StatefulCountProcessor");
        assertNotNull(importedProcessor);

        final Map<String, String> importedLocalState = getProcessorState(importedProcessor.getId(), Scope.LOCAL);
        assertTrue(importedLocalState.isEmpty(), "State should be empty when imported without componentState");
    }

    @Test
    public void testNestedProcessGroupsStateExportedRecursively() throws NiFiClientException, IOException, InterruptedException {
        final ProcessGroupEntity parent = getClientUtil().createProcessGroup("ParentGroup", "root");
        final ProcessGroupEntity child = getClientUtil().createProcessGroup("ChildGroup", parent.getId());
        final ProcessorEntity stateful = getClientUtil().createProcessor("StatefulCountProcessor", child.getId());
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile", child.getId());
        getClientUtil().createConnection(stateful, terminate, "success");

        getClientUtil().startProcessor(stateful);
        waitForStatePopulated(stateful.getId(), Scope.LOCAL);
        getClientUtil().stopProcessor(stateful);
        getClientUtil().waitForStoppedProcessor(stateful.getId());

        final Map<String, String> originalLocalState = getProcessorState(stateful.getId(), Scope.LOCAL);

        final File exportFile = new File("target/st10-export.json");
        getNifiClient().getProcessGroupClient().exportProcessGroup(parent.getId(), true, true, exportFile);

        final RegisteredFlowSnapshot snapshot = MAPPER.readValue(exportFile, RegisteredFlowSnapshot.class);
        final VersionedProcessGroup childGroup = snapshot.getFlowContents().getProcessGroups().iterator().next();
        final VersionedProcessor nestedProcessor = findProcessorByType(childGroup, "StatefulCountProcessor");
        assertNotNull(nestedProcessor);
        assertNotNull(nestedProcessor.getComponentState(), "Nested processor should have componentState");
        assertNotNull(nestedProcessor.getComponentState().getLocalNodeStates(), "Nested processor should have localNodeStates");

        emptyQueuesAndDeleteProcessGroup(parent);

        final ProcessGroupEntity uploaded = getNifiClient().getProcessGroupClient().upload("root", exportFile, "ImportedParent", 0.0, 0.0);
        final ProcessGroupEntity importedChild = getNifiClient().getFlowClient().getProcessGroup(uploaded.getId())
                .getProcessGroupFlow().getFlow().getProcessGroups().iterator().next();
        final ProcessorEntity importedProcessor = findProcessorByTypeInGroup(importedChild.getId(), "StatefulCountProcessor");
        assertNotNull(importedProcessor);

        assertEquals(originalLocalState.get("count"), getProcessorState(importedProcessor.getId(), Scope.LOCAL).get("count"));
    }

    @Test
    public void testMultipleStatefulProcessorsEachGetsOwnState() throws NiFiClientException, IOException, InterruptedException {
        final ProcessGroupEntity pg = getClientUtil().createProcessGroup("TestGroup", "root");
        final ProcessorEntity gen1 = getClientUtil().createProcessor("GenerateFlowFile", pg.getId());
        final ProcessorEntity gen2 = getClientUtil().createProcessor("StatefulCountProcessor", pg.getId());
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile", pg.getId());
        getClientUtil().createConnection(gen1, terminate, "success");
        getClientUtil().createConnection(gen2, terminate, "success");

        getClientUtil().startProcessor(gen1);
        getClientUtil().startProcessor(gen2);
        waitForStatePopulated(gen1.getId(), Scope.LOCAL);
        waitForStatePopulated(gen2.getId(), Scope.LOCAL);
        getClientUtil().stopProcessor(gen1);
        getClientUtil().stopProcessor(gen2);
        getClientUtil().waitForStoppedProcessor(gen1.getId());
        getClientUtil().waitForStoppedProcessor(gen2.getId());

        final Map<String, String> state1 = getProcessorState(gen1.getId(), Scope.LOCAL);
        final Map<String, String> state2 = getProcessorState(gen2.getId(), Scope.LOCAL);

        final File exportFile = new File("target/st11-export.json");
        getNifiClient().getProcessGroupClient().exportProcessGroup(pg.getId(), true, true, exportFile);

        emptyQueuesAndDeleteProcessGroup(pg);

        final ProcessGroupEntity uploaded = getNifiClient().getProcessGroupClient().upload("root", exportFile, "ImportedGroup", 0.0, 0.0);
        final ProcessorEntity imported1 = findProcessorByTypeInGroup(uploaded.getId(), "GenerateFlowFile");
        final ProcessorEntity imported2 = findProcessorByTypeInGroup(uploaded.getId(), "StatefulCountProcessor");

        assertEquals(state1.get("count"), getProcessorState(imported1.getId(), Scope.LOCAL).get("count"));
        assertEquals(state2.get("count"), getProcessorState(imported2.getId(), Scope.LOCAL).get("count"));
    }

    @Test
    public void testExportWithStateEmptyStateNeverRan() throws NiFiClientException, IOException {
        final ProcessGroupEntity pg = getClientUtil().createProcessGroup("TestGroup", "root");
        getClientUtil().createProcessor("StatefulCountProcessor", pg.getId());

        final File exportFile = new File("target/st23-export.json");
        getNifiClient().getProcessGroupClient().exportProcessGroup(pg.getId(), true, true, exportFile);

        final RegisteredFlowSnapshot snapshot = MAPPER.readValue(exportFile, RegisteredFlowSnapshot.class);
        final VersionedProcessor proc = findProcessorByType(snapshot.getFlowContents(), "StatefulCountProcessor");
        assertNull(proc.getComponentState(), "componentState should be null when processor has never run");
    }

    @Test
    public void testExportWithStateNoStatefulComponents() throws NiFiClientException, IOException {
        final ProcessGroupEntity pg = getClientUtil().createProcessGroup("TestGroup", "root");
        getClientUtil().createProcessor("TerminateFlowFile", pg.getId());

        final File exportFile = new File("target/st24-export.json");
        getNifiClient().getProcessGroupClient().exportProcessGroup(pg.getId(), true, true, exportFile);

        final RegisteredFlowSnapshot snapshot = MAPPER.readValue(exportFile, RegisteredFlowSnapshot.class);
        for (final VersionedProcessor vp : snapshot.getFlowContents().getProcessors()) {
            assertNull(vp.getComponentState(), "Non-stateful processor should have null componentState");
        }
    }

    @Test
    public void testReplaceSucceedsWhenFlowExportedWithoutComponentState() throws NiFiClientException, IOException, InterruptedException {
        final ProcessGroupEntity pg = getClientUtil().createProcessGroup("TestGroup", "root");
        final ProcessorEntity stateful = getClientUtil().createProcessor("StatefulCountProcessor", pg.getId());
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile", pg.getId());
        getClientUtil().createConnection(stateful, terminate, "success");

        getClientUtil().startProcessor(stateful);
        waitForStatePopulated(stateful.getId(), Scope.LOCAL);
        getClientUtil().stopProcessor(stateful);
        getClientUtil().waitForStoppedProcessor(stateful.getId());

        final File exportFile = new File("target/st27-export.json");
        getNifiClient().getProcessGroupClient().exportProcessGroup(pg.getId(), true, false, exportFile);

        getNifiClient().getProcessGroupClient().emptyQueues(pg.getId());
        waitFor(() -> {
            try {
                return getNifiClient().getProcessGroupClient().getProcessGroup(pg.getId())
                        .getStatus().getAggregateSnapshot().getQueuedCount().equals("0");
            } catch (final Exception e) {
                return false;
            }
        });

        final RegisteredFlowSnapshot snapshot = MAPPER.readValue(exportFile, RegisteredFlowSnapshot.class);
        replaceProcessGroup(pg, snapshot);

        final ProcessorEntity importedProcessor = findProcessorByTypeInGroup(pg.getId(), "StatefulCountProcessor");
        assertNotNull(importedProcessor);
    }

    private void waitForStatePopulated(final String processorId, final Scope scope) throws InterruptedException {
        waitFor(() -> {
            try {
                final Map<String, String> state = getProcessorState(processorId, scope);
                return state.get("count") != null;
            } catch (final Exception e) {
                return false;
            }
        });
    }

    private Map<String, String> getProcessorState(final String processorId, final Scope scope) throws NiFiClientException, IOException {
        final ComponentStateEntity stateEntity = getNifiClient().getProcessorClient().getProcessorState(processorId);
        final ComponentStateDTO componentState = stateEntity.getComponentState();
        final Map<String, String> result = new HashMap<>();

        switch (scope) {
            case LOCAL:
                if (componentState != null && componentState.getLocalState() != null && componentState.getLocalState().getState() != null) {
                    for (final StateEntryDTO entry : componentState.getLocalState().getState()) {
                        result.put(entry.getKey(), entry.getValue());
                    }
                }
                break;
            case CLUSTER:
                if (componentState != null && componentState.getClusterState() != null && componentState.getClusterState().getState() != null) {
                    for (final StateEntryDTO entry : componentState.getClusterState().getState()) {
                        result.put(entry.getKey(), entry.getValue());
                    }
                }
                break;
        }
        return result;
    }

    private VersionedProcessor findProcessorByType(final VersionedProcessGroup group, final String typeSuffix) {
        if (group.getProcessors() != null) {
            for (final VersionedProcessor vp : group.getProcessors()) {
                if (vp.getType() != null && vp.getType().endsWith(typeSuffix)) {
                    return vp;
                }
            }
        }
        return null;
    }

    private ProcessorEntity findProcessorByTypeInGroup(final String groupId, final String typeSuffix) throws NiFiClientException, IOException {
        return getNifiClient().getFlowClient().getProcessGroup(groupId)
                .getProcessGroupFlow().getFlow().getProcessors().stream()
                .filter(pe -> pe.getComponent().getType().endsWith(typeSuffix))
                .findFirst().orElse(null);
    }

    private void emptyQueuesAndDeleteProcessGroup(final ProcessGroupEntity pg) throws NiFiClientException, IOException, InterruptedException {
        getNifiClient().getProcessGroupClient().emptyQueues(pg.getId());
        waitFor(() -> {
            try {
                return getNifiClient().getProcessGroupClient().getProcessGroup(pg.getId())
                        .getStatus().getAggregateSnapshot().getQueuedCount().equals("0");
            } catch (final Exception e) {
                return false;
            }
        });
        final ProcessGroupEntity refreshed = getNifiClient().getProcessGroupClient().getProcessGroup(pg.getId());
        refreshed.setDisconnectedNodeAcknowledged(true);
        getNifiClient().getProcessGroupClient().deleteProcessGroup(refreshed);
    }

    private void replaceProcessGroup(final ProcessGroupEntity pg, final RegisteredFlowSnapshot snapshot) throws NiFiClientException, IOException, InterruptedException {
        final ProcessGroupImportEntity importEntity = new ProcessGroupImportEntity();
        importEntity.setVersionedFlowSnapshot(snapshot);
        importEntity.setProcessGroupRevision(getNifiClient().getProcessGroupClient().getProcessGroup(pg.getId()).getRevision());

        final ProcessGroupReplaceRequestEntity replaceRequest =
                getNifiClient().getProcessGroupClient().replaceProcessGroup(pg.getId(), importEntity);
        final String requestId = replaceRequest.getRequest().getRequestId();

        waitFor(() -> {
            try {
                final ProcessGroupReplaceRequestEntity req =
                        getNifiClient().getProcessGroupClient().getProcessGroupReplaceRequest(pg.getId(), requestId);
                return req != null && req.getRequest().isComplete();
            } catch (final Exception e) {
                return false;
            }
        });

        final ProcessGroupReplaceRequestEntity finalRequest =
                getNifiClient().getProcessGroupClient().getProcessGroupReplaceRequest(pg.getId(), requestId);
        assertNull(finalRequest.getRequest().getFailureReason(),
                "Replace failed: %s".formatted(finalRequest.getRequest().getFailureReason()));
    }
}
