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
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.apache.nifi.tests.system.NiFiInstanceFactory;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.ComponentStateDTO;
import org.apache.nifi.web.api.dto.StateEntryDTO;
import org.apache.nifi.web.api.entity.ComponentStateEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupImportEntity;
import org.apache.nifi.web.api.entity.ProcessGroupReplaceRequestEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClusterFlowDefinitionExportImportStateIT extends NiFiSystemIT {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Override
    public NiFiInstanceFactory getInstanceFactory() {
        return createTwoNodeInstanceFactory();
    }

    @Test
    public void testClusterExportCapturesClusterState() throws NiFiClientException, IOException, InterruptedException {
        final ProcessGroupEntity pg = getClientUtil().createProcessGroup("TestGroup", "root");
        final ProcessorEntity stateful = getClientUtil().createProcessor("GenerateFlowFile", pg.getId());
        getClientUtil().updateProcessorProperties(stateful, Collections.singletonMap("State Scope", "CLUSTER"));
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile", pg.getId());
        getClientUtil().createConnection(stateful, terminate, "success");

        getClientUtil().startProcessor(stateful);
        waitForStatePopulated(stateful.getId(), Scope.CLUSTER);
        getClientUtil().stopProcessor(stateful);
        getClientUtil().waitForStoppedProcessor(stateful.getId());

        final File exportFile = new File("target/st13-export.json");
        getNifiClient().getProcessGroupClient().exportProcessGroup(pg.getId(), true, true, exportFile);

        final RegisteredFlowSnapshot snapshot = MAPPER.readValue(exportFile, RegisteredFlowSnapshot.class);
        final VersionedProcessor proc = findProcessorByType(snapshot.getFlowContents(), "GenerateFlowFile");
        assertNotNull(proc);
        assertNotNull(proc.getComponentState());
        assertNotNull(proc.getComponentState().getClusterState());
        assertNotNull(proc.getComponentState().getClusterState().get("count"));
    }

    @Test
    public void testClusterExportCapturesLocalStateFromBothNodes() throws NiFiClientException, IOException, InterruptedException {
        final ProcessGroupEntity pg = getClientUtil().createProcessGroup("TestGroup", "root");
        final ProcessorEntity stateful = getClientUtil().createProcessor("GenerateFlowFile", pg.getId());
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile", pg.getId());
        getClientUtil().createConnection(stateful, terminate, "success");

        getClientUtil().startProcessor(stateful);
        waitForStatePopulated(stateful.getId(), Scope.LOCAL);
        getClientUtil().stopProcessor(stateful);
        getClientUtil().waitForStoppedProcessor(stateful.getId());

        final File exportFile = new File("target/st14-export.json");
        getNifiClient().getProcessGroupClient().exportProcessGroup(pg.getId(), true, true, exportFile);

        final RegisteredFlowSnapshot snapshot = MAPPER.readValue(exportFile, RegisteredFlowSnapshot.class);
        final VersionedProcessor proc = findProcessorByType(snapshot.getFlowContents(), "GenerateFlowFile");
        assertNotNull(proc);
        assertNotNull(proc.getComponentState());
        assertNotNull(proc.getComponentState().getLocalNodeStates());
        assertEquals(2, proc.getComponentState().getLocalNodeStates().size(),
                "Should have local state from both nodes");
        assertNotNull(proc.getComponentState().getLocalNodeStates().get(0));
        assertNotNull(proc.getComponentState().getLocalNodeStates().get(1));
    }

    @Test
    public void testClusterExportCapturesBothScopes() throws NiFiClientException, IOException, InterruptedException {
        final ProcessGroupEntity pg = getClientUtil().createProcessGroup("TestGroup", "root");
        final ProcessorEntity stateful = getClientUtil().createProcessor("StatefulCountProcessor", pg.getId());
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile", pg.getId());
        getClientUtil().createConnection(stateful, terminate, "success");

        getClientUtil().startProcessor(stateful);
        waitForStatePopulated(stateful.getId(), Scope.CLUSTER);
        waitForStatePopulated(stateful.getId(), Scope.LOCAL);
        getClientUtil().stopProcessor(stateful);
        getClientUtil().waitForStoppedProcessor(stateful.getId());

        final File exportFile = new File("target/st15-export.json");
        getNifiClient().getProcessGroupClient().exportProcessGroup(pg.getId(), true, true, exportFile);

        final RegisteredFlowSnapshot snapshot = MAPPER.readValue(exportFile, RegisteredFlowSnapshot.class);
        final VersionedProcessor proc = findProcessorByType(snapshot.getFlowContents(), "StatefulCountProcessor");
        assertNotNull(proc);

        final VersionedComponentState state = proc.getComponentState();
        assertNotNull(state);
        assertNotNull(state.getClusterState(), "Cluster state should be present");
        assertNotNull(state.getClusterState().get("count"), "Cluster state should contain count");
        assertNotNull(state.getLocalNodeStates(), "Local node states should be present");
        assertEquals(2, state.getLocalNodeStates().size());
        assertNotNull(state.getLocalNodeStates().get(0).getState().get("count"));
        assertNotNull(state.getLocalNodeStates().get(1).getState().get("count"));
    }

    @Test
    public void testClusterRoundTripSameTopologyBothScopes() throws NiFiClientException, IOException, InterruptedException {
        final ProcessGroupEntity pg = getClientUtil().createProcessGroup("TestGroup", "root");
        final ProcessorEntity stateful = getClientUtil().createProcessor("StatefulCountProcessor", pg.getId());
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile", pg.getId());
        getClientUtil().createConnection(stateful, terminate, "success");

        getClientUtil().startProcessor(stateful);
        waitForStatePopulated(stateful.getId(), Scope.CLUSTER);
        waitForStatePopulated(stateful.getId(), Scope.LOCAL);
        getClientUtil().stopProcessor(stateful);
        getClientUtil().waitForStoppedProcessor(stateful.getId());

        final Map<String, String> originalClusterState = getProcessorState(stateful.getId(), Scope.CLUSTER);

        final File exportFile = new File("target/st16-export.json");
        getNifiClient().getProcessGroupClient().exportProcessGroup(pg.getId(), true, true, exportFile);

        emptyQueuesAndDeleteProcessGroup(pg);

        final ProcessGroupEntity uploaded = getNifiClient().getProcessGroupClient().upload("root", exportFile, "ImportedGroup", 0.0, 0.0);
        final ProcessorEntity importedProcessor = findProcessorByTypeInGroup(uploaded.getId(), "StatefulCountProcessor");
        assertNotNull(importedProcessor);

        final Map<String, String> importedClusterState = getProcessorState(importedProcessor.getId(), Scope.CLUSTER);
        assertEquals(originalClusterState.get("count"), importedClusterState.get("count"),
                "Cluster state should be restored after round-trip");
    }

    @Test
    public void testClusterExportRunningProcessorReturns409() throws NiFiClientException, IOException, InterruptedException {
        final ProcessGroupEntity pg = getClientUtil().createProcessGroup("TestGroup", "root");
        final ProcessorEntity stateful = getClientUtil().createProcessor("StatefulCountProcessor", pg.getId());
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile", pg.getId());
        getClientUtil().createConnection(stateful, terminate, "success");

        getClientUtil().startProcessor(stateful);
        waitForStatePopulated(stateful.getId(), Scope.LOCAL);

        try {
            final File exportFile = new File("target/st20-export.json");
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
    public void testClusterReplaceRejectsFlowDefinitionWithComponentState() throws NiFiClientException, IOException, InterruptedException {
        final ProcessGroupEntity pg = getClientUtil().createProcessGroup("TestGroup", "root");
        final ProcessorEntity stateful = getClientUtil().createProcessor("StatefulCountProcessor", pg.getId());
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile", pg.getId());
        getClientUtil().createConnection(stateful, terminate, "success");

        getClientUtil().startProcessor(stateful);
        waitForStatePopulated(stateful.getId(), Scope.CLUSTER);
        waitForStatePopulated(stateful.getId(), Scope.LOCAL);
        getClientUtil().stopProcessor(stateful);
        getClientUtil().waitForStoppedProcessor(stateful.getId());

        final File exportFile = new File("target/st21-export.json");
        getNifiClient().getProcessGroupClient().exportProcessGroup(pg.getId(), true, true, exportFile);

        final RegisteredFlowSnapshot snapshot = MAPPER.readValue(exportFile, RegisteredFlowSnapshot.class);
        final NiFiClientException exception = assertThrows(NiFiClientException.class, () -> replaceProcessGroup(pg, snapshot));
        assertTrue(exception.getMessage().contains("component state"),
                "Expected rejection message about component state but got: " + exception.getMessage());
    }

    @Test
    public void testClusterImportWithoutComponentStateHasNoState() throws NiFiClientException, IOException, InterruptedException {
        final ProcessGroupEntity pg = getClientUtil().createProcessGroup("TestGroup", "root");
        final ProcessorEntity stateful = getClientUtil().createProcessor("StatefulCountProcessor", pg.getId());
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile", pg.getId());
        getClientUtil().createConnection(stateful, terminate, "success");

        getClientUtil().startProcessor(stateful);
        waitForStatePopulated(stateful.getId(), Scope.CLUSTER);
        getClientUtil().stopProcessor(stateful);
        getClientUtil().waitForStoppedProcessor(stateful.getId());

        final File exportFile = new File("target/st22-export.json");
        getNifiClient().getProcessGroupClient().exportProcessGroup(pg.getId(), true, false, exportFile);

        emptyQueuesAndDeleteProcessGroup(pg);

        final ProcessGroupEntity uploaded = getNifiClient().getProcessGroupClient().upload("root", exportFile, "ImportedGroup", 0.0, 0.0);
        final ProcessorEntity importedProcessor = findProcessorByTypeInGroup(uploaded.getId(), "StatefulCountProcessor");
        assertNotNull(importedProcessor);

        final Map<String, String> importedClusterState = getProcessorState(importedProcessor.getId(), Scope.CLUSTER);
        assertTrue(importedClusterState.isEmpty(), "State should be empty when imported without componentState");
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
