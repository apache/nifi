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

import jakarta.ws.rs.WebApplicationException;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.groups.StatelessGroupScheduledState;
import org.apache.nifi.tests.system.NiFiInstanceFactory;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.ScheduleComponentsEntity;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

public class ClusteredStopSourcesIT extends NiFiSystemIT {

    @Override
    public NiFiInstanceFactory getInstanceFactory() {
        return createTwoNodeInstanceFactory();
    }

    @Test
    public void testStopSourcesSkipsStatelessDescendants() throws NiFiClientException, IOException, InterruptedException {
        final MixedEngineFlow flow = createMixedEngineFlow();
        startMixedEngineFlow(flow);

        final ScheduleComponentsEntity response = getNifiClient().getFlowClient()
                .stopProcessGroupSources(flow.parentGroup().getId(), stopSourcesRequest(flow.parentGroup().getId()));

        assertEquals(Set.of(flow.standardSource().getId()), response.getComponents().keySet());
        assertProcessorStateOnAllNodes(flow.standardSource().getId(), ScheduledState.STOPPED);
        assertProcessorStateOnAllNodes(flow.standardDownstream().getId(), ScheduledState.RUNNING);
        assertProcessorStateOnAllNodes(flow.statelessSource().getId(), ScheduledState.RUNNING);
        assertProcessorStateOnAllNodes(flow.statelessDownstream().getId(), ScheduledState.RUNNING);
        assertStatelessGroupStateOnAllNodes(flow.statelessGroup().getId(), StatelessGroupScheduledState.RUNNING);
    }

    @Test
    public void testStopSourcesRejectsStatelessProcessGroup() throws NiFiClientException, IOException, InterruptedException {
        final MixedEngineFlow flow = createMixedEngineFlow();
        startMixedEngineFlow(flow);

        final NiFiClientException exception = assertThrows(NiFiClientException.class, () -> getNifiClient().getFlowClient()
                .stopProcessGroupSources(flow.statelessGroup().getId(), stopSourcesRequest(flow.statelessGroup().getId())));

        assertConflict(exception);
        assertProcessorStateOnAllNodes(flow.statelessSource().getId(), ScheduledState.RUNNING);
        assertProcessorStateOnAllNodes(flow.statelessDownstream().getId(), ScheduledState.RUNNING);
        assertStatelessGroupStateOnAllNodes(flow.statelessGroup().getId(), StatelessGroupScheduledState.RUNNING);
    }

    @Test
    public void testStopSourcesRejectsMismatchedComponentIds() throws NiFiClientException, IOException, InterruptedException {
        final MixedEngineFlow flow = createMixedEngineFlow();
        startMixedEngineFlow(flow);
        final ScheduleComponentsEntity request = stopSourcesRequest(flow.parentGroup().getId());
        request.setComponents(Map.of());

        final NiFiClientException exception = assertThrows(NiFiClientException.class, () -> getNifiClient().getFlowClient()
                .stopProcessGroupSources(flow.parentGroup().getId(), request));

        assertConflict(exception);
        assertProcessorStateOnAllNodes(flow.standardSource().getId(), ScheduledState.RUNNING);
        assertProcessorStateOnAllNodes(flow.standardDownstream().getId(), ScheduledState.RUNNING);
        assertProcessorStateOnAllNodes(flow.statelessSource().getId(), ScheduledState.RUNNING);
        assertProcessorStateOnAllNodes(flow.statelessDownstream().getId(), ScheduledState.RUNNING);
        assertStatelessGroupStateOnAllNodes(flow.statelessGroup().getId(), StatelessGroupScheduledState.RUNNING);
    }

    @Test
    public void testStopSourcesRejectsWhenNodeIdentifiesDifferentSources() throws NiFiClientException, IOException, InterruptedException {
        final MixedEngineFlow flow = createMixedEngineFlow();
        startMixedEngineFlow(flow);
        final long node1Revision = stopAndRestartProcessorOnNode(flow.standardSource().getId(), 1);
        final long node2Revision = stopAndRenameProcessorOnNode(flow.standardSource().getId(), 2);

        assertEquals(node1Revision, node2Revision, "Processor revisions must match so source-set verification rejects the request");
        assertProcessorStateOnNode(flow.standardSource().getId(), ScheduledState.RUNNING, 1);
        assertProcessorStateOnNode(flow.standardSource().getId(), ScheduledState.STOPPED, 2);

        final NiFiClientException exception = assertThrows(NiFiClientException.class, () -> getNifiClient().getFlowClient()
                .stopProcessGroupSources(flow.parentGroup().getId(), stopSourcesRequest(flow.parentGroup().getId())));

        assertConflict(exception);
        assertProcessorStateOnNode(flow.standardSource().getId(), ScheduledState.RUNNING, 1);
        assertProcessorStateOnNode(flow.standardSource().getId(), ScheduledState.STOPPED, 2);
        assertProcessorStateOnAllNodes(flow.standardDownstream().getId(), ScheduledState.RUNNING);
        assertProcessorStateOnAllNodes(flow.statelessSource().getId(), ScheduledState.RUNNING);
        assertProcessorStateOnAllNodes(flow.statelessDownstream().getId(), ScheduledState.RUNNING);
        assertStatelessGroupStateOnAllNodes(flow.statelessGroup().getId(), StatelessGroupScheduledState.RUNNING);
    }

    private MixedEngineFlow createMixedEngineFlow() throws NiFiClientException, IOException, InterruptedException {
        final ProcessGroupEntity parentGroup = getClientUtil().createProcessGroup("Parent", "root");
        final ProcessGroupEntity standardGroup = getClientUtil().createProcessGroup("Standard", parentGroup.getId());
        final ProcessGroupEntity statelessGroup = getClientUtil().createProcessGroup("Stateless", parentGroup.getId());
        getClientUtil().markStateless(statelessGroup, "1 min");

        final ProcessorEntity standardSource = getClientUtil().createProcessor(GENERATE_FLOWFILE, standardGroup.getId());
        final ProcessorEntity standardDownstream = getClientUtil().createProcessor(TERMINATE_FLOWFILE, standardGroup.getId());
        getClientUtil().createConnection(standardSource, standardDownstream, SUCCESS, standardGroup.getId());

        final ProcessorEntity statelessSource = getClientUtil().createProcessor(GENERATE_FLOWFILE, statelessGroup.getId());
        final ProcessorEntity statelessDownstream = getClientUtil().createProcessor(TERMINATE_FLOWFILE, statelessGroup.getId());
        getClientUtil().createConnection(statelessSource, statelessDownstream, SUCCESS, statelessGroup.getId());

        getClientUtil().waitForValidProcessor(standardSource.getId());
        getClientUtil().waitForValidProcessor(standardDownstream.getId());
        getClientUtil().waitForValidProcessor(statelessSource.getId());
        getClientUtil().waitForValidProcessor(statelessDownstream.getId());

        return new MixedEngineFlow(parentGroup, standardGroup, statelessGroup,
                standardSource, standardDownstream, statelessSource, statelessDownstream);
    }

    private void startMixedEngineFlow(final MixedEngineFlow flow) throws NiFiClientException, IOException, InterruptedException {
        getClientUtil().startProcessGroupComponents(flow.standardGroup().getId());
        getClientUtil().startProcessGroupComponents(flow.statelessGroup().getId());
        getClientUtil().waitForRunningProcessor(flow.standardSource().getId());
        getClientUtil().waitForRunningProcessor(flow.standardDownstream().getId());
        getClientUtil().waitForRunningProcessor(flow.statelessSource().getId());
        getClientUtil().waitForRunningProcessor(flow.statelessDownstream().getId());
    }

    private ScheduleComponentsEntity stopSourcesRequest(final String groupId) {
        final ScheduleComponentsEntity request = new ScheduleComponentsEntity();
        request.setId(groupId);
        request.setState(ScheduledState.STOPPED.name());
        request.setDisconnectedNodeAcknowledged(true);
        return request;
    }

    private long stopAndRestartProcessorOnNode(final String processorId, final int nodeIndex)
            throws NiFiClientException, IOException, InterruptedException {
        try {
            switchClientToNode(nodeIndex);
            final ProcessorEntity processor = getNifiClient().getProcessorClient(DO_NOT_REPLICATE).getProcessor(processorId);
            processor.setDisconnectedNodeAcknowledged(true);
            getNifiClient().getProcessorClient(DO_NOT_REPLICATE).stopProcessor(processor);
            waitForProcessorStateOnCurrentNode(processorId, ScheduledState.STOPPED);

            final ProcessorEntity stoppedProcessor = getNifiClient().getProcessorClient(DO_NOT_REPLICATE).getProcessor(processorId);
            stoppedProcessor.setDisconnectedNodeAcknowledged(true);
            final ProcessorEntity restartedProcessor = getNifiClient().getProcessorClient(DO_NOT_REPLICATE).startProcessor(stoppedProcessor);
            return restartedProcessor.getRevision().getVersion();
        } finally {
            switchClientToNode(1);
        }
    }

    private long stopAndRenameProcessorOnNode(final String processorId, final int nodeIndex)
            throws NiFiClientException, IOException, InterruptedException {
        try {
            switchClientToNode(nodeIndex);
            final ProcessorEntity processor = getNifiClient().getProcessorClient(DO_NOT_REPLICATE).getProcessor(processorId);
            processor.setDisconnectedNodeAcknowledged(true);
            getNifiClient().getProcessorClient(DO_NOT_REPLICATE).stopProcessor(processor);
            waitForProcessorStateOnCurrentNode(processorId, ScheduledState.STOPPED);

            final ProcessorEntity stoppedProcessor = getNifiClient().getProcessorClient(DO_NOT_REPLICATE).getProcessor(processorId);
            stoppedProcessor.getComponent().setName(stoppedProcessor.getComponent().getName() + " Node " + nodeIndex);
            stoppedProcessor.setDisconnectedNodeAcknowledged(true);
            final ProcessorEntity renamedProcessor = getNifiClient().getProcessorClient(DO_NOT_REPLICATE).updateProcessor(stoppedProcessor);
            return renamedProcessor.getRevision().getVersion();
        } finally {
            switchClientToNode(1);
        }
    }

    private void waitForProcessorStateOnCurrentNode(final String processorId, final ScheduledState expectedState) throws InterruptedException {
        waitFor(() -> {
            final ProcessorEntity processor = getNifiClient().getProcessorClient(DO_NOT_REPLICATE).getProcessor(processorId);
            return expectedState.name().equals(processor.getComponent().getState())
                    && expectedState.name().equals(processor.getComponent().getPhysicalState());
        });
    }

    private void assertProcessorStateOnNode(final String processorId, final ScheduledState expectedState, final int nodeIndex)
            throws NiFiClientException, IOException {
        try {
            switchClientToNode(nodeIndex);
            final ProcessorEntity processor = getNifiClient().getProcessorClient(DO_NOT_REPLICATE).getProcessor(processorId);
            assertEquals(expectedState.name(), processor.getComponent().getState(),
                    "Unexpected state for Processor %s on Node %d".formatted(processorId, nodeIndex));
        } finally {
            switchClientToNode(1);
        }
    }

    private void assertProcessorStateOnAllNodes(final String processorId, final ScheduledState expectedState)
            throws NiFiClientException, IOException {
        try {
            for (int nodeIndex = 1; nodeIndex <= 2; nodeIndex++) {
                switchClientToNode(nodeIndex);
                final ProcessorEntity processor = getNifiClient().getProcessorClient(DO_NOT_REPLICATE).getProcessor(processorId);
                assertEquals(expectedState.name(), processor.getComponent().getState(),
                        "Unexpected state for Processor %s on Node %d".formatted(processorId, nodeIndex));
            }
        } finally {
            switchClientToNode(1);
        }
    }

    private void assertStatelessGroupStateOnAllNodes(final String groupId, final StatelessGroupScheduledState expectedState)
            throws NiFiClientException, IOException {
        try {
            for (int nodeIndex = 1; nodeIndex <= 2; nodeIndex++) {
                switchClientToNode(nodeIndex);
                final ProcessGroupEntity group = getNifiClient().getProcessGroupClient(DO_NOT_REPLICATE).getProcessGroup(groupId);
                assertEquals(expectedState.name(), group.getComponent().getStatelessGroupScheduledState(),
                        "Unexpected state for Stateless Process Group %s on Node %d".formatted(groupId, nodeIndex));
            }
        } finally {
            switchClientToNode(1);
        }
    }

    private void assertConflict(final NiFiClientException exception) {
        final Throwable cause = exception.getCause();
        if (cause instanceof final WebApplicationException webApplicationException) {
            assertEquals(409, webApplicationException.getResponse().getStatus());
            return;
        }

        fail("Expected WebApplicationException 409, got: " + cause);
    }

    private record MixedEngineFlow(
            ProcessGroupEntity parentGroup,
            ProcessGroupEntity standardGroup,
            ProcessGroupEntity statelessGroup,
            ProcessorEntity standardSource,
            ProcessorEntity standardDownstream,
            ProcessorEntity statelessSource,
            ProcessorEntity statelessDownstream) {
    }
}
