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

import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.queue.LoadBalanceCompression;
import org.apache.nifi.controller.queue.LoadBalanceStrategy;
import org.apache.nifi.tests.system.NiFiInstanceFactory;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDrainRequestDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that the drain stays active on every node until every node is drained, at which point NiFi ends it on every node.
 */
public class ClusteredProcessGroupDrainIT extends NiFiSystemIT {
    private static final String DRAINING = "DRAINING";
    private static final String ENDED = "ENDED";
    private static final String FAILED = "FAILED";
    private static final String GATE_FILE_NAME = "drain-gate.txt";

    // Each node runs in its own instance directory, so this relative path points to a different gate file on each node
    private static final String GATE_FILE_PATH = "./" + GATE_FILE_NAME;

    @Override
    public NiFiInstanceFactory getInstanceFactory() {
        return createTwoNodeInstanceFactory();
    }

    @Override
    protected void destroyFlow() throws NiFiClientException, IOException, InterruptedException {
        getClientUtil().endProcessGroupDrains("root");
        super.destroyFlow();
    }

    /**
     * The drain is begun through node 2, which then checks and ends the drain. The gate is opened on node 2 first, so node 2 reports that no
     * FlowFiles are queued while node 1 still has FlowFiles queued. Through more than two checks, the drain stays active on both nodes. Once the
     * gate is opened on node 1 as well, NiFi ends the drain on both nodes without any request from the client.
     */
    @Test
    public void testDrainEndsOnlyWhenEveryNodeIsDrained() throws NiFiClientException, IOException, InterruptedException {
        final File node1GateFile = prepareGateFile(1);
        final File node2GateFile = prepareGateFile(2);
        final GatedFlow flow = createGatedFlowWithDataOnEachNode();

        // Node 1 is normally the Cluster Coordinator, so beginning the drain through node 2 has node 2 forward the request and watch the drain
        final String drainRequestId;
        try {
            switchClientToNode(2);
            drainRequestId = getClientUtil().startProcessGroupDrain(flow.group().getId()).getDrainRequest().getId();
        } finally {
            switchClientToNode(1);
        }
        getClientUtil().waitForRunningProcessor(flow.terminate().getId());

        assertTrue(node2GateFile.createNewFile());
        waitFor(() -> getDrainRequestOnNode(2, flow.group().getId(), drainRequestId).getCurrentCount() == 0);

        // NiFi checks the drain every two seconds, so this covers more than two checks
        Thread.sleep(5_000L);

        final ProcessGroupDrainRequestDTO node1Request = getDrainRequestOnNode(1, flow.group().getId(), drainRequestId);
        assertEquals(DRAINING, node1Request.getState());
        assertTrue(node1Request.getCurrentCount() > 0);

        assertEquals(DRAINING, getDrainRequestOnNode(2, flow.group().getId(), drainRequestId).getState());

        assertTrue(node1GateFile.createNewFile());
        getClientUtil().waitForProcessGroupDrainEnded(flow.group().getId(), drainRequestId);
        assertDrainEndedOnEachNode(flow.group().getId(), Map.of(flow.generate().getId(), ScheduledState.STOPPED, flow.terminate().getId(), ScheduledState.RUNNING));
    }

    /**
     * Node 1 begins and watches the drain, and drains its own data, while node 2 still has FlowFiles queued. Node 2 then restarts, so it no longer
     * has a record of the drain but still has its FlowFiles queued. While node 2 is stopped, the drain stays active on node 1. Once node 2 has
     * reconnected, the merged request reports that the drain failed on node 2, and NiFi ends the drain on node 1.
     */
    @Test
    public void testDrainFailsWhenNodeRestarts() throws NiFiClientException, IOException, InterruptedException {
        final File node1GateFile = prepareGateFile(1);
        prepareGateFile(2);
        final GatedFlow flow = createGatedFlowWithDataOnEachNode();

        final String drainRequestId = getClientUtil().startProcessGroupDrain(flow.group().getId()).getDrainRequest().getId();
        getClientUtil().waitForRunningProcessor(flow.terminate().getId());

        assertTrue(node1GateFile.createNewFile());
        waitFor(() -> getDrainRequestOnNode(1, flow.group().getId(), drainRequestId).getCurrentCount() == 0);

        // NiFi checks the drain every two seconds, so this covers more than two checks while node 2 is not connected
        getNiFiInstance().getNodeInstance(2).stop();
        Thread.sleep(5_000L);
        assertEquals(DRAINING, getDrainRequestOnNode(1, flow.group().getId(), drainRequestId).getState());

        getNiFiInstance().getNodeInstance(2).start(true);
        waitForAllNodesConnected();

        waitFor(() -> ENDED.equals(getDrainRequestOnNode(1, flow.group().getId(), drainRequestId).getState()));

        final ProcessGroupDrainRequestDTO mergedRequest = getNifiClient().getProcessGroupClient().getDrainRequest(flow.group().getId(), drainRequestId).getDrainRequest();
        assertNotNull(mergedRequest);
        assertEquals(FAILED, mergedRequest.getState());
        assertTrue(mergedRequest.getFailureReason().contains("no record of the drain"), mergedRequest.getFailureReason());
        assertTrue(getQueueSizeOnNode(2, flow.connection().getId()) > 0);
    }

    /**
     * Creates a group in which a source generates 10 FlowFiles that are load balanced across both nodes, to a Processor that removes FlowFiles
     * only once the gate file of its node exists. Returns once load balancing is complete and each node has FlowFiles queued.
     */
    private GatedFlow createGatedFlowWithDataOnEachNode() throws NiFiClientException, IOException, InterruptedException {
        final ProcessGroupEntity group = getClientUtil().createProcessGroup("Drained", "root");
        final ProcessorEntity generate = getClientUtil().createProcessor(GENERATE_FLOWFILE, group.getId());
        final ProcessorEntity terminate = getClientUtil().createProcessor(TERMINATE_FLOWFILE, group.getId());
        final ConnectionEntity connection = getClientUtil().createConnection(generate, terminate, SUCCESS, group.getId());
        getClientUtil().updateConnectionLoadBalancing(connection, LoadBalanceStrategy.ROUND_ROBIN, LoadBalanceCompression.DO_NOT_COMPRESS, null);
        getClientUtil().updateProcessorProperties(generate, Map.of("Batch Size", "5"));
        getClientUtil().updateProcessorProperties(terminate, Map.of("Gate File", GATE_FILE_PATH));

        getClientUtil().runProcessorOnce(getNifiClient().getProcessorClient().getProcessor(generate.getId()));
        waitForQueueCount(connection.getId(), 10);
        getClientUtil().waitForStoppedProcessor(generate.getId());
        waitFor(() -> ConnectionDTO.LOAD_BALANCE_INACTIVE.equals(getNifiClient().getConnectionClient().getConnection(connection.getId()).getComponent().getLoadBalanceStatus()));
        assertTrue(getQueueSizeOnNode(1, connection.getId()) > 0);
        assertTrue(getQueueSizeOnNode(2, connection.getId()) > 0);

        return new GatedFlow(group, generate, terminate, connection);
    }

    /**
     * Returns the gate file of the given node, deleting it first if an earlier test left it in place.
     */
    private File prepareGateFile(final int nodeIndex) throws IOException {
        final File gateFile = new File(getNiFiInstance().getNodeInstance(nodeIndex).getInstanceDirectory(), GATE_FILE_NAME);
        gateFile.deleteOnExit();
        Files.deleteIfExists(gateFile.toPath());
        return gateFile;
    }

    private ProcessGroupDrainRequestDTO getDrainRequestOnNode(final int nodeIndex, final String groupId, final String drainRequestId) throws NiFiClientException, IOException {
        try {
            switchClientToNode(nodeIndex);
            return getNifiClient().getProcessGroupClient(DO_NOT_REPLICATE).getDrainRequest(groupId, drainRequestId).getDrainRequest();
        } finally {
            switchClientToNode(1);
        }
    }

    private int getQueueSizeOnNode(final int nodeIndex, final String connectionId) throws NiFiClientException, IOException {
        try {
            switchClientToNode(nodeIndex);
            return getNifiClient().getFlowClient(DO_NOT_REPLICATE).getConnectionStatus(connectionId, false).getConnectionStatus().getAggregateSnapshot().getFlowFilesQueued();
        } finally {
            switchClientToNode(1);
        }
    }

    private void assertDrainEndedOnEachNode(final String groupId, final Map<String, ScheduledState> expectedProcessorStates) throws NiFiClientException, IOException {
        try {
            for (int nodeIndex = 1; nodeIndex <= 2; nodeIndex++) {
                switchClientToNode(nodeIndex);
                final ProcessGroupEntity group = getNifiClient().getProcessGroupClient(DO_NOT_REPLICATE).getProcessGroup(groupId);
                assertNull(group.getComponent().getDrainRequestId(), "Node " + nodeIndex);

                for (final Map.Entry<String, ScheduledState> entry : expectedProcessorStates.entrySet()) {
                    final ProcessorEntity processor = getNifiClient().getProcessorClient(DO_NOT_REPLICATE).getProcessor(entry.getKey());
                    assertEquals(entry.getValue().name(), processor.getComponent().getState(), "Processor " + processor.getComponent().getName() + " on Node " + nodeIndex);
                }
            }
        } finally {
            switchClientToNode(1);
        }
    }

    private record GatedFlow(ProcessGroupEntity group, ProcessorEntity generate, ProcessorEntity terminate, ConnectionEntity connection) {
    }
}
