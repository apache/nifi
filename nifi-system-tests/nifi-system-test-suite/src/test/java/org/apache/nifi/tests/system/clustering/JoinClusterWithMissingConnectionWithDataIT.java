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
package org.apache.nifi.tests.system.clustering;

import org.apache.nifi.tests.system.InstanceConfiguration;
import org.apache.nifi.tests.system.NiFiInstance;
import org.apache.nifi.tests.system.NiFiInstanceFactory;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.tests.system.SpawnedClusterNiFiInstanceFactory;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.NodeDTO;
import org.apache.nifi.web.api.dto.flow.FlowDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.NodeEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class JoinClusterWithMissingConnectionWithDataIT extends NiFiSystemIT {
    private static final Logger logger = LoggerFactory.getLogger(JoinClusterWithMissingConnectionWithDataIT.class);

    private static final String GENERATE_UUID = "6be9a7e7-016e-1000-0000-00004700499d";
    private static final String CONNECTION_UUID = "6be9a991-016e-1000-ffff-fffffebf0217";

    @Override
    public NiFiInstanceFactory getInstanceFactory() {
        return new SpawnedClusterNiFiInstanceFactory(
            new InstanceConfiguration.Builder()
                .bootstrapConfig("src/test/resources/conf/clustered/node1/bootstrap.conf")
                .instanceDirectory("target/node1")
                .flowJson("src/test/resources/flows/missing-connection/with-connection.json.gz")
                .build(),
            new InstanceConfiguration.Builder()
                .bootstrapConfig("src/test/resources/conf/clustered/node2/bootstrap.conf")
                .instanceDirectory("target/node2")
                .flowJson("src/test/resources/flows/missing-connection/with-connection.json.gz")
                .build()
        );
    }

    @Override
    protected boolean isDestroyFlowAfterEachTest() {
        // Do not destroy the flow because there is only a single test in the class and because the expected state
        // is for Node 2 to be a part of the cluster but disconnected.
        return false;
    }

    @Override
    protected boolean isAllowFactoryReuse() {
        // Do not allow reuse of the factory because we expect the node to be in a disconnected state at the end.
        return false;
    }

    @Test
    public void testFailsToJoinWithMissingConnectionThatHasData() throws NiFiClientException, IOException, InterruptedException {
        // Create the flow
        ProcessorEntity generate = getNifiClient().getProcessorClient().getProcessor(GENERATE_UUID);

        // Start Generate Processor
        generate = getClientUtil().startProcessor(generate);

        // Wait for data to be queued up, one FlowFile for each node.
        waitFor(this::isDataQueued);

        // Stop the processor
        getNifiClient().getProcessorClient().stopProcessor(generate);
        getClientUtil().waitForStoppedProcessor(generate.getId());

        // Disconnect and remove node 2 from the cluster. Then stop the node.
        final NodeDTO node2Dto = getNifiClient().getControllerClient().getNodes().getCluster().getNodes().stream()
            .filter(nodeDto -> nodeDto.getApiPort() == 5672)
            .findAny()
            .get();

        final NodeEntity nodeEntity = new NodeEntity();
        nodeEntity.setNode(node2Dto);
        node2Dto.setStatus("DISCONNECTING");
        getNifiClient().getControllerClient().disconnectNode(node2Dto.getNodeId(), nodeEntity);
        waitFor(() -> isNodeDisconnected(5672));

        final NiFiInstance node2 = getNiFiInstance().getNodeInstance(2);
        node2.stop();

        // Remove node from the cluster
        getNifiClient().getControllerClient().deleteNode(node2Dto.getNodeId());
        waitFor(() -> isNodeRemoved(5672));

        // Drop the data in the queue and delete the queue.
        getClientUtil().emptyQueue(CONNECTION_UUID);

        getNifiClient().getConnectionClient().deleteConnection(CONNECTION_UUID, "test-client-id", 0);

        node2.start(false);

        // Node should fail to connect but instead should be disconnected.
        waitFor(() -> isNodeDisconnected(5672));
    }

    private boolean isNodeRemoved(final int apiPort) {
        try {
            return getNifiClient().getControllerClient().getNodes().getCluster().getNodes().stream()
                .noneMatch(dto -> dto.getApiPort() == apiPort);
        } catch (Exception e) {
            return false;
        }
    }

    private boolean isNodeDisconnected(final int apiPort) {
        try {
            final NodeDTO nodeDto = getNifiClient().getControllerClient().getNodes().getCluster().getNodes().stream()
                .filter(dto -> dto.getApiPort() == apiPort)
                .findAny()
                .get();

            return "DISCONNECTED".equals(nodeDto.getStatus());
        } catch (Exception e) {
            return false;
        }
    }

    private boolean isDataQueued() {
        final FlowDTO flowDto;
        try {
            flowDto = getNifiClient().getFlowClient().getProcessGroup("root").getProcessGroupFlow().getFlow();
        } catch (final Exception e) {
            logger.error("Failed to retrieve flow", e);
            return false;
        }

        final ConnectionEntity connectionEntity = flowDto.getConnections().iterator().next();
        final Integer queuedCount = connectionEntity.getStatus().getAggregateSnapshot().getFlowFilesQueued();
        return queuedCount == 2;
    }
}