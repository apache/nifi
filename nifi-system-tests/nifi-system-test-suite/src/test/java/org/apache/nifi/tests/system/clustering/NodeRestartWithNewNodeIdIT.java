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

import org.apache.nifi.tests.system.NiFiInstance;
import org.apache.nifi.tests.system.NiFiInstanceFactory;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.tests.system.SpawnedClusterNiFiInstanceFactory;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.web.api.dto.NodeDTO;
import org.apache.nifi.web.api.entity.ClusteSummaryEntity;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class NodeRestartWithNewNodeIdIT extends NiFiSystemIT {

    @Override
    protected NiFiInstanceFactory getInstanceFactory() {
        return new SpawnedClusterNiFiInstanceFactory(
            "src/test/resources/conf/clustered/node1/bootstrap.conf",
            "src/test/resources/conf/clustered/node2/bootstrap.conf");
    }

    @Test
    public void testRestartNodeWithDifferentNodeId() throws IOException, NiFiClientException {
        // Get a view of the cluster as it is currently.
        final Collection<NodeDTO> originalNodeDTOs = getNifiClient().getControllerClient().getNodes().getCluster().getNodes();
        final NodeDTO originalNode1 = originalNodeDTOs.stream().filter(dto -> dto.getApiPort() == 5671).findFirst().orElse(null);
        final NodeDTO originalNode2 = originalNodeDTOs.stream().filter(dto -> dto.getApiPort() == 5672).findFirst().orElse(null);

        // Stop the second instance, delete its state directory, and restart it.
        final NiFiInstance secondNode = getNiFiInstance().getNodeInstance(2);
        secondNode.stop();

        final File destinationDir = new File(secondNode.getInstanceDirectory(), "state/local");
        deleteChildren(destinationDir);
        secondNode.start();

        // Wait for the second node to reconnect.
        waitForAllNodesConnected();

        final ClusteSummaryEntity clusterSummary = getNifiClient().getFlowClient().getClusterSummary();
        // 2/2 because we replaced the state directory with a different state that had a different identifier. This will result in
        // 2 nodes each with a different UUID but the same hostname & port, but we should see that the cluster removes the old Node
        // Identifier, so we will have only 2 nodes total.
        assertEquals("2 / 2", clusterSummary.getClusterSummary().getConnectedNodes());

        // Get the updated view of the cluster.
        final Collection<NodeDTO> updatedNodeDTOs = getNifiClient().getControllerClient().getNodes().getCluster().getNodes();

        // Node 1 should be the same, but Node 2 should have a new UUID. This is because on startup, the node will not have found any local state. As a result, it
        // will create a new UUID for its Node Identifier. However, it should have the same hostname and port. As a result, the cluster will remove the old Node that has
        // a different UUID but the same hostname and port.
        final NodeDTO updatedNode1 = updatedNodeDTOs.stream().filter(dto -> dto.getApiPort() == 5671).findFirst().orElse(null);
        final NodeDTO updatedNode2 = updatedNodeDTOs.stream().filter(dto -> dto.getApiPort() == 5672).findFirst().orElse(null);

        assertEquals(originalNode1.getNodeId(), updatedNode1.getNodeId());
        assertEquals(originalNode1.getAddress(), updatedNode1.getAddress());
        assertEquals(originalNode1.getApiPort(), updatedNode1.getApiPort());

        assertNotEquals(originalNode2.getNodeId(), updatedNode2.getNodeId()); // the UUID of Node 2 will be different, but the hostnames & ports should be the same.
        assertEquals(originalNode2.getAddress(), updatedNode2.getAddress());
        assertEquals(originalNode2.getApiPort(), updatedNode2.getApiPort());
    }


    private void deleteChildren(final File dir) {
        if (!dir.exists()) {
            return;
        }

        for (final File file : dir.listFiles()) {
            if (file.isDirectory()) {
                deleteChildren(file);
            }

            assertTrue(file.delete());
        }
    }

}
