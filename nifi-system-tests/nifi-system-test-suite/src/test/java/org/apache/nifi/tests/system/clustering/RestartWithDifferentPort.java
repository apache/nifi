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
import org.apache.nifi.web.api.entity.ClusterEntity;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RestartWithDifferentPort extends NiFiSystemIT {
    @Override
    protected NiFiInstanceFactory getInstanceFactory() {
        return new SpawnedClusterNiFiInstanceFactory(
            "src/test/resources/conf/clustered/node1/bootstrap.conf",
            "src/test/resources/conf/clustered/node2/bootstrap.conf");
    }

    @Test
    public void testRestartWithDifferentPortKeepsNodeIdUnchanged() throws IOException, NiFiClientException {
        // Get the set of Node UUID's
        ClusterEntity clusterEntity = getNifiClient().getControllerClient().getNodes();
        Collection<NodeDTO> nodeDtos = clusterEntity.getCluster().getNodes();
        final Set<String> nodeUuids = nodeDtos.stream().map(NodeDTO::getNodeId).collect(Collectors.toSet());

        // Stop the second instance and change its web api port
        final NiFiInstance secondNode = getNiFiInstance().getNodeInstance(2);
        secondNode.stop();

        // Change the value of the nifi.web.http.port property from 5672 to 5673
        secondNode.setProperty("nifi.web.http.port", "5673");

        // Restart the second node
        secondNode.start();
        waitForAllNodesConnected(getNumberOfNodes(true), 2000L);

        // Wait for the second node to reconnect.
        final ClusteSummaryEntity clusterSummary = getNifiClient().getFlowClient().getClusterSummary();
        assertEquals("2 / 2", clusterSummary.getClusterSummary().getConnectedNodes());

        // Ensure that the Node UUID's are the same and that we now have 2 nodes: localhost:5671 and localhost:5673, but NOT localhost:5672
        clusterEntity = getNifiClient().getControllerClient().getNodes();
        nodeDtos = clusterEntity.getCluster().getNodes();

        final Set<String> updatedNodeUuids = nodeDtos.stream().map(NodeDTO::getNodeId).collect(Collectors.toSet());
        assertEquals(nodeUuids, updatedNodeUuids);

        final Set<String> nodeAddresses = nodeDtos.stream().map(dto -> dto.getAddress() + ":" + dto.getApiPort()).collect(Collectors.toSet());
        assertEquals(2, nodeAddresses.size());
        assertTrue(nodeAddresses.contains("localhost:5671"));
        assertTrue(nodeAddresses.contains("localhost:5673"));
    }
}
