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
package org.apache.nifi.tests.system.loadbalance;

import org.apache.nifi.controller.queue.LoadBalanceCompression;
import org.apache.nifi.controller.queue.LoadBalanceStrategy;
import org.apache.nifi.scheduling.ExecutionNode;
import org.apache.nifi.tests.system.NiFiInstanceFactory;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.tests.system.SpawnedClusterNiFiInstanceFactory;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.NodeDTO;
import org.apache.nifi.web.api.dto.status.ConnectionStatusDTO;
import org.apache.nifi.web.api.dto.status.ConnectionStatusSnapshotDTO;
import org.apache.nifi.web.api.dto.status.NodeConnectionStatusSnapshotDTO;
import org.apache.nifi.web.api.entity.ClusterEntity;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ConnectionStatusEntity;
import org.apache.nifi.web.api.entity.FlowFileEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Set;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class LoadBalanceIT extends NiFiSystemIT {
    @Override
    protected NiFiInstanceFactory getInstanceFactory() {
        return new SpawnedClusterNiFiInstanceFactory(
            "src/test/resources/conf/clustered/node1/bootstrap.conf",
            "src/test/resources/conf/clustered/node2/bootstrap.conf");
    }

    @Test
    public void testRoundRobinStrategyNoCompression() throws NiFiClientException, IOException, InterruptedException {
        testRoundRobinStrategy(LoadBalanceCompression.DO_NOT_COMPRESS);
    }

    @Test
    public void testRoundRobinStrategyCompressAttributesOnly() throws NiFiClientException, IOException, InterruptedException {
        testRoundRobinStrategy(LoadBalanceCompression.COMPRESS_ATTRIBUTES_ONLY);
    }

    @Test
    public void testRoundRobinStrategyCompressAll() throws NiFiClientException, IOException, InterruptedException {
        testRoundRobinStrategy(LoadBalanceCompression.COMPRESS_ATTRIBUTES_AND_CONTENT);
    }

    private void testRoundRobinStrategy(final LoadBalanceCompression compression) throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        final ProcessorEntity count = getClientUtil().createProcessor("CountEvents");

        final ConnectionEntity connection = getClientUtil().createConnection(generate, count, "success");
        getClientUtil().setAutoTerminatedRelationships(count, "success");

        // Configure Processor to generate 20 FlowFiles, each 1 MB and run on Primary Node.
        final Map<String, String> generateProperties = new HashMap<>();
        generateProperties.put("File Size", "1 MB");
        generateProperties.put("Batch Size", "20");
        getClientUtil().updateProcessorProperties(generate, generateProperties);
        getClientUtil().updateProcessorExecutionNode(generate, ExecutionNode.PRIMARY);

        // Round Robin between nodes. This should result in 10 FlowFiles on each node.
        getClientUtil().updateConnectionLoadBalancing(connection, LoadBalanceStrategy.ROUND_ROBIN, compression, null);

        // Generate the data.
        getNifiClient().getProcessorClient().startProcessor(generate);

        // Wait until all 20 FlowFiles are queued up.
        waitFor(() -> {
            final ConnectionStatusEntity statusEntity = getConnectionStatus(connection.getId());
            return statusEntity.getConnectionStatus().getAggregateSnapshot().getFlowFilesQueued() == 20;
        });

        // Wait until load balancing is complete
        waitFor(() -> isConnectionDoneLoadBalancing(connection.getId()));

        // Ensure that the FlowFiles are evenly distributed between the nodes.
        final ConnectionStatusEntity statusEntity = getConnectionStatus(connection.getId());
        assertTrue(isEvenlyDistributed(statusEntity));

        assertEquals(20, getQueueSize(connection.getId()));
        assertEquals(20 * 1024 * 1024, getQueueBytes(connection.getId()));
    }


    @Test
    public void testSingleNodeStrategy() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        final ProcessorEntity count = getClientUtil().createProcessor("CountEvents");

        final ConnectionEntity connection = getClientUtil().createConnection(generate, count, "success");
        getClientUtil().setAutoTerminatedRelationships(count, "success");

        // Configure Processor to generate 10 FlowFiles, each 1 MB, on each node, for a total of 20 FlowFiles.
        final Map<String, String> generateProperties = new HashMap<>();
        generateProperties.put("File Size", "1 MB");
        generateProperties.put("Batch Size", "10");
        getClientUtil().updateProcessorProperties(generate, generateProperties);

        // Round Robin between nodes. This should result in 10 FlowFiles on each node.
        getClientUtil().updateConnectionLoadBalancing(connection, LoadBalanceStrategy.SINGLE_NODE, LoadBalanceCompression.DO_NOT_COMPRESS, null);

        // Generate the data.
        getNifiClient().getProcessorClient().startProcessor(generate);

        // Wait until all 20 FlowFiles are queued up.
        waitFor(() -> {
            final ConnectionStatusEntity statusEntity = getConnectionStatus(connection.getId());
            return statusEntity.getConnectionStatus().getAggregateSnapshot().getFlowFilesQueued() == 20;
        });

        // Wait until load balancing is complete
        waitFor(() -> isConnectionDoneLoadBalancing(connection.getId()));

        // Ensure that all FlowFiles are on the same node.
        final ConnectionStatusEntity statusEntity = getConnectionStatus(connection.getId());
        final ConnectionStatusDTO connectionStatusDto = statusEntity.getConnectionStatus();
        final int numNodes = connectionStatusDto.getNodeSnapshots().size();

        int emptyNodes = 0;

        for (final NodeConnectionStatusSnapshotDTO nodeStatusDto : connectionStatusDto.getNodeSnapshots()) {
            final ConnectionStatusSnapshotDTO snapshotDto = nodeStatusDto.getStatusSnapshot();
            final int flowFilesQueued = snapshotDto.getFlowFilesQueued();

            // Number of flowfiles should either be 0 or should be equal to the total number of FlowFiles in the queue.
            if (flowFilesQueued == 0) {
                emptyNodes++;
            } else {
                assertEquals(statusEntity.getConnectionStatus().getAggregateSnapshot().getFlowFilesQueued().intValue(), flowFilesQueued);
            }
        }

        // Number of empty nodes should be one less than total number of nodes.
        assertEquals(numNodes - 1, emptyNodes);
    }

    @Test
    public void testPartitionByAttribute() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        final ProcessorEntity count = getClientUtil().createProcessor("CountEvents");

        final ConnectionEntity connection = getClientUtil().createConnection(generate, count, "success");
        getClientUtil().setAutoTerminatedRelationships(count, "success");

        // Configure Processor to generate 10 FlowFiles, each 1 MB, on each node, for a total of 20 FlowFiles.
        final Map<String, String> generateProperties = new HashMap<>();
        generateProperties.put("File Size", "1 MB");
        generateProperties.put("Batch Size", "10");
        generateProperties.put("number", "0");
        getClientUtil().updateProcessorProperties(generate, generateProperties);
        getClientUtil().updateProcessorExecutionNode(generate, ExecutionNode.PRIMARY);

        // Round Robin between nodes. This should result in 10 FlowFiles on each node.
        getClientUtil().updateConnectionLoadBalancing(connection, LoadBalanceStrategy.PARTITION_BY_ATTRIBUTE, LoadBalanceCompression.DO_NOT_COMPRESS, "number");

        // Queue 100 FlowFiles. 10 with number=0, 10 with number=1, 10 with number=2, etc. to up 10 with number=9
        for (int i=1; i <= 10; i++) {
            // Generate the data.
            getNifiClient().getProcessorClient().startProcessor(generate);

            final int expectedQueueSize = 10 * i;

            // Wait until all 10 FlowFiles are queued up.
            waitFor(() -> {
                final ConnectionStatusEntity statusEntity = getConnectionStatus(connection.getId());
                return statusEntity.getConnectionStatus().getAggregateSnapshot().getFlowFilesQueued() == expectedQueueSize;
            });


            getNifiClient().getProcessorClient().stopProcessor(generate);
            getClientUtil().waitForStoppedProcessor(generate.getId());

            generateProperties.put("number", String.valueOf(i));
            getClientUtil().updateProcessorProperties(generate, generateProperties);
        }

        // Wait until load balancing is complete
        waitFor(() -> isConnectionDoneLoadBalancing(connection.getId()));

        final Map<String, Set<String>> nodesByAttribute = new HashMap<>();
        for (int i=0; i < 100; i++) {
            final FlowFileEntity flowFile = getClientUtil().getQueueFlowFile(connection.getId(), i);
            final String numberValue = flowFile.getFlowFile().getAttributes().get("number");
            final Set<String> nodes = nodesByAttribute.computeIfAbsent(numberValue, key -> new HashSet<>());
            nodes.add(flowFile.getFlowFile().getClusterNodeId());
        }

        assertEquals(10, nodesByAttribute.size());
        for (final Map.Entry<String, Set<String>> entry : nodesByAttribute.entrySet()) {
            final Set<String> nodes = entry.getValue();
            assertEquals("FlowFile with attribute number=" + entry.getKey() + " went to nodes " + nodes, 1, nodes.size());
        }
    }

    @Test
    public void testOffload() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        final ProcessorEntity count = getClientUtil().createProcessor("CountEvents");

        final ConnectionEntity connection = getClientUtil().createConnection(generate, count, "success");
        getClientUtil().setAutoTerminatedRelationships(count, "success");

        // Configure Processor to generate 10 FlowFiles, each 1 MB, on each node (for a total of 20)
        final Map<String, String> generateProperties = new HashMap<>();
        generateProperties.put("File Size", "1 MB");
        generateProperties.put("Batch Size", "10");
        getClientUtil().updateProcessorProperties(generate, generateProperties);

        // Generate the data.
        getNifiClient().getProcessorClient().startProcessor(generate);

        // Wait until all 20 FlowFiles are queued up.
        waitFor(() -> {
            final ConnectionStatusEntity statusEntity = getConnectionStatus(connection.getId());
            return statusEntity.getConnectionStatus().getAggregateSnapshot().getFlowFilesQueued() == 20;
        });

        final ClusterEntity clusterEntity = getNifiClient().getControllerClient().getNodes();
        final Collection<NodeDTO> nodes = clusterEntity.getCluster().getNodes();

        // Do not disconnect the node that the client is pointing out
        final NodeDTO firstNodeDto = nodes.stream()
            .filter(nodeDto -> nodeDto.getApiPort() != 5671)
            .findFirst()
            .get();

        final String nodeId = firstNodeDto.getNodeId();

        getClientUtil().disconnectNode(nodeId);
        getClientUtil().offloadNode(nodeId);

        waitFor(this::isNodeOffloaded);

        assertEquals(20, getQueueSize(connection.getId()));
        assertEquals(20 * 1024 * 1024, getQueueBytes(connection.getId()));

        getClientUtil().connectNode(nodeId);
        waitForAllNodesConnected();

        // The node that was disconnected will have stopped its processor when it was told to offload. When it joins back into the cluster,
        // the node will determine that the cluster wants the GenerateFlowFile processor running and as a result start the Processor again. This will
        // Trigger the processor to then generate another batch of 10 FlowFiles.
        waitFor(() -> getQueueSize(connection.getId()) == 30);
        assertEquals(30 * 1024 * 1024, getQueueBytes(connection.getId()));
    }

    private int getQueueSize(final String connectionId) {
        final ConnectionStatusEntity statusEntity = getConnectionStatus(connectionId);
        final ConnectionStatusDTO connectionStatusDto = statusEntity.getConnectionStatus();
        return connectionStatusDto.getAggregateSnapshot().getFlowFilesQueued().intValue();
    }

    private long getQueueBytes(final String connectionId) {
        final ConnectionStatusEntity statusEntity = getConnectionStatus(connectionId);
        final ConnectionStatusDTO connectionStatusDto = statusEntity.getConnectionStatus();
        return connectionStatusDto.getAggregateSnapshot().getBytesQueued().longValue();
    }


    private boolean isNodeOffloaded() {
        final ClusterEntity clusterEntity;
        try {
            clusterEntity = getNifiClient().getControllerClient().getNodes();
        } catch (final Exception e) {
            e.printStackTrace();
            return false;
        }

        final Collection<NodeDTO> nodeDtos = clusterEntity.getCluster().getNodes();

        for (final NodeDTO dto : nodeDtos) {
            final String status = dto.getStatus();
            if (status.equalsIgnoreCase("OFFLOADED")) {
                return true;
            }
        }

        return false;
    }

    private boolean isConnectionDoneLoadBalancing(final String connectionId) {
        try {
            final ConnectionEntity connectionEntity = getNifiClient().getConnectionClient().getConnection(connectionId);
            final String loadBalanceStatus = connectionEntity.getComponent().getLoadBalanceStatus();
            return ConnectionDTO.LOAD_BALANCE_INACTIVE.equals(loadBalanceStatus);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    private ConnectionStatusEntity getConnectionStatus(final String connectionId) {
        try {
            return getNifiClient().getFlowClient().getConnectionStatus(connectionId, true);
        } catch (final Exception e) {
            Assert.fail("Failed to obtain connection status");
            return null;
        }
    }

    private boolean isEvenlyDistributed(final ConnectionStatusEntity statusEntity) {
        final ConnectionStatusDTO connectionStatusDto = statusEntity.getConnectionStatus();

        final LongSummaryStatistics stats = connectionStatusDto.getNodeSnapshots().stream()
            .map(NodeConnectionStatusSnapshotDTO::getStatusSnapshot)
            .mapToLong(ConnectionStatusSnapshotDTO::getFlowFilesQueued)
            .summaryStatistics();

        return stats.getMin() == stats.getMax();
    }

}
