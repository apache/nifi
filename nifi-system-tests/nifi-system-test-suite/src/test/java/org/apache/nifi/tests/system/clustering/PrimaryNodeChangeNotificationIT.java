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

import org.apache.nifi.cluster.coordination.node.ClusterRoles;
import org.apache.nifi.tests.system.NiFiInstanceFactory;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.toolkit.client.ProcessorClient;
import org.apache.nifi.web.api.dto.NodeDTO;
import org.apache.nifi.web.api.entity.ClusterEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class PrimaryNodeChangeNotificationIT extends NiFiSystemIT {
    @Override
    public NiFiInstanceFactory getInstanceFactory() {
        return createTwoNodeInstanceFactory();
    }

    @Test
    public void testNotifications() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity processor = getClientUtil().createProcessor("CountPrimaryNodeChangeEvents");
        getClientUtil().startProcessor(processor);

        // Wait for processor to be triggered on both nodes
        Map<String, Long> counters = new HashMap<>();
        while (!counters.containsKey("Triggers-1") || !counters.containsKey("Triggers-2")) {
            Thread.sleep(10L);
            counters = getClientUtil().getCountersAsMap(processor.getId());
        }

        final NodeDTO primaryNode = getNode(ClusterRoles.PRIMARY_NODE, false);
        final NodeDTO nonPrimaryNode = getNode(ClusterRoles.PRIMARY_NODE, true);

        getClientUtil().disconnectNode(primaryNode.getNodeId());
        setupClient(nonPrimaryNode.getApiPort());
        waitForNodeStatus(primaryNode, "DISCONNECTED");

        getClientUtil().connectNode(primaryNode.getNodeId());
        waitForAllNodesConnected();

        waitFor(() -> {
            final Map<String, Long> counterMap = getClientUtil().getCountersAsMap(processor.getId());
            final Long notificationCalledNode1 = counterMap.get("PrimaryNodeChangeCalled-1");
            final Long notificationCalledNode2 = counterMap.get("PrimaryNodeChangeCalled-2");
            final Long notificationCompletedNode1 = counterMap.get("PrimaryNodeChangeCompleted-1");
            final Long notificationCompletedNode2 = counterMap.get("PrimaryNodeChangeCompleted-2");

            return notificationCalledNode1 > 0 && notificationCalledNode2 > 0 && notificationCompletedNode1 > 0 && notificationCompletedNode2 > 0;
        });
    }

    @Test
    public void testTerminateNotificationWhenBlocked() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity processor = getClientUtil().createProcessor("CountPrimaryNodeChangeEvents");

        // Set the event sleep duration to 10 minutes so that the processor will be blocked for a while when the primary node changes
        getClientUtil().updateProcessorProperties(processor, Collections.singletonMap("Event Sleep Duration", "10 mins"));
        getClientUtil().startProcessor(processor);

        // Wait for processor to be triggered on both nodes
        Map<String, Long> counters = new HashMap<>();
        while (!counters.containsKey("Triggers-1") || !counters.containsKey("Triggers-2")) {
            Thread.sleep(10L);
            counters = getClientUtil().getCountersAsMap(processor.getId());
        }

        final NodeDTO primaryNode = getNode(ClusterRoles.PRIMARY_NODE, false);
        final NodeDTO nonPrimaryNode = getNode(ClusterRoles.PRIMARY_NODE, true);

        getClientUtil().disconnectNode(primaryNode.getNodeId());
        setupClient(nonPrimaryNode.getApiPort());
        waitForNodeStatus(primaryNode, "DISCONNECTED");

        getClientUtil().connectNode(primaryNode.getNodeId());
        waitForAllNodesConnected();

        // Wait until the "called" counter is incremented but hte ChangeCompleted counter is not
        waitFor(() -> {
            final Map<String, Long> counterMap = getClientUtil().getCountersAsMap(processor.getId());
            final Long notificationCalledNode1 = counterMap.get("PrimaryNodeChangeCalled-1");
            final Long notificationCalledNode2 = counterMap.get("PrimaryNodeChangeCalled-2");
            final Long notificationCompletedNode1 = counterMap.get("PrimaryNodeChangeCompleted-1");
            final Long notificationCompletedNode2 = counterMap.get("PrimaryNodeChangeCompleted-2");

            return notificationCalledNode1 > 0 && notificationCalledNode2 > 0 && notificationCompletedNode1 == null && notificationCompletedNode2 == null;
        });

        // wait 1 second and check again to make sure the ChangeCompleted counter is still not incremented
        Thread.sleep(1000L);

        waitFor(() -> {
            final Map<String, Long> counterMap = getClientUtil().getCountersAsMap(processor.getId());
            final Long notificationCompletedNode1 = counterMap.get("PrimaryNodeChangeCompleted-1");
            final Long notificationCompletedNode2 = counterMap.get("PrimaryNodeChangeCompleted-2");

            return notificationCompletedNode1 == null && notificationCompletedNode2 == null;
        });

        final ProcessorClient processorClient = getNifiClient().getProcessorClient();
        assertTrue(processorClient.getProcessor(processor.getId()).getStatus().getAggregateSnapshot().getActiveThreadCount() > 0);
        processorClient.stopProcessor(processor);

        // Wait a bit and make sure we still see a thread
        Thread.sleep(1000L);
        assertTrue(processorClient.getProcessor(processor.getId()).getStatus().getAggregateSnapshot().getActiveThreadCount() > 0);

        // Terminate the processor
        processorClient.terminateProcessor(processor.getId());

        // Wait for no threads to be active
        waitFor(() -> processorClient.getProcessor(processor.getId()).getStatus().getAggregateSnapshot().getActiveThreadCount() == 0);
    }


    private NodeDTO getNode(final String roleName, final boolean invert) throws InterruptedException, NiFiClientException, IOException {
        while (true) {
            final ClusterEntity clusterEntity = getNifiClient().getControllerClient().getNodes();
            final Optional<NodeDTO> optionalPrimaryNodeDto = clusterEntity.getCluster().getNodes().stream()
                .filter(node -> invert != node.getRoles().contains(roleName))
                .findFirst();

            if (optionalPrimaryNodeDto.isPresent()) {
                return optionalPrimaryNodeDto.get();
            }

            Thread.sleep(100L);
        }
    }
}
