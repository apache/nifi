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
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.entity.ClusterSummaryEntity;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies that disconnecting a node via the REST API and then immediately restarting its JVM
 * does not cause the coordinator's pending disconnect retry to disconnect the freshly-rejoined node.
 *
 * Reproduces the scenario described in NIFI-16006: the coordinator enqueues a disconnect notification
 * for the node, but the node's JVM is killed before the notification is delivered. When the node restarts
 * and rejoins, the stale disconnect notification must not be delivered to (or honored by) the new JVM.
 */
public class DisconnectAndRestartIT extends NiFiSystemIT {

    @Override
    public NiFiInstanceFactory getInstanceFactory() {
        return createTwoNodeInstanceFactory();
    }

    @Test
    public void testDisconnectThenImmediateRestartDoesNotDisconnectRejoinedNode() throws NiFiClientException, IOException, InterruptedException {
        final NiFiInstance secondNode = getNiFiInstance().getNodeInstance(2);

        disconnectNode(2);

        secondNode.stop();
        secondNode.start();

        waitForAllNodesConnected();

        // Wait long enough for the coordinator's stale disconnect retry to have been delivered
        // if it were not properly cancelled. The coordinator retries every 5 seconds for up to
        // 10 attempts (50 seconds total), but with the fix, the pending disconnect is cancelled
        // as soon as the Connection Request arrives. Wait 20 seconds to be safe.
        Thread.sleep(20_000L);

        final ClusterSummaryEntity clusterSummary = getNifiClient().getFlowClient().getClusterSummary();
        assertEquals("2 / 2", clusterSummary.getClusterSummary().getConnectedNodes());
    }
}
