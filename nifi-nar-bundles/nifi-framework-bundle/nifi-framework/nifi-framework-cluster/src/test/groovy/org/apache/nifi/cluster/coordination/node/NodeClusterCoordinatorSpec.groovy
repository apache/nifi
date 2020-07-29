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
package org.apache.nifi.cluster.coordination.node

import org.apache.nifi.cluster.coordination.flow.FlowElection
import org.apache.nifi.cluster.firewall.ClusterNodeFirewall
import org.apache.nifi.cluster.protocol.NodeIdentifier
import org.apache.nifi.cluster.protocol.NodeProtocolSender
import org.apache.nifi.cluster.protocol.impl.ClusterCoordinationProtocolSenderListener
import org.apache.nifi.cluster.protocol.message.OffloadMessage
import org.apache.nifi.components.state.Scope
import org.apache.nifi.components.state.StateManager
import org.apache.nifi.components.state.StateManagerProvider
import org.apache.nifi.controller.leader.election.LeaderElectionManager
import org.apache.nifi.events.EventReporter
import org.apache.nifi.reporting.Severity
import org.apache.nifi.state.MockStateMap
import org.apache.nifi.util.NiFiProperties
import org.apache.nifi.web.revision.RevisionManager
import spock.lang.Specification
import spock.util.concurrent.BlockingVariable

import java.util.concurrent.TimeUnit

class NodeClusterCoordinatorSpec extends Specification {
    def "requestNodeOffload"() {
        given: 'mocked collaborators'
        def clusterCoordinationProtocolSenderListener = Mock(ClusterCoordinationProtocolSenderListener)
        def eventReporter = Mock EventReporter
        def stateManager = Mock StateManager
        def stateMap = new MockStateMap([:], 1)
        stateManager.getState(_ as Scope) >> stateMap
        def stateManagerProvider = Mock StateManagerProvider
        stateManagerProvider.getStateManager(_ as String) >> stateManager

        and: 'a NodeClusterCoordinator that manages node status in a synchronized list'
        List<NodeConnectionStatus> nodeStatuses = [].asSynchronized()
        def clusterCoordinator = new NodeClusterCoordinator(clusterCoordinationProtocolSenderListener, eventReporter, Mock(LeaderElectionManager),
                Mock(FlowElection), Mock(ClusterNodeFirewall),
                Mock(RevisionManager), NiFiProperties.createBasicNiFiProperties('src/test/resources/conf/nifi.properties', [:]),
                Mock(NodeProtocolSender), stateManagerProvider) {
            @Override
            void notifyOthersOfNodeStatusChange(NodeConnectionStatus updatedStatus, boolean notifyAllNodes, boolean waitForCoordinator) {
                nodeStatuses.add(updatedStatus)
            }
        }

        and: 'two nodes'
        def nodeIdentifier1 = createNodeIdentifier 1
        def nodeIdentifier2 = createNodeIdentifier 2

        and: 'node 1 is connected, node 2 is disconnected'
        clusterCoordinator.updateNodeStatus new NodeConnectionStatus(nodeIdentifier1, NodeConnectionState.CONNECTED)
        clusterCoordinator.updateNodeStatus new NodeConnectionStatus(nodeIdentifier2, NodeConnectionState.DISCONNECTED)
        while (nodeStatuses.size() < 2) {
            Thread.sleep(10)
        }
        nodeStatuses.clear()

        def waitForReportEvent = new BlockingVariable(5, TimeUnit.SECONDS)

        when: 'a node is requested to offload'
        clusterCoordinator.requestNodeOffload nodeIdentifier2, OffloadCode.OFFLOADED, 'unit test for offloading node'
        waitForReportEvent.get()

        then: 'no exceptions are thrown'
        noExceptionThrown()

        and: 'expected methods on collaborators are invoked'
        1 * clusterCoordinationProtocolSenderListener.offload({ OffloadMessage msg -> msg.nodeId == nodeIdentifier2 } as OffloadMessage)
        1 * eventReporter.reportEvent(Severity.INFO, 'Clustering', { msg -> msg.contains "$nodeIdentifier2.apiAddress:$nodeIdentifier2.apiPort" } as String) >> {
            waitForReportEvent.set(it)
        }

        and: 'the status of the offloaded node is known by the cluster coordinator to be offloading'
        nodeStatuses[0].nodeIdentifier == nodeIdentifier2
        nodeStatuses[0].state == NodeConnectionState.OFFLOADING
    }

    private static NodeIdentifier createNodeIdentifier(final int index) {
        new NodeIdentifier("node-id-$index", "localhost", 8000 + index, "localhost", 9000 + index,
                "localhost", 10000 + index, 11000 + index, false)
    }

}
