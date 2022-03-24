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
package org.apache.nifi.cluster.integration

import org.apache.nifi.cluster.coordination.node.DisconnectionCode
import org.apache.nifi.cluster.coordination.node.OffloadCode
import spock.lang.Specification

import java.util.concurrent.TimeUnit

class OffloadNodeITSpec extends Specification {
    def "requestNodeOffload"() {
        given: 'a cluster with 3 nodes'
        System.setProperty 'nifi.properties.file.path', 'src/test/resources/conf/nifi.properties'
        def cluster = new Cluster()
        cluster.start()
        cluster.createNode()
        def nodeToOffload = cluster.createNode()
        cluster.createNode()
        cluster.waitUntilAllNodesConnected 20, TimeUnit.SECONDS

        when: 'the node to offload is disconnected successfully'
        cluster.currentClusterCoordinator.clusterCoordinator.requestNodeDisconnect nodeToOffload.identifier, DisconnectionCode.USER_DISCONNECTED,
                'integration test user disconnect'
        cluster.currentClusterCoordinator.assertNodeDisconnects nodeToOffload.identifier, 10, TimeUnit.SECONDS

        and: 'the node to offload is requested to offload'
        nodeToOffload.getClusterCoordinator().requestNodeOffload nodeToOffload.identifier, OffloadCode.OFFLOADED, 'integration test offload'

        then: 'the node has been successfully offloaded'
        cluster.currentClusterCoordinator.assertNodeIsOffloaded nodeToOffload.identifier, 10, TimeUnit.SECONDS

        cleanup:
        cluster.stop()
    }
}
