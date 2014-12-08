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
package org.apache.nifi.remote;

import org.apache.nifi.remote.StandardRemoteGroupPort;
import org.apache.nifi.remote.PeerStatus;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.nifi.cluster.ClusterNodeInformation;
import org.apache.nifi.cluster.NodeInformation;
import org.apache.nifi.connectable.ConnectableType;

import org.junit.Test;

public class TestStandardRemoteGroupPort {

    @Test
    public void testFormulateDestinationListForOutput() throws IOException {
        final ClusterNodeInformation clusterNodeInfo = new ClusterNodeInformation();
        final List<NodeInformation> collection = new ArrayList<>();
        collection.add(new NodeInformation("ShouldGetMedium", 1, 1111, true, 4096));
        collection.add(new NodeInformation("ShouldGetLots", 2, 2222, true, 10240));
        collection.add(new NodeInformation("ShouldGetLittle", 3, 3333, true, 1024));
        collection.add(new NodeInformation("ShouldGetMedium", 4, 4444, true, 4096));
        collection.add(new NodeInformation("ShouldGetMedium", 5, 5555, true, 4096));

        clusterNodeInfo.setNodeInformation(collection);
        final List<PeerStatus> destinations = StandardRemoteGroupPort.formulateDestinationList(clusterNodeInfo, ConnectableType.REMOTE_OUTPUT_PORT);
        for ( final PeerStatus peerStatus : destinations ) {
            System.out.println(peerStatus.getHostname() + ":" + peerStatus.getPort());
        }
    }
    
    @Test
    public void testFormulateDestinationListForOutputHugeDifference() throws IOException {
        final ClusterNodeInformation clusterNodeInfo = new ClusterNodeInformation();
        final List<NodeInformation> collection = new ArrayList<>();
        collection.add(new NodeInformation("ShouldGetLittle", 1, 1111, true, 500));
        collection.add(new NodeInformation("ShouldGetLots", 2, 2222, true, 50000));

        clusterNodeInfo.setNodeInformation(collection);
        final List<PeerStatus> destinations = StandardRemoteGroupPort.formulateDestinationList(clusterNodeInfo, ConnectableType.REMOTE_OUTPUT_PORT);
        for ( final PeerStatus peerStatus : destinations ) {
            System.out.println(peerStatus.getHostname() + ":" + peerStatus.getPort());
        }
    }
    
    
    
    
    @Test
    public void testFormulateDestinationListForInputPorts() throws IOException {
        final ClusterNodeInformation clusterNodeInfo = new ClusterNodeInformation();
        final List<NodeInformation> collection = new ArrayList<>();
        collection.add(new NodeInformation("ShouldGetMedium", 1, 1111, true, 4096));
        collection.add(new NodeInformation("ShouldGetLittle", 2, 2222, true, 10240));
        collection.add(new NodeInformation("ShouldGetLots", 3, 3333, true, 1024));
        collection.add(new NodeInformation("ShouldGetMedium", 4, 4444, true, 4096));
        collection.add(new NodeInformation("ShouldGetMedium", 5, 5555, true, 4096));

        clusterNodeInfo.setNodeInformation(collection);
        final List<PeerStatus> destinations = StandardRemoteGroupPort.formulateDestinationList(clusterNodeInfo, ConnectableType.REMOTE_INPUT_PORT);
        for ( final PeerStatus peerStatus : destinations ) {
            System.out.println(peerStatus.getHostname() + ":" + peerStatus.getPort());
        }
    }
    
    @Test
    public void testFormulateDestinationListForInputPortsHugeDifference() throws IOException {
        final ClusterNodeInformation clusterNodeInfo = new ClusterNodeInformation();
        final List<NodeInformation> collection = new ArrayList<>();
        collection.add(new NodeInformation("ShouldGetLots", 1, 1111, true, 500));
        collection.add(new NodeInformation("ShouldGetLittle", 2, 2222, true, 50000));

        clusterNodeInfo.setNodeInformation(collection);
        final List<PeerStatus> destinations = StandardRemoteGroupPort.formulateDestinationList(clusterNodeInfo, ConnectableType.REMOTE_INPUT_PORT);
        for ( final PeerStatus peerStatus : destinations ) {
            System.out.println(peerStatus.getHostname() + ":" + peerStatus.getPort());
        }
    }
}
