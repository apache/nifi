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

//package nifi.remote;
//
//import static org.junit.Assert.assertEquals;
//
//import java.io.IOException;
//import java.util.LinkedHashMap;
//import java.util.List;
//import java.util.Map;
//
//import nifi.cluster.NodeInformation;
//import nifi.remote.StandardSiteToSiteProtocol.Destination;
//
//import org.junit.Assert;
//import org.junit.Test;
//import org.mockito.Mockito;
//
//public class TestStandardSiteToSiteProtocol {
//
//    @Test
//    public void testWeightedDistributionWithTwoNodes() throws IOException {
//        final Map<NodeInformation, Destination> destinationMap = new LinkedHashMap<>();
//        final NodeInformation node1 = new NodeInformation("hostA", 80, 90, true, 3);
//        final NodeInformation node2 = new NodeInformation("hostB", 80, 90, true, 500);
//
//        final Destination node1Destination = new Destination(createRemoteGroupPort("PortA"), null, node1, TransferDirection.SEND, true, null);
//        final Destination node2Destination = new Destination(createRemoteGroupPort("PortB"), null, node2, TransferDirection.SEND, true, null);
//
//        destinationMap.put(node1, node1Destination);
//        destinationMap.put(node2, node2Destination);
//
//        final List<Destination> destinations = StandardSiteToSiteProtocol.formulateDestinationList(destinationMap, TransferDirection.SEND);
//        int node1Count = 0, node2Count = 0;
//        for ( final Destination destination : destinations ) {
//            if ( destination.getNodeInformation() == node1 ) {
//                node1Count++;
//            } else if ( destination.getNodeInformation() == node2 ) {
//                node2Count++;
//            } else {
//                Assert.fail("Got Destination for unknkown NodeInformation");
//            }
//        }
//
//        System.out.println(node1Count);
//        System.out.println(node2Count);
//
//        final double node1Pct = (double) node1Count / (double) (node1Count + node2Count);
//        assertEquals(0.80, node1Pct, 0.01);
//        // node1  should get the most but is not allowed to have more than approximately 80% of the data.
//    }
//
//    @Test
//    public void testWeightedDistributionWithThreeNodes() throws IOException {
//        final Map<NodeInformation, Destination> destinationMap = new LinkedHashMap<>();
//        final NodeInformation node1 = new NodeInformation("hostA", 80, 90, true, 3);
//        final NodeInformation node2 = new NodeInformation("hostB", 80, 90, true, 500);
//        final NodeInformation node3 = new NodeInformation("hostC", 80, 90, true, 500);
//
//        final Destination node1Destination = new Destination(createRemoteGroupPort("PortA"), null, node1, TransferDirection.SEND, true, null);
//        final Destination node2Destination = new Destination(createRemoteGroupPort("PortB"), null, node2, TransferDirection.SEND, true, null);
//        final Destination node3Destination = new Destination(createRemoteGroupPort("PortC"), null, node3, TransferDirection.SEND, true, null);
//
//        destinationMap.put(node1, node1Destination);
//        destinationMap.put(node2, node2Destination);
//        destinationMap.put(node3, node3Destination);
//
//        final List<Destination> destinations = StandardSiteToSiteProtocol.formulateDestinationList(destinationMap, TransferDirection.SEND);
//        int node1Count = 0, node2Count = 0, node3Count = 0;
//        for ( final Destination destination : destinations ) {
//            if ( destination.getNodeInformation() == node1 ) {
//                node1Count++;
//            } else if ( destination.getNodeInformation() == node2 ) {
//                node2Count++;
//            } else if ( destination.getNodeInformation() == node3 ) {
//                node3Count++;
//            } else {
//                Assert.fail("Got Destination for unknkown NodeInformation");
//            }
//        }
//
//        System.out.println(node1Count);
//        System.out.println(node2Count);
//        System.out.println(node3Count);
//
//        final double node1Pct = (double) node1Count / (double) (node1Count + node2Count + node3Count);
//        final double node2Pct = (double) node2Count / (double) (node1Count + node2Count + node3Count);
//        final double node3Pct = (double) node3Count / (double) (node1Count + node2Count + node3Count);
//
//        assertEquals(0.5, node1Pct, 0.02);
//        assertEquals(0.25, node2Pct, 0.02);
//        assertEquals(node2Pct, node3Pct, 0.02);
//    }
//
//    private RemoteGroupPort createRemoteGroupPort(final String portName) {
//        RemoteGroupPort port = Mockito.mock(RemoteGroupPort.class);
//        Mockito.when(port.getName()).thenReturn(portName);
//        return port;
//    }
//}
