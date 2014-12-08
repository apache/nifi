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
package org.apache.nifi.cluster.protocol.impl;

import org.apache.nifi.cluster.protocol.impl.ClusterServiceDiscovery;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import org.apache.nifi.cluster.protocol.ProtocolContext;
import org.apache.nifi.cluster.protocol.ProtocolMessageMarshaller;
import org.apache.nifi.cluster.protocol.jaxb.JaxbProtocolContext;
import org.apache.nifi.cluster.protocol.jaxb.message.JaxbProtocolUtils;
import org.apache.nifi.cluster.protocol.message.PingMessage;
import org.apache.nifi.cluster.protocol.message.ProtocolMessage;
import org.apache.nifi.cluster.protocol.message.ServiceBroadcastMessage;
import org.apache.nifi.io.socket.multicast.MulticastConfiguration;
import org.apache.nifi.io.socket.multicast.MulticastUtils;
import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author unattributed
 */
public class ClusterServiceDiscoveryTest {
    
    private ClusterServiceDiscovery discovery;
    
    private String serviceName;
    
    private MulticastSocket socket;
    
    private InetSocketAddress multicastAddress;
    
    private MulticastConfiguration configuration;
    
    private ProtocolContext protocolContext;
    
    @Before
    public void setup() throws Exception {

        serviceName = "some-service";
        multicastAddress = new InetSocketAddress("225.1.1.1", 22222);
        configuration = new MulticastConfiguration();
        
        protocolContext = new JaxbProtocolContext(JaxbProtocolUtils.JAXB_CONTEXT);
        
        discovery = new ClusterServiceDiscovery(serviceName, multicastAddress, configuration, protocolContext);
        discovery.start();

        socket = MulticastUtils.createMulticastSocket(multicastAddress.getPort(), configuration);
    }
    
    @After
    public void teardown() throws IOException {
        try {
            if(discovery.isRunning()) {
                discovery.stop();
            }
        } finally {
            MulticastUtils.closeQuietly(socket);
        }
    }
    
    @Test
    public void testGetAddressOnStartup() {
        assertNull(discovery.getService());
    }   
            
    @Ignore("This test has an NPE after ignoring another...perhaps has a bad inter-test dependency")
    @Test
    public void testGetAddressAfterBroadcast() throws Exception {
        
        ServiceBroadcastMessage msg = new ServiceBroadcastMessage();
        msg.setServiceName("some-service");
        msg.setAddress("3.3.3.3");
        msg.setPort(1234);
        
        // marshal message to output stream
        ProtocolMessageMarshaller marshaller = protocolContext.createMarshaller();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        marshaller.marshal(msg, baos);
        byte[] requestPacketBytes = baos.toByteArray();
        DatagramPacket packet = new DatagramPacket(requestPacketBytes, requestPacketBytes.length, multicastAddress);
        socket.send(packet);
        
        Thread.sleep(250);
       
        InetSocketAddress updatedAddress = discovery.getService().getServiceAddress();
        assertEquals("some-service", discovery.getServiceName());
        assertEquals("3.3.3.3", updatedAddress.getHostName());
        assertEquals(1234, updatedAddress.getPort());
        
    }
    
    @Test
    public void testBadBroadcastMessage() throws Exception {
        
        ProtocolMessage msg = new PingMessage();
        
        // marshal message to output stream
        ProtocolMessageMarshaller marshaller = protocolContext.createMarshaller();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        marshaller.marshal(msg, baos);
        byte[] requestPacketBytes = baos.toByteArray();
        DatagramPacket packet = new DatagramPacket(requestPacketBytes, requestPacketBytes.length, multicastAddress);
        socket.send(packet);
        
        Thread.sleep(250);
       
        assertNull(discovery.getService());
        
    }
    
}
