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

import org.apache.nifi.cluster.protocol.impl.ClusterServicesBroadcaster;
import org.apache.nifi.cluster.protocol.impl.MulticastProtocolListener;
import java.net.InetSocketAddress;
import org.apache.nifi.cluster.protocol.ProtocolContext;
import org.apache.nifi.cluster.protocol.ProtocolException;
import org.apache.nifi.cluster.protocol.ProtocolHandler;
import org.apache.nifi.cluster.protocol.jaxb.JaxbProtocolContext;
import org.apache.nifi.cluster.protocol.jaxb.message.JaxbProtocolUtils;
import org.apache.nifi.cluster.protocol.message.ProtocolMessage;
import org.apache.nifi.cluster.protocol.message.ServiceBroadcastMessage;
import org.apache.nifi.io.socket.multicast.DiscoverableService;
import org.apache.nifi.io.socket.multicast.DiscoverableServiceImpl;
import org.apache.nifi.io.socket.multicast.MulticastConfiguration;
import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author unattributed
 */
public class ClusterServicesBroadcasterTest {
    
    private ClusterServicesBroadcaster broadcaster;
    
    private MulticastProtocolListener listener;
    
    private DummyProtocolHandler handler;
    
    private InetSocketAddress multicastAddress;
    
    private DiscoverableService broadcastedService;

    private ProtocolContext protocolContext;
    
    private MulticastConfiguration configuration;
    
    @Before
    public void setup() throws Exception {

        broadcastedService = new DiscoverableServiceImpl("some-service", new InetSocketAddress("localhost", 11111));
        
        multicastAddress = new InetSocketAddress("225.1.1.1", 22222);
        
        configuration = new MulticastConfiguration();
        
        protocolContext = new JaxbProtocolContext(JaxbProtocolUtils.JAXB_CONTEXT);
        
        broadcaster = new ClusterServicesBroadcaster(multicastAddress, configuration, protocolContext, "500 ms");
        broadcaster.addService(broadcastedService);
        
        handler = new DummyProtocolHandler();
        listener = new MulticastProtocolListener(5, multicastAddress, configuration, protocolContext);
        listener.addHandler(handler);
    }
    
    @After
    public void teardown() {
        
        if(broadcaster.isRunning()) {
            broadcaster.stop();
        }
        
        try {
            if(listener.isRunning()) {
                listener.stop();
            }
        } catch(Exception ex) {
            ex.printStackTrace(System.out);
        }
        
    }
    
    @Ignore("fails needs to be fixed")
    @Test
    public void testBroadcastReceived() throws Exception {
        
        broadcaster.start();
        listener.start();
        
        Thread.sleep(1000);
        
        listener.stop();
        
        assertNotNull(handler.getProtocolMessage());
        assertEquals(ProtocolMessage.MessageType.SERVICE_BROADCAST, handler.getProtocolMessage().getType());
        final ServiceBroadcastMessage msg = (ServiceBroadcastMessage) handler.getProtocolMessage();
        assertEquals(broadcastedService.getServiceName(), msg.getServiceName());
        assertEquals(broadcastedService.getServiceAddress().getHostName(), msg.getAddress());
        assertEquals(broadcastedService.getServiceAddress().getPort(), msg.getPort());
    }
    
    private class DummyProtocolHandler implements ProtocolHandler {

        private ProtocolMessage protocolMessage;
        
        @Override
        public boolean canHandle(ProtocolMessage msg) {
            return true;
        }

        @Override
        public ProtocolMessage handle(ProtocolMessage msg) throws ProtocolException {
            this.protocolMessage = msg;
            return null;
        }
        
        public ProtocolMessage getProtocolMessage() {
            return protocolMessage;
        }
        
    }
    
}
