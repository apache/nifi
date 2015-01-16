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

import java.io.IOException;
import java.net.InetAddress;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.cluster.protocol.ProtocolContext;
import org.apache.nifi.cluster.protocol.ProtocolException;
import org.apache.nifi.cluster.protocol.ProtocolHandler;
import org.apache.nifi.cluster.protocol.jaxb.JaxbProtocolContext;
import org.apache.nifi.cluster.protocol.jaxb.message.JaxbProtocolUtils;
import org.apache.nifi.cluster.protocol.message.FlowRequestMessage;
import org.apache.nifi.cluster.protocol.message.FlowResponseMessage;
import org.apache.nifi.cluster.protocol.message.PingMessage;
import org.apache.nifi.cluster.protocol.message.ProtocolMessage;
import org.apache.nifi.io.socket.ServerSocketConfiguration;
import org.apache.nifi.io.socket.SocketConfiguration;
import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * @author unattributed
 */
public class ClusterManagerProtocolSenderImplTest {

    private InetAddress address;

    private int port;

    private SocketProtocolListener listener;

    private ClusterManagerProtocolSenderImpl sender;

    private ProtocolHandler mockHandler;

    @Before
    public void setup() throws IOException {

        address = InetAddress.getLocalHost();
        ServerSocketConfiguration serverSocketConfiguration = new ServerSocketConfiguration();

        mockHandler = mock(ProtocolHandler.class);

        ProtocolContext protocolContext = new JaxbProtocolContext(JaxbProtocolUtils.JAXB_CONTEXT);

        listener = new SocketProtocolListener(5, 0, serverSocketConfiguration, protocolContext);
        listener.addHandler(mockHandler);
        listener.start();

        port = listener.getPort();

        SocketConfiguration socketConfiguration = new SocketConfiguration();
        sender = new ClusterManagerProtocolSenderImpl(socketConfiguration, protocolContext);
    }

    @After
    public void teardown() throws IOException {
        if (listener.isRunning()) {
            listener.stop();
        }
    }

    @Test
    public void testRequestFlow() throws Exception {

        when(mockHandler.canHandle(any(ProtocolMessage.class))).thenReturn(Boolean.TRUE);
        when(mockHandler.handle(any(ProtocolMessage.class))).thenReturn(new FlowResponseMessage());
        FlowRequestMessage request = new FlowRequestMessage();
        request.setNodeId(new NodeIdentifier("id", "api-address", 1, address.getHostAddress(), port));
        FlowResponseMessage response = sender.requestFlow(request);
        assertNotNull(response);
    }

    @Test
    public void testRequestFlowWithBadResponseMessage() throws Exception {

        when(mockHandler.canHandle(any(ProtocolMessage.class))).thenReturn(Boolean.TRUE);
        when(mockHandler.handle(any(ProtocolMessage.class))).thenReturn(new PingMessage());
        FlowRequestMessage request = new FlowRequestMessage();
        request.setNodeId(new NodeIdentifier("id", "api-address", 1, address.getHostAddress(), port));
        try {
            sender.requestFlow(request);
            fail("failed to throw exception");
        } catch (ProtocolException pe) {
        }

    }

    @Test
    public void testRequestFlowDelayedResponse() throws Exception {

        final int time = 250;
        sender.getSocketConfiguration().setSocketTimeout(time);

        when(mockHandler.canHandle(any(ProtocolMessage.class))).thenReturn(Boolean.TRUE);
        when(mockHandler.handle(any(ProtocolMessage.class))).thenAnswer(new Answer<FlowResponseMessage>() {
            @Override
            public FlowResponseMessage answer(InvocationOnMock invocation) throws Throwable {
                Thread.sleep(time * 3);
                return new FlowResponseMessage();
            }
        });
        FlowRequestMessage request = new FlowRequestMessage();
        request.setNodeId(new NodeIdentifier("id", "api-address", 1, address.getHostAddress(), port));
        try {
            sender.requestFlow(request);
            fail("failed to throw exception");
        } catch (ProtocolException pe) {
        }
    }

}
