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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.UUID;

import org.apache.nifi.cluster.HeartbeatPayload;
import org.apache.nifi.cluster.protocol.ConnectionRequest;
import org.apache.nifi.cluster.protocol.ConnectionResponse;
import org.apache.nifi.cluster.protocol.Heartbeat;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.cluster.protocol.ProtocolContext;
import org.apache.nifi.cluster.protocol.ProtocolException;
import org.apache.nifi.cluster.protocol.ProtocolHandler;
import org.apache.nifi.cluster.protocol.StandardDataFlow;
import org.apache.nifi.cluster.protocol.UnknownServiceAddressException;
import org.apache.nifi.cluster.protocol.jaxb.JaxbProtocolContext;
import org.apache.nifi.cluster.protocol.jaxb.message.JaxbProtocolUtils;
import org.apache.nifi.cluster.protocol.message.ConnectionRequestMessage;
import org.apache.nifi.cluster.protocol.message.ConnectionResponseMessage;
import org.apache.nifi.cluster.protocol.message.ControllerStartupFailureMessage;
import org.apache.nifi.cluster.protocol.message.HeartbeatMessage;
import org.apache.nifi.cluster.protocol.message.PingMessage;
import org.apache.nifi.cluster.protocol.message.ProtocolMessage;
import org.apache.nifi.io.socket.ServerSocketConfiguration;
import org.apache.nifi.io.socket.SocketConfiguration;
import org.apache.nifi.io.socket.multicast.DiscoverableService;
import org.apache.nifi.io.socket.multicast.DiscoverableServiceImpl;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * @author unattributed
 */
public class NodeProtocolSenderImplTest {

    private SocketProtocolListener listener;

    private NodeProtocolSenderImpl sender;

    private DiscoverableService service;

    private ServerSocketConfiguration serverSocketConfiguration;

    private ClusterServiceLocator mockServiceLocator;

    private ProtocolHandler mockHandler;

    private NodeIdentifier nodeIdentifier;

    @Before
    public void setup() throws IOException {

        serverSocketConfiguration = new ServerSocketConfiguration();

        mockServiceLocator = mock(ClusterServiceLocator.class);
        mockHandler = mock(ProtocolHandler.class);

        nodeIdentifier = new NodeIdentifier("1", "localhost", 1234, "localhost", 5678);

        ProtocolContext protocolContext = new JaxbProtocolContext(JaxbProtocolUtils.JAXB_CONTEXT);

        listener = new SocketProtocolListener(5, 0, serverSocketConfiguration, protocolContext);
        listener.setShutdownListenerSeconds(3);
        listener.addHandler(mockHandler);
        listener.start();

        service = new DiscoverableServiceImpl("some-service", new InetSocketAddress("localhost", listener.getPort()));

        SocketConfiguration socketConfiguration = new SocketConfiguration();
        socketConfiguration.setReuseAddress(true);
        sender = new NodeProtocolSenderImpl(mockServiceLocator, socketConfiguration, protocolContext);
    }

    @After
    public void teardown() throws IOException {
        if (listener.isRunning()) {
            listener.stop();
        }
    }

    @Test
    public void testConnect() throws Exception {

        when(mockServiceLocator.getService()).thenReturn(service);
        when(mockHandler.canHandle(any(ProtocolMessage.class))).thenReturn(Boolean.TRUE);
        ConnectionResponseMessage mockMessage = new ConnectionResponseMessage();
        mockMessage.setConnectionResponse(new ConnectionResponse(nodeIdentifier, new StandardDataFlow("flow".getBytes("UTF-8"), new byte[0], new byte[0]), false, null, null, UUID.randomUUID().toString()));
        when(mockHandler.handle(any(ProtocolMessage.class))).thenReturn(mockMessage);

        ConnectionRequestMessage request = new ConnectionRequestMessage();
        request.setConnectionRequest(new ConnectionRequest(nodeIdentifier));
        ConnectionResponseMessage response = sender.requestConnection(request);
        assertNotNull(response);
    }

    @Test(expected = UnknownServiceAddressException.class)
    public void testConnectNoClusterManagerAddress() throws Exception {

        when(mockServiceLocator.getService()).thenReturn(null);
        when(mockHandler.canHandle(any(ProtocolMessage.class))).thenReturn(Boolean.TRUE);
        when(mockHandler.handle(any(ProtocolMessage.class))).thenReturn(new ConnectionResponseMessage());

        ConnectionRequestMessage request = new ConnectionRequestMessage();
        request.setConnectionRequest(new ConnectionRequest(nodeIdentifier));

        sender.requestConnection(request);
        fail("failed to throw exception");
    }

    @Test(expected = ProtocolException.class)
    public void testConnectBadResponse() throws Exception {

        when(mockServiceLocator.getService()).thenReturn(service);
        when(mockHandler.canHandle(any(ProtocolMessage.class))).thenReturn(Boolean.TRUE);
        when(mockHandler.handle(any(ProtocolMessage.class))).thenReturn(new PingMessage());

        ConnectionRequestMessage request = new ConnectionRequestMessage();
        request.setConnectionRequest(new ConnectionRequest(nodeIdentifier));

        sender.requestConnection(request);
        fail("failed to throw exception");

    }

    @Test(expected = ProtocolException.class)
    public void testConnectDelayedResponse() throws Exception {

        final int time = 250;
        sender.getSocketConfiguration().setSocketTimeout(time);
        when(mockServiceLocator.getService()).thenReturn(service);
        when(mockHandler.canHandle(any(ProtocolMessage.class))).thenReturn(Boolean.TRUE);
        when(mockHandler.handle(any(ProtocolMessage.class))).thenAnswer(new Answer<ConnectionResponseMessage>() {
            @Override
            public ConnectionResponseMessage answer(InvocationOnMock invocation) throws Throwable {
                Thread.sleep(time * 3);
                return new ConnectionResponseMessage();
            }
        });
        ConnectionRequestMessage request = new ConnectionRequestMessage();
        request.setConnectionRequest(new ConnectionRequest(nodeIdentifier));

        sender.requestConnection(request);
        fail("failed to throw exception");

    }

    @Test
    public void testHeartbeat() throws Exception {

        when(mockServiceLocator.getService()).thenReturn(service);
        when(mockHandler.canHandle(any(ProtocolMessage.class))).thenReturn(Boolean.TRUE);
        when(mockHandler.handle(any(ProtocolMessage.class))).thenReturn(null);

        HeartbeatMessage msg = new HeartbeatMessage();
        HeartbeatPayload hbPayload = new HeartbeatPayload();
        Heartbeat hb = new Heartbeat(new NodeIdentifier("id", "localhost", 3, "localhost", 4), false, false, hbPayload.marshal());
        msg.setHeartbeat(hb);
        sender.heartbeat(msg);
    }

    @Test
    public void testNotifyControllerStartupFailure() throws Exception {

        when(mockServiceLocator.getService()).thenReturn(service);
        when(mockHandler.canHandle(any(ProtocolMessage.class))).thenReturn(Boolean.TRUE);
        when(mockHandler.handle(any(ProtocolMessage.class))).thenReturn(null);

        ControllerStartupFailureMessage msg = new ControllerStartupFailureMessage();
        msg.setNodeId(new NodeIdentifier("some-id", "some-addr", 1, "some-addr", 1));
        msg.setExceptionMessage("some exception");
        sender.notifyControllerStartupFailure(msg);
    }

}
