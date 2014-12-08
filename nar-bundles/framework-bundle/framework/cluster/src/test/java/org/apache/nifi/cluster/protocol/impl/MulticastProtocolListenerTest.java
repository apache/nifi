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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.util.ArrayList;
import java.util.List;
import org.apache.nifi.cluster.protocol.ProtocolContext;
import org.apache.nifi.cluster.protocol.ProtocolException;
import org.apache.nifi.cluster.protocol.ProtocolHandler;
import org.apache.nifi.cluster.protocol.ProtocolMessageMarshaller;
import org.apache.nifi.cluster.protocol.jaxb.JaxbProtocolContext;
import org.apache.nifi.cluster.protocol.jaxb.message.JaxbProtocolUtils;
import org.apache.nifi.cluster.protocol.message.MulticastProtocolMessage;
import org.apache.nifi.cluster.protocol.message.PingMessage;
import org.apache.nifi.cluster.protocol.message.ProtocolMessage;
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
public class MulticastProtocolListenerTest {

    private MulticastProtocolListener listener;

    private MulticastSocket socket;

    private InetSocketAddress address;

    private MulticastConfiguration configuration;

    private ProtocolContext protocolContext;

    @Before
    public void setup() throws Exception {

        address = new InetSocketAddress("226.1.1.1", 60000);
        configuration = new MulticastConfiguration();

        protocolContext = new JaxbProtocolContext(JaxbProtocolUtils.JAXB_CONTEXT);

        listener = new MulticastProtocolListener(5, address, configuration, protocolContext);
        listener.start();

        socket = MulticastUtils.createMulticastSocket(address.getPort(), configuration);
    }

    @After
    public void teardown() throws IOException {
        try {
            if (listener.isRunning()) {
                listener.stop();
            }
        } finally {
            MulticastUtils.closeQuietly(socket);
        }
    }

    @Test
    public void testBadRequest() throws Exception {
        DelayedProtocolHandler handler = new DelayedProtocolHandler(0);
        listener.addHandler(handler);
        DatagramPacket packet = new DatagramPacket(new byte[]{5}, 1, address);
        socket.send(packet);
        Thread.sleep(250);
        assertEquals(0, handler.getMessages().size());
    }

    @Test
    @Ignore
    public void testRequest() throws Exception {

        ReflexiveProtocolHandler handler = new ReflexiveProtocolHandler();
        listener.addHandler(handler);

        ProtocolMessage msg = new PingMessage();
        MulticastProtocolMessage multicastMsg = new MulticastProtocolMessage("some-id", msg);

        // marshal message to output stream
        ProtocolMessageMarshaller marshaller = protocolContext.createMarshaller();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        marshaller.marshal(multicastMsg, baos);
        byte[] requestPacketBytes = baos.toByteArray();
        DatagramPacket packet = new DatagramPacket(requestPacketBytes, requestPacketBytes.length, address);
        socket.send(packet);

        Thread.sleep(250);
        assertEquals(1, handler.getMessages().size());
        assertEquals(msg.getType(), handler.getMessages().get(0).getType());

    }

    private class ReflexiveProtocolHandler implements ProtocolHandler {

        private List<ProtocolMessage> messages = new ArrayList<>();

        @Override
        public ProtocolMessage handle(ProtocolMessage msg) throws ProtocolException {
            messages.add(msg);
            return msg;
        }

        @Override
        public boolean canHandle(ProtocolMessage msg) {
            return true;
        }

        public List<ProtocolMessage> getMessages() {
            return messages;
        }

    }

    private class DelayedProtocolHandler implements ProtocolHandler {

        private int delay = 0;

        private List<ProtocolMessage> messages = new ArrayList<>();

        public DelayedProtocolHandler(int delay) {
            this.delay = delay;
        }

        @Override
        public ProtocolMessage handle(ProtocolMessage msg) throws ProtocolException {
            try {
                messages.add(msg);
                Thread.sleep(delay);
                return null;
            } catch (final InterruptedException ie) {
                throw new ProtocolException(ie);
            }

        }

        @Override
        public boolean canHandle(ProtocolMessage msg) {
            return true;
        }

        public List<ProtocolMessage> getMessages() {
            return messages;
        }

    }
}
