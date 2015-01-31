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
package org.apache.nifi.cluster.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.nifi.cluster.protocol.ProtocolContext;
import org.apache.nifi.cluster.protocol.ProtocolException;
import org.apache.nifi.cluster.protocol.ProtocolHandler;
import org.apache.nifi.cluster.protocol.ProtocolMessageMarshaller;
import org.apache.nifi.cluster.protocol.impl.MulticastProtocolListener;
import org.apache.nifi.cluster.protocol.jaxb.JaxbProtocolContext;
import org.apache.nifi.cluster.protocol.jaxb.message.JaxbProtocolUtils;
import org.apache.nifi.cluster.protocol.message.PingMessage;
import org.apache.nifi.cluster.protocol.message.ProtocolMessage;
import org.apache.nifi.io.socket.multicast.MulticastConfiguration;
import org.apache.nifi.io.socket.multicast.MulticastUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple multicast test client that sends ping messages to a group address.
 */
public class MulticastTestClient {

    private static final Logger logger = LoggerFactory.getLogger(MulticastTestClient.class);

    private static final int PING_DELAY_SECONDS = 3;

    public static void main(final String... args) throws IOException {

        String group = System.getProperty("group", "225.0.0.0");
        if (group == null) {
            System.out.println("Host system property 'group' was not given.");
            return;
        }
        group = group.trim();
        if (group.length() == 0) {
            System.out.println("Host system property 'group' must be non-empty.");
            return;
        }

        final String portStr = System.getProperty("port", "2222");
        final int port;
        try {
            port = Integer.parseInt(portStr);
        } catch (final NumberFormatException nfe) {
            System.out.println("Port system property 'port' was not a valid port.");
            return;
        }

        logger.info(String.format("Pinging every %s seconds using multicast address: %s:%s.", PING_DELAY_SECONDS, group, port));
        logger.info("Override defaults by using system properties '-Dgroup=<Class D IP>' and '-Dport=<unused port>'.");
        logger.info("The test client may be stopped by entering a newline at the command line.");

        final InetSocketAddress addr = new InetSocketAddress(group, port);
        final ProtocolContext<ProtocolMessage> protocolContext = new JaxbProtocolContext<ProtocolMessage>(JaxbProtocolUtils.JAXB_CONTEXT);
        final MulticastConfiguration multicastConfig = new MulticastConfiguration();
        multicastConfig.setReuseAddress(true);

        // setup listener
        final MulticastProtocolListener listener = new MulticastProtocolListener(1, addr, multicastConfig, protocolContext);
        listener.addHandler(new ProtocolHandler() {
            @Override
            public ProtocolMessage handle(ProtocolMessage msg) throws ProtocolException {
                final PingMessage pingMsg = (PingMessage) msg;
                final SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss", Locale.US);
                logger.info("Pinged at: " + sdf.format(pingMsg.getDate()));
                return null;
            }

            @Override
            public boolean canHandle(ProtocolMessage msg) {
                return true;
            }
        });

        // setup socket
        final MulticastSocket multicastSocket = MulticastUtils.createMulticastSocket(multicastConfig);

        // setup broadcaster
        final Timer broadcaster = new Timer("Multicast Test Client", /**
                 * is daemon *
                 */
                true);

        try {

            // start listening
            listener.start();

            // start broadcasting
            broadcaster.schedule(new TimerTask() {

                @Override
                public void run() {
                    try {

                        final PingMessage msg = new PingMessage();
                        msg.setDate(new Date());

                        // marshal message to output stream
                        final ProtocolMessageMarshaller<ProtocolMessage> marshaller = protocolContext.createMarshaller();
                        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                        marshaller.marshal(msg, baos);
                        final byte[] packetBytes = baos.toByteArray();

                        // send message
                        final DatagramPacket packet = new DatagramPacket(packetBytes, packetBytes.length, addr);
                        multicastSocket.send(packet);

                    } catch (final Exception ex) {
                        logger.warn("Failed to send message due to: " + ex, ex);
                    }
                }
            }, 0, PING_DELAY_SECONDS * 1000);

            // block until any input is received
            System.in.read();

        } finally {
            broadcaster.cancel();
            if (listener.isRunning()) {
                listener.stop();
            }
        }
    }
}
