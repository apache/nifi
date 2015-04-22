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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.nifi.io.socket.multicast.MulticastConfiguration;
import org.apache.nifi.io.socket.multicast.MulticastListener;
import org.apache.nifi.logging.NiFiLog;
import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.cluster.protocol.ProtocolContext;
import org.apache.nifi.cluster.protocol.ProtocolException;
import org.apache.nifi.cluster.protocol.ProtocolHandler;
import org.apache.nifi.cluster.protocol.ProtocolListener;
import org.apache.nifi.cluster.protocol.ProtocolMessageMarshaller;
import org.apache.nifi.cluster.protocol.ProtocolMessageUnmarshaller;
import org.apache.nifi.cluster.protocol.message.MulticastProtocolMessage;
import org.apache.nifi.cluster.protocol.message.ProtocolMessage;
import org.apache.nifi.events.BulletinFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements a listener for protocol messages sent over multicast. If a message
 * is of type MulticastProtocolMessage, then the underlying protocol message is
 * passed to the handler. If the receiving handler produces a message response,
 * then the message is wrapped with a MulticastProtocolMessage before being sent
 * to the originator.
 *
 * The client caller is responsible for starting and stopping the listener. The
 * instance must be stopped before termination of the JVM to ensure proper
 * resource clean-up.
 *
 * @author unattributed
 */
public class MulticastProtocolListener extends MulticastListener implements ProtocolListener {

    private static final Logger logger = new NiFiLog(LoggerFactory.getLogger(MulticastProtocolListener.class));

    // immutable members
    private final Collection<ProtocolHandler> handlers = new CopyOnWriteArrayList<>();
    private final String listenerId = UUID.randomUUID().toString();
    private final ProtocolContext<ProtocolMessage> protocolContext;
    private volatile BulletinRepository bulletinRepository;

    public MulticastProtocolListener(
            final int numThreads,
            final InetSocketAddress multicastAddress,
            final MulticastConfiguration configuration,
            final ProtocolContext<ProtocolMessage> protocolContext) {

        super(numThreads, multicastAddress, configuration);

        if (protocolContext == null) {
            throw new IllegalArgumentException("Protocol Context may not be null.");
        }
        this.protocolContext = protocolContext;
    }

    @Override
    public void setBulletinRepository(final BulletinRepository bulletinRepository) {
        this.bulletinRepository = bulletinRepository;
    }

    @Override
    public void start() throws IOException {

        if (super.isRunning()) {
            throw new IllegalStateException("Instance is already started.");
        }

        super.start();

    }

    @Override
    public void stop() throws IOException {

        if (super.isRunning() == false) {
            throw new IllegalStateException("Instance is already stopped.");
        }

        // shutdown listener
        super.stop();

    }

    @Override
    public Collection<ProtocolHandler> getHandlers() {
        return Collections.unmodifiableCollection(handlers);
    }

    @Override
    public void addHandler(final ProtocolHandler handler) {
        if (handler == null) {
            throw new NullPointerException("Protocol handler may not be null.");
        }
        handlers.add(handler);
    }

    @Override
    public boolean removeHandler(final ProtocolHandler handler) {
        return handlers.remove(handler);
    }

    @Override
    public void dispatchRequest(final MulticastSocket multicastSocket, final DatagramPacket packet) {

        try {

            // unmarshall message
            final ProtocolMessageUnmarshaller<ProtocolMessage> unmarshaller = protocolContext.createUnmarshaller();
            final ProtocolMessage request = unmarshaller.unmarshal(new ByteArrayInputStream(packet.getData(), 0, packet.getLength()));

            // unwrap multicast message, if necessary
            final ProtocolMessage unwrappedRequest;
            if (request instanceof MulticastProtocolMessage) {
                final MulticastProtocolMessage multicastRequest = (MulticastProtocolMessage) request;
                // don't process a message we sent
                if (listenerId.equals(multicastRequest.getId())) {
                    return;
                } else {
                    unwrappedRequest = multicastRequest.getProtocolMessage();
                }
            } else {
                unwrappedRequest = request;
            }

            // dispatch message to handler
            ProtocolHandler desiredHandler = null;
            for (final ProtocolHandler handler : getHandlers()) {
                if (handler.canHandle(unwrappedRequest)) {
                    desiredHandler = handler;
                    break;
                }
            }

            // if no handler found, throw exception; otherwise handle request
            if (desiredHandler == null) {
                throw new ProtocolException("No handler assigned to handle message type: " + request.getType());
            } else {
                final ProtocolMessage response = desiredHandler.handle(request);
                if (response != null) {
                    try {

                        // wrap with listener id
                        final MulticastProtocolMessage multicastResponse = new MulticastProtocolMessage(listenerId, response);

                        // marshal message
                        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                        final ProtocolMessageMarshaller<ProtocolMessage> marshaller = protocolContext.createMarshaller();
                        marshaller.marshal(multicastResponse, baos);
                        final byte[] responseBytes = baos.toByteArray();

                        final int maxPacketSizeBytes = getMaxPacketSizeBytes();
                        if (responseBytes.length > maxPacketSizeBytes) {
                            logger.warn("Cluster protocol handler '" + desiredHandler.getClass()
                                    + "' produced a multicast response with length greater than configured max packet size '" + maxPacketSizeBytes + "'");
                        }

                        // create and send packet
                        final DatagramPacket responseDatagram = new DatagramPacket(responseBytes, responseBytes.length, getMulticastAddress().getAddress(), getMulticastAddress().getPort());
                        multicastSocket.send(responseDatagram);

                    } catch (final IOException ioe) {
                        throw new ProtocolException("Failed marshalling protocol message in response to message type: " + request.getType() + " due to: " + ioe, ioe);
                    }
                }
            }

        } catch (final Throwable t) {
            logger.warn("Failed processing protocol message due to " + t, t);

            if (bulletinRepository != null) {
                final Bulletin bulletin = BulletinFactory.createBulletin("Clustering", "WARNING", "Failed to process Protocol Message due to " + t.toString());
                bulletinRepository.addBulletin(bulletin);
            }
        }
    }
}
