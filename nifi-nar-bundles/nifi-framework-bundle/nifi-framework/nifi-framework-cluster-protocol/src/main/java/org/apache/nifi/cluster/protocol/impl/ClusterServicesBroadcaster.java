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
import java.util.Collections;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.io.socket.multicast.DiscoverableService;
import org.apache.nifi.io.socket.multicast.MulticastConfiguration;
import org.apache.nifi.io.socket.multicast.MulticastServicesBroadcaster;
import org.apache.nifi.io.socket.multicast.MulticastUtils;
import org.apache.nifi.logging.NiFiLog;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.cluster.protocol.ProtocolContext;
import org.apache.nifi.cluster.protocol.ProtocolMessageMarshaller;
import org.apache.nifi.cluster.protocol.message.ProtocolMessage;
import org.apache.nifi.cluster.protocol.message.ServiceBroadcastMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Broadcasts services used by the clustering software using multicast
 * communication. A configurable delay occurs after broadcasting the collection
 * of services.
 *
 * The client caller is responsible for starting and stopping the broadcasting.
 * The instance must be stopped before termination of the JVM to ensure proper
 * resource clean-up.
 *
 */
public class ClusterServicesBroadcaster implements MulticastServicesBroadcaster {

    private static final Logger logger = new NiFiLog(LoggerFactory.getLogger(ClusterServicesBroadcaster.class));

    private final Set<DiscoverableService> services = new CopyOnWriteArraySet<>();

    private final InetSocketAddress multicastAddress;

    private final MulticastConfiguration multicastConfiguration;

    private final ProtocolContext<ProtocolMessage> protocolContext;

    private final int broadcastDelayMs;

    private Timer broadcaster;

    private MulticastSocket multicastSocket;

    public ClusterServicesBroadcaster(final InetSocketAddress multicastAddress,
            final MulticastConfiguration multicastConfiguration,
            final ProtocolContext<ProtocolMessage> protocolContext, final String broadcastDelay) {

        if (multicastAddress == null) {
            throw new IllegalArgumentException("Multicast address may not be null.");
        } else if (multicastAddress.getAddress().isMulticastAddress() == false) {
            throw new IllegalArgumentException("Multicast group address is not a Class D IP address.");
        } else if (protocolContext == null) {
            throw new IllegalArgumentException("Protocol Context may not be null.");
        } else if (multicastConfiguration == null) {
            throw new IllegalArgumentException("Multicast configuration may not be null.");
        }

        this.services.addAll(services);
        this.multicastAddress = multicastAddress;
        this.multicastConfiguration = multicastConfiguration;
        this.protocolContext = protocolContext;
        this.broadcastDelayMs = (int) FormatUtils.getTimeDuration(broadcastDelay, TimeUnit.MILLISECONDS);
    }

    public void start() throws IOException {

        if (isRunning()) {
            throw new IllegalStateException("Instance is already started.");
        }

        // setup socket
        multicastSocket = MulticastUtils.createMulticastSocket(multicastConfiguration);

        // setup broadcaster
        broadcaster = new Timer("Cluster Services Broadcaster", /* is daemon */ true);
        broadcaster.schedule(new TimerTask() {
            @Override
            public void run() {
                for (final DiscoverableService service : services) {
                    try {

                        final InetSocketAddress serviceAddress = service.getServiceAddress();
                        logger.debug(String.format("Broadcasting Cluster Service '%s' at address %s:%d",
                                service.getServiceName(), serviceAddress.getHostName(), serviceAddress.getPort()));

                        // create message
                        final ServiceBroadcastMessage msg = new ServiceBroadcastMessage();
                        msg.setServiceName(service.getServiceName());
                        msg.setAddress(serviceAddress.getHostName());
                        msg.setPort(serviceAddress.getPort());

                        // marshal message to output stream
                        final ProtocolMessageMarshaller<ProtocolMessage> marshaller = protocolContext.createMarshaller();
                        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                        marshaller.marshal(msg, baos);
                        final byte[] packetBytes = baos.toByteArray();

                        // send message
                        final DatagramPacket packet = new DatagramPacket(packetBytes, packetBytes.length, multicastAddress);
                        multicastSocket.send(packet);

                    } catch (final Exception ex) {
                        logger.warn(String.format("Cluster Services Broadcaster failed broadcasting service '%s' due to: %s", service.getServiceName(), ex), ex);
                    }
                }
            }
        }, 0, broadcastDelayMs);
    }

    public boolean isRunning() {
        return (broadcaster != null);
    }

    public void stop() {

        if (isRunning() == false) {
            throw new IllegalStateException("Instance is already stopped.");
        }

        broadcaster.cancel();
        broadcaster = null;

        // close socket
        MulticastUtils.closeQuietly(multicastSocket);

    }

    @Override
    public int getBroadcastDelayMs() {
        return broadcastDelayMs;
    }

    @Override
    public Set<DiscoverableService> getServices() {
        return Collections.unmodifiableSet(services);
    }

    @Override
    public InetSocketAddress getMulticastAddress() {
        return multicastAddress;
    }

    @Override
    public boolean addService(final DiscoverableService service) {
        return services.add(service);
    }

    @Override
    public boolean removeService(final String serviceName) {
        for (final DiscoverableService service : services) {
            if (service.getServiceName().equals(serviceName)) {
                return services.remove(service);
            }
        }
        return false;
    }
}
