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
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import org.apache.nifi.io.socket.multicast.DiscoverableService;
import org.apache.nifi.io.socket.multicast.DiscoverableServiceImpl;
import org.apache.nifi.io.socket.multicast.MulticastConfiguration;
import org.apache.nifi.io.socket.multicast.MulticastServiceDiscovery;
import org.apache.nifi.reporting.BulletinRepository;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.cluster.protocol.ProtocolContext;
import org.apache.nifi.cluster.protocol.ProtocolException;
import org.apache.nifi.cluster.protocol.ProtocolHandler;
import org.apache.nifi.cluster.protocol.ProtocolListener;
import org.apache.nifi.cluster.protocol.message.ProtocolMessage;
import org.apache.nifi.cluster.protocol.message.ProtocolMessage.MessageType;
import org.apache.nifi.cluster.protocol.message.ServiceBroadcastMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation for discovering services by way of "service broadcast" type
 * protocol messages over multicast.
 *
 * The client caller is responsible for starting and stopping the service
 * discovery. The instance must be stopped before termination of the JVM to
 * ensure proper resource clean-up.
 *
 * @author unattributed
 */
public class ClusterServiceDiscovery implements MulticastServiceDiscovery, ProtocolListener {

    private static final Logger logger = LoggerFactory.getLogger(ClusterServiceDiscovery.class);
    private final String serviceName;
    private final MulticastConfiguration multicastConfiguration;
    private final MulticastProtocolListener listener;
    private volatile BulletinRepository bulletinRepository;

    /*
     * guarded by this
     */
    private DiscoverableService service;

    
    public ClusterServiceDiscovery(final String serviceName, final InetSocketAddress multicastAddress,
            final MulticastConfiguration multicastConfiguration, final ProtocolContext<ProtocolMessage> protocolContext) {

        if (StringUtils.isBlank(serviceName)) {
            throw new IllegalArgumentException("Service name may not be null or empty.");
        } else if (multicastAddress == null) {
            throw new IllegalArgumentException("Multicast address may not be null.");
        } else if (multicastAddress.getAddress().isMulticastAddress() == false) {
            throw new IllegalArgumentException("Multicast group must be a Class D address.");
        } else if (protocolContext == null) {
            throw new IllegalArgumentException("Protocol Context may not be null.");
        } else if (multicastConfiguration == null) {
            throw new IllegalArgumentException("Multicast configuration may not be null.");
        }

        this.serviceName = serviceName;
        this.multicastConfiguration = multicastConfiguration;
        this.listener = new MulticastProtocolListener(1, multicastAddress, multicastConfiguration, protocolContext);
        listener.addHandler(new ClusterManagerServiceBroadcastHandler());
    }

    @Override
    public void setBulletinRepository(final BulletinRepository bulletinRepository) {
        this.bulletinRepository = bulletinRepository;
    }

    @Override
    public synchronized DiscoverableService getService() {
        return service;
    }

    @Override
    public InetSocketAddress getMulticastAddress() {
        return listener.getMulticastAddress();
    }

    @Override
    public Collection<ProtocolHandler> getHandlers() {
        return Collections.unmodifiableCollection(listener.getHandlers());
    }

    @Override
    public void addHandler(ProtocolHandler handler) {
        listener.addHandler(handler);
    }

    @Override
    public boolean removeHandler(ProtocolHandler handler) {
        return listener.removeHandler(handler);
    }

    @Override
    public boolean isRunning() {
        return listener.isRunning();
    }

    @Override
    public void start() throws IOException {
        if (isRunning()) {
            throw new IllegalStateException("Instance is already running.");
        }
        listener.start();
    }

    @Override
    public void stop() throws IOException {
        if (isRunning() == false) {
            throw new IllegalStateException("Instance is already stopped.");
        }
        listener.stop();
    }

    public String getServiceName() {
        return serviceName;
    }

    public MulticastConfiguration getMulticastConfiguration() {
        return multicastConfiguration;
    }

    private class ClusterManagerServiceBroadcastHandler implements ProtocolHandler {

        @Override
        public boolean canHandle(final ProtocolMessage msg) {
            return MessageType.SERVICE_BROADCAST == msg.getType();
        }

        @Override
        public ProtocolMessage handle(final ProtocolMessage msg) throws ProtocolException {
            synchronized (ClusterServiceDiscovery.this) {
                if (canHandle(msg) == false) {
                    throw new ProtocolException("Handler cannot handle message type: " + msg.getType());
                } else {
                    final ServiceBroadcastMessage broadcastMsg = (ServiceBroadcastMessage) msg;
                    if (serviceName.equals(broadcastMsg.getServiceName())) {
                        final DiscoverableService oldService = service;
                        if (oldService == null
                                || broadcastMsg.getAddress().equalsIgnoreCase(oldService.getServiceAddress().getHostName()) == false
                                || broadcastMsg.getPort() != oldService.getServiceAddress().getPort()) {
                            service = new DiscoverableServiceImpl(serviceName, InetSocketAddress.createUnresolved(broadcastMsg.getAddress(), broadcastMsg.getPort()));
                            final InetSocketAddress oldServiceAddress = (oldService == null) ? null : oldService.getServiceAddress();
                            logger.info(String.format("Updating cluster service address for '%s' from '%s' to '%s'", serviceName, prettyPrint(oldServiceAddress), prettyPrint(service.getServiceAddress())));
                        }
                    }
                    return null;
                }
            }
        }
    }

    private String prettyPrint(final InetSocketAddress address) {
        if (address == null) {
            return "0.0.0.0:0";
        } else {
            return address.getHostName() + ":" + address.getPort();
        }
    }
}
