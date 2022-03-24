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
import java.util.Collection;
import java.util.Set;

import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.protocol.ClusterCoordinationProtocolSender;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.cluster.protocol.ProtocolException;
import org.apache.nifi.cluster.protocol.ProtocolHandler;
import org.apache.nifi.cluster.protocol.ProtocolListener;
import org.apache.nifi.cluster.protocol.message.OffloadMessage;
import org.apache.nifi.cluster.protocol.message.DisconnectMessage;
import org.apache.nifi.cluster.protocol.message.NodeStatusChangeMessage;
import org.apache.nifi.cluster.protocol.message.ReconnectionRequestMessage;
import org.apache.nifi.cluster.protocol.message.ReconnectionResponseMessage;
import org.apache.nifi.reporting.BulletinRepository;

/**
 * A wrapper class for consolidating a protocol sender and listener for the
 * cluster manager.
 *
 */
public class ClusterCoordinationProtocolSenderListener implements ClusterCoordinationProtocolSender, ProtocolListener {

    private final ClusterCoordinationProtocolSender sender;

    private final ProtocolListener listener;

    public ClusterCoordinationProtocolSenderListener(final ClusterCoordinationProtocolSender sender, final ProtocolListener listener) {
        if (sender == null) {
            throw new IllegalArgumentException("ClusterManagerProtocolSender may not be null.");
        } else if (listener == null) {
            throw new IllegalArgumentException("ProtocolListener may not be null.");
        }
        this.sender = sender;
        this.listener = listener;
    }

    @Override
    public void stop() throws IOException {
        if (!isRunning()) {
            throw new IllegalStateException("Instance is already stopped.");
        }
        listener.stop();
    }

    @Override
    public void start() throws IOException {
        if (isRunning()) {
            throw new IllegalStateException("Instance is already started.");
        }
        listener.start();
    }

    @Override
    public boolean isRunning() {
        return listener.isRunning();
    }

    @Override
    public boolean removeHandler(final ProtocolHandler handler) {
        return listener.removeHandler(handler);
    }

    @Override
    public Collection<ProtocolHandler> getHandlers() {
        return listener.getHandlers();
    }

    @Override
    public void addHandler(final ProtocolHandler handler) {
        listener.addHandler(handler);
    }

    @Override
    public void setBulletinRepository(final BulletinRepository bulletinRepository) {
        listener.setBulletinRepository(bulletinRepository);
        sender.setBulletinRepository(bulletinRepository);
    }

    @Override
    public ReconnectionResponseMessage requestReconnection(final ReconnectionRequestMessage msg) throws ProtocolException {
        return sender.requestReconnection(msg);
    }

    @Override
    public void offload(OffloadMessage msg) throws ProtocolException {
        sender.offload(msg);
    }

    @Override
    public void disconnect(DisconnectMessage msg) throws ProtocolException {
        sender.disconnect(msg);
    }

    @Override
    public void notifyNodeStatusChange(final Set<NodeIdentifier> nodesToNotify, final NodeStatusChangeMessage msg) {
        sender.notifyNodeStatusChange(nodesToNotify, msg);
    }

    @Override
    public NodeConnectionStatus requestNodeConnectionStatus(final String hostname, final int port) {
        return sender.requestNodeConnectionStatus(hostname, port);
    }
}
