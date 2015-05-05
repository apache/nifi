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
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.cluster.protocol.ClusterManagerProtocolSender;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.cluster.protocol.ProtocolContext;
import org.apache.nifi.cluster.protocol.ProtocolException;
import org.apache.nifi.cluster.protocol.ProtocolMessageMarshaller;
import org.apache.nifi.cluster.protocol.ProtocolMessageUnmarshaller;
import org.apache.nifi.cluster.protocol.message.DisconnectMessage;
import org.apache.nifi.cluster.protocol.message.FlowRequestMessage;
import org.apache.nifi.cluster.protocol.message.FlowResponseMessage;
import org.apache.nifi.cluster.protocol.message.PrimaryRoleAssignmentMessage;
import org.apache.nifi.cluster.protocol.message.ProtocolMessage;
import org.apache.nifi.cluster.protocol.message.ProtocolMessage.MessageType;
import org.apache.nifi.cluster.protocol.message.ReconnectionRequestMessage;
import org.apache.nifi.cluster.protocol.message.ReconnectionResponseMessage;
import org.apache.nifi.io.socket.SocketConfiguration;
import org.apache.nifi.io.socket.SocketUtils;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.util.FormatUtils;

/**
 * A protocol sender for sending protocol messages from the cluster manager to
 * nodes.
 *
 * Connection-type requests (e.g., reconnection, disconnection) by nature of
 * starting/stopping flow controllers take longer than other types of protocol
 * messages. Therefore, a handshake timeout may be specified to lengthen the
 * allowable time for communication with the node.
 *
 */
public class ClusterManagerProtocolSenderImpl implements ClusterManagerProtocolSender {

    private final ProtocolContext<ProtocolMessage> protocolContext;
    private final SocketConfiguration socketConfiguration;
    private int handshakeTimeoutSeconds;
    private volatile BulletinRepository bulletinRepository;

    public ClusterManagerProtocolSenderImpl(final SocketConfiguration socketConfiguration, final ProtocolContext<ProtocolMessage> protocolContext) {
        if (socketConfiguration == null) {
            throw new IllegalArgumentException("Socket configuration may not be null.");
        } else if (protocolContext == null) {
            throw new IllegalArgumentException("Protocol Context may not be null.");
        }
        this.socketConfiguration = socketConfiguration;
        this.protocolContext = protocolContext;
        this.handshakeTimeoutSeconds = -1;  // less than zero denotes variable not configured
    }

    @Override
    public void setBulletinRepository(final BulletinRepository bulletinRepository) {
        this.bulletinRepository = bulletinRepository;
    }

    /**
     * Requests the data flow from a node.
     *
     * @param msg a message
     * @return the message response
     * @throws ProtocolException if the message failed to be sent or the
     * response was malformed
     */
    @Override
    public FlowResponseMessage requestFlow(final FlowRequestMessage msg) throws ProtocolException {
        Socket socket = null;
        try {
            socket = createSocket(msg.getNodeId(), false);

            try {
                // marshal message to output stream
                final ProtocolMessageMarshaller<ProtocolMessage> marshaller = protocolContext.createMarshaller();
                marshaller.marshal(msg, socket.getOutputStream());
            } catch (final IOException ioe) {
                throw new ProtocolException("Failed marshalling '" + msg.getType() + "' protocol message due to: " + ioe, ioe);
            }

            final ProtocolMessage response;
            try {
                // unmarshall response and return
                final ProtocolMessageUnmarshaller<ProtocolMessage> unmarshaller = protocolContext.createUnmarshaller();
                response = unmarshaller.unmarshal(socket.getInputStream());
            } catch (final IOException ioe) {
                throw new ProtocolException("Failed unmarshalling '" + MessageType.FLOW_RESPONSE + "' protocol message due to: " + ioe, ioe);
            }

            if (MessageType.FLOW_RESPONSE == response.getType()) {
                return (FlowResponseMessage) response;
            } else {
                throw new ProtocolException("Expected message type '" + MessageType.FLOW_RESPONSE + "' but found '" + response.getType() + "'");
            }

        } finally {
            SocketUtils.closeQuietly(socket);
        }
    }

    /**
     * Requests a node to reconnect to the cluster. The configured value for
     * handshake timeout is applied to the socket before making the request.
     *
     * @param msg a message
     * @return the response
     * @throws ProtocolException if the message failed to be sent or the
     * response was malformed
     */
    @Override
    public ReconnectionResponseMessage requestReconnection(final ReconnectionRequestMessage msg) throws ProtocolException {
        Socket socket = null;
        try {
            socket = createSocket(msg.getNodeId(), true);

            // marshal message to output stream
            try {
                final ProtocolMessageMarshaller<ProtocolMessage> marshaller = protocolContext.createMarshaller();
                marshaller.marshal(msg, socket.getOutputStream());
            } catch (final IOException ioe) {
                throw new ProtocolException("Failed marshalling '" + msg.getType() + "' protocol message due to: " + ioe, ioe);
            }

            final ProtocolMessage response;
            try {
                // unmarshall response and return
                final ProtocolMessageUnmarshaller<ProtocolMessage> unmarshaller = protocolContext.createUnmarshaller();
                response = unmarshaller.unmarshal(socket.getInputStream());
            } catch (final IOException ioe) {
                throw new ProtocolException("Failed unmarshalling '" + MessageType.RECONNECTION_RESPONSE + "' protocol message due to: " + ioe, ioe);
            }

            if (MessageType.RECONNECTION_RESPONSE == response.getType()) {
                return (ReconnectionResponseMessage) response;
            } else {
                throw new ProtocolException("Expected message type '" + MessageType.FLOW_RESPONSE + "' but found '" + response.getType() + "'");
            }
        } finally {
            SocketUtils.closeQuietly(socket);
        }
    }

    /**
     * Requests a node to disconnect from the cluster. The configured value for
     * handshake timeout is applied to the socket before making the request.
     *
     * @param msg a message
     * @throws ProtocolException if the message failed to be sent
     */
    @Override
    public void disconnect(final DisconnectMessage msg) throws ProtocolException {
        Socket socket = null;
        try {
            socket = createSocket(msg.getNodeId(), true);

            // marshal message to output stream
            try {
                final ProtocolMessageMarshaller<ProtocolMessage> marshaller = protocolContext.createMarshaller();
                marshaller.marshal(msg, socket.getOutputStream());
            } catch (final IOException ioe) {
                throw new ProtocolException("Failed marshalling '" + msg.getType() + "' protocol message due to: " + ioe, ioe);
            }
        } finally {
            SocketUtils.closeQuietly(socket);
        }
    }

    /**
     * Assigns the primary role to a node.
     *
     * @param msg a message
     *
     * @throws ProtocolException if the message failed to be sent
     */
    @Override
    public void assignPrimaryRole(final PrimaryRoleAssignmentMessage msg) throws ProtocolException {
        Socket socket = null;
        try {
            socket = createSocket(msg.getNodeId(), true);

            try {
                // marshal message to output stream
                final ProtocolMessageMarshaller<ProtocolMessage> marshaller = protocolContext.createMarshaller();
                marshaller.marshal(msg, socket.getOutputStream());
            } catch (final IOException ioe) {
                throw new ProtocolException("Failed marshalling '" + msg.getType() + "' protocol message due to: " + ioe, ioe);
            }
        } finally {
            SocketUtils.closeQuietly(socket);
        }
    }

    private void setConnectionHandshakeTimeoutOnSocket(final Socket socket) throws SocketException {
        // update socket timeout, if handshake timeout was set; otherwise use socket's current timeout
        if (handshakeTimeoutSeconds >= 0) {
            socket.setSoTimeout(handshakeTimeoutSeconds * 1000);
        }
    }

    public SocketConfiguration getSocketConfiguration() {
        return socketConfiguration;
    }

    public int getHandshakeTimeoutSeconds() {
        return handshakeTimeoutSeconds;
    }

    public void setHandshakeTimeout(final String handshakeTimeout) {
        this.handshakeTimeoutSeconds = (int) FormatUtils.getTimeDuration(handshakeTimeout, TimeUnit.SECONDS);
    }

    private Socket createSocket(final NodeIdentifier nodeId, final boolean applyHandshakeTimeout) {
        return createSocket(nodeId.getSocketAddress(), nodeId.getSocketPort(), applyHandshakeTimeout);
    }

    private Socket createSocket(final String host, final int port, final boolean applyHandshakeTimeout) {
        try {
            // create a socket
            final Socket socket = SocketUtils.createSocket(InetSocketAddress.createUnresolved(host, port), socketConfiguration);
            if (applyHandshakeTimeout) {
                setConnectionHandshakeTimeoutOnSocket(socket);
            }
            return socket;
        } catch (final IOException ioe) {
            throw new ProtocolException("Failed to create socket due to: " + ioe, ioe);
        }
    }
}
