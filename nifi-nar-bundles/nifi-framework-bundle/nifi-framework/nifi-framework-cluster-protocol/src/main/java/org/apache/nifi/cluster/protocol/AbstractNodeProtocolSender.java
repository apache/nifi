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

package org.apache.nifi.cluster.protocol;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.apache.nifi.cluster.protocol.message.ClusterWorkloadRequestMessage;
import org.apache.nifi.cluster.protocol.message.ClusterWorkloadResponseMessage;
import org.apache.nifi.cluster.protocol.message.ConnectionRequestMessage;
import org.apache.nifi.cluster.protocol.message.ConnectionResponseMessage;
import org.apache.nifi.cluster.protocol.message.HeartbeatMessage;
import org.apache.nifi.cluster.protocol.message.HeartbeatResponseMessage;
import org.apache.nifi.cluster.protocol.message.ProtocolMessage;
import org.apache.nifi.cluster.protocol.message.ProtocolMessage.MessageType;
import org.apache.nifi.io.socket.SocketConfiguration;
import org.apache.nifi.io.socket.SocketUtils;

public abstract class AbstractNodeProtocolSender implements NodeProtocolSender {
    private final SocketConfiguration socketConfiguration;
    private final ProtocolContext<ProtocolMessage> protocolContext;

    public AbstractNodeProtocolSender(final SocketConfiguration socketConfiguration, final ProtocolContext<ProtocolMessage> protocolContext) {
        this.socketConfiguration = socketConfiguration;
        this.protocolContext = protocolContext;
    }

    @Override
    public ConnectionResponseMessage requestConnection(final ConnectionRequestMessage msg) throws ProtocolException, UnknownServiceAddressException {
        Socket socket = null;
        try {
            socket = createSocket();

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
                throw new ProtocolException("Failed unmarshalling '" + MessageType.CONNECTION_RESPONSE + "' protocol message from "
                        + socket.getRemoteSocketAddress() + " due to: " + ioe, ioe);
            }

            if (MessageType.CONNECTION_RESPONSE == response.getType()) {
                final ConnectionResponseMessage connectionResponse = (ConnectionResponseMessage) response;
                return connectionResponse;
            } else {
                throw new ProtocolException("Expected message type '" + MessageType.CONNECTION_RESPONSE + "' but found '" + response.getType() + "'");
            }
        } finally {
            SocketUtils.closeQuietly(socket);
        }
    }

    @Override
    public HeartbeatResponseMessage heartbeat(final HeartbeatMessage msg, final String address) throws ProtocolException {
        final String hostname;
        final int port;
        try {
            final String[] parts = address.split(":");
            hostname = parts[0];
            port = Integer.parseInt(parts[1]);
        } catch (final Exception e) {
            throw new IllegalArgumentException("Cannot send heartbeat to address [" + address + "]. Address must be in <hostname>:<port> format");
        }

        final ProtocolMessage responseMessage = sendProtocolMessage(msg, hostname, port);
        if (MessageType.HEARTBEAT_RESPONSE == responseMessage.getType()) {
            return (HeartbeatResponseMessage) responseMessage;
        }

        throw new ProtocolException("Expected message type '" + MessageType.HEARTBEAT_RESPONSE + "' but found '" + responseMessage.getType() + "'");
    }

    @Override
    public ClusterWorkloadResponseMessage clusterWorkload(final ClusterWorkloadRequestMessage msg) throws ProtocolException {
        final InetSocketAddress serviceAddress;
        try {
            serviceAddress = getServiceAddress();
        } catch (IOException e) {
            throw new ProtocolException("Failed to getServiceAddress due to " + e, e);
        }
        final ProtocolMessage responseMessage = sendProtocolMessage(msg, serviceAddress.getHostName(), serviceAddress.getPort());
        if (MessageType.CLUSTER_WORKLOAD_RESPONSE == responseMessage.getType()) {
            return (ClusterWorkloadResponseMessage) responseMessage;
        }

        throw new ProtocolException("Expected message type '" + MessageType.CLUSTER_WORKLOAD_RESPONSE + "' but found '" + responseMessage.getType() + "'");
    }

    private Socket createSocket() {
        InetSocketAddress socketAddress = null;
        try {
            // create a socket
            socketAddress = getServiceAddress();
            return SocketUtils.createSocket(socketAddress, socketConfiguration);
        } catch (final IOException ioe) {
            if (socketAddress == null) {
                throw new ProtocolException("Failed to create socket due to: " + ioe, ioe);
            } else {
                throw new ProtocolException("Failed to create socket to " + socketAddress + " due to: " + ioe, ioe);
            }
        }
    }

    public SocketConfiguration getSocketConfiguration() {
        return socketConfiguration;
    }

    private ProtocolMessage sendProtocolMessage(final ProtocolMessage msg, final String hostname, final int port) {
        Socket socket = null;
        try {
            try {
                socket = SocketUtils.createSocket(new InetSocketAddress(hostname, port), socketConfiguration);
            } catch (IOException e) {
                throw new ProtocolException("Failed to send message to Cluster Coordinator due to: " + e, e);
            }

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
                throw new ProtocolException("Failed unmarshalling '" + MessageType.CONNECTION_RESPONSE + "' protocol message from "
                        + socket.getRemoteSocketAddress() + " due to: " + ioe, ioe);
            }

            return response;
        } finally {
            SocketUtils.closeQuietly(socket);
        }
    }

    protected abstract InetSocketAddress getServiceAddress() throws IOException;
}
