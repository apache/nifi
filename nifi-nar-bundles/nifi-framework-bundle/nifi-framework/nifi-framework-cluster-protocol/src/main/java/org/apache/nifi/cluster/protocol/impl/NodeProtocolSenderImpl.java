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
import java.security.cert.CertificateException;
import org.apache.nifi.cluster.protocol.NodeProtocolSender;
import org.apache.nifi.cluster.protocol.ProtocolContext;
import org.apache.nifi.cluster.protocol.ProtocolException;
import org.apache.nifi.cluster.protocol.ProtocolMessageMarshaller;
import org.apache.nifi.cluster.protocol.ProtocolMessageUnmarshaller;
import org.apache.nifi.cluster.protocol.UnknownServiceAddressException;
import org.apache.nifi.cluster.protocol.message.ConnectionRequestMessage;
import org.apache.nifi.cluster.protocol.message.ConnectionResponseMessage;
import org.apache.nifi.cluster.protocol.message.HeartbeatMessage;
import org.apache.nifi.cluster.protocol.message.ProtocolMessage;
import org.apache.nifi.cluster.protocol.message.ProtocolMessage.MessageType;
import org.apache.nifi.io.socket.SocketConfiguration;
import org.apache.nifi.io.socket.SocketUtils;
import org.apache.nifi.io.socket.multicast.DiscoverableService;
import org.apache.nifi.security.util.CertificateUtils;

public class NodeProtocolSenderImpl implements NodeProtocolSender {

    private final SocketConfiguration socketConfiguration;
    private final ClusterServiceLocator clusterManagerProtocolServiceLocator;
    private final ProtocolContext<ProtocolMessage> protocolContext;

    public NodeProtocolSenderImpl(final ClusterServiceLocator clusterManagerProtocolServiceLocator,
                                  final SocketConfiguration socketConfiguration, final ProtocolContext<ProtocolMessage> protocolContext) {
        if (clusterManagerProtocolServiceLocator == null) {
            throw new IllegalArgumentException("Protocol Service Locator may not be null.");
        } else if (socketConfiguration == null) {
            throw new IllegalArgumentException("Socket configuration may not be null.");
        } else if (protocolContext == null) {
            throw new IllegalArgumentException("Protocol Context may not be null.");
        }

        this.clusterManagerProtocolServiceLocator = clusterManagerProtocolServiceLocator;
        this.socketConfiguration = socketConfiguration;
        this.protocolContext = protocolContext;
    }

    @Override
    public ConnectionResponseMessage requestConnection(final ConnectionRequestMessage msg) throws ProtocolException, UnknownServiceAddressException {
        Socket socket = null;
        try {
            socket = createSocket();

            String ncmDn = getNCMDN(socket);

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
                throw new ProtocolException("Failed unmarshalling '" + MessageType.CONNECTION_RESPONSE + "' protocol message due to: " + ioe, ioe);
            }

            if (MessageType.CONNECTION_RESPONSE == response.getType()) {
                final ConnectionResponseMessage connectionResponse = (ConnectionResponseMessage) response;
                connectionResponse.setClusterManagerDN(ncmDn);
                return connectionResponse;
            } else {
                throw new ProtocolException("Expected message type '" + MessageType.CONNECTION_RESPONSE + "' but found '" + response.getType() + "'");
            }
        } finally {
            SocketUtils.closeQuietly(socket);
        }
    }

    @Override
    public void heartbeat(final HeartbeatMessage msg, final String address) throws ProtocolException {
        final String hostname;
        final int port;
        try {
            final String[] parts = address.split(":");
            hostname = parts[0];
            port = Integer.parseInt(parts[1]);
        } catch (final Exception e) {
            throw new IllegalArgumentException("Cannot send heartbeat to address [" + address + "]. Address must be in <hostname>:<port> format");
        }

        sendProtocolMessage(msg, hostname, port);
    }

    private String getNCMDN(Socket socket) {
        try {
            return CertificateUtils.extractClientDNFromSSLSocket(socket);
        } catch (CertificateException e) {
            throw new ProtocolException(e);
        }
    }

    private Socket createSocket() {
        // determine the cluster manager's address
        final DiscoverableService service = clusterManagerProtocolServiceLocator.getService();
        if (service == null) {
            throw new UnknownServiceAddressException("Cluster Manager's service is not known.  Verify a cluster manager is running.");
        }

        try {
            // create a socket
            return SocketUtils.createSocket(service.getServiceAddress(), socketConfiguration);
        } catch (final IOException ioe) {
            throw new ProtocolException("Failed to create socket due to: " + ioe, ioe);
        }
    }

    public SocketConfiguration getSocketConfiguration() {
        return socketConfiguration;
    }

    private void sendProtocolMessage(final ProtocolMessage msg, final String hostname, final int port) {
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
        } finally {
            SocketUtils.closeQuietly(socket);
        }
    }
}
