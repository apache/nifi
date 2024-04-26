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

import org.apache.nifi.cluster.protocol.message.ClusterWorkloadRequestMessage;
import org.apache.nifi.cluster.protocol.message.ClusterWorkloadResponseMessage;
import org.apache.nifi.cluster.protocol.message.CommsTimingDetails;
import org.apache.nifi.cluster.protocol.message.ConnectionRequestMessage;
import org.apache.nifi.cluster.protocol.message.ConnectionResponseMessage;
import org.apache.nifi.cluster.protocol.message.HeartbeatMessage;
import org.apache.nifi.cluster.protocol.message.HeartbeatResponseMessage;
import org.apache.nifi.cluster.protocol.message.ProtocolMessage;
import org.apache.nifi.cluster.protocol.message.ProtocolMessage.MessageType;
import org.apache.nifi.io.socket.SocketConfiguration;
import org.apache.nifi.io.socket.SocketUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Objects;

public abstract class AbstractNodeProtocolSender implements NodeProtocolSender {
    private static final Logger logger = LoggerFactory.getLogger(AbstractNodeProtocolSender.class);
    private final SocketConfiguration socketConfiguration;
    private final ProtocolContext<ProtocolMessage> protocolContext;
    private final ProtocolMessageMarshaller<ProtocolMessage> marshaller;
    private final ProtocolMessageUnmarshaller<ProtocolMessage> unmarshaller;

    public AbstractNodeProtocolSender(final SocketConfiguration socketConfiguration, final ProtocolContext<ProtocolMessage> protocolContext) {
        this.socketConfiguration = socketConfiguration;
        this.protocolContext = protocolContext;

        marshaller = protocolContext.createMarshaller();
        unmarshaller = protocolContext.createUnmarshaller();
    }

    @Override
    public ConnectionResponseMessage requestConnection(final ConnectionRequestMessage msg, final boolean allowConnectToSelf) throws ProtocolException, UnknownServiceAddressException {
        Socket socket = null;
        try {
            final InetSocketAddress socketAddress;
            try {
                socketAddress = getServiceAddress();
            } catch (final IOException e) {
                throw new ProtocolException("Could not determined address of Cluster Coordinator", e);
            }

            // If node is not allowed to connect to itself, then we need to check the address of the Cluster Coordinator.
            // If the Cluster Coordinator is currently set to this node, then we will throw an UnknownServiceAddressException
            if (!allowConnectToSelf) {
                validateNotConnectingToSelf(msg, socketAddress);
            }

            logger.info("Cluster Coordinator is located at {}. Will send Cluster Connection Request to this address", socketAddress);
            socket = createSocket(socketAddress);

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

    private void validateNotConnectingToSelf(final ConnectionRequestMessage msg, final InetSocketAddress socketAddress) {
        final NodeIdentifier localNodeIdentifier = msg.getConnectionRequest().getProposedNodeIdentifier();
        if (localNodeIdentifier == null) {
            return;
        }

        final String localAddress = localNodeIdentifier.getSocketAddress();
        final int localPort = localNodeIdentifier.getSocketPort();

        if (Objects.equals(localAddress, socketAddress.getHostString()) && localPort == socketAddress.getPort()) {
            throw new UnknownServiceAddressException("Cluster Coordinator is currently " + socketAddress.getHostString() + ":" + socketAddress.getPort() + ", which is this node, but " +
                "connecting to self is not allowed at this phase of the lifecycle. This node must wait for a new Cluster Coordinator to be elected before connecting to the cluster.");
        }
    }

    @Override
    public HeartbeatResponseMessage heartbeat(final HeartbeatMessage msg, final String address) throws ProtocolException {
        final CommsTimingDetails timingDetails = new CommsTimingDetails();

        final String[] parts = address.split(":");
        final String hostname = parts[0];
        final int port = Integer.parseInt(parts[1]);

        final ProtocolMessage responseMessage = sendProtocolMessage(msg, hostname, port, timingDetails);

        if (MessageType.HEARTBEAT_RESPONSE == responseMessage.getType()) {
            final HeartbeatResponseMessage heartbeatResponseMessage = (HeartbeatResponseMessage) responseMessage;
            heartbeatResponseMessage.setCommsTimingDetails(timingDetails);
            return heartbeatResponseMessage;
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

        final ProtocolMessage responseMessage = sendProtocolMessage(msg, serviceAddress.getHostName(), serviceAddress.getPort(), new CommsTimingDetails());
        if (MessageType.CLUSTER_WORKLOAD_RESPONSE == responseMessage.getType()) {
            return (ClusterWorkloadResponseMessage) responseMessage;
        }

        throw new ProtocolException("Expected message type '" + MessageType.CLUSTER_WORKLOAD_RESPONSE + "' but found '" + responseMessage.getType() + "'");
    }

    private Socket createSocket(final InetSocketAddress socketAddress) {
        try {
            // create a socket
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

    private ProtocolMessage sendProtocolMessage(final ProtocolMessage msg, final String hostname, final int port, final CommsTimingDetails timingDetails) {
        final long dnsLookupStart = System.currentTimeMillis();
        final InetSocketAddress socketAddress = new InetSocketAddress(hostname, port);

        final long connectStart = System.currentTimeMillis();
        try (final Socket socket = SocketUtils.createSocket(socketAddress, socketConfiguration);
             final InputStream in = new BufferedInputStream(socket.getInputStream());
             final OutputStream out = new BufferedOutputStream(socket.getOutputStream())) {

            final long sendStart = System.currentTimeMillis();
            try {
                // marshal message to output stream
                marshaller.marshal(msg, out);
            } catch (final IOException ioe) {
                throw new ProtocolException("Failed marshalling '" + msg.getType() + "' protocol message", ioe);
            }

            // Read the first byte to see how long it takes, and then reset it so that
            // we can consume it in the unmarshaller. This provides us helpful information in logs if heartbeats are not being produced/consumed
            // as frequently/efficiently as we'd like.
            final long receiveStart = System.currentTimeMillis();
            in.mark(1);
            in.read();
            in.reset();

            final long receiveFullStart = System.currentTimeMillis();
            final ProtocolMessage response;
            try {
                // unmarshall response and return
                response = unmarshaller.unmarshal(in);
            } catch (final IOException ioe) {
                throw new ProtocolException("Failed unmarshalling '" + MessageType.CONNECTION_RESPONSE + "' protocol message from "
                        + socket.getRemoteSocketAddress(), ioe);
            }

            final long receiveEnd = System.currentTimeMillis();

            timingDetails.setDnsLookupMillis(connectStart - dnsLookupStart);
            timingDetails.setConnectMillis(sendStart - connectStart);
            timingDetails.setSendRequestMillis(receiveStart - sendStart);
            timingDetails.setReceiveFirstByteMillis(receiveFullStart - receiveStart);
            timingDetails.setReceiveFullResponseMillis(receiveEnd - receiveStart);

            return response;
        } catch (IOException e) {
            throw new ProtocolException("Failed to send message to Cluster Coordinator", e);
        }
    }


    protected abstract InetSocketAddress getServiceAddress() throws IOException;
}
