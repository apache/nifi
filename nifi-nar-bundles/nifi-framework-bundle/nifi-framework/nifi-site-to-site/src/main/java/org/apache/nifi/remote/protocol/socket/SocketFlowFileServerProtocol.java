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
package org.apache.nifi.remote.protocol.socket;

import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.PortAuthorizationResult;
import org.apache.nifi.remote.RemoteResourceFactory;
import org.apache.nifi.remote.RootGroupPort;
import org.apache.nifi.remote.StandardVersionNegotiator;
import org.apache.nifi.remote.VersionNegotiator;
import org.apache.nifi.remote.codec.FlowFileCodec;
import org.apache.nifi.remote.exception.HandshakeException;
import org.apache.nifi.remote.exception.ProtocolException;
import org.apache.nifi.remote.protocol.AbstractFlowFileServerProtocol;
import org.apache.nifi.remote.protocol.CommunicationsSession;
import org.apache.nifi.remote.protocol.HandshakenProperties;
import org.apache.nifi.remote.protocol.RequestType;
import org.apache.nifi.util.NiFiProperties;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class SocketFlowFileServerProtocol extends AbstractFlowFileServerProtocol {

    public static final String RESOURCE_NAME = "SocketFlowFileProtocol";

    private final VersionNegotiator versionNegotiator = new StandardVersionNegotiator(5, 4, 3, 2, 1);

    @Override
    protected HandshakenProperties doHandshake(Peer peer) throws IOException, HandshakeException {

        HandshakenProperties confirmed = new HandshakenProperties();

        final CommunicationsSession commsSession = peer.getCommunicationsSession();
        final DataInputStream dis = new DataInputStream(commsSession.getInput().getInputStream());
        final DataOutputStream dos = new DataOutputStream(commsSession.getOutput().getOutputStream());

        confirmed.setCommsIdentifier(dis.readUTF());;

        if (versionNegotiator.getVersion() >= 3) {
            String transitUriPrefix = dis.readUTF();
            if (!transitUriPrefix.endsWith("/")) {
                transitUriPrefix = transitUriPrefix + "/";
            }
            confirmed.setTransitUriPrefix(transitUriPrefix);
        }

        final Map<String, String> properties = new HashMap<>();
        final int numProperties = dis.readInt();
        for (int i = 0; i < numProperties; i++) {
            final String propertyName = dis.readUTF();
            final String propertyValue = dis.readUTF();
            properties.put(propertyName, propertyValue);
        }

        // evaluate the properties received
        boolean responseWritten = false;
        Boolean useGzip = null;
        for (final Map.Entry<String, String> entry : properties.entrySet()) {
            final String propertyName = entry.getKey();
            final String value = entry.getValue();

            final HandshakeProperty property;
            try {
                property = HandshakeProperty.valueOf(propertyName);
            } catch (final Exception e) {
                ResponseCode.UNKNOWN_PROPERTY_NAME.writeResponse(dos, "Unknown Property Name: " + propertyName);
                throw new HandshakeException("Received unknown property: " + propertyName);
            }

            try {
                switch (property) {
                    case GZIP: {
                        useGzip = Boolean.parseBoolean(value);
                        confirmed.setUseGzip(useGzip);
                        break;
                    }
                    case REQUEST_EXPIRATION_MILLIS:
                        confirmed.setExpirationMillis(Long.parseLong(value));
                        break;
                    case BATCH_COUNT:
                        confirmed.setBatchBytes(Integer.parseInt(value));
                        break;
                    case BATCH_SIZE:
                        confirmed.setBatchBytes(Long.parseLong(value));
                        break;
                    case BATCH_DURATION:
                        confirmed.setBatchDurationNanos(TimeUnit.MILLISECONDS.toNanos(Long.parseLong(value)));
                        break;
                    case PORT_IDENTIFIER: {
                        Port receivedPort = rootGroup.getInputPort(value);
                        if (receivedPort == null) {
                            receivedPort = rootGroup.getOutputPort(value);
                        }
                        if (receivedPort == null) {
                            logger.debug("Responding with ResponseCode UNKNOWN_PORT for identifier {}", value);
                            ResponseCode.UNKNOWN_PORT.writeResponse(dos);
                            throw new HandshakeException("Received unknown port identifier: " + value);
                        }
                        if (!(receivedPort instanceof RootGroupPort)) {
                            logger.debug("Responding with ResponseCode UNKNOWN_PORT for identifier {}", value);
                            ResponseCode.UNKNOWN_PORT.writeResponse(dos);
                            throw new HandshakeException("Received port identifier " + value + ", but this Port is not a RootGroupPort");
                        }

                        this.port = (RootGroupPort) receivedPort;
                        final PortAuthorizationResult portAuthResult = this.port.checkUserAuthorization(peer.getCommunicationsSession().getUserDn());
                        if (!portAuthResult.isAuthorized()) {
                            logger.debug("Responding with ResponseCode UNAUTHORIZED: ", portAuthResult.getExplanation());
                            ResponseCode.UNAUTHORIZED.writeResponse(dos, portAuthResult.getExplanation());
                            responseWritten = true;
                            break;
                        }

                        if (!receivedPort.isValid()) {
                            logger.debug("Responding with ResponseCode PORT_NOT_IN_VALID_STATE for {}", receivedPort);
                            ResponseCode.PORT_NOT_IN_VALID_STATE.writeResponse(dos, "Port is not valid");
                            responseWritten = true;
                            break;
                        }

                        if (!receivedPort.isRunning()) {
                            logger.debug("Responding with ResponseCode PORT_NOT_IN_VALID_STATE for {}", receivedPort);
                            ResponseCode.PORT_NOT_IN_VALID_STATE.writeResponse(dos, "Port not running");
                            responseWritten = true;
                            break;
                        }

                        // PORTS_DESTINATION_FULL was introduced in version 2. If version 1, just ignore this
                        // we we will simply not service the request but the sender will timeout
                        if (getVersionNegotiator().getVersion() > 1) {
                            for (final Connection connection : port.getConnections()) {
                                if (connection.getFlowFileQueue().isFull()) {
                                    logger.debug("Responding with ResponseCode PORTS_DESTINATION_FULL for {}", receivedPort);
                                    ResponseCode.PORTS_DESTINATION_FULL.writeResponse(dos);
                                    responseWritten = true;
                                    break;
                                }
                            }
                        }

                        break;
                    }
                }
            } catch (final NumberFormatException nfe) {
                throw new HandshakeException("Received invalid value for property '" + property + "'; invalid value: " + value);
            }
        }

        if (useGzip == null) {
            logger.debug("Responding with ResponseCode MISSING_PROPERTY because GZIP Property missing");
            ResponseCode.MISSING_PROPERTY.writeResponse(dos, HandshakeProperty.GZIP.name());
            throw new HandshakeException("Missing Property " + HandshakeProperty.GZIP.name());
        }

        // send "OK" response
        if (!responseWritten) {
            ResponseCode.PROPERTIES_OK.writeResponse(dos);
        }

        return confirmed;
    }

    @Override
    public FlowFileCodec negotiateCodec(final Peer peer) throws IOException, ProtocolException {
        if (!handshakeCompleted) {
            throw new IllegalStateException("Handshake has not been completed");
        }
        if (shutdown) {
            throw new IllegalStateException("Protocol is shutdown");
        }

        logger.debug("{} Negotiating Codec with {} using {}", new Object[]{this, peer, peer.getCommunicationsSession()});
        final CommunicationsSession commsSession = peer.getCommunicationsSession();
        final DataInputStream dis = new DataInputStream(commsSession.getInput().getInputStream());
        final DataOutputStream dos = new DataOutputStream(commsSession.getOutput().getOutputStream());

        if (port == null) {
            RemoteResourceFactory.rejectCodecNegotiation(dis, dos, "Cannot transfer FlowFiles because no port was specified");
        }

        // Negotiate the FlowFileCodec to use.
        try {
            negotiatedFlowFileCodec = RemoteResourceFactory.receiveCodecNegotiation(dis, dos);
            logger.debug("{} Negotiated Codec {} with {}", new Object[]{this, negotiatedFlowFileCodec, peer});
            return negotiatedFlowFileCodec;
        } catch (final HandshakeException e) {
            throw new ProtocolException(e.toString());
        }
    }


    @Override
    public RequestType getRequestType(final Peer peer) throws IOException {
        if (!handshakeCompleted) {
            throw new IllegalStateException("Handshake has not been completed");
        }
        if (shutdown) {
            throw new IllegalStateException("Protocol is shutdown");
        }

        logger.debug("{} Reading Request Type from {} using {}", new Object[]{this, peer, peer.getCommunicationsSession()});
        final RequestType requestType = RequestType.readRequestType(new DataInputStream(peer.getCommunicationsSession().getInput().getInputStream()));
        logger.debug("{} Got Request Type {} from {}", new Object[]{this, requestType, peer});

        return requestType;
    }

    @Override
    public void sendPeerList(final Peer peer) throws IOException {
        if (!handshakeCompleted) {
            throw new IllegalStateException("Handshake has not been completed");
        }
        if (shutdown) {
            throw new IllegalStateException("Protocol is shutdown");
        }

        logger.debug("{} Sending Peer List to {}", this, peer);
        final CommunicationsSession commsSession = peer.getCommunicationsSession();
        final DataOutputStream dos = new DataOutputStream(commsSession.getOutput().getOutputStream());

        final NiFiProperties properties = NiFiProperties.getInstance();

        String remoteInputHost = properties.getRemoteInputHost();
        if (remoteInputHost == null) {
            remoteInputHost = InetAddress.getLocalHost().getHostName();
        }
        logger.debug("{} Advertising Remote Input host name {}", this, peer);

        // we have only 1 peer: ourselves.
        dos.writeInt(1);
        dos.writeUTF(remoteInputHost);
        dos.writeInt(properties.getRemoteInputPort());
        dos.writeBoolean(properties.isSiteToSiteSecure());
        dos.writeInt(0);    // doesn't matter how many FlowFiles we have, because we're the only host.
        dos.flush();
    }

    @Override
    public String getResourceName() {
        return RESOURCE_NAME;
    }


}
