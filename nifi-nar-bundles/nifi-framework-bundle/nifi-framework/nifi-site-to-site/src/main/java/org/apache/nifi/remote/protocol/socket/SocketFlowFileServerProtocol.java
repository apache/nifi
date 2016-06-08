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

import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.RemoteResourceFactory;
import org.apache.nifi.remote.StandardVersionNegotiator;
import org.apache.nifi.remote.VersionNegotiator;
import org.apache.nifi.remote.codec.FlowFileCodec;
import org.apache.nifi.remote.exception.HandshakeException;
import org.apache.nifi.remote.exception.ProtocolException;
import org.apache.nifi.remote.protocol.AbstractFlowFileServerProtocol;
import org.apache.nifi.remote.protocol.CommunicationsSession;
import org.apache.nifi.remote.protocol.HandshakenProperties;
import org.apache.nifi.remote.protocol.RequestType;
import org.apache.nifi.remote.protocol.ResponseCode;
import org.apache.nifi.util.NiFiProperties;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

public class SocketFlowFileServerProtocol extends AbstractFlowFileServerProtocol {

    public static final String RESOURCE_NAME = "SocketFlowFileProtocol";

    private final VersionNegotiator versionNegotiator = new StandardVersionNegotiator(5, 4, 3, 2, 1);

    @Override
    protected HandshakenProperties doHandshake(Peer peer) throws IOException, HandshakeException {

        HandshakenProperties confirmed = new HandshakenProperties();

        final CommunicationsSession commsSession = peer.getCommunicationsSession();
        final DataInputStream dis = new DataInputStream(commsSession.getInput().getInputStream());
        final DataOutputStream dos = new DataOutputStream(commsSession.getOutput().getOutputStream());

        confirmed.setCommsIdentifier(dis.readUTF());

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

        try {
            validateHandshakeRequest(confirmed, peer, properties);
        } catch (HandshakeException e) {
            ResponseCode handshakeResult = e.getResponseCode();
            if(handshakeResult.containsMessage()){
                handshakeResult.writeResponse(dos, e.getMessage());
            } else {
                handshakeResult.writeResponse(dos);
            }
            switch (handshakeResult) {
                case UNAUTHORIZED:
                case PORT_NOT_IN_VALID_STATE:
                case PORTS_DESTINATION_FULL:
                    responseWritten = true;
                    break;
                default:
                    throw e;
            }
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


    @Override
    public VersionNegotiator getVersionNegotiator() {
        return versionNegotiator;
    }
}
