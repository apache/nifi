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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.PeerDescription;
import org.apache.nifi.remote.PeerDescriptionModifiable;
import org.apache.nifi.remote.PeerDescriptionModifier;
import org.apache.nifi.remote.RemoteResourceFactory;
import org.apache.nifi.remote.StandardVersionNegotiator;
import org.apache.nifi.remote.VersionNegotiator;
import org.apache.nifi.remote.cluster.ClusterNodeInformation;
import org.apache.nifi.remote.cluster.NodeInformation;
import org.apache.nifi.remote.codec.FlowFileCodec;
import org.apache.nifi.remote.exception.HandshakeException;
import org.apache.nifi.remote.exception.ProtocolException;
import org.apache.nifi.remote.protocol.AbstractFlowFileServerProtocol;
import org.apache.nifi.remote.protocol.CommunicationsSession;
import org.apache.nifi.remote.protocol.HandshakeProperties;
import org.apache.nifi.remote.protocol.RequestType;
import org.apache.nifi.remote.protocol.ResponseCode;
import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol;

public class SocketFlowFileServerProtocol extends AbstractFlowFileServerProtocol implements PeerDescriptionModifiable {

    public static final String RESOURCE_NAME = "SocketFlowFileProtocol";

    // Version 6 added to support Zero-Leader Clustering, which was introduced in NiFi 1.0.0
    private final VersionNegotiator versionNegotiator = new StandardVersionNegotiator(6, 5, 4, 3, 2, 1);

    private PeerDescriptionModifier peerDescriptionModifier;

    @Override
    public void setPeerDescriptionModifier(PeerDescriptionModifier modifier) {
        peerDescriptionModifier = modifier;
    }

    @Override
    protected HandshakeProperties doHandshake(Peer peer) throws IOException {

        HandshakeProperties confirmed = new HandshakeProperties();

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
            if (handshakeResult.containsMessage()) {
                handshakeResult.writeResponse(dos, e.getMessage());
            } else {
                handshakeResult.writeResponse(dos);
            }
            responseWritten = switch (handshakeResult) {
                case UNAUTHORIZED, PORT_NOT_IN_VALID_STATE, PORTS_DESTINATION_FULL -> true;
                default -> throw e;
            };
        }

        // send "OK" response
        if (!responseWritten) {
            ResponseCode.PROPERTIES_OK.writeResponse(dos);
        }

        return confirmed;
    }

    @Override
    public FlowFileCodec negotiateCodec(final Peer peer) throws IOException {
        if (!handshakeCompleted) {
            throw new IllegalStateException("Handshake has not been completed");
        }
        if (shutdown) {
            throw new IllegalStateException("Protocol is shutdown");
        }

        logger.debug("{} Negotiating Codec with {} using {}", this, peer, peer.getCommunicationsSession());
        final CommunicationsSession commsSession = peer.getCommunicationsSession();
        final DataInputStream dis = new DataInputStream(commsSession.getInput().getInputStream());
        final DataOutputStream dos = new DataOutputStream(commsSession.getOutput().getOutputStream());

        if (port == null) {
            RemoteResourceFactory.rejectCodecNegotiation(dis, dos, "Cannot transfer FlowFiles because no port was specified");
        }

        // Negotiate the FlowFileCodec to use.
        try {
            negotiatedFlowFileCodec = RemoteResourceFactory.receiveCodecNegotiation(dis, dos);
            logger.debug("{} Negotiated Codec {} with {}", this, negotiatedFlowFileCodec, peer);
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

        logger.debug("{} Reading Request Type from {} using {}", this, peer, peer.getCommunicationsSession());
        final RequestType requestType = RequestType.readRequestType(new DataInputStream(peer.getCommunicationsSession().getInput().getInputStream()));
        logger.debug("{} Got Request Type {} from {}", this, requestType, peer);

        return requestType;
    }

    @Override
    public void sendPeerList(
            final Peer peer,
            final Optional<ClusterNodeInformation> clusterNodeInfo,
            final NodeInformation self) throws IOException {
        if (!handshakeCompleted) {
            throw new IllegalStateException("Handshake has not been completed");
        }
        if (shutdown) {
            throw new IllegalStateException("Protocol is shutdown");
        }

        logger.debug("{} Sending Peer List to {}", this, peer);
        final CommunicationsSession commsSession = peer.getCommunicationsSession();
        final DataOutputStream dos = new DataOutputStream(commsSession.getOutput().getOutputStream());

        logger.debug("{} Advertising Remote Input host name {}", this, peer);

        List<NodeInformation> nodeInfos;
        if (clusterNodeInfo.isPresent()) {
            nodeInfos = new ArrayList<>(clusterNodeInfo.get().getNodeInformation());
        } else {
            nodeInfos = Collections.singletonList(self);
        }

        // determine how many nodes have Site-to-site enabled
        int numPeers = 0;
        for (final NodeInformation nodeInfo : nodeInfos) {
            if (nodeInfo.getSiteToSitePort() != null) {
                numPeers++;
            }
        }

        dos.writeInt(numPeers);
        for (final NodeInformation nodeInfo : nodeInfos) {
            if (nodeInfo.getSiteToSitePort() == null) {
                continue;
            }

            if (peerDescriptionModifier != null && peerDescriptionModifier.isModificationNeeded(SiteToSiteTransportProtocol.RAW)) {
                final PeerDescription target = new PeerDescription(nodeInfo.getSiteToSiteHostname(), nodeInfo.getSiteToSitePort(), nodeInfo.isSiteToSiteSecure());
                final PeerDescription modifiedTarget = peerDescriptionModifier.modify(peer.getDescription(), target,
                        SiteToSiteTransportProtocol.RAW, PeerDescriptionModifier.RequestType.Peers, new HashMap<>());

                dos.writeUTF(modifiedTarget.getHostname());
                dos.writeInt(modifiedTarget.getPort());
                dos.writeBoolean(modifiedTarget.isSecure());

            } else {
                dos.writeUTF(nodeInfo.getSiteToSiteHostname());
                dos.writeInt(nodeInfo.getSiteToSitePort());
                dos.writeBoolean(nodeInfo.isSiteToSiteSecure());
            }

            dos.writeInt(nodeInfo.getTotalFlowFiles());
        }

        logger.info("Sending list of {} peers back to client {}", numPeers, peer);
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

    @Override
    protected String createTransitUri(Peer peer, String sourceFlowFileIdentifier) {
        String transitUriPrefix = handshakeProperties.getTransitUriPrefix();
        return (transitUriPrefix == null) ? peer.getUrl() : transitUriPrefix + sourceFlowFileIdentifier;
    }
}
