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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.PeerDescription;
import org.apache.nifi.remote.PeerStatus;
import org.apache.nifi.remote.RemoteDestination;
import org.apache.nifi.remote.RemoteResourceInitiator;
import org.apache.nifi.remote.StandardVersionNegotiator;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.VersionNegotiator;
import org.apache.nifi.remote.codec.FlowFileCodec;
import org.apache.nifi.remote.codec.StandardFlowFileCodec;
import org.apache.nifi.remote.exception.HandshakeException;
import org.apache.nifi.remote.exception.ProtocolException;
import org.apache.nifi.remote.protocol.ClientProtocol;
import org.apache.nifi.remote.protocol.CommunicationsSession;
import org.apache.nifi.remote.protocol.HandshakeProperty;
import org.apache.nifi.remote.protocol.RequestType;
import org.apache.nifi.remote.protocol.Response;
import org.apache.nifi.remote.protocol.ResponseCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SocketClientProtocol implements ClientProtocol {

    // Version 6 added to support Zero-Leader Clustering, which was introduced in NiFi 1.0.0
    private final VersionNegotiator versionNegotiator = new StandardVersionNegotiator(6, 5, 4, 3, 2, 1);

    private RemoteDestination destination;
    private boolean useCompression = false;

    private String commsIdentifier;
    private boolean handshakeComplete = false;

    private final Logger logger = LoggerFactory.getLogger(SocketClientProtocol.class);

    private Response handshakeResponse = null;
    private boolean readyForFileTransfer = false;
    private String transitUriPrefix = null;
    private int timeoutMillis = 30000;

    private int batchCount;
    private long batchSize;
    private long batchMillis;
    private EventReporter eventReporter;

    public SocketClientProtocol() {
    }

    public void setPreferredBatchCount(final int count) {
        this.batchCount = count;
    }

    public void setPreferredBatchSize(final long bytes) {
        this.batchSize = bytes;
    }

    public void setPreferredBatchDuration(final long millis) {
        this.batchMillis = millis;
    }

    public void setEventReporter(final EventReporter eventReporter) {
        this.eventReporter = eventReporter;
    }

    public void setDestination(final RemoteDestination destination) {
        this.destination = destination;
        this.useCompression = destination.isUseCompression();
    }

    public void setTimeout(final int timeoutMillis) {
        this.timeoutMillis = timeoutMillis;
    }

    @Override
    public void handshake(final Peer peer) throws IOException {
        handshake(peer, destination.getIdentifier());
    }

    public void handshake(final Peer peer, final String destinationId) throws IOException {
        if (handshakeComplete) {
            throw new IllegalStateException("Handshake has already been completed");
        }
        commsIdentifier = UUID.randomUUID().toString();
        logger.debug("{} handshaking with {}", this, peer);

        final Map<HandshakeProperty, String> properties = new HashMap<>();
        properties.put(HandshakeProperty.GZIP, String.valueOf(useCompression));

        if (destinationId != null) {
            properties.put(HandshakeProperty.PORT_IDENTIFIER, destinationId);
        }

        properties.put(HandshakeProperty.REQUEST_EXPIRATION_MILLIS, String.valueOf(timeoutMillis));

        if (versionNegotiator.getVersion() >= 5) {
            if (batchCount > 0) {
                properties.put(HandshakeProperty.BATCH_COUNT, String.valueOf(batchCount));
            }
            if (batchSize > 0L) {
                properties.put(HandshakeProperty.BATCH_SIZE, String.valueOf(batchSize));
            }
            if (batchMillis > 0L) {
                properties.put(HandshakeProperty.BATCH_DURATION, String.valueOf(batchMillis));
            }
        }

        final CommunicationsSession commsSession = peer.getCommunicationsSession();
        commsSession.setTimeout(timeoutMillis);
        final DataInputStream dis = new DataInputStream(commsSession.getInput().getInputStream());
        final DataOutputStream dos = new DataOutputStream(commsSession.getOutput().getOutputStream());

        dos.writeUTF(commsIdentifier);

        if (versionNegotiator.getVersion() >= 3) {
            dos.writeUTF(peer.getUrl());
            transitUriPrefix = peer.getUrl();

            if (!transitUriPrefix.endsWith("/")) {
                transitUriPrefix = transitUriPrefix + "/";
            }
        }

        logger.debug("Handshaking with properties {}", properties);
        dos.writeInt(properties.size());
        for (final Map.Entry<HandshakeProperty, String> entry : properties.entrySet()) {
            dos.writeUTF(entry.getKey().name());
            dos.writeUTF(entry.getValue());
        }

        dos.flush();

        try {
            handshakeResponse = Response.read(dis);
        } catch (final ProtocolException e) {
            throw new HandshakeException(e);
        }

        switch (handshakeResponse.getCode()) {
            case PORT_NOT_IN_VALID_STATE:
            case UNKNOWN_PORT:
            case PORTS_DESTINATION_FULL:
                break;
            case PROPERTIES_OK:
                readyForFileTransfer = true;
                break;
            default:
                logger.error("{} received unexpected response {} from {} when negotiating Codec", this, handshakeResponse, peer);
                peer.close();
                throw new HandshakeException("Received unexpected response " + handshakeResponse);
        }

        logger.debug("{} Finished handshake with {}", this, peer);
        handshakeComplete = true;
    }

    @Override
    public boolean isPortInvalid() {
        if (!handshakeComplete) {
            throw new IllegalStateException("Handshake has not completed successfully");
        }
        return handshakeResponse.getCode() == ResponseCode.PORT_NOT_IN_VALID_STATE;
    }

    @Override
    public boolean isPortUnknown() {
        if (!handshakeComplete) {
            throw new IllegalStateException("Handshake has not completed successfully");
        }
        return handshakeResponse.getCode() == ResponseCode.UNKNOWN_PORT;
    }

    @Override
    public boolean isDestinationFull() {
        if (!handshakeComplete) {
            throw new IllegalStateException("Handshake has not completed successfully");
        }
        return handshakeResponse.getCode() == ResponseCode.PORTS_DESTINATION_FULL;
    }

    @Override
    public Set<PeerStatus> getPeerStatuses(final Peer peer) throws IOException {
        if (!handshakeComplete) {
            throw new IllegalStateException("Handshake has not been performed");
        }

        logger.debug("{} Get Peer Statuses from {}", this, peer);
        final CommunicationsSession commsSession = peer.getCommunicationsSession();
        final DataInputStream dis = new DataInputStream(commsSession.getInput().getInputStream());
        final DataOutputStream dos = new DataOutputStream(commsSession.getOutput().getOutputStream());

        final boolean queryPeersForOtherPeers = getVersionNegotiator().getVersion() >= 6;

        RequestType.REQUEST_PEER_LIST.writeRequestType(dos);
        dos.flush();
        final int numPeers = dis.readInt();
        final Set<PeerStatus> peers = new HashSet<>(numPeers);
        for (int i = 0; i < numPeers; i++) {
            final String hostname = dis.readUTF();
            final int port = dis.readInt();
            final boolean secure = dis.readBoolean();
            final int flowFileCount = dis.readInt();
            peers.add(new PeerStatus(new PeerDescription(hostname, port, secure), flowFileCount, queryPeersForOtherPeers));
        }

        logger.debug("{} Received {} Peer Statuses from {}", this, peers.size(), peer);
        return peers;
    }

    @Override
    public FlowFileCodec negotiateCodec(final Peer peer) throws IOException {
        if (!handshakeComplete) {
            throw new IllegalStateException("Handshake has not been performed");
        }

        logger.debug("{} Negotiating Codec with {}", this, peer);
        final CommunicationsSession commsSession = peer.getCommunicationsSession();
        final DataInputStream dis = new DataInputStream(commsSession.getInput().getInputStream());
        final DataOutputStream dos = new DataOutputStream(commsSession.getOutput().getOutputStream());

        RequestType.NEGOTIATE_FLOWFILE_CODEC.writeRequestType(dos);

        FlowFileCodec codec = new StandardFlowFileCodec();
        try {
            codec = (FlowFileCodec) RemoteResourceInitiator.initiateResourceNegotiation(codec, dis, dos);
        } catch (HandshakeException e) {
            throw new ProtocolException(e.toString());
        }
        logger.debug("{} negotiated FlowFileCodec {} with {}", this, codec, commsSession);

        return codec;
    }

    @Override
    public Transaction startTransaction(final Peer peer, final FlowFileCodec codec, final TransferDirection direction) throws IOException {
        if (!handshakeComplete) {
            throw new IllegalStateException("Handshake has not been performed");
        }
        if (!readyForFileTransfer) {
            throw new IllegalStateException("Cannot start transaction; handshake resolution was " + handshakeResponse);
        }

        return new SocketClientTransaction(versionNegotiator.getVersion(), destination.getIdentifier(), peer, codec,
                direction, useCompression, (int) destination.getYieldPeriod(TimeUnit.MILLISECONDS), eventReporter);
    }

    @Override
    public VersionNegotiator getVersionNegotiator() {
        return versionNegotiator;
    }

    @Override
    public void shutdown(final Peer peer) throws IOException {
        readyForFileTransfer = false;
        final CommunicationsSession commsSession = peer.getCommunicationsSession();
        final DataOutputStream dos = new DataOutputStream(commsSession.getOutput().getOutputStream());

        logger.debug("{} Shutting down with {}", this, peer);
        // Indicate that we would like to have some data
        RequestType.SHUTDOWN.writeRequestType(dos);
        dos.flush();
    }

    @Override
    public String getResourceName() {
        return "SocketFlowFileProtocol";
    }

    @Override
    public String toString() {
        return "SocketClientProtocol[CommsID=" + commsIdentifier + "]";
    }
}
