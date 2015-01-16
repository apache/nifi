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
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.PeerStatus;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.remote.RemoteResourceFactory;
import org.apache.nifi.remote.StandardVersionNegotiator;
import org.apache.nifi.remote.VersionNegotiator;
import org.apache.nifi.remote.codec.FlowFileCodec;
import org.apache.nifi.remote.codec.StandardFlowFileCodec;
import org.apache.nifi.remote.exception.HandshakeException;
import org.apache.nifi.remote.exception.ProtocolException;
import org.apache.nifi.remote.io.CompressionInputStream;
import org.apache.nifi.remote.io.CompressionOutputStream;
import org.apache.nifi.remote.protocol.ClientProtocol;
import org.apache.nifi.remote.protocol.CommunicationsSession;
import org.apache.nifi.remote.protocol.RequestType;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.StopWatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SocketClientProtocol implements ClientProtocol {
    private final VersionNegotiator versionNegotiator = new StandardVersionNegotiator(4, 3, 2, 1);

    
    private RemoteGroupPort port;
    private boolean useCompression;
    
    private String commsIdentifier;
    private boolean handshakeComplete = false;
    
    private final Logger logger = LoggerFactory.getLogger(SocketClientProtocol.class);
    
    private Response handshakeResponse = null;
    private boolean readyForFileTransfer = false;
    private String transitUriPrefix = null;
    
    private static final long BATCH_SEND_NANOS = TimeUnit.SECONDS.toNanos(5L); // send batches of up to 5 seconds
    
    public SocketClientProtocol() {
    }

    public void setPort(final RemoteGroupPort port) {
        this.port = port;
        this.useCompression = port.isUseCompression();
    }
    
    @Override
    public void handshake(final Peer peer) throws IOException, HandshakeException {
        if ( handshakeComplete ) {
            throw new IllegalStateException("Handshake has already been completed");
        }
        commsIdentifier = UUID.randomUUID().toString();
        logger.debug("{} handshaking with {}", this, peer);
        
        final Map<HandshakeProperty, String> properties = new HashMap<>();
        properties.put(HandshakeProperty.GZIP, String.valueOf(useCompression));
        properties.put(HandshakeProperty.PORT_IDENTIFIER, port.getIdentifier());
        properties.put(HandshakeProperty.REQUEST_EXPIRATION_MILLIS, String.valueOf(
            port.getRemoteProcessGroup().getCommunicationsTimeout(TimeUnit.MILLISECONDS)) );
        
        final CommunicationsSession commsSession = peer.getCommunicationsSession();
        commsSession.setTimeout(port.getRemoteProcessGroup().getCommunicationsTimeout(TimeUnit.MILLISECONDS));
        final DataInputStream dis = new DataInputStream(commsSession.getInput().getInputStream());
        final DataOutputStream dos = new DataOutputStream(commsSession.getOutput().getOutputStream());
        
        dos.writeUTF(commsIdentifier);
        
        if ( versionNegotiator.getVersion() >= 3 ) {
            dos.writeUTF(peer.getUrl());
            transitUriPrefix = peer.getUrl();
            
            if ( !transitUriPrefix.endsWith("/") ) {
                transitUriPrefix = transitUriPrefix + "/";
            }
        }
        
        dos.writeInt(properties.size());
        for ( final Map.Entry<HandshakeProperty, String> entry : properties.entrySet() ) {
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
                logger.error("{} received unexpected response {} from {} when negotiating Codec", new Object[] {
                    this, handshakeResponse, peer});
                peer.close();
                throw new HandshakeException("Received unexpected response " + handshakeResponse);
        }
        
        logger.debug("{} Finished handshake with {}", this, peer);
        handshakeComplete = true;
    }
    
    public boolean isReadyForFileTransfer() {
        return readyForFileTransfer;
    }
    
    public boolean isPortInvalid() {
        if ( !handshakeComplete ) {
            throw new IllegalStateException("Handshake has not completed successfully");
        }
        return handshakeResponse.getCode() == ResponseCode.PORT_NOT_IN_VALID_STATE;
    }
    
    public boolean isPortUnknown() {
        if ( !handshakeComplete ) {
            throw new IllegalStateException("Handshake has not completed successfully");
        }
        return handshakeResponse.getCode() == ResponseCode.UNKNOWN_PORT;
    }
    
    public boolean isDestinationFull() {
        if ( !handshakeComplete ) {
            throw new IllegalStateException("Handshake has not completed successfully");
        }
        return handshakeResponse.getCode() == ResponseCode.PORTS_DESTINATION_FULL;
    }
    
    @Override
    public Set<PeerStatus> getPeerStatuses(final Peer peer) throws IOException {
        if ( !handshakeComplete ) {
            throw new IllegalStateException("Handshake has not been performed");
        }
        
        logger.debug("{} Get Peer Statuses from {}", this, peer);
        final CommunicationsSession commsSession = peer.getCommunicationsSession();
        final DataInputStream dis = new DataInputStream(commsSession.getInput().getInputStream());
        final DataOutputStream dos = new DataOutputStream(commsSession.getOutput().getOutputStream());
        
        RequestType.REQUEST_PEER_LIST.writeRequestType(dos);
        dos.flush();
        final int numPeers = dis.readInt();
        final Set<PeerStatus> peers = new HashSet<>(numPeers);
        for (int i=0; i < numPeers; i++) {
            final String hostname = dis.readUTF();
            final int port = dis.readInt();
            final boolean secure = dis.readBoolean();
            final int flowFileCount = dis.readInt();
            peers.add(new PeerStatus(hostname, port, secure, flowFileCount));
        }
        
        logger.debug("{} Received {} Peer Statuses from {}", this, peers.size(), peer);
        return peers;
    }
    
    @Override
    public FlowFileCodec negotiateCodec(final Peer peer) throws IOException, ProtocolException {
        if ( !handshakeComplete ) {
            throw new IllegalStateException("Handshake has not been performed");
        }

        logger.debug("{} Negotiating Codec with {}", this, peer);
        final CommunicationsSession commsSession = peer.getCommunicationsSession();
        final DataInputStream dis = new DataInputStream(commsSession.getInput().getInputStream());
        final DataOutputStream dos = new DataOutputStream(commsSession.getOutput().getOutputStream());

        RequestType.NEGOTIATE_FLOWFILE_CODEC.writeRequestType(dos);
        
        FlowFileCodec codec = new StandardFlowFileCodec();
        try {
            codec = (FlowFileCodec) RemoteResourceFactory.initiateResourceNegotiation(codec, dis, dos);
        } catch (HandshakeException e) {
            throw new ProtocolException(e.toString());
        }
        logger.debug("{} negotiated FlowFileCodec {} with {}", new Object[] {this, codec, commsSession});

        return codec;
    }

    
    @Override
    public void receiveFlowFiles(final Peer peer, final ProcessContext context, final ProcessSession session, final FlowFileCodec codec) throws IOException, ProtocolException {
        if ( !handshakeComplete ) {
            throw new IllegalStateException("Handshake has not been performed");
        }
        if ( !readyForFileTransfer ) {
            throw new IllegalStateException("Cannot receive files; handshake resolution was " + handshakeResponse);
        }

        logger.debug("{} Receiving FlowFiles from {}", this, peer);
        final CommunicationsSession commsSession = peer.getCommunicationsSession();
        final DataInputStream dis = new DataInputStream(commsSession.getInput().getInputStream());
        final DataOutputStream dos = new DataOutputStream(commsSession.getOutput().getOutputStream());
        String userDn = commsSession.getUserDn();
        if ( userDn == null ) {
            userDn = "none";
        }
        
        // Indicate that we would like to have some data
        RequestType.RECEIVE_FLOWFILES.writeRequestType(dos);
        dos.flush();
        
        // Determine if Peer will send us data or has no data to send us
        final Response dataAvailableCode = Response.read(dis);
        switch (dataAvailableCode.getCode()) {
            case MORE_DATA:
                logger.debug("{} {} Indicates that data is available", this, peer);
                break;
            case NO_MORE_DATA:
                logger.debug("{} No data available from {}", peer);
                return;
            default:
                throw new ProtocolException("Got unexpected response when asking for data: " + dataAvailableCode);
        }

        final StopWatch stopWatch = new StopWatch(true);
        final Set<FlowFile> flowFilesReceived = new HashSet<>();
        long bytesReceived = 0L;
        final CRC32 crc = new CRC32();
        
        // Peer has data. Decode the bytes into FlowFiles until peer says he's finished sending data.
        boolean continueTransaction = true;
        String calculatedCRC = "";
        while (continueTransaction) {
            final InputStream flowFileInputStream = useCompression ? new CompressionInputStream(dis) : dis;
            final CheckedInputStream checkedIn = new CheckedInputStream(flowFileInputStream, crc);
            
            final long startNanos = System.nanoTime();
            FlowFile flowFile = codec.decode(checkedIn, session);
            final long transmissionNanos = System.nanoTime() - startNanos;
            final long transmissionMillis = TimeUnit.MILLISECONDS.convert(transmissionNanos, TimeUnit.NANOSECONDS);
            
            final String sourceFlowFileIdentifier = flowFile.getAttribute(CoreAttributes.UUID.key());
            flowFile = session.putAttribute(flowFile, CoreAttributes.UUID.key(), UUID.randomUUID().toString());
            
            final String transitUri = (transitUriPrefix == null) ? peer.getUrl() : transitUriPrefix + sourceFlowFileIdentifier;
            session.getProvenanceReporter().receive(flowFile, transitUri, "urn:nifi:" + sourceFlowFileIdentifier, "Remote Host=" + peer.getHost() + ", Remote DN=" + userDn, transmissionMillis);
            
            session.transfer(flowFile, Relationship.ANONYMOUS);
            bytesReceived += flowFile.getSize();
            flowFilesReceived.add(flowFile);
            logger.debug("{} Received {} from {}", this, flowFile, peer);
            
            final Response transactionCode = Response.read(dis);
            switch (transactionCode.getCode()) {
                case CONTINUE_TRANSACTION:
                    logger.trace("{} Received ContinueTransaction indicator from {}", this, peer);
                    break;
                case FINISH_TRANSACTION:
                    logger.trace("{} Received FinishTransaction indicator from {}", this, peer);
                    continueTransaction = false;
                    calculatedCRC = String.valueOf(checkedIn.getChecksum().getValue());
                    break;
                default:
                    throw new ProtocolException("Received unexpected response from peer: when expecting Continue Transaction or Finish Transaction, received" + transactionCode);
            }
        }
        
        // we received a FINISH_TRANSACTION indicator. Send back a CONFIRM_TRANSACTION message
        // to peer so that we can verify that the connection is still open. This is a two-phase commit,
        // which helps to prevent the chances of data duplication. Without doing this, we may commit the
        // session and then when we send the response back to the peer, the peer may have timed out and may not
        // be listening. As a result, it will re-send the data. By doing this two-phase commit, we narrow the
        // Critical Section involved in this transaction so that rather than the Critical Section being the
        // time window involved in the entire transaction, it is reduced to a simple round-trip conversation.
        logger.trace("{} Sending CONFIRM_TRANSACTION Response Code to {}", this, peer);
        ResponseCode.CONFIRM_TRANSACTION.writeResponse(dos, calculatedCRC);
        
        final Response confirmTransactionResponse = Response.read(dis);
        logger.trace("{} Received {} from {}", this, confirmTransactionResponse, peer);
        
        switch (confirmTransactionResponse.getCode()) {
            case CONFIRM_TRANSACTION:
                break;
            case BAD_CHECKSUM:
                session.rollback();
                throw new IOException(this + " Received a BadChecksum response from peer " + peer);
            default:
                throw new ProtocolException(this + " Received unexpected Response from peer " + peer + " : " + confirmTransactionResponse + "; expected 'Confirm Transaction' Response Code");
        }
        
        // Commit the session so that we have persisted the data
        session.commit();
        
        if ( session.getAvailableRelationships().isEmpty() ) {
            // Confirm that we received the data and the peer can now discard it but that the peer should not
            // send any more data for a bit
            logger.debug("{} Sending TRANSACTION_FINISHED_BUT_DESTINATION_FULL to {}", this, peer);
            ResponseCode.TRANSACTION_FINISHED_BUT_DESTINATION_FULL.writeResponse(dos);
        } else {
            // Confirm that we received the data and the peer can now discard it
            logger.debug("{} Sending TRANSACTION_FINISHED to {}", this, peer);
            ResponseCode.TRANSACTION_FINISHED.writeResponse(dos);
        }
        
        stopWatch.stop();
        final String flowFileDescription = flowFilesReceived.size() < 20 ? flowFilesReceived.toString() : flowFilesReceived.size() + " FlowFiles";
        final String uploadDataRate = stopWatch.calculateDataRate(bytesReceived);
        final long uploadMillis = stopWatch.getDuration(TimeUnit.MILLISECONDS);
        final String dataSize = FormatUtils.formatDataSize(bytesReceived);
        logger.info("{} Successfully receveied {} ({}) from {} in {} milliseconds at a rate of {}", new Object[] {
            this, flowFileDescription, dataSize, peer, uploadMillis, uploadDataRate});
    }

    @Override
    public void transferFlowFiles(final Peer peer, final ProcessContext context, final ProcessSession session, final FlowFileCodec codec) throws IOException, ProtocolException {
        if ( !handshakeComplete ) {
            throw new IllegalStateException("Handshake has not been performed");
        }
        if ( !readyForFileTransfer ) {
            throw new IllegalStateException("Cannot transfer files; handshake resolution was " + handshakeResponse);
        }

        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        logger.debug("{} Sending FlowFiles to {}", this, peer);
        final CommunicationsSession commsSession = peer.getCommunicationsSession();
        final DataInputStream dis = new DataInputStream(commsSession.getInput().getInputStream());
        final DataOutputStream dos = new DataOutputStream(commsSession.getOutput().getOutputStream());
        String userDn = commsSession.getUserDn();
        if ( userDn == null ) {
            userDn = "none";
        }
        
        // Indicate that we would like to have some data
        RequestType.SEND_FLOWFILES.writeRequestType(dos);
        dos.flush();
        
        final StopWatch stopWatch = new StopWatch(true);
        final CRC32 crc = new CRC32();
        
        long bytesSent = 0L;
        final Set<FlowFile> flowFilesSent = new HashSet<>();
        boolean continueTransaction = true;
        String calculatedCRC = "";
        final long startSendingNanos = System.nanoTime();
        while (continueTransaction) {
            final OutputStream flowFileOutputStream = useCompression ? new CompressionOutputStream(dos) : dos;
            logger.debug("{} Sending {} to {}", this, flowFile, peer);
            
            final CheckedOutputStream checkedOutStream = new CheckedOutputStream(flowFileOutputStream, crc);
            
            final long startNanos = System.nanoTime();
            flowFile = codec.encode(flowFile, session, checkedOutStream);
            final long transferNanos = System.nanoTime() - startNanos;
            final long transferMillis = TimeUnit.MILLISECONDS.convert(transferNanos, TimeUnit.NANOSECONDS);
            
            // need to close the CompressionOutputStream in order to force it write out any remaining bytes.
            // Otherwise, do NOT close it because we don't want to close the underlying stream
            // (CompressionOutputStream will not close the underlying stream when it's closed)
            if ( useCompression ) {
                checkedOutStream.close();
            }
            
            flowFilesSent.add(flowFile);
            bytesSent += flowFile.getSize();
            logger.debug("{} Sent {} to {}", this, flowFile, peer);
            
            final String transitUri = (transitUriPrefix == null) ? peer.getUrl() : transitUriPrefix + flowFile.getAttribute(CoreAttributes.UUID.key());
            session.getProvenanceReporter().send(flowFile, transitUri, "Remote Host=" + peer.getHost() + ", Remote DN=" + userDn, transferMillis, false);
            session.remove(flowFile);
            
            final long sendingNanos = System.nanoTime() - startSendingNanos;
            if ( sendingNanos < BATCH_SEND_NANOS ) { 
                flowFile = session.get();
            } else {
                flowFile = null;
            }
            
            continueTransaction = (flowFile != null);
            if ( continueTransaction ) {
                logger.debug("{} Sent CONTINUE_TRANSACTION indicator to {}", this, peer);
                ResponseCode.CONTINUE_TRANSACTION.writeResponse(dos);
            } else {
                logger.debug("{} Sent FINISH_TRANSACTION indicator to {}", this, peer);
                ResponseCode.FINISH_TRANSACTION.writeResponse(dos);
                
                calculatedCRC = String.valueOf( checkedOutStream.getChecksum().getValue() );
            }
        }
        
        // we've sent a FINISH_TRANSACTION. Now we'll wait for the peer to send a 'Confirm Transaction' response
        final Response transactionConfirmationResponse = Response.read(dis);
        if ( transactionConfirmationResponse.getCode() == ResponseCode.CONFIRM_TRANSACTION ) {
            // Confirm checksum and echo back the confirmation.
            logger.trace("{} Received {} from {}", this, transactionConfirmationResponse, peer);
            final String receivedCRC = transactionConfirmationResponse.getMessage();
            
            if ( versionNegotiator.getVersion() > 3 ) {
                if ( !receivedCRC.equals(calculatedCRC) ) {
                    ResponseCode.BAD_CHECKSUM.writeResponse(dos);
                    session.rollback();
                    throw new IOException(this + " Sent data to peer " + peer + " but calculated CRC32 Checksum as " + calculatedCRC + " while peer calculated CRC32 Checksum as " + receivedCRC + "; canceling transaction and rolling back session");
                }
            }
            
            ResponseCode.CONFIRM_TRANSACTION.writeResponse(dos, "");
        } else {
            throw new ProtocolException("Expected to receive 'Confirm Transaction' response from peer " + peer + " but received " + transactionConfirmationResponse);
        }

        final String flowFileDescription = (flowFilesSent.size() < 20) ? flowFilesSent.toString() : flowFilesSent.size() + " FlowFiles";

        final Response transactionResponse;
        try {
            transactionResponse = Response.read(dis);
        } catch (final IOException e) {
            logger.error("{} Failed to receive a response from {} when expecting a TransactionFinished Indicator." +
                    " It is unknown whether or not the peer successfully received/processed the data." +
                    " Therefore, {} will be rolled back, possibly resulting in data duplication of {}", 
                    this, peer, session, flowFileDescription);
            session.rollback();
            throw e;
        }
        
        logger.debug("{} Received {} from {}", this, transactionResponse, peer);
        if ( transactionResponse.getCode() == ResponseCode.TRANSACTION_FINISHED_BUT_DESTINATION_FULL ) {
            peer.penalize(port.getYieldPeriod(TimeUnit.MILLISECONDS));
        } else if ( transactionResponse.getCode() != ResponseCode.TRANSACTION_FINISHED ) {
            throw new ProtocolException("After sending data, expected TRANSACTION_FINISHED response but got " + transactionResponse);
        }
        
        // consume input stream entirely, ignoring its contents. If we
        // don't do this, the Connection will not be returned to the pool
        stopWatch.stop();
        final String uploadDataRate = stopWatch.calculateDataRate(bytesSent);
        final long uploadMillis = stopWatch.getDuration(TimeUnit.MILLISECONDS);
        final String dataSize = FormatUtils.formatDataSize(bytesSent);
        
        session.commit();
        
        logger.info("{} Successfully sent {} ({}) to {} in {} milliseconds at a rate of {}", new Object[] {
            this, flowFileDescription, dataSize, peer, uploadMillis, uploadDataRate});
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
