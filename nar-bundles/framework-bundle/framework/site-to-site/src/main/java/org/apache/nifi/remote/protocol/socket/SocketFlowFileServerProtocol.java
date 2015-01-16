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
import java.net.InetAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

import org.apache.nifi.cluster.NodeInformant;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.PortAuthorizationResult;
import org.apache.nifi.remote.RemoteResourceFactory;
import org.apache.nifi.remote.RootGroupPort;
import org.apache.nifi.remote.StandardVersionNegotiator;
import org.apache.nifi.remote.VersionNegotiator;
import org.apache.nifi.remote.codec.FlowFileCodec;
import org.apache.nifi.remote.exception.HandshakeException;
import org.apache.nifi.remote.exception.ProtocolException;
import org.apache.nifi.remote.io.CompressionInputStream;
import org.apache.nifi.remote.io.CompressionOutputStream;
import org.apache.nifi.remote.protocol.CommunicationsSession;
import org.apache.nifi.remote.protocol.RequestType;
import org.apache.nifi.remote.protocol.ServerProtocol;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.StopWatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SocketFlowFileServerProtocol implements ServerProtocol {
    public static final String RESOURCE_NAME = "SocketFlowFileProtocol";
    
    private ProcessGroup rootGroup;
    private String commsIdentifier;
    private boolean handshakeCompleted;
    
    private Boolean useGzip;
    private long requestExpirationMillis;
    private RootGroupPort port;
    private boolean shutdown = false;
    private FlowFileCodec negotiatedFlowFileCodec = null;
    private String transitUriPrefix = null;
    
    private final VersionNegotiator versionNegotiator = new StandardVersionNegotiator(4, 3, 2, 1);
    private final Logger logger = LoggerFactory.getLogger(SocketFlowFileServerProtocol.class);
    
    private static final long BATCH_NANOS = TimeUnit.SECONDS.toNanos(5L); // send batches of up to 5 seconds

    
    @Override
    public void setRootProcessGroup(final ProcessGroup group) {
        if ( !group.isRootGroup() ) {
            throw new IllegalArgumentException();
        }
        this.rootGroup = group;
    }
    
    @Override
    public void handshake(final Peer peer) throws IOException, HandshakeException {
        if ( handshakeCompleted ) {
            throw new IllegalStateException("Handshake has already been completed");
        }
        if ( shutdown ) {
            throw new IllegalStateException("Protocol is shutdown");
        }

        logger.debug("{} Handshaking with {}", this, peer);
        final CommunicationsSession commsSession = peer.getCommunicationsSession();
        final DataInputStream dis = new DataInputStream(commsSession.getInput().getInputStream());
        final DataOutputStream dos = new DataOutputStream(commsSession.getOutput().getOutputStream());
        
        commsIdentifier = dis.readUTF();
        
        if ( versionNegotiator.getVersion() >= 3 ) {
            transitUriPrefix = dis.readUTF();
            if ( !transitUriPrefix.endsWith("/") ) {
                transitUriPrefix = transitUriPrefix + "/";
            }
        }
        
        final Map<String, String> properties = new HashMap<>();
        final int numProperties = dis.readInt();
        for (int i=0; i < numProperties; i++) {
            final String propertyName = dis.readUTF();
            final String propertyValue = dis.readUTF();
            properties.put(propertyName, propertyValue);
        }
        
        // evaluate the properties received
        boolean responseWritten = false;
        for ( final Map.Entry<String, String> entry : properties.entrySet() ) {
            final String propertyName = entry.getKey();
            final String value = entry.getValue();
            
            final HandshakeProperty property;
            try {
                property = HandshakeProperty.valueOf(propertyName);
            } catch (final Exception e) {
                ResponseCode.UNKNOWN_PROPERTY_NAME.writeResponse(dos, "Unknown Property Name: " + propertyName);
                throw new HandshakeException("Received unknown property: " + propertyName);
            }
            
            switch (property) {
                case GZIP: {
                    useGzip = Boolean.parseBoolean(value);
                    break;
                }
                case REQUEST_EXPIRATION_MILLIS:
                    requestExpirationMillis = Long.parseLong(value);
                    break;
                case PORT_IDENTIFIER: {
                    Port receivedPort = rootGroup.getInputPort(value);
                    if ( receivedPort == null ) {
                        receivedPort = rootGroup.getOutputPort(value);
                    }
                    if ( receivedPort == null ) {
                        logger.debug("Responding with ResponseCode UNKNOWN_PORT for identifier {}", value);
                        ResponseCode.UNKNOWN_PORT.writeResponse(dos);
                        throw new HandshakeException("Received unknown port identifier: " + value);
                    }
                    if ( !(receivedPort instanceof RootGroupPort) ) {
                        logger.debug("Responding with ResponseCode UNKNOWN_PORT for identifier {}", value);
                        ResponseCode.UNKNOWN_PORT.writeResponse(dos);
                        throw new HandshakeException("Received port identifier " + value + ", but this Port is not a RootGroupPort");
                    }
                    
                    this.port = (RootGroupPort) receivedPort;
                    final PortAuthorizationResult portAuthResult = this.port.checkUserAuthorization(peer.getCommunicationsSession().getUserDn());
                    if ( !portAuthResult.isAuthorized() ) {
                        logger.debug("Responding with ResponseCode UNAUTHORIZED: ", portAuthResult.getExplanation());
                        ResponseCode.UNAUTHORIZED.writeResponse(dos, portAuthResult.getExplanation());
                        responseWritten = true;
                        break;
                    }
                    
                    if ( !receivedPort.isValid() ) {
                        logger.debug("Responding with ResponseCode PORT_NOT_IN_VALID_STATE for {}", receivedPort);
                        ResponseCode.PORT_NOT_IN_VALID_STATE.writeResponse(dos, "Port is not valid");
                        responseWritten = true;
                        break;
                    }
                    
                    if ( !receivedPort.isRunning() ) {
                        logger.debug("Responding with ResponseCode PORT_NOT_IN_VALID_STATE for {}", receivedPort);
                        ResponseCode.PORT_NOT_IN_VALID_STATE.writeResponse(dos, "Port not running");
                        responseWritten = true;
                        break;
                    }
                    
                    // PORTS_DESTINATION_FULL was introduced in version 2. If version 1, just ignore this
                    // we we will simply not service the request but the sender will timeout
                    if ( getVersionNegotiator().getVersion() > 1 ) {
                        for ( final Connection connection : port.getConnections() ) {
                            if ( connection.getFlowFileQueue().isFull() ) {
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
        }
        
        if ( useGzip == null ) {
            logger.debug("Responding with ResponseCode MISSING_PROPERTY because GZIP Property missing");
            ResponseCode.MISSING_PROPERTY.writeResponse(dos, HandshakeProperty.GZIP.name());
            throw new HandshakeException("Missing Property " + HandshakeProperty.GZIP.name());
        }
        if ( port == null ) {
            logger.debug("Responding with ResponseCode MISSING_PROPERTY because Port Identifier property is missing");
            ResponseCode.MISSING_PROPERTY.writeResponse(dos, HandshakeProperty.PORT_IDENTIFIER.name());
            throw new HandshakeException("Missing Property " + HandshakeProperty.PORT_IDENTIFIER.name());
        }
        
        // send "OK" response
        if ( !responseWritten ) {
            ResponseCode.PROPERTIES_OK.writeResponse(dos);
        }
        
        logger.debug("{} Finished handshake with {}", this, peer);
        handshakeCompleted = true;
    }
    
    @Override
    public boolean isHandshakeSuccessful() {
        return handshakeCompleted;
    }
    
    @Override
    public RootGroupPort getPort() {
        return port;
    }
    
    @Override
    public FlowFileCodec negotiateCodec(final Peer peer) throws IOException, ProtocolException {
        if ( !handshakeCompleted ) {
            throw new IllegalStateException("Handshake has not been completed");
        }
        if ( shutdown ) {
            throw new IllegalStateException("Protocol is shutdown");
        }

        logger.debug("{} Negotiating Codec with {} using {}", new Object[] {this, peer, peer.getCommunicationsSession()});
        final CommunicationsSession commsSession = peer.getCommunicationsSession();
        final DataInputStream dis = new DataInputStream(commsSession.getInput().getInputStream());
        final DataOutputStream dos = new DataOutputStream(commsSession.getOutput().getOutputStream());
        
        // Negotiate the FlowFileCodec to use.
        try {
            negotiatedFlowFileCodec = RemoteResourceFactory.receiveCodecNegotiation(dis, dos);
            logger.debug("{} Negotiated Codec {} with {}", new Object[] {this, negotiatedFlowFileCodec, peer});
            return negotiatedFlowFileCodec;
        } catch (final HandshakeException e) {
            throw new ProtocolException(e.toString());
        }
    }

    @Override
    public FlowFileCodec getPreNegotiatedCodec() {
        return negotiatedFlowFileCodec;
    }

    
    @Override
    public int transferFlowFiles(final Peer peer, final ProcessContext context, final ProcessSession session, final FlowFileCodec codec) throws IOException, ProtocolException {
        if ( !handshakeCompleted ) {
            throw new IllegalStateException("Handshake has not been completed");
        }
        if ( shutdown ) {
            throw new IllegalStateException("Protocol is shutdown");
        }

        logger.debug("{} Sending FlowFiles to {}", this, peer);
        final CommunicationsSession commsSession = peer.getCommunicationsSession();
        final DataInputStream dis = new DataInputStream(commsSession.getInput().getInputStream());
        final DataOutputStream dos = new DataOutputStream(commsSession.getOutput().getOutputStream());
        String remoteDn = commsSession.getUserDn();
        if ( remoteDn == null ) {
            remoteDn = "none";
        }

        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            // we have no data to send. Notify the peer.
            logger.debug("{} No data to send to {}", this, peer);
            ResponseCode.NO_MORE_DATA.writeResponse(dos);
            return 0;
        }
        
        // we have data to send.
        logger.debug("{} Data is available to send to {}", this, peer);
        ResponseCode.MORE_DATA.writeResponse(dos);
        
        final StopWatch stopWatch = new StopWatch(true);
        long bytesSent = 0L;
        final Set<FlowFile> flowFilesSent = new HashSet<>();
        final CRC32 crc = new CRC32();

        // send data until we reach some batch size
        boolean continueTransaction = true;
        final long startNanos = System.nanoTime();
        String calculatedCRC = "";
        while (continueTransaction) {
            final OutputStream flowFileOutputStream = useGzip ? new CompressionOutputStream(dos) : dos;
            logger.debug("{} Sending {} to {}", new Object[] {this, flowFile, peer});
            
            final CheckedOutputStream checkedOutputStream = new CheckedOutputStream(flowFileOutputStream, crc);

            final StopWatch transferWatch = new StopWatch(true);
            flowFile = codec.encode(flowFile, session, checkedOutputStream);
            final long transmissionMillis = transferWatch.getElapsed(TimeUnit.MILLISECONDS);
            
            // need to close the CompressionOutputStream in order to force it write out any remaining bytes.
            // Otherwise, do NOT close it because we don't want to close the underlying stream
            // (CompressionOutputStream will not close the underlying stream when it's closed)
            if ( useGzip ) {
                checkedOutputStream.close();
            }

            flowFilesSent.add(flowFile);
            bytesSent += flowFile.getSize();

            final String transitUri = (transitUriPrefix == null) ? peer.getUrl() : transitUriPrefix + flowFile.getAttribute(CoreAttributes.UUID.key());
            session.getProvenanceReporter().send(flowFile, transitUri, "Remote Host=" + peer.getHost() + ", Remote DN=" + remoteDn, transmissionMillis, false);
            session.remove(flowFile);
            
            final long sendingNanos = System.nanoTime() - startNanos;
            if ( sendingNanos < BATCH_NANOS ) { 
                flowFile = session.get();
            } else {
                flowFile = null;
            }
            
            continueTransaction = (flowFile != null);
            if ( continueTransaction ) {
                logger.debug("{} Sending ContinueTransaction indicator to {}", this, peer);
                ResponseCode.CONTINUE_TRANSACTION.writeResponse(dos);
            } else {
                logger.debug("{} Sending FinishTransaction indicator to {}", this, peer);
                ResponseCode.FINISH_TRANSACTION.writeResponse(dos);
                calculatedCRC = String.valueOf(checkedOutputStream.getChecksum().getValue());
            }
        }
        
        // we've sent a FINISH_TRANSACTION. Now we'll wait for the peer to send a 'Confirm Transaction' response
        final Response transactionConfirmationResponse = Response.read(dis);
        if ( transactionConfirmationResponse.getCode() == ResponseCode.CONFIRM_TRANSACTION ) {
            // Confirm Checksum and echo back the confirmation.
            logger.debug("{} Received {}  from {}", this, transactionConfirmationResponse, peer);
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

        final String flowFileDescription = flowFilesSent.size() < 20 ? flowFilesSent.toString() : flowFilesSent.size() + " FlowFiles";
        
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
        
        logger.debug("{} received {} from {}", new Object[] {this, transactionResponse, peer});
        if ( transactionResponse.getCode() == ResponseCode.TRANSACTION_FINISHED_BUT_DESTINATION_FULL ) {
            peer.penalize(port.getYieldPeriod(TimeUnit.MILLISECONDS));
        } else if ( transactionResponse.getCode() != ResponseCode.TRANSACTION_FINISHED ) {
            throw new ProtocolException("After sending data, expected TRANSACTION_FINISHED response but got " + transactionResponse);
        }
        
        session.commit();
        
        stopWatch.stop();
        final String uploadDataRate = stopWatch.calculateDataRate(bytesSent);
        final long uploadMillis = stopWatch.getDuration(TimeUnit.MILLISECONDS);
        final String dataSize = FormatUtils.formatDataSize(bytesSent);
        logger.info("{} Successfully sent {} ({}) to {} in {} milliseconds at a rate of {}", new Object[] {
            this, flowFileDescription, dataSize, peer, uploadMillis, uploadDataRate});

        return flowFilesSent.size();
    }
    
    
    @Override
    public int receiveFlowFiles(final Peer peer, final ProcessContext context, final ProcessSession session, final FlowFileCodec codec) throws IOException, ProtocolException {
        if ( !handshakeCompleted ) {
            throw new IllegalStateException("Handshake has not been completed");
        }
        if ( shutdown ) {
            throw new IllegalStateException("Protocol is shutdown");
        }

        logger.debug("{} receiving FlowFiles from {}", this, peer);
        
        final CommunicationsSession commsSession = peer.getCommunicationsSession();
        final DataInputStream dis = new DataInputStream(commsSession.getInput().getInputStream());
        final DataOutputStream dos = new DataOutputStream(commsSession.getOutput().getOutputStream());
        String remoteDn = commsSession.getUserDn();
        if ( remoteDn == null ) {
            remoteDn = "none";
        }

        final StopWatch stopWatch = new StopWatch(true);
        final CRC32 crc = new CRC32();
        
        // Peer has data. Otherwise, we would not have been called, because they would not have sent
        // a SEND_FLOWFILES request to use. Just decode the bytes into FlowFiles until peer says he's
        // finished sending data.
        final Set<FlowFile> flowFilesReceived = new HashSet<>();
        long bytesReceived = 0L;
        boolean continueTransaction = true;
        String calculatedCRC = "";
        while (continueTransaction) {
            final long startNanos = System.nanoTime();
            final InputStream flowFileInputStream = useGzip ? new CompressionInputStream(dis) : dis;
            final CheckedInputStream checkedInputStream = new CheckedInputStream(flowFileInputStream, crc);

            FlowFile flowFile = codec.decode(checkedInputStream, session);
            final long transferNanos = System.nanoTime() - startNanos;
            final long transferMillis = TimeUnit.MILLISECONDS.convert(transferNanos, TimeUnit.NANOSECONDS);
            final String sourceSystemFlowFileUuid = flowFile.getAttribute(CoreAttributes.UUID.key());
            flowFile = session.putAttribute(flowFile, CoreAttributes.UUID.key(), UUID.randomUUID().toString());
            
            final String transitUri = (transitUriPrefix == null) ? peer.getUrl() : transitUriPrefix + sourceSystemFlowFileUuid;
            session.getProvenanceReporter().receive(flowFile, transitUri, "urn:nifi:" + sourceSystemFlowFileUuid, "Remote Host=" + peer.getHost() + ", Remote DN=" + remoteDn, transferMillis);
            session.transfer(flowFile, Relationship.ANONYMOUS);
            flowFilesReceived.add(flowFile);
            bytesReceived += flowFile.getSize();
            
            final Response transactionResponse = Response.read(dis);
            switch (transactionResponse.getCode()) {
                case CONTINUE_TRANSACTION:
                    logger.debug("{} Received ContinueTransaction indicator from {}", this, peer);
                    break;
                case FINISH_TRANSACTION:
                    logger.debug("{} Received FinishTransaction indicator from {}", this, peer);
                    continueTransaction = false;
                    calculatedCRC = String.valueOf(checkedInputStream.getChecksum().getValue());
                    break;
                default:
                    throw new ProtocolException("Received unexpected response from peer: when expecting Continue Transaction or Finish Transaction, received" + transactionResponse);
            }
        }
        
        // we received a FINISH_TRANSACTION indicator. Send back a CONFIRM_TRANSACTION message
        // to peer so that we can verify that the connection is still open. This is a two-phase commit,
        // which helps to prevent the chances of data duplication. Without doing this, we may commit the
        // session and then when we send the response back to the peer, the peer may have timed out and may not
        // be listening. As a result, it will re-send the data. By doing this two-phase commit, we narrow the
        // Critical Section involved in this transaction so that rather than the Critical Section being the
        // time window involved in the entire transaction, it is reduced to a simple round-trip conversation.
        logger.debug("{} Sending CONFIRM_TRANSACTION Response Code to {}", this, peer);
        ResponseCode.CONFIRM_TRANSACTION.writeResponse(dos, calculatedCRC);
        
        final Response confirmTransactionResponse = Response.read(dis);
        logger.debug("{} Received {} from {}", this, confirmTransactionResponse, peer);

        switch (confirmTransactionResponse.getCode()) {
            case CONFIRM_TRANSACTION:
                break;
            case BAD_CHECKSUM:
                session.rollback();
                throw new IOException(this + " Received a BadChecksum response from peer " + peer);
            default:
                throw new ProtocolException(this + " Received unexpected Response Code from peer " + peer + " : " + confirmTransactionResponse + "; expected 'Confirm Transaction' Response Code");
        }
        
        // Commit the session so that we have persisted the data
        session.commit();
        
        if ( context.getAvailableRelationships().isEmpty() ) {
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
        logger.info("{} Successfully received {} ({}) from {} in {} milliseconds at a rate of {}", new Object[] {
            this, flowFileDescription, dataSize, peer, uploadMillis, uploadDataRate});

        return flowFilesReceived.size();
    }
    
    @Override
    public RequestType getRequestType(final Peer peer) throws IOException {
        if ( !handshakeCompleted ) {
            throw new IllegalStateException("Handshake has not been completed");
        }
        if ( shutdown ) {
            throw new IllegalStateException("Protocol is shutdown");
        }

        logger.debug("{} Reading Request Type from {} using {}", new Object[] {this, peer, peer.getCommunicationsSession()});
        final RequestType requestType = RequestType.readRequestType(new DataInputStream(peer.getCommunicationsSession().getInput().getInputStream()));
        logger.debug("{} Got Request Type {} from {}", new Object[] {this, requestType, peer});

        return requestType;
    }

    @Override
    public VersionNegotiator getVersionNegotiator() {
        return versionNegotiator;
    }

    @Override
    public void shutdown(final Peer peer) {
        logger.debug("{} Shutting down with {}", this, peer);
        shutdown = true;
    }

    @Override
    public boolean isShutdown() {
        return shutdown;
    }

    @Override
    public void sendPeerList(final Peer peer) throws IOException {
        if ( !handshakeCompleted ) {
            throw new IllegalStateException("Handshake has not been completed");
        }
        if ( shutdown ) {
            throw new IllegalStateException("Protocol is shutdown");
        }

        logger.debug("{} Sending Peer List to {}", this, peer);
        final CommunicationsSession commsSession = peer.getCommunicationsSession();
        final DataOutputStream dos = new DataOutputStream(commsSession.getOutput().getOutputStream());

        final NiFiProperties properties = NiFiProperties.getInstance();
        
        // we have only 1 peer: ourselves.
        dos.writeInt(1);
        dos.writeUTF(InetAddress.getLocalHost().getHostName());
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
    public void setNodeInformant(final NodeInformant nodeInformant) {
    }

    @Override
    public long getRequestExpiration() {
        return requestExpirationMillis;
    }
    
    @Override
    public String toString() {
        return "SocketFlowFileServerProtocol[CommsID=" + commsIdentifier + "]";
    }
}
