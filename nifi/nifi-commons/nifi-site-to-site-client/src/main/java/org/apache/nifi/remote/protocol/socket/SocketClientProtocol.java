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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.remote.Peer;
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
import org.apache.nifi.remote.protocol.DataPacket;
import org.apache.nifi.remote.protocol.RequestType;
import org.apache.nifi.remote.util.StandardDataPacket;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SocketClientProtocol implements ClientProtocol {
    private final VersionNegotiator versionNegotiator = new StandardVersionNegotiator(5, 4, 3, 2, 1);

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

    private static final long BATCH_SEND_NANOS = TimeUnit.SECONDS.toNanos(5L); // send batches of up to 5 seconds
    
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
    
    public void setDestination(final RemoteDestination destination) {
        this.destination = destination;
        this.useCompression = destination.isUseCompression();
    }
    
    public void setTimeout(final int timeoutMillis) {
    	this.timeoutMillis = timeoutMillis;
    }
    
    @Override
    public void handshake(final Peer peer) throws IOException, HandshakeException {
    	handshake(peer, destination.getIdentifier());
    }
    
    public void handshake(final Peer peer, final String destinationId) throws IOException, HandshakeException {
        if ( handshakeComplete ) {
            throw new IllegalStateException("Handshake has already been completed");
        }
        commsIdentifier = UUID.randomUUID().toString();
        logger.debug("{} handshaking with {}", this, peer);
        
        final Map<HandshakeProperty, String> properties = new HashMap<>();
        properties.put(HandshakeProperty.GZIP, String.valueOf(useCompression));
        
        if ( destinationId != null ) {
        	properties.put(HandshakeProperty.PORT_IDENTIFIER, destination.getIdentifier());
        }
        
        properties.put(HandshakeProperty.REQUEST_EXPIRATION_MILLIS, String.valueOf(timeoutMillis) );
        
        if ( versionNegotiator.getVersion() >= 5 ) {
            if ( batchCount > 0 ) {
                properties.put(HandshakeProperty.BATCH_COUNT, String.valueOf(batchCount));
            }
            if ( batchSize > 0L ) {
                properties.put(HandshakeProperty.BATCH_SIZE, String.valueOf(batchSize));
            }
            if ( batchMillis > 0L ) {
                properties.put(HandshakeProperty.BATCH_DURATION, String.valueOf(batchMillis));
            }
        }
        
        final CommunicationsSession commsSession = peer.getCommunicationsSession();
        commsSession.setTimeout(timeoutMillis);
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
            codec = (FlowFileCodec) RemoteResourceInitiator.initiateResourceNegotiation(codec, dis, dos);
        } catch (HandshakeException e) {
            throw new ProtocolException(e.toString());
        }
        logger.debug("{} negotiated FlowFileCodec {} with {}", new Object[] {this, codec, commsSession});

        return codec;
    }


    @Override
    public Transaction startTransaction(final Peer peer, final FlowFileCodec codec, final TransferDirection direction) throws IOException, ProtocolException {
        if ( !handshakeComplete ) {
            throw new IllegalStateException("Handshake has not been performed");
        }
        if ( !readyForFileTransfer ) {
            throw new IllegalStateException("Cannot start transaction; handshake resolution was " + handshakeResponse);
        }
        
        return new SocketClientTransaction(versionNegotiator.getVersion(), peer, codec, 
        		direction, useCompression, (int) destination.getYieldPeriod(TimeUnit.MILLISECONDS));
    }


    @Override
    public void receiveFlowFiles(final Peer peer, final ProcessContext context, final ProcessSession session, final FlowFileCodec codec) throws IOException, ProtocolException {
    	final String userDn = peer.getCommunicationsSession().getUserDn();
    	final Transaction transaction = startTransaction(peer, codec, TransferDirection.RECEIVE);
    	
    	final StopWatch stopWatch = new StopWatch(true);
    	final Set<FlowFile> flowFilesReceived = new HashSet<>();
    	long bytesReceived = 0L;
    	
    	while (true) {
    		final long start = System.nanoTime();
    		final DataPacket dataPacket = transaction.receive();
    		if ( dataPacket == null ) {
    		    if ( flowFilesReceived.isEmpty() ) {
    		        peer.penalize(destination.getYieldPeriod(TimeUnit.MILLISECONDS));
    		    }
    			break;
    		}
    		
    		FlowFile flowFile = session.create();
    		flowFile = session.putAllAttributes(flowFile, dataPacket.getAttributes());
    		flowFile = session.importFrom(dataPacket.getData(), flowFile);
    		final long receiveNanos = System.nanoTime() - start;
    		
			String sourceFlowFileIdentifier = dataPacket.getAttributes().get(CoreAttributes.UUID.key());
			if ( sourceFlowFileIdentifier == null ) {
				sourceFlowFileIdentifier = "<Unknown Identifier>";
			}
			
			final String transitUri = (transitUriPrefix == null) ? peer.getUrl() : transitUriPrefix + sourceFlowFileIdentifier;
			session.getProvenanceReporter().receive(flowFile, transitUri, "urn:nifi:" + sourceFlowFileIdentifier, "Remote Host=" + peer.getHost() + ", Remote DN=" + userDn, TimeUnit.NANOSECONDS.toMillis(receiveNanos));

    		session.transfer(flowFile, Relationship.ANONYMOUS);
    		bytesReceived += dataPacket.getSize();
    	}

    	// Confirm that what we received was the correct data.
    	transaction.confirm();
    	
		// Commit the session so that we have persisted the data
		session.commit();

		// We want to apply backpressure if the outgoing connections are full. I.e., there are no available relationships.
		final boolean applyBackpressure = context.getAvailableRelationships().isEmpty();

		transaction.complete(applyBackpressure);
		logger.debug("{} Sending TRANSACTION_FINISHED_BUT_DESTINATION_FULL to {}", this, peer);

		if ( flowFilesReceived.isEmpty() ) {
		    return;
		}
		
		stopWatch.stop();
		final String flowFileDescription = flowFilesReceived.size() < 20 ? flowFilesReceived.toString() : flowFilesReceived.size() + " FlowFiles";
		final String uploadDataRate = stopWatch.calculateDataRate(bytesReceived);
		final long uploadMillis = stopWatch.getDuration(TimeUnit.MILLISECONDS);
		final String dataSize = FormatUtils.formatDataSize(bytesReceived);
		logger.info("{} Successfully receveied {} ({}) from {} in {} milliseconds at a rate of {}", new Object[] { 
				this, flowFileDescription, dataSize, peer, uploadMillis, uploadDataRate });
    }

    
    @Override
    public void transferFlowFiles(final Peer peer, final ProcessContext context, final ProcessSession session, final FlowFileCodec codec) throws IOException, ProtocolException {
		FlowFile flowFile = session.get();
		if (flowFile == null) {
			return;
		}

		try {
			final String userDn = peer.getCommunicationsSession().getUserDn();
			final long startSendingNanos = System.nanoTime();
			final StopWatch stopWatch = new StopWatch(true);
			long bytesSent = 0L;
			
			final Transaction transaction = startTransaction(peer, codec, TransferDirection.SEND);
			
			final Set<FlowFile> flowFilesSent = new HashSet<>();
	        boolean continueTransaction = true;
	        while (continueTransaction) {
	        	final long startNanos = System.nanoTime();
	            // call codec.encode within a session callback so that we have the InputStream to read the FlowFile
	            final FlowFile toWrap = flowFile;
	            session.read(flowFile, new InputStreamCallback() {
	                @Override
	                public void process(final InputStream in) throws IOException {
	                    final DataPacket dataPacket = new StandardDataPacket(toWrap.getAttributes(), in, toWrap.getSize());
	                    transaction.send(dataPacket);
	                }
	            });
	            
	            final long transferNanos = System.nanoTime() - startNanos;
	            final long transferMillis = TimeUnit.MILLISECONDS.convert(transferNanos, TimeUnit.NANOSECONDS);
	            
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
	        }
	        
	        transaction.confirm();
	        
	        // consume input stream entirely, ignoring its contents. If we
	        // don't do this, the Connection will not be returned to the pool
	        stopWatch.stop();
	        final String uploadDataRate = stopWatch.calculateDataRate(bytesSent);
	        final long uploadMillis = stopWatch.getDuration(TimeUnit.MILLISECONDS);
	        final String dataSize = FormatUtils.formatDataSize(bytesSent);
	        
	        session.commit();
	        transaction.complete(false);
	        
	        final String flowFileDescription = (flowFilesSent.size() < 20) ? flowFilesSent.toString() : flowFilesSent.size() + " FlowFiles";
	        logger.info("{} Successfully sent {} ({}) to {} in {} milliseconds at a rate of {}", new Object[] {
	            this, flowFileDescription, dataSize, peer, uploadMillis, uploadDataRate});
		} catch (final Exception e) {
			session.rollback();
			throw e;
		}
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
