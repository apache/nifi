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

package org.apache.nifi.controller.queue.clustered.server;

import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.queue.LoadBalanceCompression;
import org.apache.nifi.controller.queue.LoadBalancedFlowFileQueue;
import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.RepositoryRecord;
import org.apache.nifi.controller.repository.StandardFlowFileRecord;
import org.apache.nifi.controller.repository.StandardRepositoryRecord;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.io.LimitedInputStream;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.provenance.ProvenanceEventBuilder;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.provenance.ProvenanceRepository;
import org.apache.nifi.provenance.StandardProvenanceEventRecord;
import org.apache.nifi.remote.StandardVersionNegotiator;
import org.apache.nifi.remote.VersionNegotiator;
import org.apache.nifi.security.util.CertificateUtils;
import org.apache.nifi.stream.io.ByteCountingInputStream;
import org.apache.nifi.stream.io.LimitingInputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.Checksum;
import java.util.zip.GZIPInputStream;

import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.ABORT_PROTOCOL_NEGOTIATION;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.ABORT_TRANSACTION;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.CHECK_SPACE;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.COMPLETE_TRANSACTION;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.CONFIRM_CHECKSUM;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.CONFIRM_COMPLETE_TRANSACTION;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.DATA_FRAME_FOLLOWS;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.MORE_FLOWFILES;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.NO_DATA_FRAME;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.NO_MORE_FLOWFILES;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.QUEUE_FULL;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.REJECT_CHECKSUM;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.REQEUST_DIFFERENT_VERSION;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.SKIP_SPACE_CHECK;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.SPACE_AVAILABLE;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.VERSION_ACCEPTED;

public class StandardLoadBalanceProtocol implements LoadBalanceProtocol {
    private static final Logger logger = LoggerFactory.getLogger(StandardLoadBalanceProtocol.class);

    private static final int SOCKET_CLOSED = -1;
    private static final int NO_DATA_AVAILABLE = 0;

    private final FlowFileRepository flowFileRepository;
    private final ContentRepository contentRepository;
    private final ProvenanceRepository provenanceRepository;
    private final FlowController flowController;
    private final LoadBalanceAuthorizer authorizer;

    private final ThreadLocal<byte[]> dataBuffer = new ThreadLocal<>();
    private final AtomicLong lineageStartIndex = new AtomicLong(0L);

    public StandardLoadBalanceProtocol(final FlowFileRepository flowFileRepository, final ContentRepository contentRepository, final ProvenanceRepository provenanceRepository,
                                       final FlowController flowController, final LoadBalanceAuthorizer authorizer) {
        this.flowFileRepository = flowFileRepository;
        this.contentRepository = contentRepository;
        this.provenanceRepository = provenanceRepository;
        this.flowController = flowController;
        this.authorizer = authorizer;
    }


    @Override
    public void receiveFlowFiles(final Socket socket) throws IOException {
        final InputStream in = new BufferedInputStream(socket.getInputStream());
        final OutputStream out = new BufferedOutputStream(socket.getOutputStream());

        String peerDescription = socket.getInetAddress().getHostName();
        if (socket instanceof SSLSocket) {
            final SSLSession sslSession = ((SSLSocket) socket).getSession();

            final Set<String> certIdentities;
            try {
                certIdentities = getCertificateIdentities(sslSession);
            } catch (final CertificateException e) {
                throw new IOException("Failed to extract Client Certificate", e);
            }

            logger.debug("Connection received from peer {}. Will perform authorization against Client Identities '{}'",
                peerDescription, certIdentities);

            peerDescription = authorizer.authorize(certIdentities);
            logger.debug("Client Identities {} are authorized to load balance data", certIdentities);
        }

        final int version = negotiateProtocolVersion(in, out, peerDescription);

        if (version == SOCKET_CLOSED) {
            socket.close();
            return;
        }
        if (version == NO_DATA_AVAILABLE) {
            logger.debug("No data is available from {}", socket.getRemoteSocketAddress());
            return;
        }

        receiveFlowFiles(in, out, peerDescription, version);
    }

    private Set<String> getCertificateIdentities(final SSLSession sslSession) throws CertificateException, SSLPeerUnverifiedException {
        final Certificate[] certs = sslSession.getPeerCertificates();
        if (certs == null || certs.length == 0) {
            throw new SSLPeerUnverifiedException("No certificates found");
        }

        final X509Certificate cert = CertificateUtils.convertAbstractX509Certificate(certs[0]);
        cert.checkValidity();

        final Set<String> identities = CertificateUtils.getSubjectAlternativeNames(cert).stream()
                .map(CertificateUtils::extractUsername)
                .collect(Collectors.toSet());

        return identities;
    }


    protected int negotiateProtocolVersion(final InputStream in, final OutputStream out, final String peerDescription) throws IOException {
        final VersionNegotiator negotiator = new StandardVersionNegotiator(1);

        for (int i=0;; i++) {
            final int requestedVersion;
            try {
                requestedVersion = in.read();
            } catch (final SocketTimeoutException ste) {
                // If first iteration, then just consider this to indicate "no data available". Otherwise, we were truly expecting data.
                if (i == 0) {
                    logger.debug("SocketTimeoutException thrown when trying to negotiate Protocol Version");
                    return NO_DATA_AVAILABLE;
                }

                throw ste;
            }

            if (requestedVersion < 0) {
                logger.debug("Encountered End-of-File when receiving the the recommended Protocol Version. Returning -1 for the protocol version");
                return -1;
            }

            final boolean supported = negotiator.isVersionSupported(requestedVersion);
            if (supported) {
                logger.debug("Peer {} requested version {} of the Load Balance Protocol. Accepting version.", peerDescription, requestedVersion);

                out.write(VERSION_ACCEPTED);
                out.flush();
                return requestedVersion;
            }

            final Integer preferredVersion = negotiator.getPreferredVersion(requestedVersion);
            if (preferredVersion == null) {
                logger.debug("Peer {} requested version {} of the Load Balance Protocol. This version is not acceptable. Aborting communications.", peerDescription, requestedVersion);

                out.write(ABORT_PROTOCOL_NEGOTIATION);
                out.flush();
                throw new IOException("Peer " + peerDescription + " requested that we use version " + requestedVersion
                    + " of the Load Balance Protocol, but this version is unacceptable. Aborted communications.");
            }

            logger.debug("Peer {} requested version {} of the Load Balance Protocol. Requesting that peer change to version {} instead.", peerDescription, requestedVersion, preferredVersion);

            out.write(REQEUST_DIFFERENT_VERSION);
            out.write(preferredVersion);
            out.flush();
        }
    }


    protected void receiveFlowFiles(final InputStream in, final OutputStream out, final String peerDescription, final int protocolVersion) throws IOException {
        logger.debug("Receiving FlowFiles from {}", peerDescription);
        final long startTimestamp = System.currentTimeMillis();

        final Checksum checksum = new CRC32();
        final InputStream checkedInput = new CheckedInputStream(in, checksum);

        final DataInputStream dataIn = new DataInputStream(checkedInput);
        final String connectionId = getConnectionID(dataIn, peerDescription);
        if (connectionId == null) {
            logger.debug("Received no Connection ID from Peer {}. Will consider receipt of FlowFiles complete", peerDescription);
            return;
        }

        final Connection connection = flowController.getConnection(connectionId);
        if (connection == null) {
            logger.error("Attempted to receive FlowFiles from Peer {} for Connection with ID {} but no connection exists with that ID", peerDescription, connectionId);
            throw new TransactionAbortedException("Attempted to receive FlowFiles from Peer " + peerDescription + " for Connection with ID " + connectionId + " but no Connection exists with that ID");
        }

        final FlowFileQueue flowFileQueue = connection.getFlowFileQueue();
        if (!(flowFileQueue instanceof LoadBalancedFlowFileQueue)) {
            throw new TransactionAbortedException("Attempted to receive FlowFiles from Peer " + peerDescription + " for Connection with ID " + connectionId + " but the Connection with that ID is " +
                    "not configured to allow for Load Balancing");
        }

        final int spaceCheck = dataIn.read();
        if (spaceCheck < 0) {
            throw new EOFException("Expected to receive a request to determine whether or not space was available for Connection with ID " + connectionId + " from Peer " + peerDescription);
        }

        if (spaceCheck == CHECK_SPACE) {
            if (flowFileQueue.isFull()) {
                logger.debug("Received a 'Check Space' request from Peer {} for Connection with ID {}; responding with QUEUE_FULL", peerDescription, connectionId);
                out.write(QUEUE_FULL);
                out.flush();
                return; // we're finished receiving flowfiles for now, and we'll restart the communication process.
            } else {
                logger.debug("Received a 'Check Space' request from Peer {} for Connection with ID {}; responding with SPACE_AVAILABLE", peerDescription, connectionId);
                out.write(SPACE_AVAILABLE);
                out.flush();
            }
        } else if (spaceCheck != SKIP_SPACE_CHECK) {
            throw new TransactionAbortedException("Expected to receive a request to determine whether or not space was available for Connection with ID "
                + connectionId + " from Peer " + peerDescription + " but instead received value " + spaceCheck);
        }

        final LoadBalanceCompression compression = connection.getFlowFileQueue().getLoadBalanceCompression();
        logger.debug("Receiving FlowFiles from Peer {} for Connection {}; Compression = {}", peerDescription, connectionId, compression);

        ContentClaim contentClaim = null;
        final List<RemoteFlowFileRecord> flowFilesReceived = new ArrayList<>();
        OutputStream contentClaimOut = null;
        long claimOffset = 0L;

        try {
            try {
                while (isMoreFlowFiles(dataIn, protocolVersion)) {
                    if (contentClaim == null) {
                        contentClaim = contentRepository.create(false);
                        contentClaimOut = contentRepository.write(contentClaim);
                    } else {
                        contentRepository.incrementClaimaintCount(contentClaim);
                    }

                    final RemoteFlowFileRecord flowFile;
                    try {
                        flowFile = receiveFlowFile(dataIn, contentClaimOut, contentClaim, claimOffset, protocolVersion, peerDescription, compression);
                    } catch (final Exception e) {
                        contentRepository.decrementClaimantCount(contentClaim);
                        throw e;
                    }

                    flowFilesReceived.add(flowFile);

                    claimOffset += flowFile.getFlowFile().getSize();
                }
            } finally {
                if (contentClaimOut != null) {
                    contentClaimOut.close();
                }
            }

            verifyChecksum(checksum, in, out, peerDescription, flowFilesReceived.size());
            completeTransaction(in, out, peerDescription, flowFilesReceived, connectionId, startTimestamp, (LoadBalancedFlowFileQueue) flowFileQueue);
        } catch (final Exception e) {
            // If any Exception occurs, we need to decrement the claimant counts for the Content Claims that we wrote to because
            // they are no longer needed.
            for (final RemoteFlowFileRecord remoteFlowFile : flowFilesReceived) {
                contentRepository.decrementClaimantCount(remoteFlowFile.getFlowFile().getContentClaim());
            }

            throw e;
        }

        logger.debug("Successfully received {} FlowFiles from Peer {} to Load Balance for Connection {}", flowFilesReceived.size(), peerDescription, connectionId);
    }

    private void completeTransaction(final InputStream in, final OutputStream out, final String peerDescription, final List<RemoteFlowFileRecord> flowFilesReceived,
                                     final String connectionId, final long startTimestamp, final LoadBalancedFlowFileQueue flowFileQueue) throws IOException {
        final int completionIndicator = in.read();
        if (completionIndicator < 0) {
            throw new EOFException("Expected to receive a Transaction Completion Indicator from Peer " + peerDescription + " but encountered EOF");
        }

        if (completionIndicator == ABORT_TRANSACTION) {
            throw new TransactionAbortedException("Peer " + peerDescription + " chose to Abort Load Balance Transaction");
        }

        if (completionIndicator != COMPLETE_TRANSACTION) {
            logger.debug("Expected to receive Transaction Completion Indicator from Peer " + peerDescription + " but instead received a value of " + completionIndicator + ". Sending back an Abort " +
                            "Transaction Flag.");
            out.write(ABORT_TRANSACTION);
            out.flush();
            throw new IOException("Expected to receive Transaction Completion Indicator from Peer " + peerDescription + " but instead received a value of " + completionIndicator);
        }

        logger.debug("Received Complete Transaction indicator from Peer {}", peerDescription);
        registerReceiveProvenanceEvents(flowFilesReceived, peerDescription, connectionId, startTimestamp);
        updateFlowFileRepository(flowFilesReceived, flowFileQueue);
        transferFlowFilesToQueue(flowFilesReceived, flowFileQueue);

        out.write(CONFIRM_COMPLETE_TRANSACTION);
        out.flush();
    }

    private void registerReceiveProvenanceEvents(final List<RemoteFlowFileRecord> flowFiles, final String nodeName, final String connectionId, final long startTimestamp) {
        final long duration = System.currentTimeMillis() - startTimestamp;

        final List<ProvenanceEventRecord> events = new ArrayList<>(flowFiles.size());
        for (final RemoteFlowFileRecord remoteFlowFile : flowFiles) {
            final FlowFileRecord flowFileRecord = remoteFlowFile.getFlowFile();

            final ProvenanceEventBuilder provenanceEventBuilder = new StandardProvenanceEventRecord.Builder()
                    .fromFlowFile(flowFileRecord)
                    .setEventType(ProvenanceEventType.RECEIVE)
                    .setTransitUri("nifi://" + nodeName + "/loadbalance/" + connectionId)
                    .setSourceSystemFlowFileIdentifier(remoteFlowFile.getRemoteUuid())
                    .setEventDuration(duration)
                    .setComponentId(connectionId)
                    .setComponentType("Load Balanced Connection");

            final ContentClaim contentClaim = flowFileRecord.getContentClaim();
            if (contentClaim != null) {
                final ResourceClaim resourceClaim = contentClaim.getResourceClaim();
                provenanceEventBuilder.setCurrentContentClaim(resourceClaim.getContainer(), resourceClaim.getSection(), resourceClaim.getId(),
                    contentClaim.getOffset() + flowFileRecord.getContentClaimOffset(), flowFileRecord.getSize());
            }

            final ProvenanceEventRecord provenanceEvent = provenanceEventBuilder.build();
            events.add(provenanceEvent);
        }

        provenanceRepository.registerEvents(events);
    }

    private void updateFlowFileRepository(final List<RemoteFlowFileRecord> flowFiles, final FlowFileQueue flowFileQueue) throws IOException {
        final List<RepositoryRecord> repoRecords = flowFiles.stream()
                .map(remoteFlowFile -> {
                    final StandardRepositoryRecord record = new StandardRepositoryRecord(flowFileQueue, remoteFlowFile.getFlowFile());
                    record.setDestination(flowFileQueue);
                    return record;
                })
                .collect(Collectors.toList());
        flowFileRepository.updateRepository(repoRecords);
    }

    private void transferFlowFilesToQueue(final List<RemoteFlowFileRecord> remoteFlowFiles, final LoadBalancedFlowFileQueue flowFileQueue) {
        final List<FlowFileRecord> flowFiles = remoteFlowFiles.stream().map(RemoteFlowFileRecord::getFlowFile).collect(Collectors.toList());
        flowFileQueue.receiveFromPeer(flowFiles);
    }

    private void verifyChecksum(final Checksum checksum, final InputStream in, final OutputStream out, final String peerDescription, final int flowFileCount) throws IOException {
        final long expectedChecksum = readChecksum(in);
        if (checksum.getValue() == expectedChecksum) {
            logger.debug("Checksum from Peer {} matched the checksum that was calculated. Writing confirmation.", peerDescription);
            out.write(CONFIRM_CHECKSUM);
            out.flush();
        } else {
            logger.error("Received {} FlowFiles from peer {} but the Checksum reported by the peer ({}) did not match the checksum that was calculated ({}). Will reject the transaction.",
                    flowFileCount, peerDescription, expectedChecksum, checksum.getValue());
            out.write(REJECT_CHECKSUM);
            out.flush();
            throw new TransactionAbortedException("Transaction with Peer " + peerDescription + " was aborted because the calculated checksum did not match the checksum provided by peer.");
        }
    }

    private long readChecksum(final InputStream in) throws IOException {
        final byte[] buffer = getDataBuffer();
        StreamUtils.read(in, buffer,8 );
        return ByteBuffer.wrap(buffer, 0, 8).getLong();
    }

    private byte[] getDataBuffer() {
        byte[] buffer = dataBuffer.get();
        if (buffer == null) {
            buffer = new byte[65536 + 4096];
            dataBuffer.set(buffer);
        }

        return buffer;
    }

    private String getConnectionID(final DataInputStream in, final String peerDescription) throws IOException {
        try {
            return in.readUTF();
        } catch (final EOFException eof) {
            logger.debug("Encountered EOFException when trying to receive Connection ID from Peer {}. Returning null for Connection ID", peerDescription);
            return null;
        }
    }

    private boolean isMoreFlowFiles(final DataInputStream in, final int protocolVersion) throws IOException {
        final int indicator = in.read();
        if (indicator < 0) {
            throw new EOFException();
        }

        if (indicator == MORE_FLOWFILES) {
            logger.debug("Peer indicates that there is another FlowFile in transaction");
            return true;
        }
        if (indicator == NO_MORE_FLOWFILES) {
            logger.debug("Peer indicates that there are no more FlowFiles in transaction");
            return false;
        }

        throw new IOException("Expected to receive 'More FlowFiles' indicator (" + MORE_FLOWFILES
            + ") or 'No More FlowFiles' indicator (" + NO_MORE_FLOWFILES + ") but received invalid value of " + indicator);
    }

    private RemoteFlowFileRecord receiveFlowFile(final DataInputStream dis, final OutputStream out, final ContentClaim contentClaim, final long claimOffset, final int protocolVersion,
                                                 final String peerDescription, final LoadBalanceCompression compression) throws IOException {
        final int metadataLength = dis.readInt();

        DataInputStream metadataIn = new DataInputStream(new LimitingInputStream(dis, metadataLength));
        if (compression != LoadBalanceCompression.DO_NOT_COMPRESS) {
            metadataIn = new DataInputStream(new GZIPInputStream(metadataIn));
        }

        final Map<String, String> attributes = readAttributes(metadataIn);
        final String sourceSystemUuid = attributes.get(CoreAttributes.UUID.key());

        logger.debug("Received Attributes {} from Peer {}", attributes, peerDescription);

        final long lineageStartDate = metadataIn.readLong();
        final long entryDate = metadataIn.readLong();

        final ContentClaimTriple contentClaimTriple = consumeContent(dis, out, contentClaim, claimOffset, peerDescription, compression == LoadBalanceCompression.COMPRESS_ATTRIBUTES_AND_CONTENT);

        final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
            .id(flowFileRepository.getNextFlowFileSequence())
            .addAttributes(attributes)
            .addAttribute(CoreAttributes.UUID.key(), UUID.randomUUID().toString())
            .contentClaim(contentClaimTriple.getContentClaim())
            .contentClaimOffset(contentClaimTriple.getClaimOffset())
            .size(contentClaimTriple.getContentLength())
            .entryDate(entryDate)
            .lineageStart(lineageStartDate, lineageStartIndex.getAndIncrement())
            .build();

        logger.debug("Received FlowFile {} with {} attributes and {} bytes of content", flowFileRecord, attributes.size(), contentClaimTriple.getContentLength());
        return new RemoteFlowFileRecord(sourceSystemUuid, flowFileRecord);
    }

    private Map<String, String> readAttributes(final DataInputStream in) throws IOException {
        final int attributeCount = in.readInt();
        final Map<String, String> attributes = new HashMap<>();
        for (int i = 0; i < attributeCount; i++) {
            final String key = readLongString(in);
            final String value = readLongString(in);

            logger.trace("Received attribute '{}' = '{}'", key, value);
            attributes.put(key, value);
        }

        return attributes;
    }

    private String readLongString(final DataInputStream in) throws IOException {
        final int stringLength = in.readInt();
        final byte[] bytes = new byte[stringLength];
        StreamUtils.fillBuffer(in, bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private ContentClaimTriple consumeContent(final DataInputStream in, final OutputStream out, final ContentClaim contentClaim, final long claimOffset,
                                              final String peerDescription, final boolean compressed) throws IOException {
        logger.debug("Consuming content from Peer {}", peerDescription);

        int dataFrameIndicator = in.read();
        if (dataFrameIndicator < 0) {
            throw new EOFException("Encountered End-of-File when expecting to read Data Frame Indicator from Peer " + peerDescription);
        }
        if (dataFrameIndicator == NO_DATA_FRAME) {
            logger.debug("Peer {} indicates that there is no Data Frame for the FlowFile", peerDescription);
            return new ContentClaimTriple(null, 0L, 0L);
        }
        if (dataFrameIndicator == ABORT_TRANSACTION) {
            throw new TransactionAbortedException("Peer " + peerDescription + " requested that transaction be aborted");
        }
        if (dataFrameIndicator != DATA_FRAME_FOLLOWS) {
            throw new IOException("Expected a Data Frame Indicator from Peer " + peerDescription + " but received a value of " + dataFrameIndicator);
        }

        int dataFrameLength = in.readUnsignedShort();
        logger.trace("Received Data Frame Length of {} for {}", dataFrameLength, peerDescription);

        byte[] buffer = getDataBuffer();

        long claimLength = 0;
        while (true) {
            final InputStream limitedIn = new LimitedInputStream(in, dataFrameLength);
            final ByteCountingInputStream bcis = new ByteCountingInputStream(limitedIn);
            final InputStream contentIn = compressed ? new GZIPInputStream(bcis) : bcis;
            final int decompressedSize = StreamUtils.fillBuffer(contentIn, buffer, false);

            if (bcis.getBytesRead() < dataFrameLength) {
                throw new EOFException("Expected to receive a Data Frame of length " + dataFrameLength + " bytes but received only " + bcis.getBytesRead() + " bytes");
            }

            out.write(buffer, 0, decompressedSize);

            claimLength += decompressedSize;

            dataFrameIndicator = in.read();
            if (dataFrameIndicator < 0) {
                throw new EOFException("Encountered End-of-File when expecting to receive a Data Frame Indicator");
            }
            if (dataFrameIndicator == NO_DATA_FRAME) {
                logger.debug("Peer {} indicated that no more data frames are available", peerDescription);
                break;
            }
            if (dataFrameIndicator == ABORT_TRANSACTION) {
                logger.debug("Peer {} requested that transaction be aborted by sending Data Frame Length of {}", peerDescription, dataFrameLength);
                throw new TransactionAbortedException("Peer " + peerDescription + " requested that transaction be aborted");
            }
            if (dataFrameIndicator != DATA_FRAME_FOLLOWS) {
                throw new IOException("Expected a Data Frame Indicator from Peer " + peerDescription + " but received a value of " + dataFrameIndicator);
            }

            dataFrameLength = in.readUnsignedShort();
            logger.trace("Received Data Frame Length of {} for {}", dataFrameLength, peerDescription);
        }

        return new ContentClaimTriple(contentClaim, claimOffset, claimLength);
    }

    private static class ContentClaimTriple {
        private final ContentClaim contentClaim;
        private final long claimOffset;
        private final long contentLength;

        public ContentClaimTriple(ContentClaim contentClaim, long claimOffset, long contentLength) {
            this.contentClaim = contentClaim;
            this.claimOffset = claimOffset;
            this.contentLength = contentLength;
        }

        public ContentClaim getContentClaim() {
            return contentClaim;
        }

        public long getClaimOffset() {
            return claimOffset;
        }

        public long getContentLength() {
            return contentLength;
        }
    }

    private static class RemoteFlowFileRecord {
        private final String remoteUuid;
        private final FlowFileRecord flowFile;

        public RemoteFlowFileRecord(final String remoteUuid, final FlowFileRecord flowFile) {
            this.remoteUuid = remoteUuid;
            this.flowFile = flowFile;
        }

        public String getRemoteUuid() {
            return remoteUuid;
        }

        public FlowFileRecord getFlowFile() {
            return flowFile;
        }
    }
}
