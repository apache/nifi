package org.apache.nifi.controller.queue.clustered.client;

import org.apache.nifi.controller.queue.clustered.server.TransactionAbortedException;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.stream.io.StreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;
import java.util.zip.Checksum;

import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.ABORT_TRANSACTION;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.COMPLETE_TRANSACTION;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.CONFIRM_COMPLETE_TRANSACTION;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.DATA_FRAME_FOLLOWS;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.MORE_FLOWFILES;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.NO_DATA_FRAME;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.NO_MORE_FLOWFILES;
import static org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants.REJECT_CHECKSUM;


public class SocketLoadBalanceTransaction implements LoadBalanceTransaction {
    private static final Logger logger = LoggerFactory.getLogger(SocketLoadBalanceTransaction.class);

    private final Socket socket;
    private final DataOutputStream out;
    private final InputStream in;
    private final LoadBalanceFlowFileCodec codec;
    private final String connectionId;
    private final String peerDescription;
    private final Checksum checksum;

    private byte[] buffer;
    private int flowFileCount = 0;

    public SocketLoadBalanceTransaction(final Socket socket, final LoadBalanceFlowFileCodec codec, final String connectionId, final String peerDescription) throws IOException {
        this.socket = socket;
        this.checksum = new CRC32();
        this.out = new DataOutputStream(new CheckedOutputStream(socket.getOutputStream(), checksum));
        this.in = socket.getInputStream();
        this.codec = codec;
        this.connectionId = connectionId;
        this.peerDescription = peerDescription;

        out.writeUTF(connectionId);
    }


    @Override
    public void close() throws IOException {
        socket.close();
    }

    @Override
    public void send(final FlowFileRecord flowFile, final InputStream content) throws IOException {
        logger.debug("Writing {} to Peer {}", flowFile, peerDescription);

        out.write(MORE_FLOWFILES);
        codec.encode(flowFile, out);

        writeContent(content, out);
        flowFileCount++;
    }

    @Override
    public void abort() throws IOException {
        out.write(ABORT_TRANSACTION);
        out.flush();
    }

    @Override
    public void complete() throws IOException {
        logger.debug("Completing transaction with Peer {}", peerDescription);

        out.write(NO_MORE_FLOWFILES);

        validateChecksum(out, in);
        completeTransaction(out, in);

        logger.info("Successfully completed Transaction to send {} FlowFiles to Peer {} for Connection {}", flowFileCount, peerDescription, connectionId);
    }

    private void validateChecksum(final DataOutputStream out, final InputStream in) throws IOException {
        final long checksumValue = checksum.getValue();
        out.writeLong(checksumValue);
        out.flush();

        logger.debug("Wrote Checksum value of {} to Peer {}", checksumValue, peerDescription);

        final int checksumResponse = in.read();
        logger.debug("Received Checksum Response of {} from Peer {}", checksumResponse, peerDescription);

        if (checksumResponse < 0) {
            throw new EOFException("Expected to receive a CONFIRM_CHECKSUM response from Peer " + peerDescription + " but encountered EOF");
        }

        if (checksumResponse == REJECT_CHECKSUM) {
            throw new TransactionAbortedException("After transferring FlowFiles to Peer " + peerDescription + " received a REJECT_CHECKSUM response. Aborting transaction.");
        }

        logger.debug("Checksum confirmed. Writing COMPLETE_TRANSACTION flag");
    }

    private void completeTransaction(final DataOutputStream out, final InputStream in) throws IOException {
        out.write(COMPLETE_TRANSACTION);
        out.flush();

        final int completeTransactionResponse = in.read();
        logger.debug("Received Complete Transaction Response of {} from Peer {}", completeTransactionResponse, peerDescription);

        if (completeTransactionResponse < 0) {
            throw new EOFException("Expected to receive a CONFIRM_TRANSACTION response from Peer " + peerDescription + " but encountered EOF");
        }

        if (completeTransactionResponse == ABORT_TRANSACTION) {
            throw new TransactionAbortedException("Confirmed checksum when writing data to Peer " + peerDescription + " but Peer aborted transaction instead of completing it");
        }

        if (completeTransactionResponse != CONFIRM_COMPLETE_TRANSACTION) {
            throw new IOException("Expected a CONFIRM_COMPLETE_TRANSACTION response from Peer " + peerDescription + " but received a value of " + completeTransactionResponse);
        }
    }


    private void writeContent(final InputStream in, final DataOutputStream out) throws IOException {
        if (buffer == null) {
            buffer = new byte[65536];
        }

        while (true) {
            final int bytesBuffered = StreamUtils.fillBuffer(in, buffer, false);
            if (bytesBuffered == 0) {
                out.write(NO_DATA_FRAME);
                return;
            }

            out.write(DATA_FRAME_FOLLOWS);
            out.writeShort(bytesBuffered);
            out.write(buffer, 0, bytesBuffered);
        }
    }
}
