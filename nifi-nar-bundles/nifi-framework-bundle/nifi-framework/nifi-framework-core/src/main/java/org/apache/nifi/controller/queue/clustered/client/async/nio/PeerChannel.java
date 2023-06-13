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

package org.apache.nifi.controller.queue.clustered.client.async.nio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.OptionalInt;

public class PeerChannel implements Closeable {
    private static final int END_OF_FILE = -1;

    private static final int EMPTY_BUFFER = 0;

    private static final Logger logger = LoggerFactory.getLogger(PeerChannel.class);

    private final SocketChannel socketChannel;
    private final SSLEngine sslEngine;
    private final String peerDescription;

    private final ByteBuffer singleByteBuffer = ByteBuffer.allocate(1);
    private ByteBuffer destinationBuffer = ByteBuffer.allocate(16 * 1024); // buffer that SSLEngine is to write into
    private final ByteBuffer streamBuffer = ByteBuffer.allocate(16 * 1024); // buffer for data that is read from SocketChannel
    private ByteBuffer applicationBuffer = ByteBuffer.allocate(0); // buffer for application-level data that is ready to be served up (i.e., already decrypted if necessary)

    public PeerChannel(final SocketChannel socketChannel, final SSLEngine sslEngine, final String peerDescription) {
        this.socketChannel = socketChannel;
        this.sslEngine = sslEngine;
        this.peerDescription = peerDescription;
    }

    /**
     * Close Socket Channel and process SSLEngine close notifications when configured
     *
     * @throws IOException Thrown on failure to close Socket Channel or process SSLEngine operations
     */
    @Override
    public void close() throws IOException {
        try {
            if (sslEngine == null) {
                logger.debug("Closing Peer Channel [{}] SSLEngine not configured", peerDescription);
            } else {
                logger.debug("Closing Peer Channel [{}] SSLEngine close started", peerDescription);
                sslEngine.closeOutbound();

                // Send TLS close notification packets available after initiating SSLEngine.closeOutbound()
                final ByteBuffer inputBuffer = ByteBuffer.allocate(0);
                final ByteBuffer outputBuffer = ByteBuffer.allocate(sslEngine.getSession().getPacketBufferSize());

                SSLEngineResult wrapResult = sslEngine.wrap(inputBuffer, outputBuffer);
                SSLEngineResult.Status status = wrapResult.getStatus();
                outputBuffer.flip();
                if (SSLEngineResult.Status.OK == status) {
                    write(outputBuffer);
                    outputBuffer.clear();
                    wrapResult = sslEngine.wrap(inputBuffer, outputBuffer);
                    status = wrapResult.getStatus();
                }
                if (SSLEngineResult.Status.CLOSED == status) {
                    write(outputBuffer);
                } else {
                    throw new SSLException(String.format("Closing Peer Channel [%s] Invalid Wrap Result Status [%s]", peerDescription, status));
                }

                logger.debug("Closing Peer Channel [{}] SSLEngine close completed", peerDescription);
            }
        } finally {
            logger.debug("Closing Peer Channel [{}] Socket Channel close started", peerDescription);
            socketChannel.close();
        }
    }

    public boolean isConnected() {
        return socketChannel.isConnected();
    }

    public boolean isOpen() {
        return socketChannel.isOpen();
    }

    public String getPeerDescription() {
        return peerDescription;
    }

    /**
     * Write one byte to the channel
     *
     * @param b Byte to be written
     * @return Status of write operation returns true on success
     * @throws IOException Thrown on failure to write to the Socket Channel
     */
    public boolean write(final byte b) throws IOException {
        singleByteBuffer.clear();
        singleByteBuffer.put(b);
        singleByteBuffer.rewind();

        final ByteBuffer prepared = prepareForWrite(singleByteBuffer);
        final int bytesWritten = write(prepared);
        return bytesWritten > 0;
    }

    /**
     * Read one byte as an unsigned integer from the channel
     *
     * @return Returns empty when zero bytes are available and returns negative one when the channel is closed
     * @throws IOException Thrown on failure to read from Socket Channel
     */
    public OptionalInt read() throws IOException {
        singleByteBuffer.clear();
        final int bytesRead = read(singleByteBuffer);
        if (bytesRead < 0) {
             return OptionalInt.of(END_OF_FILE);
        } else if (bytesRead == EMPTY_BUFFER) {
            return OptionalInt.empty();
        }

        singleByteBuffer.flip();

        final byte read = singleByteBuffer.get();
        return OptionalInt.of(read & 0xFF);
    }

    /**
     * Reads the given ByteBuffer of data and returns a new ByteBuffer (which is "flipped" / ready to be read). The newly returned
     * ByteBuffer will be written to be written via the {@link #write(ByteBuffer)} method. I.e., it will have already been encrypted, if
     * necessary, and any other decorations that need to be applied before sending will already have been applied.
     *
     * @param plaintext the data to be prepped
     * @return a ByteBuffer containing the prepared data
     * @throws IOException if a failure occurs while encrypting the data
     */
    public ByteBuffer prepareForWrite(final ByteBuffer plaintext) throws IOException {
        logger.trace("Channel [{}] Buffer wrap started: Input Bytes [{}]", peerDescription, plaintext.remaining());
        if (sslEngine == null) {
            return plaintext;
        }

        ByteBuffer prepared = ByteBuffer.allocate(Math.min(85, plaintext.capacity() - plaintext.position()));
        while (plaintext.hasRemaining()) {
            encrypt(plaintext);

            final int destinationBufferRemaining = destinationBuffer.remaining();
            if (prepared.remaining() < destinationBufferRemaining) {
                // Expand Prepared Buffer to hold current bytes plus remaining size of Destination Buffer
                final ByteBuffer expanded = ByteBuffer.allocate(prepared.capacity() + destinationBufferRemaining);
                prepared.flip();
                expanded.put(prepared);
                prepared = expanded;
            }

            prepared.put(destinationBuffer);
        }

        prepared.flip();
        logger.trace("Channel [{}] Buffer wrap completed: Prepared Bytes [{}]", peerDescription, prepared.remaining());
        return prepared;
    }

    /**
     * Write prepared buffer to Socket Channel
     *
     * @param preparedBuffer Buffer must contain bytes processed through prepareForWrite() when TLS is enabled
     * @return Number of bytes written according to SocketChannel.write()
     * @throws IOException Thrown on failure to write to the Socket Channel
     */
    public int write(final ByteBuffer preparedBuffer) throws IOException {
        return socketChannel.write(preparedBuffer);
    }

    /**
     * Read application data bytes into the provided buffer
     *
     * @param dst Buffer to be populated with application data bytes
     * @return Number of bytes read into the provided buffer
     * @throws IOException Thrown on failure to read from the Socket Channel
     */
    public int read(final ByteBuffer dst) throws IOException {
        // If we have data ready to go, then go ahead and copy it.
        final int bytesCopied = copy(applicationBuffer, dst);
        if (bytesCopied != 0) {
            return bytesCopied;
        }

        final int bytesRead = socketChannel.read(streamBuffer);
        logger.trace("Channel [{}] Socket read completed: bytes [{}]", peerDescription, bytesRead);
        if (bytesRead == END_OF_FILE) {
            return END_OF_FILE;
        } else if (streamBuffer.remaining() == EMPTY_BUFFER) {
            return EMPTY_BUFFER;
        }

        streamBuffer.flip();

        try {
            if (sslEngine == null) {
                cloneToApplicationBuffer(streamBuffer);
                return copy(applicationBuffer, dst);
            } else {
                final boolean decrypted = decrypt(streamBuffer);
                logger.trace("Channel [{}] Decryption completed [{}]", peerDescription, decrypted);

                if (decrypted) {
                    cloneToApplicationBuffer(destinationBuffer);
                    logger.trace("Cloned destination buffer to application buffer");

                    return copy(applicationBuffer, dst);
                } else {
                    // Not enough data to decrypt. Compact the buffer so that we keep the data we have
                    // but prepare the buffer to be written to again.
                    logger.trace("Channel [{}] Socket Channel read required", peerDescription);
                    return EMPTY_BUFFER;
                }
            }
        } finally {
            streamBuffer.compact();
        }
    }

    private void cloneToApplicationBuffer(final ByteBuffer buffer) {
        if (applicationBuffer.capacity() < buffer.remaining()) {
            applicationBuffer = ByteBuffer.allocate(buffer.remaining());
        } else {
            applicationBuffer.clear();
        }

        applicationBuffer.put(buffer);
        applicationBuffer.flip();
    }

    private int copy(final ByteBuffer src, final ByteBuffer dst) {
        if (src != null && src.hasRemaining()) {
            final int bytesToCopy = Math.min(dst.remaining(), src.remaining());
            if (bytesToCopy < 1) {
                return bytesToCopy;
            }

            final byte[] buff = new byte[bytesToCopy];
            src.get(buff);
            dst.put(buff);
            return bytesToCopy;
        }

        return 0;
    }


    /**
     * Encrypts the given buffer of data, writing the result into {@link #destinationBuffer}.
     * @param plaintext the data to encrypt
     * @throws IOException if the Peer closes the connection abruptly or if unable to perform the encryption
     */
    private void encrypt(final ByteBuffer plaintext) throws IOException {
        if (sslEngine == null) {
            throw new SSLException("Unable to encrypt message because no SSLEngine has been configured");
        }

        destinationBuffer.clear();

        while (true) {
            final SSLEngineResult result = sslEngine.wrap(plaintext, destinationBuffer);
            logOperationResult("WRAP", result);

            switch (result.getStatus()) {
                case OK:
                    destinationBuffer.flip();
                    return;
                case CLOSED:
                    throw new IOException("Failed to encrypt data to write to Peer " + peerDescription + " because Peer unexpectedly closed connection");
                case BUFFER_OVERFLOW:
                    // Expand Destination Buffer using current capacity plus encrypted Packet Buffer Size
                    final int packetBufferSize = sslEngine.getSession().getPacketBufferSize();
                    final ByteBuffer expanded = ByteBuffer.allocate(destinationBuffer.capacity() + packetBufferSize);
                    destinationBuffer.flip();
                    expanded.put(destinationBuffer);
                    destinationBuffer = expanded;
                    break;
                case BUFFER_UNDERFLOW:
                    // We should never get this result on a call to SSLEngine.wrap(), only on a call to unwrap().
                    throw new IOException("Received unexpected Buffer Underflow result when encrypting data to write to Peer " + peerDescription);
            }
        }
    }

    /**
     * Attempts to decrypt the given buffer of data, writing the result into {@link #destinationBuffer}. If successful, will return <code>true</code>.
     * If more data is needed in order to perform the decryption, will return <code>false</code>.
     *
     * @param encrypted the ByteBuffer containing the data to decrypt
     * @return <code>true</code> if decryption was successful, <code>false</code> otherwise
     * @throws IOException if the Peer closed the connection or if unable to decrypt the message
     */
    private boolean decrypt(final ByteBuffer encrypted) throws IOException {
        if (sslEngine == null) {
            throw new SSLException("Unable to decrypt message because no SSLEngine has been configured");
        }

        destinationBuffer.clear();

        while (true) {
            final SSLEngineResult result = sslEngine.unwrap(encrypted, destinationBuffer);
            logOperationResult("UNWRAP", result);

            switch (result.getStatus()) {
                case OK:
                    if (SSLEngineResult.HandshakeStatus.FINISHED == result.getHandshakeStatus()) {
                        // RFC 8446 Section 4.6 describes Post-Handshake Messages for TLS 1.3
                        // Break out of switch statement to call SSLEngine.unwrap() again
                        break;
                    }
                    destinationBuffer.flip();
                    return true;
                case CLOSED:
                    throw new IOException("Failed to decrypt data from Peer " + peerDescription + " because Peer unexpectedly closed connection");
                case BUFFER_OVERFLOW:
                    // encryptedBuffer is not large enough. Need to increase the size.
                    final ByteBuffer tempBuffer = ByteBuffer.allocate(encrypted.position() + sslEngine.getSession().getApplicationBufferSize());
                    destinationBuffer.flip();
                    tempBuffer.put(destinationBuffer);
                    destinationBuffer = tempBuffer;

                    break;
                case BUFFER_UNDERFLOW:
                    // Not enough data to decrypt. Must read more from the channel.
                    return false;
            }
        }
    }

    /**
     * Perform TLS handshake when SSLEngine configured
     *
     * @throws IOException Thrown on failure to handle socket communication or TLS packet processing
     */
    public void performHandshake() throws IOException {
        if (sslEngine == null) {
            return;
        }

        sslEngine.beginHandshake();

        final ByteBuffer emptyMessage = ByteBuffer.allocate(0);
        ByteBuffer unwrapBuffer = ByteBuffer.allocate(0);

        while (true) {
            final SSLEngineResult.HandshakeStatus handshakeStatus = sslEngine.getHandshakeStatus();
            logHandshakeStatus(handshakeStatus);

            switch (handshakeStatus) {
                case FINISHED:
                case NOT_HANDSHAKING:
                    streamBuffer.clear();
                    destinationBuffer.clear();
                    logHandshakeCompleted();
                    return;

                case NEED_TASK:
                    Runnable runnable;
                    while ((runnable = sslEngine.getDelegatedTask()) != null) {
                        runnable.run();
                    }
                    break;

                case NEED_WRAP:
                    encrypt(emptyMessage);
                    final int bytesWritten = write(destinationBuffer);
                    logHandshakeStatusBytes(handshakeStatus, "Socket write completed", bytesWritten);
                    break;

                case NEED_UNWRAP:
                    while (sslEngine.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_UNWRAP) {
                        final boolean decrypted = decrypt(unwrapBuffer);
                        final SSLEngineResult.HandshakeStatus unwrapHandshakeStatus = sslEngine.getHandshakeStatus();
                        if (decrypted || unwrapHandshakeStatus == SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
                            logHandshakeStatus(unwrapHandshakeStatus, "Decryption completed");
                            break;
                        }

                        if (unwrapBuffer.capacity() - unwrapBuffer.position() < 1) {
                            logHandshakeStatus(unwrapHandshakeStatus, "Increasing unwrap buffer for decryption");
                            final ByteBuffer tempBuffer = ByteBuffer.allocate(unwrapBuffer.capacity() + sslEngine.getSession().getApplicationBufferSize());
                            tempBuffer.put(unwrapBuffer);
                            unwrapBuffer = tempBuffer;
                            unwrapBuffer.flip();
                            continue;
                        }

                        logHandshakeStatus(unwrapHandshakeStatus, "Socket read started");
                        unwrapBuffer.compact();
                        final int bytesRead = socketChannel.read(unwrapBuffer);
                        unwrapBuffer.flip();

                        logHandshakeStatusBytes(unwrapHandshakeStatus, "Socket read completed", bytesRead);
                    }

                    break;
            }
        }
    }

    private void logOperationResult(final String operation, final SSLEngineResult sslEngineResult) {
        logger.trace("Channel [{}] {} [{}]", peerDescription, operation, sslEngineResult);
    }

    private void logHandshakeCompleted() {
        final SSLSession sslSession = sslEngine.getSession();
        logger.debug("Channel [{}] Handshake Completed Protocol [{}] Cipher Suite [{}]", peerDescription, sslSession.getProtocol(), sslSession.getCipherSuite());
    }

    private void logHandshakeStatus(final SSLEngineResult.HandshakeStatus handshakeStatus) {
        logger.debug("Channel [{}] Handshake Status [{}]", peerDescription, handshakeStatus);
    }

    private void logHandshakeStatus(final SSLEngineResult.HandshakeStatus handshakeStatus, final String operation) {
        logger.debug("Channel [{}] Handshake Status [{}] {}", peerDescription, handshakeStatus, operation);
    }

    private void logHandshakeStatusBytes(final SSLEngineResult.HandshakeStatus handshakeStatus, final String operation, final int bytes) {
        logger.debug("Channel [{}] Handshake Status [{}] {} Bytes [{}]", peerDescription, handshakeStatus, operation, bytes);
    }
}
