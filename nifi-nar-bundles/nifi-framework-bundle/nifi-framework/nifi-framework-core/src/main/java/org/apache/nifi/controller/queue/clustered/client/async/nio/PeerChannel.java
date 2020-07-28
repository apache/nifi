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
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.OptionalInt;

public class PeerChannel implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(PeerChannel.class);

    private final SocketChannel socketChannel;
    private final SSLEngine sslEngine;
    private final String peerDescription;

    private final ByteBuffer singleByteBuffer = ByteBuffer.allocate(1);
    private ByteBuffer destinationBuffer = ByteBuffer.allocate(16 * 1024); // buffer that SSLEngine is to write into
    private ByteBuffer streamBuffer = ByteBuffer.allocate(16 * 1024); // buffer for data that is read from SocketChannel
    private ByteBuffer applicationBuffer = ByteBuffer.allocate(0); // buffer for application-level data that is ready to be served up (i.e., already decrypted if necessary)

    public PeerChannel(final SocketChannel socketChannel, final SSLEngine sslEngine, final String peerDescription) {
        this.socketChannel = socketChannel;
        this.sslEngine = sslEngine;
        this.peerDescription = peerDescription;
    }


    @Override
    public void close() throws IOException {
        socketChannel.close();
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

    public boolean write(final byte b) throws IOException {
        singleByteBuffer.clear();
        singleByteBuffer.put(b);
        singleByteBuffer.rewind();

        final ByteBuffer prepared = prepareForWrite(singleByteBuffer);
        final int bytesWritten = write(prepared);
        return bytesWritten > 0;
    }

    public OptionalInt read() throws IOException {
        singleByteBuffer.clear();
        final int bytesRead = read(singleByteBuffer);
        if (bytesRead < 0) {
             return OptionalInt.of(-1);
        }
        if (bytesRead == 0) {
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
        if (sslEngine == null) {
            return plaintext;
        }


        ByteBuffer prepared = ByteBuffer.allocate(Math.min(85, plaintext.capacity() - plaintext.position()));
        while (plaintext.hasRemaining()) {
            encrypt(plaintext);

            final int bytesRemaining = prepared.capacity() - prepared.position();
            if (bytesRemaining < destinationBuffer.remaining()) {
                final ByteBuffer temp = ByteBuffer.allocate(prepared.capacity() + sslEngine.getSession().getApplicationBufferSize());
                prepared.flip();
                temp.put(prepared);
                prepared = temp;
            }

            prepared.put(destinationBuffer);
        }

        prepared.flip();
        return prepared;
    }

    public int write(final ByteBuffer preparedBuffer) throws IOException {
        return socketChannel.write(preparedBuffer);
    }


    public int read(final ByteBuffer dst) throws IOException {
        // If we have data ready to go, then go ahead and copy it.
        final int bytesCopied = copy(applicationBuffer, dst);
        if (bytesCopied != 0) {
            return bytesCopied;
        }

        final int bytesRead = socketChannel.read(streamBuffer);
        if (bytesRead < 1) {
            return bytesRead;
        }

        if (bytesRead > 0) {
            logger.trace("Read {} bytes from SocketChannel", bytesRead);
        }

        streamBuffer.flip();

        try {
            if (sslEngine == null) {
                cloneToApplicationBuffer(streamBuffer);
                return copy(applicationBuffer, dst);
            } else {
                final boolean decrypted = decrypt(streamBuffer);
                logger.trace("Decryption after reading those bytes successful = {}", decrypted);

                if (decrypted) {
                    cloneToApplicationBuffer(destinationBuffer);
                    logger.trace("Cloned destination buffer to application buffer");

                    return copy(applicationBuffer, dst);
                } else {
                    // Not enough data to decrypt. Compact the buffer so that we keep the data we have
                    // but prepare the buffer to be written to again.
                    logger.debug("Not enough data to decrypt. Will need to consume more data before decrypting");
                    streamBuffer.compact();
                    return 0;
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

            switch (result.getStatus()) {
                case OK:
                    destinationBuffer.flip();
                    return;
                case CLOSED:
                    throw new IOException("Failed to encrypt data to write to Peer " + peerDescription + " because Peer unexpectedly closed connection");
                case BUFFER_OVERFLOW:
                    // destinationBuffer is not large enough. Need to increase the size.
                    final ByteBuffer tempBuffer = ByteBuffer.allocate(destinationBuffer.capacity() + sslEngine.getSession().getApplicationBufferSize());
                    destinationBuffer.flip();
                    tempBuffer.put(destinationBuffer);
                    destinationBuffer = tempBuffer;
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

            switch (result.getStatus()) {
                case OK:
                    destinationBuffer.flip();
                    return true;
                case CLOSED:
                    throw new IOException("Failed to decrypt data from Peer " + peerDescription + " because Peer unexpectedly closed connection");
                case BUFFER_OVERFLOW:
                    // ecnryptedBuffer is not large enough. Need to increase the size.
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


    public void performHandshake() throws IOException {
        if (sslEngine == null) {
            return;
        }

        sslEngine.beginHandshake();

        final ByteBuffer emptyMessage = ByteBuffer.allocate(0);
        ByteBuffer unwrapBuffer = ByteBuffer.allocate(0);

        while (true) {
            final SSLEngineResult.HandshakeStatus handshakeStatus = sslEngine.getHandshakeStatus();

            switch (handshakeStatus) {
                case FINISHED:
                case NOT_HANDSHAKING:
                    streamBuffer.clear();
                    destinationBuffer.clear();
                    logger.debug("Completed SSL Handshake with Peer {}", peerDescription);
                    return;

                case NEED_TASK:
                    logger.debug("SSL Handshake with Peer {} Needs Task", peerDescription);

                    Runnable runnable;
                    while ((runnable = sslEngine.getDelegatedTask()) != null) {
                        runnable.run();
                    }
                    break;

                case NEED_WRAP:
                    logger.trace("SSL Handshake with Peer {} Needs Wrap", peerDescription);

                    encrypt(emptyMessage);
                    final int bytesWritten = write(destinationBuffer);
                    logger.debug("Wrote {} bytes for NEED_WRAP portion of Handshake", bytesWritten);
                    break;

                case NEED_UNWRAP:
                    logger.trace("SSL Handshake with Peer {} Needs Unwrap", peerDescription);

                    while (sslEngine.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_UNWRAP) {
                        final boolean decrypted = decrypt(unwrapBuffer);
                        if (decrypted) {
                            logger.trace("Decryption was successful for NEED_UNWRAP portion of Handshake");
                            break;
                        }

                        if (unwrapBuffer.capacity() - unwrapBuffer.position() < 1) {
                            logger.trace("Enlarging size of Buffer for NEED_UNWRAP portion of Handshake");

                            // destinationBuffer is not large enough. Need to increase the size.
                            final ByteBuffer tempBuffer = ByteBuffer.allocate(unwrapBuffer.capacity() + sslEngine.getSession().getApplicationBufferSize());
                            tempBuffer.put(unwrapBuffer);
                            unwrapBuffer = tempBuffer;
                            unwrapBuffer.flip();
                            continue;
                        }

                        logger.trace("Need to read more bytes for NEED_UNWRAP portion of Handshake");

                        // Need to read more data.
                        unwrapBuffer.compact();
                        final int bytesRead = socketChannel.read(unwrapBuffer);
                        unwrapBuffer.flip();
                        logger.debug("Read {} bytes for NEED_UNWRAP portion of Handshake", bytesRead);
                    }

                    break;
            }
        }
    }
}
