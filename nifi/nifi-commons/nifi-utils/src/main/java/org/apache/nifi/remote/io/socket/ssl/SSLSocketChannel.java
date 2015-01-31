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
package org.apache.nifi.remote.io.socket.ssl;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.Status;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.security.cert.CertificateExpiredException;
import javax.security.cert.CertificateNotYetValidException;
import javax.security.cert.X509Certificate;

import org.apache.nifi.remote.exception.TransmissionDisabledException;
import org.apache.nifi.remote.io.socket.BufferStateManager;
import org.apache.nifi.remote.io.socket.BufferStateManager.Direction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSLSocketChannel implements Closeable {

    public static final int MAX_WRITE_SIZE = 65536;

    private static final Logger logger = LoggerFactory.getLogger(SSLSocketChannel.class);
    private static final long BUFFER_FULL_EMPTY_WAIT_NANOS = TimeUnit.NANOSECONDS.convert(10, TimeUnit.MILLISECONDS);

    private final String hostname;
    private final int port;
    private final SSLEngine engine;
    private final SocketAddress socketAddress;

    private BufferStateManager streamInManager;
    private BufferStateManager streamOutManager;
    private BufferStateManager appDataManager;

    private SocketChannel channel;

    private final byte[] oneByteBuffer = new byte[1];

    private int timeoutMillis = 30000;
    private volatile boolean connected = false;
    private boolean handshaking = false;
    private boolean closed = false;
    private volatile boolean interrupted = false;

    public SSLSocketChannel(final SSLContext sslContext, final String hostname, final int port, final boolean client) throws IOException {
        this.socketAddress = new InetSocketAddress(hostname, port);
        this.channel = SocketChannel.open();
        this.hostname = hostname;
        this.port = port;
        this.engine = sslContext.createSSLEngine();
        this.engine.setUseClientMode(client);
        engine.setNeedClientAuth(true);

        streamInManager = new BufferStateManager(ByteBuffer.allocate(engine.getSession().getPacketBufferSize()));
        streamOutManager = new BufferStateManager(ByteBuffer.allocate(engine.getSession().getPacketBufferSize()));
        appDataManager = new BufferStateManager(ByteBuffer.allocate(engine.getSession().getApplicationBufferSize()));
    }

    public SSLSocketChannel(final SSLContext sslContext, final SocketChannel socketChannel, final boolean client) throws IOException {
        if (!socketChannel.isConnected()) {
            throw new IllegalArgumentException("Cannot pass an un-connected SocketChannel");
        }

        this.channel = socketChannel;

        this.socketAddress = socketChannel.getRemoteAddress();
        final Socket socket = socketChannel.socket();
        this.hostname = socket.getInetAddress().getHostName();
        this.port = socket.getPort();

        this.engine = sslContext.createSSLEngine();
        this.engine.setUseClientMode(client);
        engine.setNeedClientAuth(true);

        streamInManager = new BufferStateManager(ByteBuffer.allocate(engine.getSession().getPacketBufferSize()));
        streamOutManager = new BufferStateManager(ByteBuffer.allocate(engine.getSession().getPacketBufferSize()));
        appDataManager = new BufferStateManager(ByteBuffer.allocate(engine.getSession().getApplicationBufferSize()));
    }

    public void setTimeout(final int millis) {
        this.timeoutMillis = millis;
    }

    public int getTimeout() {
        return timeoutMillis;
    }

    public void connect() throws SSLHandshakeException, IOException {
        try {
            channel.configureBlocking(false);
            if (!channel.isConnected()) {
                final long startTime = System.currentTimeMillis();

                if (!channel.connect(socketAddress)) {
                    while (!channel.finishConnect()) {
                        if (interrupted) {
                            throw new TransmissionDisabledException();
                        }
                        if (System.currentTimeMillis() > startTime + timeoutMillis) {
                            throw new SocketTimeoutException("Timed out connecting to " + hostname + ":" + port);
                        }

                        try {
                            Thread.sleep(50L);
                        } catch (final InterruptedException e) {
                        }
                    }
                }
            }
            engine.beginHandshake();

            performHandshake();
            logger.debug("{} Successfully completed SSL handshake", this);

            streamInManager.clear();
            streamOutManager.clear();
            appDataManager.clear();

            connected = true;
        } catch (final Exception e) {
            logger.error("{} Failed to connect due to {}", this, e);
            if (logger.isDebugEnabled()) {
                logger.error("", e);
            }
            closeQuietly(channel);
            engine.closeInbound();
            engine.closeOutbound();
            throw e;
        }
    }

    public String getDn() throws CertificateExpiredException, CertificateNotYetValidException, SSLPeerUnverifiedException {
        final X509Certificate[] certs = engine.getSession().getPeerCertificateChain();
        if (certs == null || certs.length == 0) {
            throw new SSLPeerUnverifiedException("No certificates found");
        }

        final X509Certificate cert = certs[0];
        cert.checkValidity();
        return cert.getSubjectDN().getName().trim();
    }

    private void performHandshake() throws IOException {
        // Generate handshake message
        final byte[] emptyMessage = new byte[0];
        handshaking = true;
        logger.debug("{} Performing Handshake", this);

        try {
            while (true) {
                switch (engine.getHandshakeStatus()) {
                    case FINISHED:
                        return;
                    case NEED_WRAP: {
                        final ByteBuffer appDataOut = ByteBuffer.wrap(emptyMessage);

                        final ByteBuffer outboundBuffer = streamOutManager.prepareForWrite(engine.getSession().getApplicationBufferSize());

                        final SSLEngineResult wrapHelloResult = engine.wrap(appDataOut, outboundBuffer);
                        if (wrapHelloResult.getStatus() == Status.BUFFER_OVERFLOW) {
                            streamOutManager.prepareForWrite(engine.getSession().getApplicationBufferSize());
                            continue;
                        }

                        if (wrapHelloResult.getStatus() != Status.OK) {
                            throw new SSLHandshakeException("Could not generate SSL Handshake information: SSLEngineResult: "
                                    + wrapHelloResult.toString());
                        }

                        logger.trace("{} Handshake response after wrapping: {}", this, wrapHelloResult);

                        final ByteBuffer readableStreamOut = streamOutManager.prepareForRead(1);
                        final int bytesToSend = readableStreamOut.remaining();
                        writeFully(readableStreamOut);
                        logger.trace("{} Sent {} bytes of wrapped data for handshake", this, bytesToSend);

                        streamOutManager.clear();
                    }
                    continue;
                    case NEED_UNWRAP: {
                        final ByteBuffer readableDataIn = streamInManager.prepareForRead(0);
                        final ByteBuffer appData = appDataManager.prepareForWrite(engine.getSession().getApplicationBufferSize());

                        // Read handshake response from other side
                        logger.trace("{} Unwrapping: {} to {}", new Object[]{this, readableDataIn, appData});
                        SSLEngineResult handshakeResponseResult = engine.unwrap(readableDataIn, appData);
                        logger.trace("{} Handshake response after unwrapping: {}", this, handshakeResponseResult);

                        if (handshakeResponseResult.getStatus() == Status.BUFFER_UNDERFLOW) {
                            final ByteBuffer writableDataIn = streamInManager.prepareForWrite(engine.getSession().getPacketBufferSize());
                            final int bytesRead = readData(writableDataIn);
                            if (bytesRead > 0) {
                                logger.trace("{} Read {} bytes for handshake", this, bytesRead);
                            }

                            if (bytesRead < 0) {
                                throw new SSLHandshakeException("Reached End-of-File marker while performing handshake");
                            }
                        } else if (handshakeResponseResult.getStatus() == Status.CLOSED) {
                            throw new IOException("Channel was closed by peer during handshake");
                        } else {
                            streamInManager.compact();
                            appDataManager.clear();
                        }
                    }
                    break;
                    case NEED_TASK:
                        performTasks();
                        continue;
                    case NOT_HANDSHAKING:
                        return;
                }
            }
        } finally {
            handshaking = false;
        }
    }

    private void performTasks() {
        Runnable runnable;
        while ((runnable = engine.getDelegatedTask()) != null) {
            runnable.run();
        }
    }

    private void closeQuietly(final Closeable closeable) {
        try {
            closeable.close();
        } catch (final Exception e) {
        }
    }

    private int readData(final ByteBuffer dest) throws IOException {
        final long startTime = System.currentTimeMillis();

        while (true) {
            if (interrupted) {
                throw new TransmissionDisabledException();
            }

            if (dest.remaining() == 0) {
                return 0;
            }

            final int readCount = channel.read(dest);

            if (readCount == 0) {
                if (System.currentTimeMillis() > startTime + timeoutMillis) {
                    throw new SocketTimeoutException("Timed out reading from socket connected to " + hostname + ":" + port);
                }
                try {
                    TimeUnit.NANOSECONDS.sleep(BUFFER_FULL_EMPTY_WAIT_NANOS);
                } catch (InterruptedException e) {
                    close();
                    Thread.currentThread().interrupt(); // set the interrupt status
                    throw new ClosedByInterruptException();
                }

                continue;
            }

            logger.trace("{} Read {} bytes", this, readCount);
            return readCount;
        }
    }

    private Status encryptAndWriteFully(final BufferStateManager src) throws IOException {
        SSLEngineResult result = null;

        final ByteBuffer buff = src.prepareForRead(0);
        final ByteBuffer outBuff = streamOutManager.prepareForWrite(engine.getSession().getApplicationBufferSize());

        logger.trace("{} Encrypting {} bytes", this, buff.remaining());
        while (buff.remaining() > 0) {
            result = engine.wrap(buff, outBuff);
            if (result.getStatus() == Status.OK) {
                final ByteBuffer readableOutBuff = streamOutManager.prepareForRead(0);
                writeFully(readableOutBuff);
                streamOutManager.clear();
            } else {
                return result.getStatus();
            }
        }

        return result.getStatus();
    }

    private void writeFully(final ByteBuffer src) throws IOException {
        long lastByteWrittenTime = System.currentTimeMillis();

        int bytesWritten = 0;
        while (src.hasRemaining()) {
            if (interrupted) {
                throw new TransmissionDisabledException();
            }

            final int written = channel.write(src);
            bytesWritten += written;
            final long now = System.currentTimeMillis();
            if (written > 0) {
                lastByteWrittenTime = now;
            } else {
                if (now > lastByteWrittenTime + timeoutMillis) {
                    throw new SocketTimeoutException("Timed out writing to socket connected to " + hostname + ":" + port);
                }
                try {
                    TimeUnit.NANOSECONDS.sleep(BUFFER_FULL_EMPTY_WAIT_NANOS);
                } catch (final InterruptedException e) {
                    close();
                    Thread.currentThread().interrupt(); // set the interrupt status
                    throw new ClosedByInterruptException();
                }
            }
        }

        logger.trace("{} Wrote {} bytes", this, bytesWritten);
    }

    public boolean isClosed() {
        if (closed) {
            return true;
        }
        // need to detect if peer has sent closure handshake...if so the answer is true
        final ByteBuffer writableInBuffer = streamInManager.prepareForWrite(engine.getSession().getPacketBufferSize());
        int readCount = 0;
        try {
            readCount = channel.read(writableInBuffer);
        } catch (IOException e) {
            logger.error("{} Failed to readData due to {}", new Object[]{this, e});
            if (logger.isDebugEnabled()) {
                logger.error("", e);
            }
            readCount = -1; // treat the condition same as if End of Stream
        }
        if (readCount == 0) {
            return false;
        }
        if (readCount > 0) {
            logger.trace("{} Read {} bytes", this, readCount);

            final ByteBuffer streamInBuffer = streamInManager.prepareForRead(1);
            final ByteBuffer appDataBuffer = appDataManager.prepareForWrite(engine.getSession().getApplicationBufferSize());
            try {
                SSLEngineResult unwrapResponse = engine.unwrap(streamInBuffer, appDataBuffer);
                logger.trace("{} When checking if closed, (handshake={}) Unwrap response: {}", new Object[]{this, handshaking, unwrapResponse});
                if (unwrapResponse.getStatus().equals(Status.CLOSED)) {
                    // Drain the incoming TCP buffer
                    final ByteBuffer discardBuffer = ByteBuffer.allocate(8192);
                    int bytesDiscarded = channel.read(discardBuffer);
                    while (bytesDiscarded > 0) {
                        discardBuffer.clear();
                        bytesDiscarded = channel.read(discardBuffer);
                    }
                    engine.closeInbound();
                } else {
                    streamInManager.compact();
                    return false;
                }
            } catch (IOException e) {
                logger.error("{} Failed to check if closed due to {}. Closing channel.", new Object[]{this, e});
                if (logger.isDebugEnabled()) {
                    logger.error("", e);
                }
            }
        }
        // either readCount is -1, indicating an end of stream, or the peer sent a closure handshake
        // so go ahead and close down the channel
        closeQuietly(channel.socket());
        closeQuietly(channel);
        closed = true;
        return true;
    }

    @Override
    public void close() throws IOException {
        logger.debug("{} Closing Connection", this);
        if (channel == null) {
            return;
        }

        if (closed) {
            return;
        }

        try {
            engine.closeOutbound();

            final byte[] emptyMessage = new byte[0];

            final ByteBuffer appDataOut = ByteBuffer.wrap(emptyMessage);
            final ByteBuffer outboundBuffer = streamOutManager.prepareForWrite(engine.getSession().getApplicationBufferSize());
            final SSLEngineResult handshakeResult = engine.wrap(appDataOut, outboundBuffer);

            if (handshakeResult.getStatus() != Status.CLOSED) {
                throw new IOException("Invalid close state - will not send network data");
            }

            final ByteBuffer readableStreamOut = streamOutManager.prepareForRead(1);
            writeFully(readableStreamOut);
        } finally {
            // Drain the incoming TCP buffer
            final ByteBuffer discardBuffer = ByteBuffer.allocate(8192);
            try {
                int bytesDiscarded = channel.read(discardBuffer);
                while (bytesDiscarded > 0) {
                    discardBuffer.clear();
                    bytesDiscarded = channel.read(discardBuffer);
                }
            } catch (Exception e) {
            }

            closeQuietly(channel.socket());
            closeQuietly(channel);
            closed = true;
        }
    }

    private int copyFromAppDataBuffer(final byte[] buffer, final int offset, final int len) {
        // If any data already exists in the application data buffer, copy it to the buffer.
        final ByteBuffer appDataBuffer = appDataManager.prepareForRead(1);

        final int appDataRemaining = appDataBuffer.remaining();
        if (appDataRemaining > 0) {
            final int bytesToCopy = Math.min(len, appDataBuffer.remaining());
            appDataBuffer.get(buffer, offset, bytesToCopy);

            final int bytesCopied = appDataRemaining - appDataBuffer.remaining();
            logger.trace("{} Copied {} ({}) bytes from unencrypted application buffer to user space",
                    new Object[]{this, bytesToCopy, bytesCopied});
            return bytesCopied;
        }
        return 0;
    }

    public int available() throws IOException {
        ByteBuffer appDataBuffer = appDataManager.prepareForRead(1);
        ByteBuffer streamDataBuffer = streamInManager.prepareForRead(1);
        final int buffered = appDataBuffer.remaining() + streamDataBuffer.remaining();
        if (buffered > 0) {
            return buffered;
        }

        final boolean wasAbleToRead = isDataAvailable();
        if (!wasAbleToRead) {
            return 0;
        }

        appDataBuffer = appDataManager.prepareForRead(1);
        streamDataBuffer = streamInManager.prepareForRead(1);
        return appDataBuffer.remaining() + streamDataBuffer.remaining();
    }

    public boolean isDataAvailable() throws IOException {
        final ByteBuffer appDataBuffer = appDataManager.prepareForRead(1);
        final ByteBuffer streamDataBuffer = streamInManager.prepareForRead(1);

        if (appDataBuffer.remaining() > 0 || streamDataBuffer.remaining() > 0) {
            return true;
        }

        final ByteBuffer writableBuffer = streamInManager.prepareForWrite(engine.getSession().getPacketBufferSize());
        final int bytesRead = channel.read(writableBuffer);
        return (bytesRead > 0);
    }

    public int read() throws IOException {
        final int bytesRead = read(oneByteBuffer);
        if (bytesRead == -1) {
            return -1;
        }
        return oneByteBuffer[0] & 0xFF;
    }

    public int read(final byte[] buffer) throws IOException {
        return read(buffer, 0, buffer.length);
    }

    public int read(final byte[] buffer, final int offset, final int len) throws IOException {
        logger.debug("{} Reading up to {} bytes of data", this, len);

        if (!connected) {
            connect();
        }

        int copied = copyFromAppDataBuffer(buffer, offset, len);
        if (copied > 0) {
            return copied;
        }

        appDataManager.clear();

        while (true) {
            // prepare buffers and call unwrap
            final ByteBuffer streamInBuffer = streamInManager.prepareForRead(1);
            SSLEngineResult unwrapResponse = null;
            final ByteBuffer appDataBuffer = appDataManager.prepareForWrite(engine.getSession().getApplicationBufferSize());
            unwrapResponse = engine.unwrap(streamInBuffer, appDataBuffer);
            logger.trace("{} When reading data, (handshake={}) Unwrap response: {}", new Object[]{this, handshaking, unwrapResponse});

            switch (unwrapResponse.getStatus()) {
                case BUFFER_OVERFLOW:
                    throw new SSLHandshakeException("Buffer Overflow, which is not allowed to happen from an unwrap");
                case BUFFER_UNDERFLOW: {
//                appDataManager.prepareForRead(engine.getSession().getApplicationBufferSize());

                    final ByteBuffer writableInBuffer = streamInManager.prepareForWrite(engine.getSession().getPacketBufferSize());
                    final int bytesRead = readData(writableInBuffer);
                    if (bytesRead < 0) {
                        return -1;
                    }

                    continue;
                }
                case CLOSED:
                    throw new IOException("Channel is closed");
                case OK: {
                    copied = copyFromAppDataBuffer(buffer, offset, len);
                    if (copied == 0) {
                        throw new IOException("Failed to decrypt data");
                    }
                    streamInManager.compact();
                    return copied;
                }
            }
        }
    }

    public void write(final int data) throws IOException {
        write(new byte[]{(byte) data}, 0, 1);
    }

    public void write(final byte[] data) throws IOException {
        write(data, 0, data.length);
    }

    public void write(final byte[] data, final int offset, final int len) throws IOException {
        logger.debug("{} Writing {} bytes of data", this, len);

        if (!connected) {
            connect();
        }

        int iterations = len / MAX_WRITE_SIZE;
        if (len % MAX_WRITE_SIZE > 0) {
            iterations++;
        }

        for (int i = 0; i < iterations; i++) {
            streamOutManager.clear();
            final int itrOffset = offset + i * MAX_WRITE_SIZE;
            final int itrLen = Math.min(len - itrOffset, MAX_WRITE_SIZE);
            final ByteBuffer byteBuffer = ByteBuffer.wrap(data, itrOffset, itrLen);

            final BufferStateManager buffMan = new BufferStateManager(byteBuffer, Direction.READ);
            final Status status = encryptAndWriteFully(buffMan);
            switch (status) {
                case BUFFER_OVERFLOW:
                    streamOutManager.ensureSize(engine.getSession().getPacketBufferSize());
                    appDataManager.ensureSize(engine.getSession().getApplicationBufferSize());
                    continue;
                case OK:
                    continue;
                case CLOSED:
                    throw new IOException("Channel is closed");
                case BUFFER_UNDERFLOW:
                    throw new AssertionError("Got Buffer Underflow but should not have...");
            }
        }
    }

    public void interrupt() {
        this.interrupted = true;
    }
}
