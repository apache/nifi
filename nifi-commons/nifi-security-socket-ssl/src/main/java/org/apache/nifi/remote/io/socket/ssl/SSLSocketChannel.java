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

import org.apache.nifi.remote.exception.TransmissionDisabledException;
import org.apache.nifi.remote.io.socket.BufferStateManager;
import org.apache.nifi.remote.io.socket.BufferStateManager.Direction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.Status;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSession;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;

/**
 * SSLSocketChannel supports reading and writing bytes using TLS and NIO SocketChannels with configurable timeouts
 */
public class SSLSocketChannel implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(SSLSocketChannel.class);

    private static final int DISCARD_BUFFER_LENGTH = 8192;
    private static final int END_OF_STREAM = -1;
    private static final byte[] EMPTY_MESSAGE = new byte[0];
    private static final long BUFFER_FULL_EMPTY_WAIT_NANOS = TimeUnit.NANOSECONDS.convert(1, TimeUnit.MILLISECONDS);
    private static final long FINISH_CONNECT_SLEEP = 50;
    private static final long INITIAL_INCREMENTAL_SLEEP = 1;
    private static final boolean CLIENT_AUTHENTICATION_REQUIRED = true;

    private final String remoteAddress;
    private final int port;
    private final SSLEngine engine;
    private final SocketAddress socketAddress;
    private final BufferStateManager streamInManager;
    private final BufferStateManager streamOutManager;
    private final BufferStateManager appDataManager;
    private final SocketChannel channel;
    private int timeoutMillis = 30000;

    private volatile boolean interrupted = false;
    private volatile ChannelStatus channelStatus = ChannelStatus.DISCONNECTED;

    /**
     * SSLSocketChannel constructor with SSLContext and remote address parameters
     *
     * @param sslContext    SSLContext used to create SSLEngine with specified client mode
     * @param remoteAddress Remote Address used for connection
     * @param port          Remote Port used for connection
     * @param bindAddress   Local address used for binding server channel when provided
     * @param useClientMode Use Client Mode
     * @throws IOException Thrown on failures creating Socket Channel
     */
    public SSLSocketChannel(final SSLContext sslContext, final String remoteAddress, final int port, final InetAddress bindAddress, final boolean useClientMode) throws IOException {
        this.engine = createEngine(sslContext, useClientMode);
        this.channel = createSocketChannel(bindAddress);
        this.socketAddress = new InetSocketAddress(remoteAddress, port);
        this.remoteAddress = remoteAddress;
        this.port = port;

        streamInManager = new BufferStateManager(ByteBuffer.allocate(engine.getSession().getPacketBufferSize()));
        streamOutManager = new BufferStateManager(ByteBuffer.allocate(engine.getSession().getPacketBufferSize()));
        appDataManager = new BufferStateManager(ByteBuffer.allocate(engine.getSession().getApplicationBufferSize()));
    }

    /**
     * SSLSocketChannel constructor with SSLContext and connected SocketChannel
     *
     * @param sslContext    SSLContext used to create SSLEngine with specified client mode
     * @param socketChannel Connected SocketChannel
     * @param useClientMode Use Client Mode
     * @throws IOException Thrown on SocketChannel.getRemoteAddress()
     */
    public SSLSocketChannel(final SSLContext sslContext, final SocketChannel socketChannel, final boolean useClientMode) throws IOException {
        this(createEngine(sslContext, useClientMode), socketChannel);
    }

    /**
     * SSLSocketChannel constructor with configured SSLEngine and connected SocketChannel
     *
     * @param sslEngine     SSLEngine configured with mode and client authentication
     * @param socketChannel Connected SocketChannel
     * @throws IOException Thrown on SocketChannel.getRemoteAddress()
     */
    public SSLSocketChannel(final SSLEngine sslEngine, final SocketChannel socketChannel) throws IOException {
        if (!socketChannel.isConnected()) {
            throw new IllegalArgumentException("Connected SocketChannel required");
        }

        socketChannel.configureBlocking(false);
        this.channel = socketChannel;
        this.socketAddress = socketChannel.getRemoteAddress();
        final Socket socket = socketChannel.socket();
        this.remoteAddress = socket.getInetAddress().toString();
        this.port = socket.getPort();
        this.engine = sslEngine;

        streamInManager = new BufferStateManager(ByteBuffer.allocate(engine.getSession().getPacketBufferSize()));
        streamOutManager = new BufferStateManager(ByteBuffer.allocate(engine.getSession().getPacketBufferSize()));
        appDataManager = new BufferStateManager(ByteBuffer.allocate(engine.getSession().getApplicationBufferSize()));
    }

    public void setTimeout(final int timeoutMillis) {
        this.timeoutMillis = timeoutMillis;
    }

    public int getTimeout() {
        return timeoutMillis;
    }

    /**
     * Connect Channel when not connected and perform handshake process
     *
     * @throws IOException Thrown on connection failures
     */
    public void connect() throws IOException {
        channelStatus = ChannelStatus.CONNECTING;

        try {
            if (!channel.isConnected()) {
                logOperation("Connection Started");
                final long started = System.currentTimeMillis();

                if (!channel.connect(socketAddress)) {
                    while (!channel.finishConnect()) {
                        checkInterrupted();
                        checkTimeoutExceeded(started);

                        try {
                            TimeUnit.MILLISECONDS.sleep(FINISH_CONNECT_SLEEP);
                        } catch (final InterruptedException e) {
                            logOperation("Connection Interrupted");
                        }
                    }
                }
            }
            channelStatus = ChannelStatus.CONNECTED;
        } catch (final Exception e) {
            close();
            throw new SSLException(String.format("[%s:%d] Connection Failed", remoteAddress, port), e);
        }

        try {
            performHandshake();
        } catch (final IOException e) {
            close();
            throw new SSLException(String.format("[%s:%d] Handshake Failed", remoteAddress, port), e);
        }
    }

    /**
     * Shutdown Socket Channel input and read available bytes
     *
     * @throws IOException Thrown on Socket Channel failures
     */
    public void consume() throws IOException {
        channel.shutdownInput();

        final byte[] byteBuffer = new byte[DISCARD_BUFFER_LENGTH];
        final ByteBuffer buffer = ByteBuffer.wrap(byteBuffer);
        int readCount;
        do {
            readCount = channel.read(buffer);
            buffer.flip();
        } while (readCount > 0);
    }

    /**
     * Is Channel Closed
     *
     * @return Channel Closed Status
     */
    public boolean isClosed() {
        if (ChannelStatus.CLOSED == channelStatus) {
            return true;
        }

        // Read Channel to determine closed status
        final ByteBuffer inputBuffer = streamInManager.prepareForWrite(engine.getSession().getPacketBufferSize());
        int bytesRead;
        try {
            bytesRead = channel.read(inputBuffer);
        } catch (final IOException e) {
            LOGGER.warn("[{}:{}] Closed Status Read Failed", remoteAddress, port, e);
            bytesRead = END_OF_STREAM;
        }
        logOperationBytes("Closed Status Read", bytesRead);

        if (bytesRead == 0) {
            return false;
        } else if (bytesRead > 0) {
            try {
                final SSLEngineResult unwrapResult = unwrap();
                if (Status.CLOSED == unwrapResult.getStatus()) {
                    readChannelDiscard();
                    engine.closeInbound();
                } else {
                    streamInManager.compact();
                    return false;
                }
            } catch (final IOException e) {
                LOGGER.warn("[{}:{}] Closed Status Unwrap Failed", remoteAddress, port, e);
            }
        }

        // Close Channel when encountering end of stream or closed status
        try {
            close();
        } catch (final IOException e) {
            LOGGER.warn("[{}:{}] Close Failed", remoteAddress, port, e);
        }
        return true;
    }

    /**
     * Close Channel and process notifications
     *
     * @throws IOException Thrown on SSLEngine.wrap() failures
     */
    @Override
    public void close() throws IOException {
        logOperation("Close Requested");
        if (channelStatus == ChannelStatus.CLOSED) {
            return;
        }

        try {
            engine.closeOutbound();

            streamOutManager.clear();
            final ByteBuffer inputBuffer = ByteBuffer.wrap(EMPTY_MESSAGE);
            final ByteBuffer outputBuffer = streamOutManager.prepareForWrite(engine.getSession().getApplicationBufferSize());
            SSLEngineResult wrapResult = wrap(inputBuffer, outputBuffer);
            Status status = wrapResult.getStatus();
            if (Status.OK == status) {
                logOperation("Clearing Outbound Buffer");
                outputBuffer.clear();
                wrapResult = wrap(inputBuffer, outputBuffer);
                status = wrapResult.getStatus();
            }
            if (Status.CLOSED == status) {
                final ByteBuffer streamOutputBuffer = streamOutManager.prepareForRead(1);
                try {
                    writeChannel(streamOutputBuffer);
                } catch (final IOException e) {
                    logOperation(String.format("Write Close Notification Failed: %s", e.getMessage()));
                }
            } else {
                throw new SSLException(String.format("[%s:%d] Invalid Wrap Result Status [%s]", remoteAddress, port, status));
            }
        } finally {
            channelStatus = ChannelStatus.CLOSED;
            readChannelDiscard();
            closeQuietly(channel.socket());
            closeQuietly(channel);
            logOperation("Close Completed");
        }
    }

    /**
     * Get application bytes available for reading
     *
     * @return Number of application bytes available for reading
     * @throws IOException Thrown on failures checking for available bytes
     */
    public int available() throws IOException {
        ByteBuffer appDataBuffer = appDataManager.prepareForRead(1);
        ByteBuffer streamDataBuffer = streamInManager.prepareForRead(1);
        final int buffered = appDataBuffer.remaining() + streamDataBuffer.remaining();
        if (buffered > 0) {
            return buffered;
        }

        if (!isDataAvailable()) {
            return 0;
        }

        appDataBuffer = appDataManager.prepareForRead(1);
        streamDataBuffer = streamInManager.prepareForRead(1);
        return appDataBuffer.remaining() + streamDataBuffer.remaining();
    }

    /**
     * Is data available for reading
     *
     * @return Data available status
     * @throws IOException Thrown on SocketChannel.read() failures
     */
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

    /**
     * Read and return one byte
     *
     * @return Byte read or -1 when end of stream reached
     * @throws IOException Thrown on read failures
     */
    public int read() throws IOException {
        final byte[] buffer = new byte[1];

        final int bytesRead = read(buffer);
        if (bytesRead == END_OF_STREAM) {
            return END_OF_STREAM;
        }

        return Byte.toUnsignedInt(buffer[0]);
    }

    /**
     * Read available bytes into buffer
     *
     * @param buffer Byte array buffer
     * @return Number of bytes read
     * @throws IOException Thrown on read failures
     */
    public int read(final byte[] buffer) throws IOException {
        return read(buffer, 0, buffer.length);
    }

    /**
     * Read available bytes into buffer based on offset and length requested
     *
     * @param buffer Byte array buffer
     * @param offset Buffer offset
     * @param len    Length of bytes to read
     * @return Number of bytes read
     * @throws IOException Thrown on read failures
     */
    public int read(final byte[] buffer, final int offset, final int len) throws IOException {
        logOperationBytes("Read Requested", len);
        checkChannelStatus();

        int applicationBytesRead = readApplicationBuffer(buffer, offset, len);
        if (applicationBytesRead > 0) {
            return applicationBytesRead;
        }
        appDataManager.clear();

        while (true) {
            final SSLEngineResult unwrapResult = unwrap();

            if (SSLEngineResult.HandshakeStatus.FINISHED == unwrapResult.getHandshakeStatus()) {
                // RFC 8446 Section 4.6 describes Post-Handshake Messages for TLS 1.3
                logOperation("Processing Post-Handshake Messages");
                continue;
            }

            final Status status = unwrapResult.getStatus();
            switch (status) {
                case BUFFER_OVERFLOW:
                    throw new IllegalStateException(String.format("SSLEngineResult Status [%s] not allowed from unwrap", status));
                case BUFFER_UNDERFLOW:
                    final ByteBuffer streamBuffer = streamInManager.prepareForWrite(engine.getSession().getPacketBufferSize());
                    final int channelBytesRead = readChannel(streamBuffer);
                    logOperationBytes("Channel Read Completed", channelBytesRead);
                    if (channelBytesRead == END_OF_STREAM) {
                        return END_OF_STREAM;
                    }
                    break;
                case CLOSED:
                    applicationBytesRead = readApplicationBuffer(buffer, offset, len);
                    if (applicationBytesRead == 0) {
                        return END_OF_STREAM;
                    }
                    streamInManager.compact();
                    return applicationBytesRead;
                case OK:
                    applicationBytesRead = readApplicationBuffer(buffer, offset, len);
                    if (applicationBytesRead == 0) {
                        throw new IOException("Read Application Buffer Failed");
                    }
                    streamInManager.compact();
                    return applicationBytesRead;
            }
        }
    }

    /**
     * Write one byte to channel
     *
     * @param data Byte to be written
     * @throws IOException Thrown on write failures
     */
    public void write(final int data) throws IOException {
        write(new byte[]{(byte) data}, 0, 1);
    }

    /**
     * Write bytes to channel
     *
     * @param data Byte array to be written
     * @throws IOException Thrown on write failures
     */
    public void write(final byte[] data) throws IOException {
        write(data, 0, data.length);
    }

    /**
     * Write data to channel performs multiple iterations based on data length
     *
     * @param data   Byte array to be written
     * @param offset Byte array offset
     * @param len    Length of bytes for writing
     * @throws IOException Thrown on write failures
     */
    public void write(final byte[] data, final int offset, final int len) throws IOException {
        logOperationBytes("Write Started", len);
        checkChannelStatus();

        final int applicationBufferSize = engine.getSession().getApplicationBufferSize();
        logOperationBytes("Write Application Buffer Size", applicationBufferSize);
        int iterations = len / applicationBufferSize;
        if (len % applicationBufferSize > 0) {
            iterations++;
        }

        for (int i = 0; i < iterations; i++) {
            streamOutManager.clear();
            final int itrOffset = offset + i * applicationBufferSize;
            final int itrLen = Math.min(len - itrOffset, applicationBufferSize);
            final ByteBuffer byteBuffer = ByteBuffer.wrap(data, itrOffset, itrLen);

            final BufferStateManager bufferStateManager = new BufferStateManager(byteBuffer, Direction.READ);
            final Status status = wrapWriteChannel(bufferStateManager);
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

    /**
     * Interrupt processing and disable transmission
     */
    public void interrupt() {
        this.interrupted = true;
    }

    private void performHandshake() throws IOException {
        logOperation("Handshake Started");
        channelStatus = ChannelStatus.HANDSHAKING;
        engine.beginHandshake();

        SSLEngineResult.HandshakeStatus handshakeStatus = engine.getHandshakeStatus();
        while (true) {
            logHandshakeStatus(handshakeStatus);

            switch (handshakeStatus) {
                case FINISHED:
                case NOT_HANDSHAKING:
                    channelStatus = ChannelStatus.ESTABLISHED;
                    final SSLSession session = engine.getSession();
                    LOGGER.debug("[{}:{}] [{}] Negotiated Protocol [{}] Cipher Suite [{}]",
                            remoteAddress,
                            port,
                            channelStatus,
                            session.getProtocol(),
                            session.getCipherSuite()
                    );
                    return;
                case NEED_TASK:
                    runDelegatedTasks();
                    handshakeStatus = engine.getHandshakeStatus();
                    break;
                case NEED_UNWRAP:
                    final SSLEngineResult unwrapResult = unwrap();
                    handshakeStatus = unwrapResult.getHandshakeStatus();
                    Status unwrapResultStatus = unwrapResult.getStatus();

                    if (unwrapResultStatus == Status.BUFFER_UNDERFLOW) {
                        final ByteBuffer writableDataIn = streamInManager.prepareForWrite(engine.getSession().getPacketBufferSize());
                        final int bytesRead = readChannel(writableDataIn);
                        logOperationBytes("Handshake Channel Read", bytesRead);

                        if (bytesRead == END_OF_STREAM) {
                            throw getHandshakeException(handshakeStatus, "End of Stream Found");
                        }
                    } else if (unwrapResultStatus == Status.CLOSED) {
                        throw getHandshakeException(handshakeStatus, "Channel Closed");
                    } else {
                        streamInManager.compact();
                        appDataManager.clear();
                    }
                    break;
                case NEED_WRAP:
                    final ByteBuffer outboundBuffer = streamOutManager.prepareForWrite(engine.getSession().getApplicationBufferSize());
                    final SSLEngineResult wrapResult = wrap(ByteBuffer.wrap(EMPTY_MESSAGE), outboundBuffer);
                    handshakeStatus = wrapResult.getHandshakeStatus();
                    final Status wrapResultStatus = wrapResult.getStatus();

                    if (wrapResultStatus == Status.BUFFER_OVERFLOW) {
                        streamOutManager.prepareForWrite(engine.getSession().getApplicationBufferSize());
                    } else if (wrapResultStatus == Status.OK) {
                        final ByteBuffer streamBuffer = streamOutManager.prepareForRead(1);
                        final int bytesRemaining = streamBuffer.remaining();
                        writeChannel(streamBuffer);
                        logOperationBytes("Handshake Channel Write Completed", bytesRemaining);
                        streamOutManager.clear();
                    } else {
                        throw getHandshakeException(handshakeStatus, String.format("Wrap Failed [%s]", wrapResult.getStatus()));
                    }
                    break;
            }
        }
    }

    private int readChannel(final ByteBuffer outputBuffer) throws IOException {
        logOperation("Channel Read Started");

        final long started = System.currentTimeMillis();
        long sleepNanoseconds = INITIAL_INCREMENTAL_SLEEP;
        while (true) {
            checkInterrupted();

            if (outputBuffer.remaining() == 0) {
                return 0;
            }

            final int channelBytesRead = channel.read(outputBuffer);
            if (channelBytesRead == 0) {
                checkTimeoutExceeded(started);
                sleepNanoseconds = incrementalSleep(sleepNanoseconds);
                continue;
            }

            return channelBytesRead;
        }
    }

    private void writeChannel(final ByteBuffer inputBuffer) throws IOException {
        long lastWriteCompleted = System.currentTimeMillis();

        int totalBytes = 0;
        long sleepNanoseconds = INITIAL_INCREMENTAL_SLEEP;
        while (inputBuffer.hasRemaining()) {
            checkInterrupted();

            final int written = channel.write(inputBuffer);
            totalBytes += written;

            if (written > 0) {
                lastWriteCompleted = System.currentTimeMillis();
            } else {
                checkTimeoutExceeded(lastWriteCompleted);
                sleepNanoseconds = incrementalSleep(sleepNanoseconds);
            }
        }

        logOperationBytes("Channel Write Completed", totalBytes);
    }

    private long incrementalSleep(final long nanoseconds) throws IOException {
        try {
            TimeUnit.NANOSECONDS.sleep(nanoseconds);
        } catch (final InterruptedException e) {
            close();
            Thread.currentThread().interrupt();
            throw new ClosedByInterruptException();
        }
        return Math.min(nanoseconds * 2, BUFFER_FULL_EMPTY_WAIT_NANOS);
    }

    private void readChannelDiscard() {
        try {
            final ByteBuffer readBuffer = ByteBuffer.allocate(DISCARD_BUFFER_LENGTH);
            int bytesRead = channel.read(readBuffer);
            while (bytesRead > 0) {
                readBuffer.clear();
                bytesRead = channel.read(readBuffer);
            }
        } catch (final IOException e) {
            LOGGER.debug("[{}:{}] Read Channel Discard Failed", remoteAddress, port, e);
        }
    }

    private int readApplicationBuffer(final byte[] buffer, final int offset, final int len) {
        logOperationBytes("Application Buffer Read Requested", len);
        final ByteBuffer appDataBuffer = appDataManager.prepareForRead(len);

        final int appDataRemaining = appDataBuffer.remaining();
        logOperationBytes("Application Buffer Remaining", appDataRemaining);
        if (appDataRemaining > 0) {
            final int bytesToCopy = Math.min(len, appDataBuffer.remaining());
            appDataBuffer.get(buffer, offset, bytesToCopy);

            final int bytesCopied = appDataRemaining - appDataBuffer.remaining();
            logOperationBytes("Application Buffer Copied", bytesCopied);
            return bytesCopied;
        }
        return 0;
    }

    private Status wrapWriteChannel(final BufferStateManager inputManager) throws IOException {
        final ByteBuffer inputBuffer = inputManager.prepareForRead(0);
        final ByteBuffer outputBuffer = streamOutManager.prepareForWrite(engine.getSession().getApplicationBufferSize());

        logOperationBytes("Wrap Started", inputBuffer.remaining());
        Status status = Status.OK;
        while (inputBuffer.remaining() > 0) {
            final SSLEngineResult result = wrap(inputBuffer, outputBuffer);
            status = result.getStatus();
            if (status == Status.OK) {
                final ByteBuffer readableOutBuff = streamOutManager.prepareForRead(0);
                writeChannel(readableOutBuff);
                streamOutManager.clear();
            } else {
                break;
            }
        }

        return status;
    }

    private SSLEngineResult wrap(final ByteBuffer inputBuffer, final ByteBuffer outputBuffer) throws SSLException {
        final SSLEngineResult result = engine.wrap(inputBuffer, outputBuffer);
        logEngineResult(result, "WRAP Completed");
        return result;
    }

    private SSLEngineResult unwrap() throws IOException {
        final ByteBuffer streamBuffer = streamInManager.prepareForRead(engine.getSession().getPacketBufferSize());
        final ByteBuffer applicationBuffer = appDataManager.prepareForWrite(engine.getSession().getApplicationBufferSize());
        final SSLEngineResult result = engine.unwrap(streamBuffer, applicationBuffer);
        logEngineResult(result, "UNWRAP Completed");
        return result;
    }

    private void runDelegatedTasks() {
        Runnable delegatedTask;
        while ((delegatedTask = engine.getDelegatedTask()) != null) {
            logOperation("Running Delegated Task");
            delegatedTask.run();
        }
    }

    private void closeQuietly(final Closeable closeable) {
        try {
            closeable.close();
        } catch (final Exception e) {
            logOperation(String.format("Close failed: %s", e.getMessage()));
        }
    }

    private SSLHandshakeException getHandshakeException(final SSLEngineResult.HandshakeStatus handshakeStatus, final String message) {
        final String formatted = String.format("[%s:%d] Handshake Status [%s] %s", remoteAddress, port, handshakeStatus, message);
        return new SSLHandshakeException(formatted);
    }

    private void checkChannelStatus() throws IOException {
        if (ChannelStatus.ESTABLISHED != channelStatus) {
            connect();
        }
    }

    private void checkInterrupted() {
        if (interrupted) {
            throw new TransmissionDisabledException();
        }
    }

    private void checkTimeoutExceeded(final long started) throws SocketTimeoutException {
        if (System.currentTimeMillis() > started + timeoutMillis) {
            throw new SocketTimeoutException(String.format("Timeout Exceeded [%d ms] for [%s:%d]", timeoutMillis, remoteAddress, port));
        }
    }

    private void logOperation(final String operation) {
        LOGGER.trace("[{}:{}] [{}] {}", remoteAddress, port, channelStatus, operation);
    }

    private void logOperationBytes(final String operation, final int bytes) {
        LOGGER.trace("[{}:{}] [{}] {} Bytes [{}]", remoteAddress, port, channelStatus, operation, bytes);
    }

    private void logHandshakeStatus(final SSLEngineResult.HandshakeStatus handshakeStatus) {
        LOGGER.trace("[{}:{}] [{}] Handshake Status [{}]", remoteAddress, port, channelStatus, handshakeStatus);
    }

    private void logEngineResult(final SSLEngineResult result, final String method) {
        LOGGER.trace("[{}:{}] [{}] {} Status [{}] Handshake Status [{}] Produced [{}] Consumed [{}]",
                remoteAddress,
                port,
                channelStatus,
                method,
                result.getStatus(),
                result.getHandshakeStatus(),
                result.bytesProduced(),
                result.bytesConsumed()
        );
    }

    private static SocketChannel createSocketChannel(final InetAddress bindAddress) throws IOException {
        final SocketChannel socketChannel = SocketChannel.open();
        if (bindAddress != null) {
            final SocketAddress socketAddress = new InetSocketAddress(bindAddress, 0);
            socketChannel.bind(socketAddress);
        }
        socketChannel.configureBlocking(false);
        return socketChannel;
    }

    private static SSLEngine createEngine(final SSLContext sslContext, final boolean useClientMode) {
        final SSLEngine sslEngine = sslContext.createSSLEngine();
        sslEngine.setUseClientMode(useClientMode);
        sslEngine.setNeedClientAuth(CLIENT_AUTHENTICATION_REQUIRED);
        return sslEngine;
    }

    private enum ChannelStatus {
        DISCONNECTED,

        CONNECTING,

        CONNECTED,

        HANDSHAKING,

        ESTABLISHED,

        CLOSED
    }
}
