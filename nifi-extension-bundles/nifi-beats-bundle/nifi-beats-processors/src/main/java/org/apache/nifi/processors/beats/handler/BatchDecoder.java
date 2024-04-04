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
package org.apache.nifi.processors.beats.handler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.beats.protocol.Batch;
import org.apache.nifi.processors.beats.protocol.BatchMessage;
import org.apache.nifi.processors.beats.protocol.FrameType;
import org.apache.nifi.processors.beats.protocol.FrameTypeDecoder;
import org.apache.nifi.processors.beats.protocol.ProtocolCodeDecoder;
import org.apache.nifi.processors.beats.protocol.ProtocolException;
import org.apache.nifi.processors.beats.protocol.ProtocolVersion;
import org.apache.nifi.processors.beats.protocol.ProtocolVersionDecoder;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.Inflater;
import java.util.zip.InflaterOutputStream;

/**
 * Byte Buffer to Batch Decoder parses bytes to batches of Beats messages
 */
public class BatchDecoder extends ByteToMessageDecoder {
    private static final int INITIAL_WINDOW_SIZE = 1;

    private static final int INITIAL_QUEUE_SIZE = 1;

    private static final int CODE_READABLE_BYTES = 1;

    private static final int INT_READABLE_BYTES = 4;

    private static final ProtocolCodeDecoder<ProtocolVersion> VERSION_DECODER = new ProtocolVersionDecoder();

    private static final ProtocolCodeDecoder<FrameType> FRAME_TYPE_DECODER = new FrameTypeDecoder();

    private final ComponentLog log;

    private final AtomicReference<ProtocolVersion> versionRef = new AtomicReference<>();

    private final AtomicReference<FrameType> frameTypeRef = new AtomicReference<>();

    private final AtomicInteger windowSize = new AtomicInteger(INITIAL_WINDOW_SIZE);

    private final AtomicReference<Integer> sequenceNumberRef = new AtomicReference<>();

    private final AtomicReference<Integer> payloadSizeRef = new AtomicReference<>();

    private final AtomicReference<Integer> compressedSizeRef = new AtomicReference<>();

    private Queue<BatchMessage> batchMessages = new ArrayBlockingQueue<>(INITIAL_QUEUE_SIZE);

    /**
     * Beats Batch Decoder with required arguments
     *
     * @param log Processor Log
     */
    public BatchDecoder(final ComponentLog log) {
        this.log = Objects.requireNonNull(log, "Component Log required");
    }

    /**
     * Decode Batch of Beats Messages from Byte Buffer
     *
     * @param context Channel Handler Context
     * @param buffer Byte Buffer
     * @param objects List of Batch objects
     */
    @Override
    protected void decode(final ChannelHandlerContext context, final ByteBuf buffer, final List<Object> objects) {
        final ProtocolVersion protocolVersion = readVersion(buffer);
        if (ProtocolVersion.VERSION_2 == protocolVersion) {
            final FrameType frameType = readFrameType(buffer);
            decodeFrameType(frameType, context, buffer, objects);
        } else if (ProtocolVersion.VERSION_1 == protocolVersion) {
            throw new ProtocolException("Protocol Version [1] not supported");
        }
    }

    private void decodeFrameType(final FrameType frameType, final ChannelHandlerContext context, final ByteBuf buffer, final List<Object> batches) {
        if (frameType == null) {
            log.trace("Frame Type not found");
        } else if (FrameType.COMPRESSED == frameType) {
            processCompressed(context, buffer, batches);
        } else if (FrameType.WINDOW_SIZE == frameType) {
            processWindowSize(context, buffer);
        } else if (FrameType.JSON == frameType) {
            processJson(context, buffer, batches);
        } else {
            final String message = String.format("Frame Type [%s] not supported", frameType);
            throw new ProtocolException(message);
        }
    }

    private void processWindowSize(final ChannelHandlerContext context, final ByteBuf buffer) {
        final Integer readWindowSize = readUnsignedInteger(buffer);
        if (readWindowSize == null) {
            log.trace("State [Read Window Size] not enough readable bytes");
        } else {
            windowSize.getAndSet(readWindowSize);
            batchMessages = new ArrayBlockingQueue<>(readWindowSize);

            resetFrameTypeVersion();
            final Channel channel = context.channel();
            log.debug("Processed Window Size [{}] Local [{}] Remote [{}]", readWindowSize, channel.localAddress(), channel.remoteAddress());
        }
    }

    private void processCompressed(final ChannelHandlerContext context, final ByteBuf buffer, final List<Object> batches) {
        final Integer readCompressedSize = readCompressedSize(buffer);
        if (readCompressedSize == null) {
            log.trace("State [Read Compressed] not enough readable bytes");
        } else {
            final int readableBytes = buffer.readableBytes();
            if (readableBytes >= readCompressedSize) {
                final Channel channel = context.channel();
                log.debug("Processing Compressed Size [{}] Local [{}] Remote [{}]", readCompressedSize, channel.localAddress(), channel.remoteAddress());

                processCompressed(context, buffer, readCompressedSize, batches);
            } else {
                log.trace("State [Read Compressed] not enough readable bytes [{}] for compressed [{}]", readableBytes, readCompressedSize);
            }
        }
    }

    private void processCompressed(
            final ChannelHandlerContext context,
            final ByteBuf buffer,
            final int compressedSize,
            final List<Object> batches
    ) {
        final ByteBuf inflated = context.alloc().buffer(compressedSize);
        try {
            readCompressedBuffer(buffer, inflated, compressedSize);

            // Clear status prior to decoding inflated frames
            resetSequenceVersionPayloadSize();
            resetFrameTypeVersion();

            while (inflated.isReadable()) {
                decode(context, inflated, batches);
            }
        } finally {
            compressedSizeRef.set(null);
            inflated.release();
        }
    }

    private void processJson(final ChannelHandlerContext context, final ByteBuf buffer, final List<Object> batches) {
        final Channel channel = context.channel();

        final Integer sequenceNumber = readSequenceNumber(buffer);
        if (sequenceNumber == null) {
            log.trace("State [Read JSON] Sequence Number not found Remote [{}]", channel.remoteAddress());
        } else {
            final Integer payloadSize = readPayloadSize(buffer);
            if (payloadSize == null) {
                log.trace("State [Read JSON] Payload Size not found Remote [{}]", channel.remoteAddress());
            } else {
                processJson(sequenceNumber, payloadSize, context, buffer, batches);
            }
        }
    }

    private void processJson(
            final int sequenceNumber,
            final int payloadSize,
            final ChannelHandlerContext context,
            final ByteBuf buffer,
            final List<Object> batches
    ) {
        final Channel channel = context.channel();

        final BatchMessage batchMessage = readJsonMessage(context, sequenceNumber, payloadSize, buffer);
        if (batchMessage == null) {
            log.trace("State [Read JSON] Message not found Remote [{}]", channel.remoteAddress());
        } else {
            processBatchMessage(batchMessage, batches);
            log.debug("Processed JSON Message Sequence Number [{}] Payload Size [{}] Local [{}] Remote [{}]", sequenceNumber, payloadSize, channel.localAddress(), channel.remoteAddress());
        }
    }

    private BatchMessage readJsonMessage(
            final ChannelHandlerContext context,
            final int sequenceNumber,
            final int payloadSize,
            final ByteBuf buffer
    ) {
        final BatchMessage batchMessage;

        final int readableBytes = buffer.readableBytes();
        if (readableBytes >= payloadSize) {
            final byte[] payload = new byte[payloadSize];
            buffer.readBytes(payload);

            final Channel channel = context.channel();
            final String sender = getRemoteHostAddress(channel);
            batchMessage = new BatchMessage(sender, payload, sequenceNumber);
        } else {
            batchMessage = null;
            log.trace("State [Read JSON] Sequence Number [{}] not enough readable bytes [{}] for payload [{}]", sequenceNumber, readableBytes, payloadSize);
        }

        return batchMessage;
    }

    private String getRemoteHostAddress(final Channel channel) {
        final String remoteHostAddress;

        final SocketAddress remoteAddress = channel.remoteAddress();
        if (remoteAddress instanceof InetSocketAddress) {
            final InetSocketAddress remoteSocketAddress = (InetSocketAddress) remoteAddress;
            final InetAddress address = remoteSocketAddress.getAddress();
            remoteHostAddress = address.getHostAddress();
        } else {
            remoteHostAddress = remoteAddress.toString();
        }

        return remoteHostAddress;
    }

    private void processBatchMessage(final BatchMessage batchMessage, final List<Object> batches) {
        if (batchMessages.offer(batchMessage)) {
            resetSequenceVersionPayloadSize();
            resetFrameTypeVersion();

            if (windowSize.get() == batchMessages.size()) {
                final Collection<BatchMessage> messages = new ArrayList<>(batchMessages);
                final Batch batch = new Batch(messages);
                batches.add(batch);

                resetWindowSize();
            }
        } else {
            final String message = String.format("Received message exceeds Window Size [%d]", windowSize.get());
            throw new ProtocolException(message);
        }
    }

    private void readCompressedBuffer(final ByteBuf compressedBuffer, final ByteBuf inflated, final int compressedSize) {
        final Inflater inflater = new Inflater();
        try (
                final ByteBufOutputStream outputStream = new ByteBufOutputStream(inflated);
                final InflaterOutputStream inflaterOutputStream = new InflaterOutputStream(outputStream, inflater)
        ) {
            compressedBuffer.readBytes(inflaterOutputStream, compressedSize);
        } catch (final IOException e) {
            final String message = String.format("Read Compressed Payload Size [%d] failed", compressedSize);
            throw new ProtocolException(message, e);
        } finally {
            inflater.end();
        }
    }

    private Integer readSequenceNumber(final ByteBuf buffer) {
        if (sequenceNumberRef.get() == null) {
            final Integer readSequenceNumber = readUnsignedInteger(buffer);
            if (readSequenceNumber == null) {
                log.trace("State [Read JSON] not enough readable bytes for Sequence Number");
            } else {
                sequenceNumberRef.set(readSequenceNumber);
            }
        }

        return sequenceNumberRef.get();
    }

    private Integer readPayloadSize(final ByteBuf buffer) {
        if (payloadSizeRef.get() == null) {
            final Integer readPayloadSize = readUnsignedInteger(buffer);
            if (readPayloadSize == null) {
                log.trace("State [Read JSON] not enough readable bytes for Payload Size");
            } else {
                payloadSizeRef.set(readPayloadSize);
            }
        }

        return payloadSizeRef.get();
    }

    private Integer readCompressedSize(final ByteBuf buffer) {
        if (compressedSizeRef.get() == null) {
            final Integer readCompressedSize = readUnsignedInteger(buffer);
            if (readCompressedSize == null) {
                log.trace("State [Read Compressed] not enough readable bytes for Compressed Size");
            } else {
                compressedSizeRef.set(readCompressedSize);
            }
        }

        return compressedSizeRef.get();
    }

    private Integer readUnsignedInteger(final ByteBuf buffer) {
        final Integer number;

        final int readableBytes = buffer.readableBytes();
        if (readableBytes >= INT_READABLE_BYTES) {
            final long unsigned = buffer.readUnsignedInt();
            number = Math.toIntExact(unsigned);
        } else {
            number = null;
        }

        return number;
    }

    private FrameType readFrameType(final ByteBuf buffer) {
        if (frameTypeRef.get() == null) {
            final int readableBytes = buffer.readableBytes();
            if (readableBytes >= CODE_READABLE_BYTES) {
                final byte frameTypeCode = buffer.readByte();
                final FrameType frameType = FRAME_TYPE_DECODER.readProtocolCode(frameTypeCode);
                frameTypeRef.set(frameType);
            } else {
                log.trace("State [Read Frame Type] not enough readable bytes [{}]", readableBytes);
            }
        }

        return frameTypeRef.get();
    }

    private ProtocolVersion readVersion(final ByteBuf buffer) {
        if (versionRef.get() == null) {
            final int readableBytes = buffer.readableBytes();
            if (readableBytes >= CODE_READABLE_BYTES) {
                final byte versionCode = buffer.readByte();
                final ProtocolVersion protocolVersion = VERSION_DECODER.readProtocolCode(versionCode);
                versionRef.set(protocolVersion);
            } else {
                log.trace("State [Read Version] not enough readable bytes [{}]", readableBytes);
            }
        }

        return versionRef.get();
    }

    private void resetSequenceVersionPayloadSize() {
        sequenceNumberRef.set(null);
        payloadSizeRef.set(null);
    }

    private void resetFrameTypeVersion() {
        frameTypeRef.set(null);
        versionRef.set(null);
    }

    private void resetWindowSize() {
        windowSize.set(INITIAL_WINDOW_SIZE);
        batchMessages.clear();
    }
}
