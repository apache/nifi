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
package org.apache.nifi.distributed.cache.server.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.nifi.distributed.cache.operations.CacheOperation;
import org.apache.nifi.distributed.cache.server.protocol.CacheRequest;
import org.apache.nifi.distributed.cache.server.protocol.CacheVersionRequest;
import org.apache.nifi.logging.ComponentLog;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Cache Request Decoder processes bytes and decodes cache version and operation requests
 */
public class CacheRequestDecoder extends ByteToMessageDecoder {
    private static final int DEFAULT_LENGTH = 0;

    private static final int HEADER_LENGTH = 4;

    private final AtomicBoolean headerReceived = new AtomicBoolean();

    private final AtomicInteger protocolVersion = new AtomicInteger();

    private final AtomicInteger valueLength = new AtomicInteger(DEFAULT_LENGTH);

    private final AtomicReference<CacheOperation> currentOperation = new AtomicReference<>();

    private final ComponentLog log;

    private final int maxLength;

    private final CacheOperation[] supportedOperations;

    public CacheRequestDecoder(
            final ComponentLog log,
            final int maxLength,
            final CacheOperation[] supportedOperations
    ) {
        this.log = log;
        this.maxLength = maxLength;
        this.supportedOperations = supportedOperations;
    }

    /**
     * Decode Byte Buffer reading header on initial connection followed by protocol version and cache operations
     *
     * @param channelHandlerContext Channel Handler Context
     * @param byteBuf Byte Buffer
     * @param objects Decoded Objects
     */
    @Override
    protected void decode(final ChannelHandlerContext channelHandlerContext, final ByteBuf byteBuf, final List<Object> objects) {
        if (!headerReceived.get()) {
            byteBuf.readBytes(HEADER_LENGTH);
            headerReceived.getAndSet(true);
            log.debug("Header Received [{}]", channelHandlerContext.channel().remoteAddress());
        }

        if (protocolVersion.get() == 0) {
            final int clientVersion = byteBuf.readInt();
            log.debug("Protocol Version [{}] Received [{}]", clientVersion, channelHandlerContext.channel().remoteAddress());
            final CacheVersionRequest cacheVersionRequest = new CacheVersionRequest(clientVersion);
            objects.add(cacheVersionRequest);
        } else {
            final CacheOperation cacheOperation = readOperation(byteBuf);
            final Object cacheRequest = readRequest(cacheOperation, byteBuf);
            if (cacheRequest == null) {
                log.debug("Cache Operation [{}] request not processed", cacheOperation);
            } else {
                objects.add(cacheRequest);
                // Reset Cache Operation after successful decoding
                currentOperation.set(null);
            }
        }
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext context, final Throwable cause) {
        log.warn("Request Decoding Failed: Closing Connection [{}]", context.channel().remoteAddress(), cause);
        context.close();
    }

    /**
     * Set Protocol Version based on version negotiated in other handlers
     *
     * @param protocolVersion Protocol Version
     */
    public void setProtocolVersion(final int protocolVersion) {
        this.protocolVersion.getAndSet(protocolVersion);
    }

    /**
     * Read Request Object based on Cache Operation
     *
     * @param cacheOperation Cache Operation
     * @param byteBuf Byte Buffer
     * @return Request Object or null when buffer does not contain sufficient bytes
     */
    protected Object readRequest(final CacheOperation cacheOperation, final ByteBuf byteBuf) {
        final int readableBytes = byteBuf.readableBytes();
        final byte[] body = readableBytes == 0 ? new byte[0] : readBytes(byteBuf);
        return body == null ? null : new CacheRequest(cacheOperation, body);
    }

    /**
     * Read Bytes from buffer based on length indicated
     *
     * @param byteBuf Byte Buffer
     * @return Bytes read or null when buffer does not contain sufficient bytes
     */
    protected byte[] readBytes(final ByteBuf byteBuf) {
        final int length = readBytesLength(byteBuf);
        final int readableBytes = byteBuf.readableBytes();
        final boolean readableBytesFound = readableBytes >= length;
        return readableBytesFound ? readBytes(byteBuf, length) : null;
    }

    protected String readUnicodeString(final ByteBuf byteBuf) {
        final int length = byteBuf.readUnsignedShort();
        if (length > maxLength) {
            throw new IllegalArgumentException(String.format("Maximum Operation Length [%d] exceeded [%d]", maxLength, length));
        }
        return byteBuf.readCharSequence(length, StandardCharsets.UTF_8).toString();
    }

    protected int readInt(final ByteBuf byteBuf) {
        final int integer = byteBuf.readInt();
        if (integer > maxLength) {
            throw new IllegalArgumentException(String.format("Maximum Length [%d] exceeded [%d]", maxLength, integer));
        }
        return integer;
    }

    private int readBytesLength(final ByteBuf byteBuf) {
        if (valueLength.get() == DEFAULT_LENGTH) {
            final int length = readInt(byteBuf);
            valueLength.getAndSet(length);
        }
        return valueLength.get();
    }

    private byte[] readBytes(final ByteBuf byteBuf, final int length) {
        final byte[] bytes = new byte[length];
        byteBuf.readBytes(bytes);
        // Reset value length after reading bytes
        valueLength.getAndSet(DEFAULT_LENGTH);
        return bytes;
    }

    private CacheOperation readOperation(final ByteBuf byteBuf) {
        if (currentOperation.get() == null) {
            final String clientOperation = readUnicodeString(byteBuf);
            final CacheOperation cacheOperation = Arrays.stream(supportedOperations)
                    .filter(supportedOperation -> supportedOperation.value().contentEquals(clientOperation))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException(String.format("Cache Operation not supported [%d]", clientOperation.length())));
            currentOperation.getAndSet(cacheOperation);
        }
        return currentOperation.get();
    }
}
