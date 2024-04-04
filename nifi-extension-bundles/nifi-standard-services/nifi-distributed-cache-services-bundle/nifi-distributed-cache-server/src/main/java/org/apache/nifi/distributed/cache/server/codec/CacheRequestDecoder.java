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
import org.apache.nifi.distributed.cache.operations.StandardCacheOperation;
import org.apache.nifi.distributed.cache.server.protocol.CacheRequest;
import org.apache.nifi.distributed.cache.server.protocol.CacheVersionRequest;
import org.apache.nifi.logging.ComponentLog;

import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Cache Request Decoder processes bytes and decodes cache version and operation requests
 */
public class CacheRequestDecoder extends ByteToMessageDecoder {
    private static final int HEADER_LENGTH = 4;

    private static final int LONG_LENGTH = 8;

    private static final int INT_LENGTH = 4;

    private static final int SHORT_LENGTH = 2;

    private final AtomicBoolean headerReceived = new AtomicBoolean();

    private final AtomicInteger protocolVersion = new AtomicInteger();

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
            readHeader(byteBuf, channelHandlerContext.channel().remoteAddress());
        }

        if (protocolVersion.get() == 0) {
            final OptionalInt clientVersion = readInt(byteBuf);
            if (clientVersion.isPresent()) {
                final int clientVersionFound = clientVersion.getAsInt();
                log.debug("Protocol Version [{}] Received [{}]", clientVersionFound, channelHandlerContext.channel().remoteAddress());
                final CacheVersionRequest cacheVersionRequest = new CacheVersionRequest(clientVersionFound);
                objects.add(cacheVersionRequest);
            }
        } else {
            // Mark ByteBuf reader index to reset when sufficient bytes are not found
            byteBuf.markReaderIndex();

            final Optional<CacheOperation> cacheOperation = readOperation(byteBuf);
            if (cacheOperation.isPresent()) {
                final CacheOperation cacheOperationFound = cacheOperation.get();

                final Optional<Object> cacheRequest = readRequest(cacheOperationFound, byteBuf);
                if (cacheRequest.isPresent()) {
                    final Object cacheRequestFound = cacheRequest.get();
                    objects.add(cacheRequestFound);
                } else if (StandardCacheOperation.CLOSE.value().contentEquals(cacheOperationFound.value())) {
                    objects.add(new CacheRequest(cacheOperationFound, null));
                } else {
                    byteBuf.resetReaderIndex();
                    log.debug("Cache Operation [{}] request not processed", cacheOperationFound);
                }
            } else {
                byteBuf.resetReaderIndex();
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
     * @return Request Object or empty when buffer does not contain sufficient bytes
     */
    protected Optional<Object> readRequest(final CacheOperation cacheOperation, final ByteBuf byteBuf) {
        final Optional<byte[]> bytes = readBytes(byteBuf);
        return bytes.map(value -> new CacheRequest(cacheOperation, value));
    }

    /**
     * Read Bytes from buffer based on length indicated
     *
     * @param byteBuf Byte Buffer
     * @return Bytes read or null when buffer does not contain sufficient bytes
     */
    protected Optional<byte[]> readBytes(final ByteBuf byteBuf) {
        final Optional<byte[]> bytesRead;

        final OptionalInt length = readInt(byteBuf);
        if (length.isPresent()) {
            final int readableBytes = byteBuf.readableBytes();
            final int lengthFound = length.getAsInt();
            if (readableBytes >= lengthFound) {
                bytesRead = Optional.of(readBytes(byteBuf, lengthFound));
            } else {
                bytesRead = Optional.empty();
            }
        } else {
            bytesRead = Optional.empty();
        }

        return bytesRead;
    }

    /**
     * Read Unicode String from buffer based on length of available bytes
     *
     * @param byteBuf Byte Buffer
     * @return String or null when buffer does not contain sufficient bytes
     */
    protected Optional<String> readUnicodeString(final ByteBuf byteBuf) {
        final String unicodeString;

        if (byteBuf.readableBytes() >= SHORT_LENGTH) {
            final int length = byteBuf.readUnsignedShort();
            if (length > maxLength) {
                throw new IllegalArgumentException(String.format("Maximum Operation Length [%d] exceeded [%d]", maxLength, length));
            }
            if (byteBuf.readableBytes() >= length) {
                unicodeString = byteBuf.readCharSequence(length, StandardCharsets.UTF_8).toString();
            } else {
                unicodeString = null;
            }
        } else {
            unicodeString = null;
        }

        return Optional.ofNullable(unicodeString);
    }

    /**
     * Read Integer from buffer
     *
     * @param byteBuf Byte Buffer
     * @return Integer or empty when buffer does not contain sufficient bytes
     */
    protected OptionalInt readInt(final ByteBuf byteBuf) {
        final Integer integer;

        final int readableBytes = byteBuf.readableBytes();
        if (readableBytes >= INT_LENGTH) {
            integer = byteBuf.readInt();
            if (integer > maxLength) {
                throw new IllegalArgumentException(String.format("Maximum Length [%d] exceeded [%d]", maxLength, integer));
            }
        } else {
            integer = null;
        }

        return integer == null ? OptionalInt.empty() : OptionalInt.of(integer);
    }

    protected OptionalLong readLong(final ByteBuf byteBuf) {
        final int readableBytes = byteBuf.readableBytes();
        return readableBytes >= LONG_LENGTH ? OptionalLong.of(byteBuf.readLong()) : OptionalLong.empty();
    }

    private byte[] readBytes(final ByteBuf byteBuf, final int length) {
        final byte[] bytes = new byte[length];
        byteBuf.readBytes(bytes);
        return bytes;
    }

    private Optional<CacheOperation> readOperation(final ByteBuf byteBuf) {
        final Optional<String> clientOperation = readUnicodeString(byteBuf);

        return clientOperation.map(operation -> Arrays.stream(supportedOperations)
                .filter(supportedOperation -> supportedOperation.value().contentEquals(operation))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(String.format("Cache Operation not supported [%d]", operation.length())))
        );
    }

    private void readHeader(final ByteBuf byteBuf, final SocketAddress remoteAddress) {
        if (byteBuf.readableBytes() >= HEADER_LENGTH) {
            byteBuf.readBytes(HEADER_LENGTH);
            headerReceived.getAndSet(true);
            log.debug("Header Received [{}]", remoteAddress);
        }
    }
}
