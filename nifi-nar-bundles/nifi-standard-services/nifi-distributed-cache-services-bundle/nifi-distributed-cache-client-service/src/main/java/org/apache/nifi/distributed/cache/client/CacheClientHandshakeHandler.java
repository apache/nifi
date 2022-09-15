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
package org.apache.nifi.distributed.cache.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import org.apache.nifi.distributed.cache.client.adapter.OutboundAdapter;
import org.apache.nifi.distributed.cache.protocol.ProtocolHandshake;
import org.apache.nifi.distributed.cache.protocol.exception.HandshakeException;
import org.apache.nifi.remote.VersionNegotiator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The {@link io.netty.channel.ChannelHandler} responsible for performing the client handshake with the
 * distributed cache server.
 */
public class CacheClientHandshakeHandler extends ChannelInboundHandlerAdapter {

    private static final int PROTOCOL_UNINITIALIZED = 0;

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * The header bytes used to initiate the server handshake.
     */
    private static final byte[] MAGIC_HEADER = new byte[]{'N', 'i', 'F', 'i'};

    /**
     * The synchronization construct used to signal the client application that the handshake has finished.
     */
    private final ChannelPromise promiseHandshakeComplete;

    /**
     * The version of the protocol negotiated between the client and server.
     */
    private final AtomicInteger protocol;

    /**
     * The coordinator used to broker the version of the distributed cache protocol with the service.
     */
    private final VersionNegotiator versionNegotiator;

    /**
     * THe network timeout associated with handshake completion
     */
    private final long timeoutMillis;

    /**
     * Constructor.
     *
     * @param channel           the channel to which this {@link io.netty.channel.ChannelHandler} is bound.
     * @param versionNegotiator coordinator used to broker the version of the distributed cache protocol with the service
     * @param timeoutMillis     the network timeout associated with handshake completion
     */
    public CacheClientHandshakeHandler(final Channel channel, final VersionNegotiator versionNegotiator,
                                       final long timeoutMillis) {
        this.promiseHandshakeComplete = channel.newPromise();
        this.protocol = new AtomicInteger(PROTOCOL_UNINITIALIZED);
        this.versionNegotiator = versionNegotiator;
        this.timeoutMillis = timeoutMillis;
    }

    /**
     * API providing client application with visibility into the handshake process.  Distributed cache requests
     * should not be sent using this {@link Channel} until the handshake is complete.  Since the handshake might fail,
     * {@link #isSuccess()} should be called after this method completes.
     */
    public void waitHandshakeComplete() {
        promiseHandshakeComplete.awaitUninterruptibly(timeoutMillis, TimeUnit.MILLISECONDS);
        if (!promiseHandshakeComplete.isSuccess()) {
            HandshakeException ex = new HandshakeException("Handshake timed out before completion.");
            promiseHandshakeComplete.setFailure(ex);
        }
    }

    /**
     * @return the coordinator used to broker the version of the distributed cache protocol with the service
     */
    public VersionNegotiator getVersionNegotiator() {
        return versionNegotiator;
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws IOException {
        final ByteBuf byteBufMagic = Unpooled.wrappedBuffer(MAGIC_HEADER);
        ctx.write(byteBufMagic);
        logger.debug("Magic header written");
        final int currentVersion = versionNegotiator.getVersion();
        final ByteBuf byteBufVersion = Unpooled.wrappedBuffer(new OutboundAdapter().write(currentVersion).toBytes());
        ctx.writeAndFlush(byteBufVersion);
        logger.debug("Protocol version {} proposed", versionNegotiator.getVersion());
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
        if (promiseHandshakeComplete.isSuccess()) {
            ctx.fireChannelRead(msg);
        } else {
            final ByteBuf byteBuf = (ByteBuf) msg;
            try {
                processHandshake(ctx, byteBuf);
            } catch (IOException | HandshakeException e) {
                throw new IllegalStateException("Handshake Processing Failed", e);
            } finally {
                byteBuf.release();
            }
        }
    }

    /**
     * Negotiate distributed cache protocol version with remote service.
     *
     * @param ctx     the {@link Channel} context
     * @param byteBuf the byte stream received from the remote service
     * @throws HandshakeException on failure to negotiate protocol version
     * @throws IOException        on write failure
     */
    private void processHandshake(final ChannelHandlerContext ctx, final ByteBuf byteBuf) throws HandshakeException, IOException {
        final short statusCode = byteBuf.readUnsignedByte();
        if (statusCode == ProtocolHandshake.RESOURCE_OK) {
            logger.debug("Protocol version {} accepted", versionNegotiator.getVersion());
            protocol.set(versionNegotiator.getVersion());
        } else if (statusCode == ProtocolHandshake.DIFFERENT_RESOURCE_VERSION) {
            final int newVersion = byteBuf.readInt();
            logger.debug("Received protocol version {} counter proposal", newVersion);
            final Integer newPreference = versionNegotiator.getPreferredVersion(newVersion);
            Optional.ofNullable(newPreference).orElseThrow(() -> new HandshakeException(
                    String.format("Received unsupported protocol version proposal [%d]", newVersion)));
            versionNegotiator.setVersion(newPreference);
            ctx.writeAndFlush(Unpooled.wrappedBuffer(new OutboundAdapter().write(newPreference).toBytes()));
        } else if (statusCode == ProtocolHandshake.ABORT) {
            final short length = byteBuf.readShort();
            final byte[] message = new byte[length];
            byteBuf.readBytes(message);
            throw new HandshakeException("Remote destination aborted connection with message: " + new String(message, StandardCharsets.UTF_8));
        } else {
            throw new HandshakeException("Unknown handshake signal: " + statusCode);
        }
    }

    @Override
    public void channelReadComplete(final ChannelHandlerContext ctx) {
        if (promiseHandshakeComplete.isSuccess()) {
            ctx.fireChannelReadComplete();
        } else if (protocol.get() > PROTOCOL_UNINITIALIZED) {
            promiseHandshakeComplete.setSuccess();
        }
    }

    /**
     * Returns if the handshake completed successfully
     *
     * @return success/failure of handshake
     */
    public boolean isSuccess() {
        return promiseHandshakeComplete.isSuccess();
    }

    /**
     * Return reason for handshake failure.
     *
     * @return cause for handshake failure or null on success
     */
    public Throwable cause() {
        return promiseHandshakeComplete.cause();
    }
}
