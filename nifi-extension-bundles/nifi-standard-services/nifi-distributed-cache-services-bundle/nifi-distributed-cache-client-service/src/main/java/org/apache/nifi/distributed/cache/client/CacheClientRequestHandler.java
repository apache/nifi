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
import org.apache.nifi.distributed.cache.client.adapter.InboundAdapter;
import org.apache.nifi.distributed.cache.client.adapter.NullInboundAdapter;
import org.apache.nifi.distributed.cache.client.adapter.OutboundAdapter;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.concurrent.TimeUnit;

/**
 * The {@link io.netty.channel.ChannelHandler} responsible for sending client requests and receiving server responses
 * in the context of a distributed cache server.
 */
public class CacheClientRequestHandler extends ChannelInboundHandlerAdapter {

    /**
     * The object used to buffer and interpret the service response byte stream.
     */
    private InboundAdapter inboundAdapter = new NullInboundAdapter();

    /**
     * The synchronization construct used to signal the client application that the server response has been received.
     */
    private ChannelPromise channelPromise;

    /**
     * THe network timeout associated with the connection
     */
    private final long timeoutMillis;

    public CacheClientRequestHandler(final long timeoutMillis) {
        this.timeoutMillis = timeoutMillis;
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
        final ByteBuf byteBuf = (ByteBuf) msg;
        try {
            final byte[] bytes = new byte[byteBuf.readableBytes()];
            byteBuf.readBytes(bytes);
            inboundAdapter.queue(bytes);
        } finally {
            byteBuf.release();
        }
    }

    @Override
    public void channelReadComplete(final ChannelHandlerContext ctx) throws IOException {
        inboundAdapter.dequeue();
        if (inboundAdapter.isComplete() && !channelPromise.isSuccess()) {
            channelPromise.setSuccess();
        }
    }

    @Override
    public void channelUnregistered(final ChannelHandlerContext ctx) {
        if (!inboundAdapter.isComplete()) {
            channelPromise.setFailure(new IOException("Channel unregistered before processing completed: " + ctx.channel().toString()));
        }
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
        channelPromise.setFailure(cause);
    }

    /**
     * Perform a synchronous method call to the server.  The server is expected to write
     * a byte stream response to the channel, which may be deserialized into a Java object
     * by the caller.
     *
     * The receipt of data outside the context of a call to this method is unexpected; the data is dropped.
     *
     * @param channel         the network channel used to make the request
     * @param outboundAdapter the request payload, which might be a method name, and [0..n] concatenated arguments
     * @param inboundAdapter  the business logic to deserialize the server response
     */
    public void invoke(final Channel channel, final OutboundAdapter outboundAdapter, final InboundAdapter inboundAdapter) throws IOException {
        final CacheClientHandshakeHandler handshakeHandler = channel.pipeline().get(CacheClientHandshakeHandler.class);
        handshakeHandler.waitHandshakeComplete();
        if (handshakeHandler.isSuccess()) {
            if (handshakeHandler.getVersionNegotiator().getVersion() < outboundAdapter.getMinimumVersion()) {
                throw new UnsupportedOperationException("Remote cache server doesn't support protocol version " + outboundAdapter.getMinimumVersion());
            }
            this.inboundAdapter = inboundAdapter;
            channelPromise = channel.newPromise();
            channel.writeAndFlush(Unpooled.wrappedBuffer(outboundAdapter.toBytes()));
            final boolean completed = channelPromise.awaitUninterruptibly(timeoutMillis, TimeUnit.MILLISECONDS);
            if (!completed) {
                throw new SocketTimeoutException(String.format("Request invocation timeout [%d ms] to remote address [%s]", timeoutMillis, channel.remoteAddress()));
            }
            this.inboundAdapter = new NullInboundAdapter();
            if (channelPromise.cause() != null) {
                throw new IOException("Request invocation failed", channelPromise.cause());
            }
        } else {
            throw new IOException("Request invocation failed", handshakeHandler.cause());
        }
    }
}
