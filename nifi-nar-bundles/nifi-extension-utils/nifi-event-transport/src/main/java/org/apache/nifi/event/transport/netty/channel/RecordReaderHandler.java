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
package org.apache.nifi.event.transport.netty.channel;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.ReadTimeoutException;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.RecordReaderFactory;

import java.io.BufferedInputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.SocketAddress;
import java.util.concurrent.BlockingQueue;

/**
 * Record Reader Handler will create piped input streams for a network based record reader, providing a mechanism for processors
 * like ListenTCPRecord to read data received by a Netty server.
 */
@ChannelHandler.Sharable
public class RecordReaderHandler extends SimpleChannelInboundHandler<ByteBuf> {
    private final RecordReaderFactory readerFactory;
    private final BlockingQueue<NetworkRecordReader> recordReaders;
    private final ComponentLog logger;
    private PipedOutputStream fromChannel;
    private PipedInputStream toReader;

    public RecordReaderHandler(final RecordReaderFactory readerFactory, final BlockingQueue<NetworkRecordReader> recordReaders, final ComponentLog logger) {
        this.logger = logger;
        this.readerFactory = readerFactory;
        this.recordReaders = recordReaders;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
        final SocketAddress remoteSender = ctx.channel().remoteAddress();
        logger.info("Netty message received to {}, sender is: {}", ctx.channel().localAddress(), remoteSender);
        fromChannel.write(ByteBufUtil.getBytes(msg));
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        fromChannel.close();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        final SocketAddress remoteSender = ctx.channel().remoteAddress();
        fromChannel = new PipedOutputStream();
        toReader = new PipedInputStream(fromChannel);
        recordReaders.offer(new NetworkRecordReader(remoteSender, new BufferedInputStream(toReader), readerFactory, logger));
        super.channelActive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
            throws Exception {
        if (cause instanceof ReadTimeoutException) {
            fromChannel.close();
        } else {
            super.exceptionCaught(ctx, cause);
        }
    }
}
