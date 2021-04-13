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

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.nifi.event.transport.message.ByteArrayMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;

/**
 * Channel Handler for queuing bytes received as Byte Array Messages
 */
@ChannelHandler.Sharable
public class ByteArrayMessageChannelHandler extends SimpleChannelInboundHandler<ByteArrayMessage> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ByteArrayMessageChannelHandler.class);

    private final BlockingQueue<ByteArrayMessage> messages;

    public ByteArrayMessageChannelHandler(final BlockingQueue<ByteArrayMessage> messages) {
        this.messages = messages;
    }

    /**
     * Read Channel message and offer to queue for external processing
     *
     * @param channelHandlerContext Channel Handler Context
     * @param message Byte Array Message for processing
     */
    @Override
    protected void channelRead0(final ChannelHandlerContext channelHandlerContext, final ByteArrayMessage message) {
        LOGGER.debug("Message Received Length [{}] Remote Address [{}] ", message.getMessage().length, message.getSender());
        messages.add(message);
    }
}
