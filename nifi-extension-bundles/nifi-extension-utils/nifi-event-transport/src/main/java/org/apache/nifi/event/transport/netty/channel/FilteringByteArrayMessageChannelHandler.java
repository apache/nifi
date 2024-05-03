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
import org.apache.nifi.event.transport.message.ByteArrayMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;

/**
 * Channel Handler for queuing bytes received as Byte Array Messages and filtering empty messages
 */
@ChannelHandler.Sharable
public class FilteringByteArrayMessageChannelHandler extends ByteArrayMessageChannelHandler {
    private static final Logger logger = LoggerFactory.getLogger(FilteringByteArrayMessageChannelHandler.class);

    public FilteringByteArrayMessageChannelHandler(final BlockingQueue<ByteArrayMessage> messages) {
        super(messages);
    }

    /**
     * Read Channel message and offer to queue for external processing
     *
     * @param channelHandlerContext Channel Handler Context
     * @param message Byte Array Message for processing
     */
    @Override
    protected void channelRead0(final ChannelHandlerContext channelHandlerContext, final ByteArrayMessage message) {
        if (message.getMessage().length == 0) {
            logger.debug("Empty Message Received from Remote Address [{}] ", message.getSender());
        } else {
            super.channelRead0(channelHandlerContext, message);
        }
    }
}
