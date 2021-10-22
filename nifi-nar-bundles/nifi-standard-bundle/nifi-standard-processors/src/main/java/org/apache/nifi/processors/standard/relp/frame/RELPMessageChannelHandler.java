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
package org.apache.nifi.processors.standard.relp.frame;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.nifi.processors.standard.relp.event.RELPMessage;
import org.apache.nifi.processors.standard.relp.response.RELPChannelResponse;
import org.apache.nifi.processors.standard.relp.response.RELPResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.concurrent.BlockingQueue;

/**
 * Decode data received into a RELPMessage
 */
@ChannelHandler.Sharable
public class RELPMessageChannelHandler extends SimpleChannelInboundHandler<RELPMessage> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RELPMessageChannelHandler.class);
    private final BlockingQueue<RELPMessage> events;
    private final RELPEncoder encoder;

    public RELPMessageChannelHandler(BlockingQueue<RELPMessage> events, final Charset charset) {
        this.events = events;
        this.encoder = new RELPEncoder(charset);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RELPMessage msg) {
        LOGGER.debug("RELP Message Received Length [{}] Remote Address [{}] ", msg.getMessage().length, msg.getSender());
        if (events.offer(msg)) {
            LOGGER.debug("Event Queued: RELP Message Sender [{}] Transaction Number [{}]", msg.getSender(), msg.getTxnr());
            ctx.writeAndFlush(Unpooled.wrappedBuffer(new RELPChannelResponse(encoder, RELPResponse.ok(msg.getTxnr())).toByteArray()));
        } else {
            LOGGER.debug("Event Queue Full: Failed RELP Message Sender [{}] Transaction Number [{}]", msg.getSender(), msg.getTxnr());
            ctx.writeAndFlush(Unpooled.wrappedBuffer(new RELPChannelResponse(encoder, RELPResponse.serverFullError(msg.getTxnr())).toByteArray()));
        }
    }
}
