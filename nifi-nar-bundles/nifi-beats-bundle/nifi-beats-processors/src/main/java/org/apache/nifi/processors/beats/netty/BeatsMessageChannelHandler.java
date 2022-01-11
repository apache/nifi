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
package org.apache.nifi.processors.beats.netty;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.nifi.processors.beats.frame.BeatsEncoder;
import org.apache.nifi.processors.beats.response.BeatsChannelResponse;
import org.apache.nifi.processors.beats.response.BeatsResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;

/**
 * Decode data received into a BeatsMessage
 */
@ChannelHandler.Sharable
public class BeatsMessageChannelHandler extends SimpleChannelInboundHandler<BeatsMessage> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BeatsMessageChannelHandler.class);
    private final BlockingQueue<BeatsMessage> events;
    private final BeatsEncoder encoder;

    public BeatsMessageChannelHandler(BlockingQueue<BeatsMessage> events) {
        this.events = events;
        this.encoder = new BeatsEncoder();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, BeatsMessage msg) {
        LOGGER.debug("Beats Message Received Length [{}] Remote Address [{}] ", msg.getMessage().length, msg.getSender());
        if (events.offer(msg)) {
            LOGGER.debug("Event Queued: Beats Message Sender [{}] Sequence Number [{}]", msg.getSender(), msg.getSeqNumber());
            ctx.writeAndFlush(Unpooled.wrappedBuffer(new BeatsChannelResponse(encoder, BeatsResponse.ok(msg.getSeqNumber())).toByteArray()));
        } else {
            LOGGER.debug("Beats Queue Full: Failed Beats Message Sender [{}] Sequence Number [{}]", msg.getSender(), msg.getSeqNumber());
            // TODO: Not sure if there's a way to respond with an error in Beats protocol..
        }
    }
}
