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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.listen.event.EventFactoryUtil;
import org.apache.nifi.processor.util.listen.response.ChannelResponse;
import org.apache.nifi.processors.standard.relp.event.RELPMessageFactory;
import org.apache.nifi.processors.standard.relp.event.RELPMetadata;
import org.apache.nifi.processors.standard.relp.response.RELPChannelResponse;
import org.apache.nifi.processors.standard.relp.response.RELPResponse;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

/**
 * Decode RELP message bytes into a RELPMessage
 */
public class RELPFrameDecoder extends ByteToMessageDecoder {

    private Charset charset;
    private RELPDecoder decoder;
    private final ComponentLog logger;
    private final RELPEncoder encoder;
    private final RELPMessageFactory eventFactory;

    static final String CMD_OPEN = "open";
    static final String CMD_CLOSE = "close";

    public RELPFrameDecoder(final ComponentLog logger, final Charset charset) {
        this.charset = charset;
        this.logger = logger;
        this.encoder = new RELPEncoder(charset);
        this.eventFactory = new RELPMessageFactory();
    }

    @Override
    protected void decode(final ChannelHandlerContext ctx, final ByteBuf in, final List<Object> out) throws Exception {
        final int total = in.readableBytes();
        final String senderSocket = ctx.channel().remoteAddress().toString();
        this.decoder = new RELPDecoder(charset, total);

        // go through the buffer parsing the RELP command
        for (int i = 0; i < total; i++) {
            byte currByte = in.readByte();
            // if we found the end of a frame, handle the frame and mark the buffer
            if (decoder.process(currByte)) {
                final RELPFrame frame = decoder.getFrame();

                logger.debug("Received RELP frame with transaction {} and command {}",
                       frame.getTxnr(), frame.getCommand());
                handle(frame, ctx, senderSocket, out);
                in.markReaderIndex();
            }
        }
    }

    private void handle(final RELPFrame frame, final ChannelHandlerContext ctx, final String sender, final List<Object> out) {
        // respond to open and close commands immediately, create and queue an event for everything else
        if (CMD_OPEN.equals(frame.getCommand())) {
            Map<String,String> offers = RELPResponse.parseOffers(frame.getData(), charset);
            ChannelResponse response = new RELPChannelResponse(encoder, RELPResponse.open(frame.getTxnr(), offers));
            ctx.writeAndFlush(Unpooled.wrappedBuffer(response.toByteArray()));
        } else if (CMD_CLOSE.equals(frame.getCommand())) {
            ChannelResponse response = new RELPChannelResponse(encoder, RELPResponse.ok(frame.getTxnr()));
            ctx.writeAndFlush(Unpooled.wrappedBuffer(response.toByteArray()));
            ctx.close();
        } else {
            final Map<String, String> metadata = EventFactoryUtil.createMapWithSender(sender);
            metadata.put(RELPMetadata.TXNR_KEY, String.valueOf(frame.getTxnr()));
            metadata.put(RELPMetadata.COMMAND_KEY, frame.getCommand());
            metadata.put(RELPMetadata.SENDER_KEY, sender);
            out.add(eventFactory.create(frame.getData(), metadata));
        }
    }
}
