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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.listen.event.EventFactoryUtil;
import org.apache.nifi.processors.beats.frame.BeatsMetadata;
import org.apache.nifi.processors.beats.frame.BeatsDecoder;
import org.apache.nifi.processors.beats.frame.BeatsEncoder;
import org.apache.nifi.processors.beats.frame.BeatsFrame;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

/**
 * Decode a Beats message's bytes into a BeatsMessage object
 */
public class BeatsFrameDecoder extends ByteToMessageDecoder {

    private Charset charset;
    private BeatsDecoder decoder;
    private final ComponentLog logger;
    private final BeatsEncoder encoder;
    private final BeatsMessageFactory messageFactory;

    public static final byte FRAME_WINDOWSIZE = 0x57, FRAME_DATA = 0x44, FRAME_COMPRESSED = 0x43, FRAME_ACK = 0x41, FRAME_JSON = 0x4a;

    public BeatsFrameDecoder(final ComponentLog logger, final Charset charset) {
        this.charset = charset;
        this.logger = logger;
        this.encoder = new BeatsEncoder();
        this.messageFactory = new BeatsMessageFactory();
    }

    @Override
    protected void decode(final ChannelHandlerContext ctx, final ByteBuf in, final List<Object> out) throws Exception {
        final int total = in.readableBytes();
        final String senderSocket = ctx.channel().remoteAddress().toString();
        this.decoder = new BeatsDecoder(charset, logger);

        for (int i = 0; i < total; i++) {
            byte currByte = in.readByte();

            // decode the bytes and once we find the end of a frame, handle the frame
            if (decoder.process(currByte)) {

                final List<BeatsFrame> frames = decoder.getFrames();

                for (BeatsFrame frame : frames) {
                    logger.debug("Received Beats frame with transaction {} and frame type {}",
                            frame.getSeqNumber(), frame.getFrameType());
                    // Ignore the WINDOW SIZE type frames as they contain no payload.
                    if (frame.getFrameType() != 0x57) {
                        handle(frame, senderSocket, out);
                    }
                }
            }
        }
        logger.debug("Done processing buffer");
    }

    private void handle(final BeatsFrame frame, final String sender, final List<Object> out) {
        final Map<String, String> metadata = EventFactoryUtil.createMapWithSender(sender);
        metadata.put(BeatsMetadata.SEQNUMBER_KEY, String.valueOf(frame.getSeqNumber()));

        /* If frameType is a JSON , parse the frame payload into a JsonElement so that all JSON elements but "message"
        are inserted into the event metadata.

        As per above, the "message" element gets added into the body of the event
        */
        if (frame.getFrameType() == FRAME_JSON) {
            // queue the raw event blocking until space is available
            final BeatsMessage event = messageFactory.create(frame.getPayload(), metadata);
            out.add(event);
        }
    }
}
