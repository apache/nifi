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
package org.apache.nifi.processors.beats.handler;


import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.listen.dispatcher.AsyncChannelDispatcher;
import org.apache.nifi.processor.util.listen.event.Event;
import org.apache.nifi.processor.util.listen.event.EventFactory;
import org.apache.nifi.processor.util.listen.event.EventFactoryUtil;
import org.apache.nifi.processor.util.listen.event.EventQueue;
import org.apache.nifi.processor.util.listen.response.ChannelResponder;
import org.apache.nifi.processors.beats.event.BeatsMetadata;
import org.apache.nifi.processors.beats.frame.BeatsEncoder;
import org.apache.nifi.processors.beats.frame.BeatsFrame;


/**
 * Encapsulates the logic to handle a BeatsFrame once it has been read from the channel.
 */
public class BeatsFrameHandler<E extends Event<SocketChannel>> {

    private final Charset charset;
    private final EventFactory<E> eventFactory;
    private final EventQueue<E> events;
    private final SelectionKey key;
    private final AsyncChannelDispatcher dispatcher;
    private final BeatsEncoder encoder;
    private final ComponentLog logger;

    public static final byte FRAME_WINDOWSIZE = 0x57, FRAME_DATA = 0x44, FRAME_COMPRESSED = 0x43, FRAME_ACK = 0x41, FRAME_JSON = 0x4a;

    public BeatsFrameHandler(final SelectionKey selectionKey,
                             final Charset charset,
                             final EventFactory<E> eventFactory,
                             final BlockingQueue<E> events,
                             final AsyncChannelDispatcher dispatcher,
                             final ComponentLog logger) {
        this.key = selectionKey;
        this.charset = charset;
        this.eventFactory = eventFactory;
        this.dispatcher = dispatcher;
        this.logger = logger;
        this.events = new EventQueue<>(events, logger);
        this.encoder = new BeatsEncoder();
    }

    public void handle(final BeatsFrame frame, final ChannelResponder<SocketChannel> responder, final String sender)
            throws IOException, InterruptedException {

        final Map<String, String> metadata = EventFactoryUtil.createMapWithSender(sender.toString());
        metadata.put(BeatsMetadata.SEQNUMBER_KEY, String.valueOf(frame.getSeqNumber()));
        String line = "";

        /* If frameType is a JSON , parse the frame payload into a JsonElement so that all JSON elements but "message"
        are inserted into the event metadata.

        As per above, the "message" element gets added into the body of the event
        */
        if (frame.getFrameType() == FRAME_JSON ) {
            // queue the raw event blocking until space is available, reset the buffer
            final E event = eventFactory.create(frame.getPayload(), metadata, responder);
            events.offer(event);
        }
    }
  }
