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
package org.apache.nifi.processors.lumberjack.handler;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.listen.dispatcher.AsyncChannelDispatcher;
import org.apache.nifi.processor.util.listen.event.Event;
import org.apache.nifi.processor.util.listen.event.EventFactory;
import org.apache.nifi.processor.util.listen.event.EventFactoryUtil;
import org.apache.nifi.processor.util.listen.event.EventQueue;
import org.apache.nifi.processor.util.listen.response.ChannelResponder;
import org.apache.nifi.processors.lumberjack.event.LumberjackMetadata;
import org.apache.nifi.processors.lumberjack.frame.LumberjackEncoder;
import org.apache.nifi.processors.lumberjack.frame.LumberjackFrame;

import com.google.gson.Gson;

/**
 * Encapsulates the logic to handle a LumberjackFrame once it has been read from the channel.
 */
@Deprecated
public class LumberjackFrameHandler<E extends Event<SocketChannel>> {

    private final Charset charset;
    private final EventFactory<E> eventFactory;
    private final EventQueue<E> events;
    private final SelectionKey key;
    private final AsyncChannelDispatcher dispatcher;
    private final LumberjackEncoder encoder;
    private final ComponentLog logger;

    public LumberjackFrameHandler(final SelectionKey selectionKey,
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
        this.encoder = new LumberjackEncoder();
    }

    public void handle(final LumberjackFrame frame, final ChannelResponder<SocketChannel> responder, final String sender)
            throws IOException, InterruptedException {

        final Map<String, String> metadata = EventFactoryUtil.createMapWithSender(sender.toString());
        metadata.put(LumberjackMetadata.SEQNUMBER_KEY, String.valueOf(frame.getSeqNumber()));
        String line = "";

        /* If frameType is a data Frame, Handle the Lumberjack data payload, iterating over it and extracting
        keys and values into metadata.

        All keys are inserted into metadata with the exception of line that gets added into the body of the event
        */
        if (frame.getFrameType() == 0x44) {
            ByteBuffer currentData = ByteBuffer.wrap(frame.getPayload());
            long pairCount = currentData.getInt() & 0x00000000ffffffffL;
            Map<String,String> fields = new HashMap<>();
            for (int i = 0; i < pairCount; i++) {
                long keyLength = currentData.getInt() & 0x00000000ffffffffL;
                byte[] bytes = new byte[(int) keyLength];
                currentData.get(bytes);
                String key = new String(bytes);
                long valueLength = currentData.getInt() & 0x00000000ffffffffL;
                bytes = new byte[(int) valueLength];
                currentData.get(bytes);
                String value = new String(bytes);

                if (key.equals("line")) {
                    line = value;
                } else {
                    fields.put(key, value);
                }
            }
            // Serialize the fields into a String to push it via metdate
            Gson serialFields = new Gson();

            metadata.put("lumberjack.fields", serialFields.toJson(fields).toString());

            // queue the raw event blocking until space is available, reset the buffer
            final E event = eventFactory.create(line.getBytes(), metadata, responder);
            events.offer(event);
        } else if (frame.getFrameType() == 0x4A ) {
            logger.error("Data type was JSON. JSON payload aren't yet supported, pending the documentation of Lumberjack protocol v2");
        }
    }
  }
