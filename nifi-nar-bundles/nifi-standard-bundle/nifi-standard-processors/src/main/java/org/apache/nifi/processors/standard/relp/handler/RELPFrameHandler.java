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
package org.apache.nifi.processors.standard.relp.handler;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.listen.dispatcher.AsyncChannelDispatcher;
import org.apache.nifi.processor.util.listen.event.Event;
import org.apache.nifi.processor.util.listen.event.EventFactory;
import org.apache.nifi.processor.util.listen.event.EventFactoryUtil;
import org.apache.nifi.processor.util.listen.event.EventQueue;
import org.apache.nifi.processor.util.listen.response.ChannelResponder;
import org.apache.nifi.processor.util.listen.response.ChannelResponse;
import org.apache.nifi.processors.standard.relp.event.RELPMetadata;
import org.apache.nifi.processors.standard.relp.frame.RELPEncoder;
import org.apache.nifi.processors.standard.relp.frame.RELPFrame;
import org.apache.nifi.processors.standard.relp.response.RELPChannelResponse;
import org.apache.nifi.processors.standard.relp.response.RELPResponse;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * Encapsulates the logic to handle a RELPFrame once it has been read from the channel.
 */
public class RELPFrameHandler<E extends Event<SocketChannel>> {

    static final String CMD_OPEN = "open";
    static final String CMD_CLOSE = "close";

    private final Charset charset;
    private final EventFactory<E> eventFactory;
    private final EventQueue<E> events;
    private final SelectionKey key;
    private final AsyncChannelDispatcher dispatcher;
    private final ComponentLog logger;
    private final RELPEncoder encoder;

    public RELPFrameHandler(final SelectionKey selectionKey,
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
        this.encoder = new RELPEncoder(charset);
    }

    public void handle(final RELPFrame frame, final ChannelResponder<SocketChannel> responder, final String sender)
            throws IOException, InterruptedException {

        // respond to open and close commands immediately, create and queue an event for everything else
        if (CMD_OPEN.equals(frame.getCommand())) {
            Map<String,String> offers = RELPResponse.parseOffers(frame.getData(), charset);
            ChannelResponse response = new RELPChannelResponse(encoder, RELPResponse.open(frame.getTxnr(), offers));
            responder.addResponse(response);
            responder.respond();
        } else if (CMD_CLOSE.equals(frame.getCommand())) {
            ChannelResponse response = new RELPChannelResponse(encoder, RELPResponse.ok(frame.getTxnr()));
            responder.addResponse(response);
            responder.respond();
            dispatcher.completeConnection(key);
        } else {
            final Map<String, String> metadata = EventFactoryUtil.createMapWithSender(sender.toString());
            metadata.put(RELPMetadata.TXNR_KEY, String.valueOf(frame.getTxnr()));
            metadata.put(RELPMetadata.COMMAND_KEY, frame.getCommand());

            final E event = eventFactory.create(frame.getData(), metadata, responder);
            events.offer(event);
        }
    }

}
