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

import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.util.listen.dispatcher.AsyncChannelDispatcher;
import org.apache.nifi.processor.util.listen.event.EventFactory;
import org.apache.nifi.processor.util.listen.response.ChannelResponder;
import org.apache.nifi.processor.util.listen.response.ChannelResponse;
import org.apache.nifi.processors.lumberjack.event.LumberjackEvent;
import org.apache.nifi.processors.lumberjack.event.LumberjackEventFactory;
import org.apache.nifi.processors.lumberjack.frame.LumberjackEncoder;
import org.apache.nifi.processors.lumberjack.frame.LumberjackFrame;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.xml.bind.DatatypeConverter;



public class TestLumberjackFrameHandler {
    private Charset charset;
    private EventFactory<LumberjackEvent> eventFactory;
    private BlockingQueue<LumberjackEvent> events;
    private SelectionKey key;
    private AsyncChannelDispatcher dispatcher;
    private LumberjackEncoder encoder;

    private ProcessorLog logger;

    private LumberjackFrameHandler<LumberjackEvent> frameHandler;

    @Before
    public void setup() {
        this.charset = StandardCharsets.UTF_8;
        this.eventFactory = new LumberjackEventFactory();
        this.events = new LinkedBlockingQueue<>();
        this.key = Mockito.mock(SelectionKey.class);
        this.dispatcher = Mockito.mock(AsyncChannelDispatcher.class);
        this.logger = Mockito.mock(ProcessorLog.class);
        this.encoder = new LumberjackEncoder();

        this.frameHandler = new LumberjackFrameHandler<>(key, charset, eventFactory, events, dispatcher, logger);
    }

    @Test
    public void testOpen() throws IOException, InterruptedException {
        final LumberjackFrame openFrame = new LumberjackFrame.Builder()
                .version((byte) 0x31)
                .frameType((byte) 0x57)
                .dataSize(0)
                .seqNumber(-1)
                .payload(DatatypeConverter.parseHexBinary("0001"))
                .build();


        final String sender = "sender1";
        final CapturingChannelResponder responder = new CapturingChannelResponder();

        // call the handler and verify respond() was called once with once response
        frameHandler.handle(openFrame, responder, sender);

        // TODO: Implement assertions
    }

    private static class CapturingChannelResponder implements ChannelResponder<SocketChannel> {

        int responded;
        List<ChannelResponse> responses = new ArrayList<>();

        @Override
        public SocketChannel getChannel() {
            return Mockito.mock(SocketChannel.class);
        }

        @Override
        public List<ChannelResponse> getResponses() {
            return responses;
        }

        @Override
        public void addResponse(ChannelResponse response) {
            responses.add(response);
        }

        @Override
        public void respond() throws IOException {
            responded++;
        }
    }
}
