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
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.listen.dispatcher.AsyncChannelDispatcher;
import org.apache.nifi.processor.util.listen.event.EventFactory;
import org.apache.nifi.processor.util.listen.response.ChannelResponder;
import org.apache.nifi.processor.util.listen.response.ChannelResponse;
import org.apache.nifi.processors.lumberjack.event.LumberjackEvent;
import org.apache.nifi.processors.lumberjack.event.LumberjackEventFactory;
import org.apache.nifi.processors.lumberjack.frame.LumberjackEncoder;
import org.apache.nifi.processors.lumberjack.frame.LumberjackFrame;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

@SuppressWarnings("deprecation")
public class TestLumberjackFrameHandler {
    private Charset charset;
    private EventFactory<LumberjackEvent> eventFactory;
    private BlockingQueue<LumberjackEvent> events;
    private SelectionKey key;
    private AsyncChannelDispatcher dispatcher;
    private LumberjackEncoder encoder;

    private ComponentLog logger;

    private LumberjackFrameHandler<LumberjackEvent> frameHandler;

    @Before
    public void setup() {
        this.charset = StandardCharsets.UTF_8;
        this.eventFactory = new LumberjackEventFactory();
        this.events = new LinkedBlockingQueue<>();
        this.key = Mockito.mock(SelectionKey.class);
        this.dispatcher = Mockito.mock(AsyncChannelDispatcher.class);
        this.logger = Mockito.mock(ComponentLog.class);

        this.frameHandler = new LumberjackFrameHandler<>(key, charset, eventFactory, events, dispatcher, logger);
    }

    @Test
    public void testWindow() throws IOException, InterruptedException {
        final LumberjackFrame openFrame = new LumberjackFrame.Builder()
            .version((byte) 0x31)
            .frameType((byte) 0x57)
            .seqNumber(-1)
            .payload(Integer.toString(1).getBytes())
            .build();


        final String sender = "sender1";
        final CapturingChannelResponder responder = new CapturingChannelResponder();

        // call the handler and verify respond() was called once with once response
        frameHandler.handle(openFrame, responder, sender);

        // No response expected
        Assert.assertEquals(0, responder.responded);
    }


    @Test
    public void testData() throws IOException, InterruptedException {
        final byte payload[] = new byte[]{
            0x00, 0x00, 0x00, 0x02,  // Number of pairs
            0x00, 0x00, 0x00, 0x04,  // Length of first pair key ('line')
            0x6C, 0x69, 0x6E, 0x65, // 'line'
            0x00, 0x00, 0x00, 0x0C, // Length of 'test-content'
            0x74, 0x65, 0x73, 0x74, //
            0x2d, 0x63, 0x6f, 0x6e, // 'test-content'
            0x74, 0x65, 0x6e, 0x74, //
            0x00, 0x00, 0x00, 0x05, // Length of 2nd pair key (field)
            0x66, 0x69, 0x65, 0x6c, //
            0x64,                               // 'field'
            0x00, 0x00, 0x00, 0x05, // Length of 'value'
            0x76, 0x61, 0x6c, 0x75, // 'value'
            0x65
        };

        final LumberjackFrame dataFrame = new LumberjackFrame.Builder()
            .version((byte) 0x31)
            .frameType((byte) 0x44)
            .seqNumber(1)
            // Payload eq { enil: hello }
            .payload(payload)
            .build();


        final String sender = "sender1";
        final CapturingChannelResponder responder = new CapturingChannelResponder();

        // call the handler and verify respond() was called once with once response
        frameHandler.handle(dataFrame, responder, sender);

        // No response expected
        Assert.assertEquals(0, responder.responded);
        // But events should contain one event
        Assert.assertEquals(1, events.size());

        final LumberjackEvent event = events.poll();
        Assert.assertEquals("{\"field\":\"value\"}", new String((event.getFields())));
        Assert.assertEquals("test-content", new String(event.getData(), charset));
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
