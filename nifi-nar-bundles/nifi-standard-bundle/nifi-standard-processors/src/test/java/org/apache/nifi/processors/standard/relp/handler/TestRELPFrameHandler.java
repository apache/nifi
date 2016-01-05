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

import org.apache.nifi.processor.util.listen.dispatcher.AsyncChannelDispatcher;
import org.apache.nifi.processor.util.listen.event.EventFactory;
import org.apache.nifi.processor.util.listen.response.ChannelResponder;
import org.apache.nifi.processor.util.listen.response.ChannelResponse;
import org.apache.nifi.processors.standard.relp.event.RELPEvent;
import org.apache.nifi.processors.standard.relp.event.RELPEventFactory;
import org.apache.nifi.processors.standard.relp.frame.RELPFrame;
import org.junit.Assert;
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

public class TestRELPFrameHandler {

    private Charset charset;
    private EventFactory<RELPEvent> eventFactory;
    private BlockingQueue<RELPEvent> events;
    private SelectionKey key;
    private AsyncChannelDispatcher dispatcher;

    private RELPFrameHandler<RELPEvent> frameHandler;

    @Before
    public void setup() {
        this.charset = StandardCharsets.UTF_8;
        this.eventFactory = new RELPEventFactory();
        this.events = new LinkedBlockingQueue<>();
        this.key = Mockito.mock(SelectionKey.class);
        this.dispatcher = Mockito.mock(AsyncChannelDispatcher.class);

        this.frameHandler = new RELPFrameHandler<>(key, charset, eventFactory, events, dispatcher);
    }

    @Test
    public void testOpen() throws IOException, InterruptedException {
        final String offer1 = "relp_version=0";
        final String offer2 = "relp_software=librelp,1.2.7,http://librelp.adiscon.com";
        final String offer3 = "commands=syslog";

        final String data = offer1 + "\n" + offer2 + "\n" + offer3;

        final RELPFrame openFrame = new RELPFrame.Builder()
                .txnr(1).command("open")
                .dataLength(data.length())
                .data(data.getBytes(charset))
                .build();

        final String sender = "sender1";
        final CapturingChannelResponder responder = new CapturingChannelResponder();

        // call the handler and verify respond() was called once with once response
        frameHandler.handle(openFrame, responder, sender);
        Assert.assertEquals(1, responder.responded);
        Assert.assertEquals(1, responder.responses.size());

        // verify the response sent back the offers that were received
        final ChannelResponse response = responder.responses.get(0);
        final String responseData = new String(response.toByteArray(), charset);
        Assert.assertTrue(responseData.contains(offer1));
        Assert.assertTrue(responseData.contains(offer2));
        Assert.assertTrue(responseData.contains(offer3));
    }

    @Test
    public void testClose() throws IOException, InterruptedException {
        final RELPFrame openFrame = new RELPFrame.Builder()
                .txnr(1).command("close")
                .dataLength(0)
                .data(new byte[0])
                .build();

        final String sender = "sender1";
        final CapturingChannelResponder responder = new CapturingChannelResponder();

        // call the handler and verify respond() was called once with once response
        frameHandler.handle(openFrame, responder, sender);
        Assert.assertEquals(1, responder.responded);
        Assert.assertEquals(1, responder.responses.size());

        // verify the response sent back the offers that were received
        final ChannelResponse response = responder.responses.get(0);
        final String responseData = new String(response.toByteArray(), charset);
        Assert.assertTrue(responseData.contains("200 OK"));
    }

    @Test
    public void testCommand() throws IOException, InterruptedException {
        final String data = "this is a syslog message";

        final RELPFrame openFrame = new RELPFrame.Builder()
                .txnr(1).command("syslog")
                .dataLength(data.length())
                .data(data.getBytes(charset))
                .build();

        final String sender = "sender1";
        final CapturingChannelResponder responder = new CapturingChannelResponder();

        // call the handler and verify respond() was called once with once response
        frameHandler.handle(openFrame, responder, sender);
        Assert.assertEquals(0, responder.responded);
        Assert.assertEquals(0, responder.responses.size());
        Assert.assertEquals(1, events.size());

        final RELPEvent event = events.poll();
        Assert.assertEquals(data, new String(event.getData(), charset));
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
