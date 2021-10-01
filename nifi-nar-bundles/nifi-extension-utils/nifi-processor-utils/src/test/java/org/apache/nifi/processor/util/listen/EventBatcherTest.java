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
package org.apache.nifi.processor.util.listen;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.listen.event.EventFactoryUtil;
import org.apache.nifi.processor.util.listen.event.StandardNettyEvent;
import org.apache.nifi.processor.util.listen.event.StandardNettyEventFactory;
import org.apache.nifi.remote.io.socket.NetworkUtils;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.SharedSessionState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mockito;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class EventBatcherTest {

    static final String SENDER1 = "sender1";
    static final String SENDER2 = "sender2";
    static final String MESSAGE_DATA_1 = "some message data";
    static final String MESSAGE_DATA_2 = "some more data";
    static Processor processor;
    static final AtomicLong idGenerator = new AtomicLong(0L);
    static final ComponentLog logger = mock(ComponentLog.class);
    static BlockingQueue events;
    static BlockingQueue errorEvents;
    static EventBatcher batcher;
    static MockProcessSession session;
    static StandardNettyEventFactory eventFactory;

    @BeforeEach
    public void setUp() {
        processor = new SimpleProcessor();
        events = new LinkedBlockingQueue<>();
        errorEvents = new LinkedBlockingQueue<>();
        batcher = new EventBatcher<StandardNettyEvent>(logger, events, errorEvents) {
            @Override
            protected String getBatchKey(StandardNettyEvent event) {
                return event.getSender().toString();
            }
        };
        session = new MockProcessSession(new SharedSessionState(processor, idGenerator), Mockito.mock(Processor.class));
        eventFactory = new StandardNettyEventFactory();
    }

    @Test
    public void testGetBatches() throws InterruptedException {
        // final ProcessSession session, final int totalBatchSize, final byte[] messageDemarcatorBytes
        InetSocketAddress sender1 = new InetSocketAddress(NetworkUtils.getAvailableTcpPort());
        InetSocketAddress sender2 = new InetSocketAddress(NetworkUtils.getAvailableTcpPort());
        final Map<String, String> sender1Metadata = EventFactoryUtil.createMapWithSender(sender1.toString());
        final Map<String, String> sender2Metadata = EventFactoryUtil.createMapWithSender(sender2.toString());
        events.put(eventFactory.create(MESSAGE_DATA_1.getBytes(StandardCharsets.UTF_8), sender1Metadata, sender1));
        events.put(eventFactory.create(MESSAGE_DATA_1.getBytes(StandardCharsets.UTF_8), sender1Metadata, sender1));
        events.put(eventFactory.create(MESSAGE_DATA_1.getBytes(StandardCharsets.UTF_8), sender1Metadata, sender1));
        events.put(eventFactory.create(MESSAGE_DATA_1.getBytes(StandardCharsets.UTF_8), sender1Metadata, sender1));
        events.put(eventFactory.create(MESSAGE_DATA_2.getBytes(StandardCharsets.UTF_8), sender2Metadata, sender2));
        events.put(eventFactory.create(MESSAGE_DATA_2.getBytes(StandardCharsets.UTF_8), sender2Metadata, sender2));
        Map<String, FlowFileNettyEventBatch> batches = batcher.getBatches(session, 100, "\n".getBytes(StandardCharsets.UTF_8));
        assertEquals(2, batches.size());
        assertEquals(4, batches.get(sender1.toString()).getEvents().size());
        assertEquals(2, batches.get(sender2.toString()).getEvents().size());
    }

    public static class SimpleProcessor extends AbstractProcessor {
        @Override
        public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        }
    }
}