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

import org.apache.nifi.event.transport.message.ByteArrayMessage;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.listen.event.EventFactoryUtil;
import org.apache.nifi.processor.util.listen.event.StandardNetworkEventFactory;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.SharedSessionState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

public class EventBatcherTest {

    final String MESSAGE_DATA_1 = "some message data";
    final String MESSAGE_DATA_2 = "some more data";
    Processor processor;
    final AtomicLong idGenerator = new AtomicLong(0L);
    final ComponentLog logger = mock(ComponentLog.class);
    BlockingQueue events;
    BlockingQueue errorEvents;
    EventBatcher batcher;
    MockProcessSession session;
    StandardNetworkEventFactory eventFactory;

    @BeforeEach
    public void setUp() {
        processor = new SimpleProcessor();
        events = new LinkedBlockingQueue<>();
        errorEvents = new LinkedBlockingQueue<>();
        batcher = new EventBatcher<ByteArrayMessage>(logger, events, errorEvents) {
            @Override
            protected String getBatchKey(ByteArrayMessage event) {
                return event.getSender();
            }
        };
        session = new MockProcessSession(new SharedSessionState(processor, idGenerator), Mockito.mock(Processor.class));
        eventFactory = new StandardNetworkEventFactory();
    }

    @Test
    public void testGetBatches() throws InterruptedException {
        String sender1 = new InetSocketAddress(0).toString();
        String sender2 = new InetSocketAddress(2).toString();
        final Map<String, String> sender1Metadata = EventFactoryUtil.createMapWithSender(sender1);
        final Map<String, String> sender2Metadata = EventFactoryUtil.createMapWithSender(sender2);
        events.put(eventFactory.create(MESSAGE_DATA_1.getBytes(StandardCharsets.UTF_8), sender1Metadata));
        events.put(eventFactory.create(MESSAGE_DATA_1.getBytes(StandardCharsets.UTF_8), sender1Metadata));
        events.put(eventFactory.create(MESSAGE_DATA_1.getBytes(StandardCharsets.UTF_8), sender1Metadata));
        events.put(eventFactory.create(MESSAGE_DATA_1.getBytes(StandardCharsets.UTF_8), sender1Metadata));
        events.put(eventFactory.create(MESSAGE_DATA_2.getBytes(StandardCharsets.UTF_8), sender2Metadata));
        events.put(eventFactory.create(MESSAGE_DATA_2.getBytes(StandardCharsets.UTF_8), sender2Metadata));
        Map<String, FlowFileEventBatch> batches = batcher.getBatches(session, 100, "\n".getBytes(StandardCharsets.UTF_8));
        assertEquals(2, batches.size());
        assertEquals(4, batches.get(sender1).getEvents().size());
        assertEquals(2, batches.get(sender2).getEvents().size());
    }

    public static class SimpleProcessor extends AbstractProcessor {
        @Override
        public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        }
    }
}