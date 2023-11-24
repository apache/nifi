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
import org.apache.nifi.util.MockFlowFile;
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
    final String MESSAGE_DATA_3 = "some more data\nwith new line";
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
        events.put(eventFactory.create(MESSAGE_DATA_3.getBytes(StandardCharsets.UTF_8), sender1Metadata));
        events.put(eventFactory.create(MESSAGE_DATA_2.getBytes(StandardCharsets.UTF_8), sender2Metadata));
        events.put(eventFactory.create(MESSAGE_DATA_2.getBytes(StandardCharsets.UTF_8), sender2Metadata));
        Map<String, FlowFileEventBatch> batches = batcher.getBatches(session, 100, "\n".getBytes(StandardCharsets.UTF_8), null);
        assertEquals(2, batches.size());
        assertEquals(4, batches.get(sender1).getEvents().size());
        assertEquals(2, batches.get(sender2).getEvents().size());

    }

    @Test
    public void testGetBatchesContent() throws InterruptedException {
        String sender1 = new InetSocketAddress(0).toString();
        final Map<String, String> sender1Metadata = EventFactoryUtil.createMapWithSender(sender1);
        events.put(eventFactory.create(MESSAGE_DATA_1.getBytes(StandardCharsets.UTF_8), sender1Metadata));
        events.put(eventFactory.create(MESSAGE_DATA_3.getBytes(StandardCharsets.UTF_8), sender1Metadata));
        String lf = "\n";
        Map<String, FlowFileEventBatch> batches = batcher.getBatches(session, 3, lf.getBytes(StandardCharsets.UTF_8), null);

        MockFlowFile mockFlowFile = new MockFlowFile(42, batches.get(sender1).getFlowFile());
        mockFlowFile.assertContentEquals(MESSAGE_DATA_1+lf+MESSAGE_DATA_3);
    }

    @Test
    public void testGetBatchesContentReplace() throws InterruptedException {
        String sender1 = new InetSocketAddress(0).toString();
        final Map<String, String> sender1Metadata = EventFactoryUtil.createMapWithSender(sender1);
        events.put(eventFactory.create(MESSAGE_DATA_1.getBytes(StandardCharsets.UTF_8), sender1Metadata));
        events.put(eventFactory.create(MESSAGE_DATA_3.getBytes(StandardCharsets.UTF_8), sender1Metadata));
        String lf = "\n";
        String lf_replace = "\\n";
        Map<String, FlowFileEventBatch> batches = batcher.getBatches(session, 3, lf.getBytes(StandardCharsets.UTF_8), lf_replace.getBytes(StandardCharsets.UTF_8));

        MockFlowFile mockFlowFile = new MockFlowFile(42, batches.get(sender1).getFlowFile());
        mockFlowFile.assertContentEquals(MESSAGE_DATA_1+lf+MESSAGE_DATA_3.replace(lf, lf_replace));
    }

    public static class SimpleProcessor extends AbstractProcessor {
        @Override
        public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        }
    }
}