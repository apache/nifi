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
package org.apache.nifi.processors.standard;

import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.util.listen.dispatcher.ChannelDispatcher;
import org.apache.nifi.processor.util.listen.event.StandardEvent;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TestListenUDPRecord {

    static final String SCHEMA_TEXT = "{\n" +
            "  \"name\": \"syslogRecord\",\n" +
            "  \"namespace\": \"nifi\",\n" +
            "  \"type\": \"record\",\n" +
            "  \"fields\": [\n" +
            "    { \"name\": \"timestamp\", \"type\": \"string\" },\n" +
            "    { \"name\": \"logsource\", \"type\": \"string\" },\n" +
            "    { \"name\": \"message\", \"type\": \"string\" }\n" +
            "  ]\n" +
            "}";

    static final String DATAGRAM_1 = "[ {\"timestamp\" : \"123456789\", \"logsource\" : \"syslog\", \"message\" : \"This is a test 1\"} ]";
    static final String DATAGRAM_2 = "[ {\"timestamp\" : \"123456789\", \"logsource\" : \"syslog\", \"message\" : \"This is a test 2\"} ]";
    static final String DATAGRAM_3 = "[ {\"timestamp\" : \"123456789\", \"logsource\" : \"syslog\", \"message\" : \"This is a test 3\"} ]";

    static final String MULTI_DATAGRAM_1 = "[" +
            "{\"timestamp\" : \"123456789\", \"logsource\" : \"syslog\", \"message\" : \"This is a test 1\"}," +
            "{\"timestamp\" : \"123456789\", \"logsource\" : \"syslog\", \"message\" : \"This is a test 2\"}," +
            "{\"timestamp\" : \"123456789\", \"logsource\" : \"syslog\", \"message\" : \"This is a test 3\"}" +
            "]";

    static final String MULTI_DATAGRAM_2 = "[" +
            "{\"timestamp\" : \"123456789\", \"logsource\" : \"syslog\", \"message\" : \"This is a test 4\"}," +
            "{\"timestamp\" : \"123456789\", \"logsource\" : \"syslog\", \"message\" : \"This is a test 5\"}," +
            "{\"timestamp\" : \"123456789\", \"logsource\" : \"syslog\", \"message\" : \"This is a test 6\"}" +
            "]";

    private TestableListenUDPRecord proc;
    private TestRunner runner;
    private MockRecordWriter mockRecordWriter;

    @Before
    public void setup() throws InitializationException {
        proc = new TestableListenUDPRecord();
        runner = TestRunners.newTestRunner(proc);
        runner.setProperty(ListenUDP.PORT, "1");

        final String readerId = "record-reader";
        final RecordReaderFactory readerFactory = new JsonTreeReader();
        runner.addControllerService(readerId, readerFactory);
        runner.setProperty(readerFactory, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY.getValue());
        runner.setProperty(readerFactory, SchemaAccessUtils.SCHEMA_TEXT, SCHEMA_TEXT);
        runner.enableControllerService(readerFactory);

        final String writerId = "record-writer";
        mockRecordWriter = new MockRecordWriter("timestamp, logsource, message");
        runner.addControllerService(writerId, mockRecordWriter);
        runner.enableControllerService(mockRecordWriter);

        runner.setProperty(ListenUDPRecord.RECORD_READER, readerId);
        runner.setProperty(ListenUDPRecord.RECORD_WRITER, writerId);
    }

    @Test
    public void testSuccessWithBatchSizeGreaterThanAvailableRecords() {
        final String sender = "foo";

        final StandardEvent event1 = new StandardEvent(sender, DATAGRAM_1.getBytes(StandardCharsets.UTF_8), null);
        proc.addEvent(event1);

        final StandardEvent event2 = new StandardEvent(sender, DATAGRAM_2.getBytes(StandardCharsets.UTF_8), null);
        proc.addEvent(event2);

        final StandardEvent event3 = new StandardEvent(sender, DATAGRAM_3.getBytes(StandardCharsets.UTF_8), null);
        proc.addEvent(event3);

        runner.run();
        runner.assertAllFlowFilesTransferred(ListenUDPRecord.REL_SUCCESS, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ListenUDPRecord.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals(ListenUDPRecord.RECORD_COUNT_ATTR, "3");
    }

    @Test
    public void testSuccessWithBatchLessThanAvailableRecords() {
        final String sender = "foo";

        final StandardEvent event1 = new StandardEvent(sender, DATAGRAM_1.getBytes(StandardCharsets.UTF_8), null);
        proc.addEvent(event1);

        final StandardEvent event2 = new StandardEvent(sender, DATAGRAM_2.getBytes(StandardCharsets.UTF_8), null);
        proc.addEvent(event2);

        final StandardEvent event3 = new StandardEvent(sender, DATAGRAM_3.getBytes(StandardCharsets.UTF_8), null);
        proc.addEvent(event3);

        runner.setProperty(ListenUDPRecord.BATCH_SIZE, "1");

        // batch 1
        runner.run();
        runner.assertAllFlowFilesTransferred(ListenUDPRecord.REL_SUCCESS, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ListenUDPRecord.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals(ListenUDPRecord.RECORD_COUNT_ATTR, "1");

        // batch 2
        runner.clearTransferState();
        runner.run();
        runner.assertAllFlowFilesTransferred(ListenUDPRecord.REL_SUCCESS, 1);

        flowFile = runner.getFlowFilesForRelationship(ListenUDPRecord.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals(ListenUDPRecord.RECORD_COUNT_ATTR, "1");

        // batch 3
        runner.clearTransferState();
        runner.run();
        runner.assertAllFlowFilesTransferred(ListenUDPRecord.REL_SUCCESS, 1);

        flowFile = runner.getFlowFilesForRelationship(ListenUDPRecord.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals(ListenUDPRecord.RECORD_COUNT_ATTR, "1");

        // no more left
        runner.clearTransferState();
        runner.run();
        runner.assertAllFlowFilesTransferred(ListenUDPRecord.REL_SUCCESS, 0);
    }

    @Test
    public void testMultipleRecordsPerDatagram() {
        final String sender = "foo";

        final StandardEvent event1 = new StandardEvent(sender, MULTI_DATAGRAM_1.getBytes(StandardCharsets.UTF_8), null);
        proc.addEvent(event1);

        final StandardEvent event2 = new StandardEvent(sender, MULTI_DATAGRAM_2.getBytes(StandardCharsets.UTF_8), null);
        proc.addEvent(event2);

        runner.run();
        runner.assertAllFlowFilesTransferred(ListenUDPRecord.REL_SUCCESS, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ListenUDPRecord.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals(ListenUDPRecord.RECORD_COUNT_ATTR, "6");
    }

    @Test
    public void testParseFailure() {
        final String sender = "foo";

        final StandardEvent event1 = new StandardEvent(sender, DATAGRAM_1.getBytes(StandardCharsets.UTF_8), null);
        proc.addEvent(event1);

        final StandardEvent event2 = new StandardEvent(sender, "WILL NOT PARSE".getBytes(StandardCharsets.UTF_8), null);
        proc.addEvent(event2);

        runner.run();
        runner.assertTransferCount(ListenUDPRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(ListenUDPRecord.REL_PARSE_FAILURE, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ListenUDPRecord.REL_PARSE_FAILURE).get(0);
        flowFile.assertContentEquals("WILL NOT PARSE");
    }

    @Test
    public void testWriterFailure() throws InitializationException {
        // re-create the writer to set fail-after 2 attempts
        final String writerId = "record-writer";
        mockRecordWriter = new MockRecordWriter("timestamp, logsource, message", false, 2);
        runner.addControllerService(writerId, mockRecordWriter);
        runner.enableControllerService(mockRecordWriter);
        runner.setProperty(ListenUDPRecord.RECORD_WRITER, writerId);

        final String sender = "foo";

        final StandardEvent event1 = new StandardEvent(sender, DATAGRAM_1.getBytes(StandardCharsets.UTF_8), null);
        proc.addEvent(event1);

        final StandardEvent event2 = new StandardEvent(sender, DATAGRAM_2.getBytes(StandardCharsets.UTF_8), null);
        proc.addEvent(event2);

        final StandardEvent event3 = new StandardEvent(sender, DATAGRAM_3.getBytes(StandardCharsets.UTF_8), null);
        proc.addEvent(event3);

        runner.run();
        runner.assertAllFlowFilesTransferred(ListenUDPRecord.REL_SUCCESS, 0);
        runner.assertAllFlowFilesTransferred(ListenUDPRecord.REL_PARSE_FAILURE, 0);
    }

    private static class TestableListenUDPRecord extends ListenUDPRecord {

        private volatile BlockingQueue<StandardEvent> testEvents = new LinkedBlockingQueue<>();
        private volatile BlockingQueue<StandardEvent> testErrorEvents = new LinkedBlockingQueue<>();

        @Override
        protected ChannelDispatcher createDispatcher(ProcessContext context, BlockingQueue<StandardEvent> events) throws IOException {
            return Mockito.mock(ChannelDispatcher.class);
        }

        public void addEvent(final StandardEvent event) {
            this.testEvents.add(event);
        }

        public void addErrorEvent(final StandardEvent event) {
            this.testErrorEvents.add(event);
        }

        @Override
        protected StandardEvent getMessage(boolean longPoll, boolean pollErrorQueue, ProcessSession session) {
            StandardEvent event = null;
            if (pollErrorQueue) {
                event = testErrorEvents.poll();
            }

            if (event == null) {
                try {
                    if (longPoll) {
                        event = testEvents.poll(getLongPollTimeout(), TimeUnit.MILLISECONDS);
                    } else {
                        event = testEvents.poll();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return null;
                }
            }

            return event;
        }
    }

}
