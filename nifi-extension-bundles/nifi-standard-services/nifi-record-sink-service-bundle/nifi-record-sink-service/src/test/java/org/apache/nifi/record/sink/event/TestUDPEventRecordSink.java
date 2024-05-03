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
package org.apache.nifi.record.sink.event;

import org.apache.nifi.event.transport.EventServer;
import org.apache.nifi.event.transport.configuration.TransportProtocol;
import org.apache.nifi.event.transport.message.ByteArrayMessage;
import org.apache.nifi.event.transport.netty.ByteArrayMessageNettyEventServerFactory;
import org.apache.nifi.event.transport.netty.NettyEventServerFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TestUDPEventRecordSink {
    private static final String IDENTIFIER = UDPEventRecordSink.class.getSimpleName();

    private static final String WRITER_IDENTIFIER = MockRecordWriter.class.getSimpleName();

    private static final String TRANSIT_URI_FORMAT = "udp://%s:%d";

    private static final String TRANSIT_URI_KEY = "record.sink.url";

    private static final String LOCALHOST = "127.0.0.1";

    private static final String ID_FIELD = "id";

    private static final String ID_FIELD_VALUE = TestUDPEventRecordSink.class.getSimpleName();

    private static final boolean SEND_ZERO_RESULTS = true;

    private static final byte[] DELIMITER = new byte[]{};

    private static final int MAX_FRAME_SIZE = 1024;

    private static final int MESSAGE_POLL_TIMEOUT = 5;

    private static final String NULL_HEADER = null;

    private static final RecordSchema RECORD_SCHEMA = getRecordSchema();

    private static final Record[] RECORDS = getRecords();

    private EventServer eventServer;

    private BlockingQueue<ByteArrayMessage> messages;

    private String transitUri;

    private UDPEventRecordSink sink;

    @BeforeEach
    void setRunner() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);

        final MockRecordWriter recordWriter = new MockRecordWriter(NULL_HEADER, false);
        runner.addControllerService(WRITER_IDENTIFIER, recordWriter);
        runner.enableControllerService(recordWriter);

        eventServer = createServer(runner, 0);
        final int port = eventServer.getListeningPort();

        sink = new UDPEventRecordSink();
        runner.addControllerService(IDENTIFIER, sink);
        runner.setProperty(sink, UDPEventRecordSink.HOSTNAME, LOCALHOST);
        runner.setProperty(sink, UDPEventRecordSink.PORT, Integer.toString(port));
        runner.setProperty(sink, UDPEventRecordSink.RECORD_WRITER_FACTORY, WRITER_IDENTIFIER);
        runner.enableControllerService(sink);

        transitUri = String.format(TRANSIT_URI_FORMAT, LOCALHOST, port);
    }

    @AfterEach
    void shutdownServer() {
        eventServer.shutdown();
    }

    @Test
    void testSendData() throws IOException, InterruptedException {
        final RecordSet recordSet = RecordSet.of(RECORD_SCHEMA, RECORDS);
        final WriteResult writeResult = sink.sendData(recordSet, Collections.emptyMap(), SEND_ZERO_RESULTS);

        assertNotNull(writeResult);
        final String resultTransitUri = writeResult.getAttributes().get(TRANSIT_URI_KEY);
        assertEquals(transitUri, resultTransitUri);
        assertEquals(RECORDS.length, writeResult.getRecordCount());

        final String firstMessage = pollMessage();
        assertEquals(ID_FIELD_VALUE, firstMessage);

        final String secondMessage = pollMessage();
        assertEquals(ID_FIELD_VALUE, secondMessage);
    }

    @Test
    void testSendDataRecordSetEmpty() throws IOException {
        final RecordSet recordSet = RecordSet.of(RECORD_SCHEMA);
        final WriteResult writeResult = sink.sendData(recordSet, Collections.emptyMap(), SEND_ZERO_RESULTS);

        assertNotNull(writeResult);
        final String resultTransitUri = writeResult.getAttributes().get(TRANSIT_URI_KEY);
        assertEquals(transitUri, resultTransitUri);
        assertEquals(0, writeResult.getRecordCount());
    }

    private String pollMessage() throws InterruptedException {
        final ByteArrayMessage record = messages.poll(MESSAGE_POLL_TIMEOUT, TimeUnit.SECONDS);
        assertNotNull(record);
        return new String(record.getMessage(), StandardCharsets.UTF_8).trim();
    }

    private EventServer createServer(final TestRunner runner, final int port) throws Exception {
        messages = new LinkedBlockingQueue<>();
        final InetAddress listenAddress = InetAddress.getByName(LOCALHOST);
        NettyEventServerFactory serverFactory = new ByteArrayMessageNettyEventServerFactory(
                runner.getLogger(),
                listenAddress,
                port,
                TransportProtocol.UDP,
                DELIMITER,
                MAX_FRAME_SIZE,
                messages
        );
        serverFactory.setShutdownQuietPeriod(Duration.ZERO);
        serverFactory.setShutdownTimeout(Duration.ZERO);
        return serverFactory.getEventServer();
    }

    private static RecordSchema getRecordSchema() {
        final RecordField idField = new RecordField(ID_FIELD, RecordFieldType.STRING.getDataType());
        return new SimpleRecordSchema(Collections.singletonList(idField));
    }

    private static Record[] getRecords() {
        final Map<String, Object> values = Collections.singletonMap(ID_FIELD, ID_FIELD_VALUE);
        final Record record = new MapRecord(RECORD_SCHEMA, values);
        return new Record[]{record, record};
    }
}
