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
package org.apache.nifi.services.slack;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.nifi.reporting.InitializationException;
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
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;
import org.apache.nifi.web.client.provider.service.StandardWebClientServiceProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestSlackRecordSink {
    private static final String RESPONSE_SUCCESS_TEXT_MSG_WITH_TIMESTAMP = "{\"ok\": true, \"ts\": \"1503435956.000247\"}";
    private static final String RESPONSE_WARNING = "{\"ok\": true, \"warning\": \"slack-warning\"}";
    private static final String RESPONSE_ERROR = "{\"ok\": false, \"error\": \"slack-error\"}";
    private static final String RESPONSE_EMPTY_JSON = "{}";
    private static final String RESPONSE_INVALID_JSON = "{invalid-json}";

    private static final String CHANNEL_NAME = "my-channel";
    private static final String BEARER_TOKEN = "bearer-token";

    private TestRunner testRunner;
    private MockWebServer mockWebServer;
    private SlackRecordSink slackRecordSink;
    private MockRecordWriter writerFactory;
    private RecordSet recordSet;
    private String recordContentsAsString;
    private ObjectMapper mapper;

    @BeforeEach
    public void setup() throws InitializationException, IOException {
        mapper = new ObjectMapper();

        mockWebServer = new MockWebServer();
        mockWebServer.start();
        String url = mockWebServer.url("/api/").toString();

        testRunner = TestRunners.newTestRunner(NoOpProcessor.class);

        final WebClientServiceProvider webClientServiceProvider = new StandardWebClientServiceProvider();
        testRunner.addControllerService("webClientServiceProvider", webClientServiceProvider);
        testRunner.enableControllerService(webClientServiceProvider);

        slackRecordSink = new SlackRecordSink();

        testRunner.addControllerService("slackRecordSink", slackRecordSink);
        testRunner.setProperty(slackRecordSink, SlackRecordSink.API_URL, url);
        testRunner.setProperty(slackRecordSink, SlackRecordSink.ACCESS_TOKEN, BEARER_TOKEN);
        testRunner.setProperty(slackRecordSink, SlackRecordSink.CHANNEL_ID, CHANNEL_NAME);
        testRunner.setProperty(slackRecordSink, SlackRecordSink.WEB_SERVICE_CLIENT_PROVIDER, "webClientServiceProvider");

        writerFactory = new MockRecordWriter();
        testRunner.addControllerService("writer", writerFactory);
        testRunner.setProperty(slackRecordSink, SlackRecordSink.RECORD_WRITER_FACTORY, "writer");

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("a", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("b", RecordFieldType.BOOLEAN.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> valueMap1 = new HashMap<>();
        valueMap1.put("a", "Hello");
        valueMap1.put("b", true);
        final Record record1 = new MapRecord(schema, valueMap1);

        final Map<String, Object> valueMap2 = new HashMap<>();
        valueMap2.put("a", "World");
        valueMap2.put("b", false);
        final Record record2 = new MapRecord(schema, valueMap2);

        recordContentsAsString = "\"Hello\",\"true\"\n\"World\",\"false\"\n";
        recordSet = RecordSet.of(schema, record1, record2);
    }

    @AfterEach
    public void cleanUp() throws IOException {
        mockWebServer.shutdown();
    }

    @Test
    public void testSendMessage() throws IOException {
        testRunner.enableControllerService(writerFactory);
        testRunner.assertValid(slackRecordSink);
        testRunner.enableControllerService(slackRecordSink);

        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(RESPONSE_SUCCESS_TEXT_MSG_WITH_TIMESTAMP));

        final WriteResult writeResult = slackRecordSink.sendData(recordSet, Collections.emptyMap(), false);

        assertNotNull(writeResult);
        assertEquals(2, writeResult.getRecordCount());
        assertEquals(Collections.EMPTY_MAP, writeResult.getAttributes());
        final JsonNode requestBodyJson = getRequestBodyJson();
        assertEquals(CHANNEL_NAME, requestBodyJson.get("channel").asText());
        assertEquals(recordContentsAsString, requestBodyJson.get("text").asText());
    }

    @Test
    public void testNotValidIfChannelEmpty() {
        testRunner.setProperty(slackRecordSink, SlackRecordSink.CHANNEL_ID, (String) null);

        testRunner.enableControllerService(writerFactory);
        testRunner.assertNotValid(slackRecordSink);
    }

    @Test
    public void testNotValidIfBearerTokenEmpty() {
        testRunner.setProperty(slackRecordSink, SlackRecordSink.ACCESS_TOKEN, (String) null);

        testRunner.enableControllerService(writerFactory);
        testRunner.assertNotValid(slackRecordSink);
    }

    @Test
    public void testFailureWhenHttpErrorCodeReturned() {
        testRunner.enableControllerService(writerFactory);
        testRunner.assertValid(slackRecordSink);
        testRunner.enableControllerService(slackRecordSink);

        mockWebServer.enqueue(new MockResponse().setResponseCode(500));

        final IOException e = assertThrows(IOException.class, () -> slackRecordSink.sendData(recordSet, Collections.emptyMap(), false));
        assertTrue(e.getCause().getMessage().contains("500"));
    }

    @Test
    public void testFailureWhenSlackReturnsError() {
        testRunner.enableControllerService(writerFactory);
        testRunner.assertValid(slackRecordSink);
        testRunner.enableControllerService(slackRecordSink);

        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(RESPONSE_ERROR));

        final IOException e = assertThrows(IOException.class, () -> slackRecordSink.sendData(recordSet, Collections.emptyMap(), false));
        assertTrue(e.getCause().getMessage().contains("slack-error"));
    }

    @Test
    public void testNoFailureWhenSlackReturnsWarning() {
        testRunner.enableControllerService(writerFactory);
        testRunner.assertValid(slackRecordSink);
        testRunner.enableControllerService(slackRecordSink);

        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(RESPONSE_WARNING));

        assertDoesNotThrow(() -> {
            slackRecordSink.sendData(recordSet, Collections.emptyMap(), false);
        });
    }

    @Test
    public void testFailureWhenSlackReturnsEmptyJson() {
        testRunner.enableControllerService(writerFactory);
        testRunner.assertValid(slackRecordSink);
        testRunner.enableControllerService(slackRecordSink);

        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(RESPONSE_EMPTY_JSON));

        final IOException e = assertThrows(IOException.class, () -> slackRecordSink.sendData(recordSet, Collections.emptyMap(), false));
        assertTrue(e.getCause().getMessage().contains("null"));
    }

    @Test
    public void testFailureWhenSlackReturnsInvalidJson() {
        testRunner.enableControllerService(writerFactory);
        testRunner.assertValid(slackRecordSink);
        testRunner.enableControllerService(slackRecordSink);

        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(RESPONSE_INVALID_JSON));

        final IOException e = assertThrows(IOException.class, () -> slackRecordSink.sendData(recordSet, Collections.emptyMap(), false));
        assertTrue(e.getCause().getMessage().contains("parsing"));
    }

    private JsonNode getRequestBodyJson() {
        try {
            final RecordedRequest recordedRequest = mockWebServer.takeRequest();
            return mapper.readTree(recordedRequest.getBody().inputStream());
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }
}
