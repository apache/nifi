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

package org.apache.nifi.record.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.MapRecord;
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
import org.eclipse.jetty.http.HttpHeader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestHttpRecordSink {
    public static final String ID = "id";
    public static final String NAME = "name";
    public static final String ACTIVE = "active";

    private TestRunner testRunner;
    private MockWebServer mockWebServer;
    private HttpRecordSink httpRecordSink;
    private RecordSetWriterFactory writerFactory;
    final private String OAUTH_ACCESS_TOKEN = "access_token";

    private static RecordSchema schema;
    private static Record[] records;
    private ObjectMapper mapper;

    @BeforeAll
    public static void setupOnce() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField(ID, RecordFieldType.INT.getDataType()));
        fields.add(new RecordField(NAME, RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField(ACTIVE, RecordFieldType.BOOLEAN.getDataType()));

        schema = new SimpleRecordSchema(fields);

        final Record record0 = createRecord(schema, 0);
        final Record record1 = createRecord(schema, 1);
        final Record record2 = createRecord(schema, 2);
        final Record record3 = createRecord(schema, 3);
        final Record record4 = createRecord(schema, 4);
        records = new Record[] {record0, record1, record2, record3, record4 };
    }

    private static Record createRecord(final RecordSchema schema, final int index) {
        final Map<String, Object> valueMap = new HashMap<>();
        valueMap.put(ID, index);
        valueMap.put(NAME, "Name_äöü_こんにちは世界_" + index);
        valueMap.put(ACTIVE, index % 2 == 0);
        return new MapRecord(schema, valueMap);
    }

    private static RecordSet createRecordSetWithSize(final int size) {
        return RecordSet.of(schema, Arrays.copyOf(records, size));
    }

    @BeforeEach
    public void setupEachTest() throws InitializationException, IOException {
        mapper = new ObjectMapper();

        mockWebServer = new MockWebServer();
        mockWebServer.start();
        String url = mockWebServer.url("/api/test").toString();

        testRunner = TestRunners.newTestRunner(NoOpProcessor.class);

        final WebClientServiceProvider webClientServiceProvider = new StandardWebClientServiceProvider();
        testRunner.addControllerService("webClientServiceProvider", webClientServiceProvider);
        testRunner.enableControllerService(webClientServiceProvider);

        httpRecordSink = new HttpRecordSink();

        testRunner.addControllerService("httpRecordSink", httpRecordSink);
        testRunner.setProperty(httpRecordSink, HttpRecordSink.API_URL, url);
        testRunner.setProperty(httpRecordSink, HttpRecordSink.WEB_SERVICE_CLIENT_PROVIDER, "webClientServiceProvider");

        writerFactory = new JsonRecordSetWriter();
        testRunner.addControllerService("writer", writerFactory);
        testRunner.setProperty(httpRecordSink, HttpRecordSink.RECORD_WRITER_FACTORY, "writer");

        setupOAuth2TokenProvider();
    }

    private void setupOAuth2TokenProvider() throws InitializationException {
        String oauth2AccessTokenProviderId = "oauth2AccessTokenProviderId";

        OAuth2AccessTokenProvider oauth2AccessTokenProvider = mock(OAuth2AccessTokenProvider.class, Answers.RETURNS_DEEP_STUBS);
        when(oauth2AccessTokenProvider.getIdentifier()).thenReturn(oauth2AccessTokenProviderId);
        when(oauth2AccessTokenProvider.getAccessDetails().getAccessToken()).thenReturn(OAUTH_ACCESS_TOKEN);

        testRunner.addControllerService(oauth2AccessTokenProviderId, oauth2AccessTokenProvider);
        testRunner.enableControllerService(oauth2AccessTokenProvider);

        testRunner.setProperty(httpRecordSink, HttpRecordSink.OAUTH2_ACCESS_TOKEN_PROVIDER, oauth2AccessTokenProviderId);
    }

    @AfterEach
    public void cleanUpEachTest() throws IOException {
        mockWebServer.shutdown();
    }

    @Test
    public void testInvalidIfApiUrlEmpty() {
        testRunner.setProperty(httpRecordSink, HttpRecordSink.API_URL, "");

        testRunner.enableControllerService(writerFactory);
        testRunner.assertNotValid(httpRecordSink);
    }

    @Test
    public void testInvalidIfWebClientServiceDoesNotExist() {
        testRunner.setProperty(httpRecordSink, HttpRecordSink.WEB_SERVICE_CLIENT_PROVIDER, "nonexistent");

        testRunner.enableControllerService(writerFactory);
        testRunner.assertNotValid(httpRecordSink);
    }

    @Test
    public void testValidContentTypeHeader() throws Exception {
        testRunner.setProperty(httpRecordSink, "Content-Type", "my_content_type");
        testRunner.setProperty(httpRecordSink, "RandomHeader", "random_value");

        testRunner.enableControllerService(writerFactory);
        testRunner.assertValid(httpRecordSink);

        testRunner.disableControllerService(writerFactory);
        testSendData(5, 2, "my_content_type", null);
    }

    @Test
    public void testInvalidContentTypeHeader() {
        testRunner.setProperty(httpRecordSink, "content-type", "anything");

        testRunner.enableControllerService(writerFactory);
        testRunner.assertNotValid(httpRecordSink);
    }

    @Test
    public void testValidAuthorizationDynamicHeader() throws Exception {
        testRunner.setProperty(httpRecordSink, "Authorization", "Bearer my_authorization");

        testRunner.enableControllerService(writerFactory);
        testRunner.assertValid(httpRecordSink);

        testRunner.disableControllerService(writerFactory);
        testSendData(3, 1, null, "my_authorization");
    }

    @Test
    public void testInvalidAuthorizationDynamicHeader() {
        testRunner.setProperty(httpRecordSink, "authorization", "anything");

        testRunner.enableControllerService(writerFactory);
        testRunner.assertNotValid(httpRecordSink);
    }

    @Test
    public void testSendDataBatchSize0() throws Exception {
        testSendData(5, 0);
    }

    @Test
    public void testSendDataBatchSize1() throws Exception {
        testSendData(4, 1);
    }

    @Test
    public void testSendDataBatchSize2() throws Exception {
        testSendData(2, 2);
    }

    @Test
    public void testSendDataBatchSize3() throws Exception {
        testSendData(2, 3);
    }

    @Test
    public void testSendDataBatchSize4() throws Exception {
        testSendData(5, 4);
    }

    @Test
    public void testSendDataBatchSize5() throws Exception {
        testSendData(2, 5);
    }

    public void testSendData(int recordCount, int maxBatchSize) throws Exception {
        testSendData(recordCount, maxBatchSize, null, null);
    }

    public void testSendData(int recordCount, int maxBatchSize,
                             String expectedContentType, String expectedAuthorization) throws Exception {
        RecordSet recordSetIn = createRecordSetWithSize(recordCount);
        int expectedRequestCount = maxBatchSize == 0
                ? 1
                : recordCount / maxBatchSize + ((recordCount % maxBatchSize == 0) ? 0 : 1);
        testRunner.setProperty(httpRecordSink, HttpRecordSink.MAX_BATCH_SIZE, String.valueOf(maxBatchSize));
        testRunner.enableControllerService(writerFactory);
        testRunner.assertValid(httpRecordSink);
        testRunner.enableControllerService(httpRecordSink);

        for (int i = 0; i < expectedRequestCount; i++) {
            mockWebServer.enqueue(new MockResponse());
        }

        final WriteResult writeResult = httpRecordSink.sendData(recordSetIn, Collections.emptyMap(), false);

        assertNotNull(writeResult);
        assertEquals(recordCount, writeResult.getRecordCount());
        assertEquals(Collections.EMPTY_MAP, writeResult.getAttributes());

        assertEquals(expectedRequestCount, mockWebServer.getRequestCount());

        for (int i = 0; i < expectedRequestCount; i++) {
            RecordedRequest recordedRequest = mockWebServer.takeRequest();
            String requestBody = recordedRequest.getBody().readString(StandardCharsets.UTF_8);
            Person[] people =
                    (maxBatchSize == 1)
                            ? new Person[] {
                                    // For maxBatchSize 1, person is not in a Json array
                                    mapper.readValue(requestBody, Person.class)
                                  }
                            : mapper.readValue(requestBody, Person[].class); // Otherwise the body is a json array

            for (int personIndex = 0; personIndex < people.length; personIndex++) {
                final int compareIndex = i * maxBatchSize + personIndex;
                assertTrue(people[personIndex].equals(records[compareIndex]), "Mismatch - Expected: " + records[compareIndex].toMap().toString() +
                        " Actual: {" + people[personIndex].toString() + "} order of fields can be ignored.");
            }
            String actualContentTypeHeader = recordedRequest.getHeader(HttpHeader.CONTENT_TYPE.toString());
            assertEquals(expectedContentType != null ? expectedContentType : "application/json", actualContentTypeHeader);

            String actualAuthorizationHeader = recordedRequest.getHeader(HttpHeader.AUTHORIZATION.toString());
            assertEquals("Bearer " + (expectedAuthorization != null ? expectedAuthorization : OAUTH_ACCESS_TOKEN),
                    actualAuthorizationHeader);
        }
    }

    static public class Person {
        public int id;
        public String name;
        public boolean active;

        public boolean equals(Record record) {
            return id == record.getAsInt(ID)
                    && name.equals(record.getAsString(NAME))
                    && active == record.getAsBoolean(ACTIVE);
        }

        public String toString() {
            return ID + "=" + id + ", " + NAME + "=" + name + ", " + ACTIVE + "=" + active;
        }
    }
}
