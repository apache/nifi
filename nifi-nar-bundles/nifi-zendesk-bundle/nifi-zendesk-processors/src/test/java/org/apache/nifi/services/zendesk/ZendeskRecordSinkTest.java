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
package org.apache.nifi.services.zendesk;

import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okio.Buffer;
import org.apache.nifi.processors.zendesk.ZendeskAuthenticationType;
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
import org.apache.nifi.web.client.StandardHttpUriBuilder;
import org.apache.nifi.web.client.api.HttpUriBuilder;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;
import org.apache.nifi.web.client.provider.service.StandardWebClientServiceProvider;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.commons.zendesk.ZendeskProperties.CONTENT_TYPE_APPLICATION_JSON;
import static org.apache.nifi.commons.zendesk.ZendeskProperties.WEB_CLIENT_SERVICE_PROVIDER_NAME;
import static org.apache.nifi.commons.zendesk.ZendeskProperties.ZENDESK_AUTHENTICATION_CREDENTIAL_NAME;
import static org.apache.nifi.commons.zendesk.ZendeskProperties.ZENDESK_AUTHENTICATION_TYPE_NAME;
import static org.apache.nifi.commons.zendesk.ZendeskProperties.ZENDESK_CREATE_TICKETS_RESOURCE;
import static org.apache.nifi.commons.zendesk.ZendeskProperties.ZENDESK_CREATE_TICKET_RESOURCE;
import static org.apache.nifi.commons.zendesk.ZendeskProperties.ZENDESK_SUBDOMAIN_NAME;
import static org.apache.nifi.commons.zendesk.ZendeskProperties.ZENDESK_TICKET_COMMENT_BODY_NAME;
import static org.apache.nifi.commons.zendesk.ZendeskProperties.ZENDESK_TICKET_PRIORITY_NAME;
import static org.apache.nifi.commons.zendesk.ZendeskProperties.ZENDESK_TICKET_SUBJECT_NAME;
import static org.apache.nifi.commons.zendesk.ZendeskProperties.ZENDESK_TICKET_TYPE_NAME;
import static org.apache.nifi.commons.zendesk.ZendeskProperties.ZENDESK_USER_NAME;
import static org.apache.nifi.processors.zendesk.PutZendeskTicket.REL_FAILURE;
import static org.apache.nifi.util.TestRunners.newTestRunner;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ZendeskRecordSinkTest {

    private static final int HTTP_OK = 200;
    private static final int HTTP_BAD_REQUEST = 400;
    private static final String EMPTY_RESPONSE = "{}";

    private MockWebServer server;
    private TestRunner testRunner;
    private MockRecordWriter writerFactory;
    private RecordSet recordSet;
    private TestZendeskRecordSink sinkZendeskTicket;

    @BeforeEach
    public void init() throws IOException, InitializationException {
        server = new MockWebServer();
        server.start();

        testRunner = newTestRunner(new NoOpProcessor());

        sinkZendeskTicket = new TestZendeskRecordSink();

        WebClientServiceProvider webClientServiceProvider = new StandardWebClientServiceProvider();
        testRunner.addControllerService("web-client-service-provider", webClientServiceProvider);
        testRunner.enableControllerService(webClientServiceProvider);

        testRunner.addControllerService("sinkZendeskTicket", sinkZendeskTicket);
        testRunner.setProperty(sinkZendeskTicket, WEB_CLIENT_SERVICE_PROVIDER_NAME, "web-client-service-provider");
        testRunner.setProperty(sinkZendeskTicket, ZENDESK_SUBDOMAIN_NAME, "default-zendesk-subdomain");
        testRunner.setProperty(sinkZendeskTicket, ZENDESK_USER_NAME, "default-zendesk-user-name");
        testRunner.setProperty(sinkZendeskTicket, ZENDESK_AUTHENTICATION_TYPE_NAME, ZendeskAuthenticationType.PASSWORD.getValue());
        testRunner.setProperty(sinkZendeskTicket, ZENDESK_AUTHENTICATION_CREDENTIAL_NAME, "default-zendesk-password");

        writerFactory = new MockRecordWriter();
        testRunner.addControllerService("writer", writerFactory);
        testRunner.setProperty(sinkZendeskTicket, ZendeskRecordSink.RECORD_WRITER_FACTORY, "writer");
    }

    @AfterEach
    void tearDown() throws IOException {
        server.shutdown();
    }

    private void initSingleTestRecord() {
        List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("description", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("subject", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("priority", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("type", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("dynamicPropertySource1", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("dynamicPropertySource2", RecordFieldType.STRING.getDataType()));
        RecordSchema schema = new SimpleRecordSchema(fields);

        Map<String, Object> valueMap = new HashMap<>();
        valueMap.put("description", "This is a test comment body.");
        valueMap.put("subject", "Test subject");
        valueMap.put("priority", "High");
        valueMap.put("type", "Development");
        valueMap.put("dynamicPropertySource1", "This is a dynamic property 1");
        valueMap.put("dynamicPropertySource2", "This is a dynamic property 2");
        Record record = new MapRecord(schema, valueMap);

        recordSet = RecordSet.of(schema, record);
    }

    private void initMultipleTestRecord() {
        List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("description", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("priority", RecordFieldType.STRING.getDataType()));

        RecordSchema schema = new SimpleRecordSchema(fields);

        Map<String, Object> valueMap1 = new HashMap<>();
        valueMap1.put("description", "This is a test comment body.");
        valueMap1.put("priority", "High");
        Record record1 = new MapRecord(schema, valueMap1);

        Map<String, Object> valueMap2 = new HashMap<>();
        valueMap2.put("description", "This is another test comment body.");
        valueMap2.put("priority", "Low");
        Record record2 = new MapRecord(schema, valueMap2);

        recordSet = RecordSet.of(schema, record1, record2);
    }

    @Test
    public void testSendMessageWithFixPropertiesAndSingleTicket() throws IOException, InterruptedException {
        testRunner.setProperty(sinkZendeskTicket, ZENDESK_TICKET_COMMENT_BODY_NAME, "%{/description}");
        testRunner.setProperty(sinkZendeskTicket, ZENDESK_TICKET_SUBJECT_NAME, "%{/subject}");
        testRunner.setProperty(sinkZendeskTicket, ZENDESK_TICKET_PRIORITY_NAME, "%{/priority}");
        testRunner.setProperty(sinkZendeskTicket, ZENDESK_TICKET_TYPE_NAME, "%{/type}");

        testRunner.enableControllerService(writerFactory);
        testRunner.assertValid(sinkZendeskTicket);
        testRunner.enableControllerService(sinkZendeskTicket);

        server.enqueue(new MockResponse().setResponseCode(HTTP_OK).setBody(EMPTY_RESPONSE));

        initSingleTestRecord();
        WriteResult writeResult = sinkZendeskTicket.sendData(recordSet, Collections.emptyMap(), false);

        // then
        RecordedRequest recordedRequest = server.takeRequest();
        assertEquals(ZENDESK_CREATE_TICKET_RESOURCE, recordedRequest.getPath());

        assertNotNull(writeResult);
        assertEquals(1, writeResult.getRecordCount());
        assertEquals(Collections.EMPTY_MAP, writeResult.getAttributes());

        String expectedBody =
                "{\n" +
                "  \"ticket\" : {\n" +
                "    \"comment\" : {\n" +
                "      \"body\" : \"This is a test comment body.\"\n" +
                "    },\n" +
                "    \"subject\" : \"Test subject\",\n" +
                "    \"priority\" : \"High\",\n" +
                "    \"type\" : \"Development\"\n" +
                "  }\n" +
                "}";

        Buffer buffer = getBuffer(ZENDESK_CREATE_TICKET_RESOURCE, expectedBody);
        assertEquals(buffer, recordedRequest.getBody());
    }

    @Test
    public void testSendMessageWithFixPropertiesAndMultipleTickets() throws IOException, InterruptedException {
        testRunner.setProperty(sinkZendeskTicket, ZENDESK_TICKET_COMMENT_BODY_NAME, "%{/description}");
        testRunner.setProperty(sinkZendeskTicket, ZENDESK_TICKET_PRIORITY_NAME, "%{/priority}");
        testRunner.enableControllerService(writerFactory);
        testRunner.assertValid(sinkZendeskTicket);
        testRunner.enableControllerService(sinkZendeskTicket);

        server.enqueue(new MockResponse().setResponseCode(HTTP_OK).setBody(EMPTY_RESPONSE));

        initMultipleTestRecord();
        WriteResult writeResult = sinkZendeskTicket.sendData(recordSet, Collections.emptyMap(), false);

        // then
        RecordedRequest recordedRequest = server.takeRequest();
        assertEquals(ZENDESK_CREATE_TICKETS_RESOURCE, recordedRequest.getPath());

        assertNotNull(writeResult);
        assertEquals(2, writeResult.getRecordCount());
        assertEquals(Collections.EMPTY_MAP, writeResult.getAttributes());

        String expectedBody =
                "{\n" +
                "  \"tickets\" : [ {\n" +
                "    \"comment\" : {\n" +
                "      \"body\" : \"This is a test comment body.\"\n" +
                "    },\n" +
                "    \"priority\" : \"High\"\n" +
                "  }, {\n" +
                "    \"comment\" : {\n" +
                "      \"body\" : \"This is another test comment body.\"\n" +
                "    },\n" +
                "    \"priority\" : \"Low\"\n" +
                "  } ]\n" +
                "}";

        Buffer buffer = getBuffer(ZENDESK_CREATE_TICKETS_RESOURCE, expectedBody);
        assertEquals(buffer, recordedRequest.getBody());
    }

    @Test
    public void testSendMessageWithRecordPathDynamicProperties() throws IOException, InterruptedException {
        testRunner.setProperty(sinkZendeskTicket, ZENDESK_TICKET_COMMENT_BODY_NAME, "%{/description}");
        testRunner.setProperty(sinkZendeskTicket, "/dp1/dynamicPropertyTarget1", "%{/dynamicPropertySource1}");
        testRunner.setProperty(sinkZendeskTicket, "/dp1/dp2/dp3/dynamicPropertyTarget2", "%{/dynamicPropertySource2}");

        testRunner.enableControllerService(writerFactory);
        testRunner.assertValid(sinkZendeskTicket);
        testRunner.enableControllerService(sinkZendeskTicket);

        server.enqueue(new MockResponse().setResponseCode(HTTP_OK).setBody(EMPTY_RESPONSE));

        initSingleTestRecord();
        WriteResult writeResult = sinkZendeskTicket.sendData(recordSet, Collections.emptyMap(), false);

        // then
        RecordedRequest recordedRequest = server.takeRequest();
        assertEquals(ZENDESK_CREATE_TICKET_RESOURCE, recordedRequest.getPath());

        assertNotNull(writeResult);
        assertEquals(1, writeResult.getRecordCount());
        assertEquals(Collections.EMPTY_MAP, writeResult.getAttributes());

        String expectedBody =
                "{\n" +
                "  \"ticket\" : {\n" +
                "    \"comment\" : {\n" +
                "      \"body\" : \"This is a test comment body.\"\n" +
                "    },\n" +
                "    \"dp1\" : {\n" +
                "      \"dp2\" : {\n" +
                "        \"dp3\" : {\n" +
                "          \"dynamicPropertyTarget2\" : \"This is a dynamic property 2\"\n" +
                "        }\n" +
                "      },\n" +
                "      \"dynamicPropertyTarget1\" : \"This is a dynamic property 1\"\n" +
                "    }\n" +
                "  }\n" +
                "}";

        Buffer buffer = getBuffer(ZENDESK_CREATE_TICKET_RESOURCE, expectedBody);
        assertEquals(buffer, recordedRequest.getBody());
    }

    @Test
    public void testSendMessageWithConstantDynamicProperties() throws IOException, InterruptedException {
        testRunner.setProperty(sinkZendeskTicket, ZENDESK_TICKET_COMMENT_BODY_NAME, "%{/description}");
        testRunner.setProperty(sinkZendeskTicket, "/dp1/dynamicPropertyTarget1", "Constant 1");
        testRunner.setProperty(sinkZendeskTicket, "/dp1/dp2/dp3/dynamicPropertyTarget2", "Constant 2");

        testRunner.enableControllerService(writerFactory);
        testRunner.assertValid(sinkZendeskTicket);
        testRunner.enableControllerService(sinkZendeskTicket);

        server.enqueue(new MockResponse().setResponseCode(HTTP_OK).setBody(EMPTY_RESPONSE));

        initSingleTestRecord();
        WriteResult writeResult = sinkZendeskTicket.sendData(recordSet, Collections.emptyMap(), false);

        // then
        RecordedRequest recordedRequest = server.takeRequest();
        assertEquals(ZENDESK_CREATE_TICKET_RESOURCE, recordedRequest.getPath());

        assertNotNull(writeResult);
        assertEquals(1, writeResult.getRecordCount());
        assertEquals(Collections.EMPTY_MAP, writeResult.getAttributes());

        String expectedBody =
                "{\n" +
                "  \"ticket\" : {\n" +
                "    \"comment\" : {\n" +
                "      \"body\" : \"This is a test comment body.\"\n" +
                "    },\n" +
                "    \"dp1\" : {\n" +
                "      \"dp2\" : {\n" +
                "        \"dp3\" : {\n" +
                "          \"dynamicPropertyTarget2\" : \"Constant 2\"\n" +
                "        }\n" +
                "      },\n" +
                "      \"dynamicPropertyTarget1\" : \"Constant 1\"\n" +
                "    }\n" +
                "  }\n" +
                "}";

        Buffer buffer = getBuffer(ZENDESK_CREATE_TICKET_RESOURCE, expectedBody);
        assertEquals(buffer, recordedRequest.getBody());
    }

    @Test
    public void testSendMessageWithErrorResponse() throws IOException, InterruptedException {
        testRunner.setProperty(sinkZendeskTicket, ZENDESK_TICKET_COMMENT_BODY_NAME, "%{/description}");

        testRunner.enableControllerService(writerFactory);
        testRunner.assertValid(sinkZendeskTicket);
        testRunner.enableControllerService(sinkZendeskTicket);

        server.enqueue(new MockResponse().setResponseCode(HTTP_BAD_REQUEST).setBody(EMPTY_RESPONSE));

        initSingleTestRecord();
        sinkZendeskTicket.sendData(recordSet, Collections.emptyMap(), false);

        // then
        testRunner.assertAllFlowFilesTransferred(REL_FAILURE);
    }


    @NotNull
    private Buffer getBuffer(String url, String body) throws IOException {
        Request request = new Request.Builder()
                .post(RequestBody.create(body, MediaType.parse(CONTENT_TYPE_APPLICATION_JSON)))
                .url(server.url(url))
                .build();

        Buffer buffer = new Buffer();
        request.body().writeTo(buffer);
        return buffer;
    }

    class TestZendeskRecordSink extends ZendeskRecordSink {
        @Override
        HttpUriBuilder uriBuilder(String subDomain, String resourcePath) {
            HttpUrl url = server.url(resourcePath);
            return new StandardHttpUriBuilder()
                    .scheme(url.scheme())
                    .host(url.host())
                    .port(url.port())
                    .encodedPath(url.encodedPath());
        }
    }
}
