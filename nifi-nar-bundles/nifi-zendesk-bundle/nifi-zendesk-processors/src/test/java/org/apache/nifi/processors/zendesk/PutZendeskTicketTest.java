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
package org.apache.nifi.processors.zendesk;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.HttpUrl;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.nifi.common.zendesk.ZendeskAuthenticationType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.web.client.StandardHttpUriBuilder;
import org.apache.nifi.web.client.api.HttpUriBuilder;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;
import org.apache.nifi.web.client.provider.service.StandardWebClientServiceProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.apache.nifi.common.zendesk.ZendeskProperties.WEB_CLIENT_SERVICE_PROVIDER;
import static org.apache.nifi.common.zendesk.ZendeskProperties.ZENDESK_AUTHENTICATION_CREDENTIAL;
import static org.apache.nifi.common.zendesk.ZendeskProperties.ZENDESK_AUTHENTICATION_TYPE;
import static org.apache.nifi.common.zendesk.ZendeskProperties.ZENDESK_CREATE_TICKETS_RESOURCE;
import static org.apache.nifi.common.zendesk.ZendeskProperties.ZENDESK_CREATE_TICKET_RESOURCE;
import static org.apache.nifi.common.zendesk.ZendeskProperties.ZENDESK_SUBDOMAIN;
import static org.apache.nifi.common.zendesk.ZendeskProperties.ZENDESK_TICKET_COMMENT_BODY;
import static org.apache.nifi.common.zendesk.ZendeskProperties.ZENDESK_TICKET_PRIORITY;
import static org.apache.nifi.common.zendesk.ZendeskProperties.ZENDESK_TICKET_SUBJECT;
import static org.apache.nifi.common.zendesk.ZendeskProperties.ZENDESK_TICKET_TYPE;
import static org.apache.nifi.common.zendesk.ZendeskProperties.ZENDESK_USER;
import static org.apache.nifi.processors.zendesk.AbstractZendesk.RECORD_COUNT_ATTRIBUTE_NAME;
import static org.apache.nifi.processors.zendesk.PutZendeskTicket.RECORD_READER;
import static org.apache.nifi.processors.zendesk.PutZendeskTicket.REL_FAILURE;
import static org.apache.nifi.processors.zendesk.PutZendeskTicket.REL_SUCCESS;
import static org.apache.nifi.util.TestRunners.newTestRunner;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class PutZendeskTicketTest {

    private static final int HTTP_OK = 200;
    private static final int HTTP_BAD_REQUEST = 400;
    private static final String EMPTY_RESPONSE = "{}";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private MockWebServer server;
    private TestRunner testRunner;

    @BeforeEach
    public void init() throws IOException, InitializationException {
        server = new MockWebServer();
        server.start();

        testRunner = newTestRunner(new TestPutZendeskTicket());

        WebClientServiceProvider webClientServiceProvider = new StandardWebClientServiceProvider();
        testRunner.addControllerService("web-client-service-provider", webClientServiceProvider);
        testRunner.enableControllerService(webClientServiceProvider);

        testRunner.setProperty(WEB_CLIENT_SERVICE_PROVIDER, "web-client-service-provider");
        testRunner.setProperty(ZENDESK_SUBDOMAIN, "default-zendesk-subdomain");
        testRunner.setProperty(ZENDESK_USER, "default-zendesk-user-name");
        testRunner.setProperty(ZENDESK_AUTHENTICATION_TYPE, ZendeskAuthenticationType.PASSWORD);
        testRunner.setProperty(ZENDESK_AUTHENTICATION_CREDENTIAL, "default-zendesk-password");
    }

    @AfterEach
    void tearDown() throws IOException {
        server.shutdown();
    }

    @Test
    public void testOnTriggerWithoutRecordReader() throws InterruptedException, IOException {
        String flowFileContent =
                "{\n" +
                "  \"ticket\" : {\n" +
                "    \"comment\" : {\n" +
                "      \"body\" : \"This is a test description\"\n" +
                "    },\n" +
                "    \"subject\" : \"Test subject\",\n" +
                "    \"priority\" : \"High\",\n" +
                "    \"type\" : \"Development\"\n" +
                "  }\n" +
                "}";

        MockFlowFile flowFile = new MockFlowFile(1L);
        flowFile.setData(flowFileContent.getBytes());

        // given
        server.enqueue(new MockResponse().setResponseCode(HTTP_OK).setBody(EMPTY_RESPONSE));

        // when
        testRunner.enqueue(flowFile);
        testRunner.run();

        // then
        RecordedRequest recordedRequest = server.takeRequest();
        assertEquals(ZENDESK_CREATE_TICKET_RESOURCE, recordedRequest.getPath());
        assertEquals(OBJECT_MAPPER.readTree(flowFileContent), OBJECT_MAPPER.readTree(recordedRequest.getBody().inputStream()));
        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS);
    }

    @Test
    public void testOnTriggerWithFixPropertiesAndSingleTicket() throws InterruptedException, InitializationException, IOException {
        MockRecordParser reader = new MockRecordParser();
        reader.addSchemaField("description", RecordFieldType.STRING);
        reader.addSchemaField("subject", RecordFieldType.STRING);
        reader.addSchemaField("priority", RecordFieldType.STRING);
        reader.addSchemaField("type", RecordFieldType.STRING);
        reader.addRecord("This is a test description", "Test subject", "High", "Development");

        testRunner.addControllerService("mock-reader-factory", reader);
        testRunner.enableControllerService(reader);
        testRunner.setProperty(RECORD_READER, "mock-reader-factory");

        // given
        server.enqueue(new MockResponse().setResponseCode(HTTP_OK).setBody(EMPTY_RESPONSE));
        testRunner.setProperty(ZENDESK_TICKET_COMMENT_BODY, "@{/description}");
        testRunner.setProperty(ZENDESK_TICKET_SUBJECT, "@{/subject}");
        testRunner.setProperty(ZENDESK_TICKET_PRIORITY, "@{/priority}");
        testRunner.setProperty(ZENDESK_TICKET_TYPE, "@{/type}");

        // when
        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        // then
        RecordedRequest recordedRequest = server.takeRequest();
        assertEquals(ZENDESK_CREATE_TICKET_RESOURCE, recordedRequest.getPath());

        String expectedBody =
                "{\n" +
                "  \"ticket\" : {\n" +
                "    \"comment\" : {\n" +
                "      \"body\" : \"This is a test description\"\n" +
                "    },\n" +
                "    \"subject\" : \"Test subject\",\n" +
                "    \"priority\" : \"High\",\n" +
                "    \"type\" : \"Development\"\n" +
                "  }\n" +
                "}";

        assertEquals(OBJECT_MAPPER.readTree(expectedBody), OBJECT_MAPPER.readTree(recordedRequest.getBody().inputStream()));
        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS);
    }

    @Test
    public void testOnTriggerWithFixPropertiesAndMultipleTickets() throws InterruptedException, InitializationException, IOException {
        MockRecordParser reader = new MockRecordParser();
        reader.addSchemaField("description", RecordFieldType.STRING);
        reader.addRecord("This is a test description1");
        reader.addRecord("This is a test description2");

        testRunner.addControllerService("mock-reader-factory", reader);
        testRunner.enableControllerService(reader);
        testRunner.setProperty(RECORD_READER, "mock-reader-factory");

        // given
        server.enqueue(new MockResponse().setResponseCode(HTTP_OK).setBody(EMPTY_RESPONSE));
        testRunner.setProperty(ZENDESK_TICKET_COMMENT_BODY, "@{/description}");

        // when
        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        // then
        RecordedRequest recordedRequest = server.takeRequest();
        assertEquals(ZENDESK_CREATE_TICKETS_RESOURCE, recordedRequest.getPath());

        String expectedBody =
                "{\n" +
                "  \"tickets\" : [ {\n" +
                "    \"comment\" : {\n" +
                "      \"body\" : \"This is a test description1\"\n" +
                "    }\n" +
                "  }, {\n" +
                "    \"comment\" : {\n" +
                "      \"body\" : \"This is a test description2\"\n" +
                "    }\n" +
                "  } ]\n" +
                "}";

        assertEquals(OBJECT_MAPPER.readTree(expectedBody), OBJECT_MAPPER.readTree(recordedRequest.getBody().inputStream()));
        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS);
    }

    @Test
    public void testOnTriggerWithRecordPathDynamicProperties() throws InterruptedException, InitializationException, IOException {
        MockRecordParser reader = new MockRecordParser();
        reader.addSchemaField("description", RecordFieldType.STRING);
        reader.addSchemaField("dynamicPropertySource1", RecordFieldType.STRING);
        reader.addSchemaField("dynamicPropertySource2", RecordFieldType.STRING);
        reader.addRecord("This is a test description", "This is a dynamic property 1", "This is a dynamic property 2");

        testRunner.addControllerService("mock-reader-factory", reader);
        testRunner.enableControllerService(reader);
        testRunner.setProperty(RECORD_READER, "mock-reader-factory");

        // given
        server.enqueue(new MockResponse().setResponseCode(HTTP_OK).setBody(EMPTY_RESPONSE));
        testRunner.setProperty(ZENDESK_TICKET_COMMENT_BODY, "@{/description}");
        testRunner.setProperty("/dp1/dynamicPropertyTarget1", "@{/dynamicPropertySource1}");
        testRunner.setProperty("/dp1/dp2/dp3/dynamicPropertyTarget2", "@{/dynamicPropertySource2}");

        // when
        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        // then
        RecordedRequest recordedRequest = server.takeRequest();
        assertEquals(ZENDESK_CREATE_TICKET_RESOURCE, recordedRequest.getPath());

        String expectedBody =
                "{\n" +
                "  \"ticket\" : {\n" +
                "    \"comment\" : {\n" +
                "      \"body\" : \"This is a test description\"\n" +
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

        assertEquals(OBJECT_MAPPER.readTree(expectedBody), OBJECT_MAPPER.readTree(recordedRequest.getBody().inputStream()));
        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS);
    }

    @Test
    public void testOnTriggerWithConstantDynamicProperties() throws InterruptedException, InitializationException, IOException {
        MockRecordParser reader = new MockRecordParser();
        reader.addSchemaField("description", RecordFieldType.STRING);
        reader.addSchemaField("dynamicPropertySource1", RecordFieldType.STRING);
        reader.addSchemaField("dynamicPropertySource2", RecordFieldType.STRING);
        reader.addRecord("This is a test description", "This is a dynamic property 1", "This is a dynamic property 2");

        testRunner.addControllerService("mock-reader-factory", reader);
        testRunner.enableControllerService(reader);
        testRunner.setProperty(RECORD_READER, "mock-reader-factory");

        // given
        server.enqueue(new MockResponse().setResponseCode(HTTP_OK).setBody(EMPTY_RESPONSE));
        testRunner.setProperty(ZENDESK_TICKET_COMMENT_BODY, "@{/description}");
        testRunner.setProperty("/dp1/dynamicPropertyTarget1", "Constant 1");
        testRunner.setProperty("/dp1/dp2/dp3/dynamicPropertyTarget2", "Constant2");

        // when
        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        // then
        RecordedRequest recordedRequest = server.takeRequest();
        assertEquals(ZENDESK_CREATE_TICKET_RESOURCE, recordedRequest.getPath());

        String expectedBody =
                "{\n" +
                "  \"ticket\" : {\n" +
                "    \"comment\" : {\n" +
                "      \"body\" : \"This is a test description\"\n" +
                "    },\n" +
                "    \"dp1\" : {\n" +
                "      \"dp2\" : {\n" +
                "        \"dp3\" : {\n" +
                "          \"dynamicPropertyTarget2\" : \"Constant2\"\n" +
                "        }\n" +
                "      },\n" +
                "      \"dynamicPropertyTarget1\" : \"Constant 1\"\n" +
                "    }\n" +
                "  }\n" +
                "}";

        assertEquals(OBJECT_MAPPER.readTree(expectedBody), OBJECT_MAPPER.readTree(recordedRequest.getBody().inputStream()));
        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS);
    }

    @Test
    public void testOnTriggerWithErrorResponse() throws InitializationException {
        MockRecordParser reader = new MockRecordParser();
        reader.addSchemaField("description", RecordFieldType.STRING);
        reader.addRecord("This is a test description");

        testRunner.addControllerService("mock-reader-factory", reader);
        testRunner.enableControllerService(reader);
        testRunner.setProperty(RECORD_READER, "mock-reader-factory");

        // given
        server.enqueue(new MockResponse().setResponseCode(HTTP_BAD_REQUEST).setBody(EMPTY_RESPONSE));
        testRunner.setProperty(ZENDESK_TICKET_COMMENT_BODY, "@{/description}");

        // when
        testRunner.enqueue(new byte[0]);
        testRunner.run(1);

        // then
        testRunner.assertAllFlowFilesTransferred(REL_FAILURE);
    }

    @Test
    public void testOnTriggerWithZeroRecord() throws InitializationException {
        MockRecordParser reader = new MockRecordParser();

        testRunner.addControllerService("mock-reader-factory", reader);
        testRunner.enableControllerService(reader);
        testRunner.setProperty(RECORD_READER, "mock-reader-factory");
        testRunner.setProperty(ZENDESK_TICKET_COMMENT_BODY, "@{/description}");

        // when
        testRunner.enqueue(new byte[0]);
        testRunner.run();

        // then
        testRunner.assertTransferCount(REL_SUCCESS, 1);
        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        assertEquals("0", flowFile.getAttribute(RECORD_COUNT_ATTRIBUTE_NAME));
    }

    @Test
    public void testOnTriggerWithEmptyFlowFileWithoutRecordReader() {
        MockFlowFile flowFile = new MockFlowFile(1L);
        flowFile.setData("".getBytes());

        // when
        testRunner.enqueue(flowFile);
        testRunner.run();

        // then
        testRunner.assertTransferCount(REL_FAILURE, 1);
    }

    class TestPutZendeskTicket extends PutZendeskTicket {
        @Override
        HttpUriBuilder uriBuilder(String resourcePath) {
            HttpUrl url = server.url(resourcePath);
            return new StandardHttpUriBuilder()
                    .scheme(url.scheme())
                    .host(url.host())
                    .port(url.port())
                    .encodedPath(url.encodedPath());
        }
    }
}
