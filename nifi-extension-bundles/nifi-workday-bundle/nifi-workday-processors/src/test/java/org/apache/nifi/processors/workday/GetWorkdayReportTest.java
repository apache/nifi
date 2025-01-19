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

package org.apache.nifi.processors.workday;

import static org.apache.nifi.processors.workday.GetWorkdayReport.FAILURE;
import static org.apache.nifi.processors.workday.GetWorkdayReport.GET_WORKDAY_REPORT_JAVA_EXCEPTION_CLASS;
import static org.apache.nifi.processors.workday.GetWorkdayReport.GET_WORKDAY_REPORT_JAVA_EXCEPTION_MESSAGE;
import static org.apache.nifi.processors.workday.GetWorkdayReport.HEADER_AUTHORIZATION;
import static org.apache.nifi.processors.workday.GetWorkdayReport.ORIGINAL;
import static org.apache.nifi.processors.workday.GetWorkdayReport.RECORD_COUNT;
import static org.apache.nifi.processors.workday.GetWorkdayReport.RECORD_READER_FACTORY;
import static org.apache.nifi.processors.workday.GetWorkdayReport.RECORD_WRITER_FACTORY;
import static org.apache.nifi.processors.workday.GetWorkdayReport.STATUS_CODE;
import static org.apache.nifi.processors.workday.GetWorkdayReport.SUCCESS;
import static org.apache.nifi.processors.workday.GetWorkdayReport.WEB_CLIENT_SERVICE;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.nifi.csv.CSVRecordSetWriter;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;
import org.apache.nifi.web.client.provider.service.StandardWebClientServiceProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;

class GetWorkdayReportTest {

    private static final String LOCALHOST = "localhost";
    private static final String REPORT_URL = "http://" + LOCALHOST;
    private static final String INVALID_URL = "invalid";
    private static final String INVALID_URL_PARAM = ":invalid_url";
    private static final String APPLICATION_JSON = "application/json";
    private static final String OK_STATUS_CODE = "200";
    private static final String CONTENT_TYPE = "Content-Type";
    private static final String TEXT_CSV = "text/csv";
    private static final String USER_NAME = "userName";
    private static final String PASSWORD = "password";

    private TestRunner runner;
    private MockWebServer mockWebServer;

    @BeforeEach
    public void setRunner() {
        runner = TestRunners.newTestRunner(new GetWorkdayReport());
        mockWebServer = new MockWebServer();
    }

    @AfterEach
    public void shutdownServer() throws IOException {
        mockWebServer.shutdown();
    }

    @Nested
    class BasicAuthPropertiesValidation {
        @Test
        void testNotValidWithoutReportUrlProperty() throws InitializationException {
            withWebClientService();
            runner.setProperty(GetWorkdayReport.WORKDAY_USERNAME, USER_NAME);
            runner.setProperty(GetWorkdayReport.WORKDAY_PASSWORD, PASSWORD);

            runner.assertNotValid();
        }

        @Test
        void testNotValidWithInvalidReportUrlProperty() throws InitializationException {
            withWebClientService();
            runner.setProperty(GetWorkdayReport.WORKDAY_USERNAME, USER_NAME);
            runner.setProperty(GetWorkdayReport.WORKDAY_PASSWORD, PASSWORD);
            runner.setProperty(GetWorkdayReport.REPORT_URL, INVALID_URL);
            runner.assertNotValid();
        }

        @Test
        void testNotValidWithoutUserName() throws InitializationException {
            withWebClientService();
            runner.setProperty(GetWorkdayReport.WORKDAY_PASSWORD, PASSWORD);
            runner.setProperty(GetWorkdayReport.REPORT_URL, REPORT_URL);

            runner.assertNotValid();
        }

        @Test
        void testNotValidWithoutPassword() throws InitializationException {
            withWebClientService();
            runner.setProperty(GetWorkdayReport.WORKDAY_USERNAME, USER_NAME);
            runner.setProperty(GetWorkdayReport.REPORT_URL, REPORT_URL);

            runner.assertNotValid();
        }

        @Test
        void testNotValidWithoutWebClient() {
            runner.setProperty(GetWorkdayReport.WORKDAY_USERNAME, USER_NAME);
            runner.setProperty(GetWorkdayReport.WORKDAY_PASSWORD, PASSWORD);
            runner.setProperty(GetWorkdayReport.REPORT_URL, REPORT_URL);

            runner.assertNotValid();
        }
    }

    @Nested
    class OAuthPropertiesValidation {
        @BeforeEach
        void setUp() {
            runner.setProperty(GetWorkdayReport.AUTH_TYPE, GetWorkdayReport.OAUTH_TYPE);
        }

        @Test
        void testNotValidWithoutOAuth2AccessTokenProvider() throws InitializationException {
            withWebClientService();
            runner.setProperty(GetWorkdayReport.REPORT_URL, REPORT_URL);

            runner.assertNotValid();
        }

        @Test
        void testNotValidWithInvalidReportUrlProperty() throws InitializationException {
            withWebClientService();
            withAccessTokenProvider();
            runner.setProperty(GetWorkdayReport.REPORT_URL, INVALID_URL);
            runner.assertNotValid();
        }

        @Test
        void testNotValidWithoutReportUrlProperty() throws InitializationException {
            withWebClientService();
            withAccessTokenProvider();

            runner.assertNotValid();
        }

        @Test
        void testNotValidWithoutWebClient() throws InitializationException {
            withAccessTokenProvider();
            runner.setProperty(GetWorkdayReport.REPORT_URL, REPORT_URL);

            runner.assertNotValid();
        }
    }

    @Test
    public void testRunIncomingConnectionsWithNonLoopConnections() throws InitializationException {
        withWebClientService();
        runner.setProperty(GetWorkdayReport.WORKDAY_USERNAME, USER_NAME);
        runner.setProperty(GetWorkdayReport.WORKDAY_PASSWORD, PASSWORD);
        runner.setProperty(GetWorkdayReport.REPORT_URL, REPORT_URL);
        runner.setIncomingConnection(true);
        runner.setNonLoopConnection(true);

        runner.run();
        runner.assertQueueEmpty();
    }

    @Test
    public void testRunThrowsURISyntaxExceptionFailure() throws InitializationException {
        withWebClientService();
        runner.setProperty(GetWorkdayReport.WORKDAY_USERNAME, USER_NAME);
        runner.setProperty(GetWorkdayReport.WORKDAY_PASSWORD, PASSWORD);

        String urlAttributeKey = "request.url";
        runner.setProperty(GetWorkdayReport.REPORT_URL, String.format("${%s}", urlAttributeKey));

        Map<String, String> attributes = new HashMap<>();
        attributes.put(urlAttributeKey, INVALID_URL_PARAM);

        runner.enqueue("", attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(FAILURE);
        runner.assertPenalizeCount(1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(GetWorkdayReport.FAILURE).getFirst();
        flowFile.assertAttributeEquals(GET_WORKDAY_REPORT_JAVA_EXCEPTION_CLASS, URISyntaxException.class.getSimpleName());
        flowFile.assertAttributeExists(GET_WORKDAY_REPORT_JAVA_EXCEPTION_MESSAGE);
    }

    @Test
    void testContextYieldIfHttpStatusIsNot2xxAndThereIsNoIncomingConnection() throws InitializationException {
        runner.setIncomingConnection(false);
        runner.setProperty(GetWorkdayReport.WORKDAY_USERNAME, USER_NAME);
        runner.setProperty(GetWorkdayReport.WORKDAY_PASSWORD, PASSWORD);
        withWebClientService();
        runner.setProperty(GetWorkdayReport.REPORT_URL, getMockWebServerUrl());

        mockWebServer.enqueue(new MockResponse().setResponseCode(500));

        runner.run();

        assertTrue(((MockProcessContext) runner.getProcessContext()).isYieldCalled());
        runner.assertTransferCount(ORIGINAL, 0);
        runner.assertTransferCount(SUCCESS, 0);
        runner.assertTransferCount(FAILURE, 0);
    }

    @Test
    void testContextYieldAndForwardFlowFileToFailureIfHttpStatusIsNot2xxAndThereIsIncomingConnection() throws InitializationException {
        runner.setIncomingConnection(true);
        runner.setProperty(GetWorkdayReport.WORKDAY_USERNAME, USER_NAME);
        runner.setProperty(GetWorkdayReport.WORKDAY_PASSWORD, PASSWORD);
        withWebClientService();
        runner.setProperty(GetWorkdayReport.REPORT_URL, getMockWebServerUrl());

        mockWebServer.enqueue(new MockResponse().setResponseCode(500));

        runner.enqueue("test");
        runner.run();

        assertFalse(((MockProcessContext) runner.getProcessContext()).isYieldCalled());
        runner.assertTransferCount(ORIGINAL, 0);
        runner.assertTransferCount(SUCCESS, 0);
        runner.assertTransferCount(FAILURE, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(FAILURE).getFirst();
        flowFile.assertAttributeEquals("getworkdayreport.status.code", "500");
    }

    @Test
    void testYieldShouldBeCalledWhenExceptionHappensAndThereIsNoRequestFlowFile() throws InitializationException {
        runner.setIncomingConnection(false);
        runner.setProperty(GetWorkdayReport.WORKDAY_USERNAME, USER_NAME);
        runner.setProperty(GetWorkdayReport.WORKDAY_PASSWORD, PASSWORD);
        withWebClientService();
        String urlAttributeKey = "request.url";
        runner.setProperty(GetWorkdayReport.REPORT_URL, String.format("${%s}", urlAttributeKey));

        runner.run();

        assertTrue(((MockProcessContext) runner.getProcessContext()).isYieldCalled());
        runner.assertTransferCount(ORIGINAL, 0);
        runner.assertTransferCount(SUCCESS, 0);
        runner.assertTransferCount(FAILURE, 0);
    }

    @Test
    void testPassThroughContentWithoutModificationIfNoRecordReaderAndWriterDefined() throws InitializationException {
        withWebClientService();
        runner.setProperty(GetWorkdayReport.WORKDAY_USERNAME, USER_NAME);
        runner.setProperty(GetWorkdayReport.WORKDAY_PASSWORD, PASSWORD);
        runner.setIncomingConnection(false);
        runner.setProperty(GetWorkdayReport.REPORT_URL, getMockWebServerUrl());

        String content = "id,name\n1,2";
        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(content).setHeader(CONTENT_TYPE, TEXT_CSV));

        runner.run();

        assertFalse(((MockProcessContext) runner.getProcessContext()).isYieldCalled());
        runner.assertTransferCount(ORIGINAL, 0);
        runner.assertTransferCount(SUCCESS, 1);
        runner.assertTransferCount(FAILURE, 0);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(SUCCESS).getFirst();
        flowFile.assertAttributeEquals(STATUS_CODE, OK_STATUS_CODE);
        flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), TEXT_CSV);
        flowFile.assertAttributeNotExists(RECORD_COUNT);
        flowFile.assertContentEquals(content);
    }

    @Test
    void testRequestFlowFileIsTransferredToOriginalRelationship() throws InitializationException {
        withWebClientService();
        runner.setProperty(GetWorkdayReport.WORKDAY_USERNAME, USER_NAME);
        runner.setProperty(GetWorkdayReport.WORKDAY_PASSWORD, PASSWORD);
        runner.setIncomingConnection(true);
        runner.setProperty(GetWorkdayReport.REPORT_URL, getMockWebServerUrl());

        String content = "id,name\n1,2";
        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(content).setHeader(CONTENT_TYPE, TEXT_CSV));
        runner.enqueue("");

        runner.run();

        assertFalse(((MockProcessContext) runner.getProcessContext()).isYieldCalled());
        runner.assertTransferCount(ORIGINAL, 1);
        runner.assertTransferCount(SUCCESS, 1);
        runner.assertTransferCount(FAILURE, 0);

        MockFlowFile originalFlowFile = runner.getFlowFilesForRelationship(ORIGINAL).getFirst();
        MockFlowFile responseFlowFile = runner.getFlowFilesForRelationship(SUCCESS).getFirst();
        originalFlowFile.assertAttributeEquals(STATUS_CODE, OK_STATUS_CODE);
        originalFlowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), TEXT_CSV);
        responseFlowFile.assertAttributeEquals(STATUS_CODE, OK_STATUS_CODE);
        responseFlowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), TEXT_CSV);
        responseFlowFile.assertAttributeNotExists(RECORD_COUNT);
        responseFlowFile.assertContentEquals(content);
    }

    @Test
    void testContentIsTransformedIfRecordReaderAndWriterIsDefined() throws InitializationException {
        withWebClientService();
        withJsonRecordReader();
        withCsvRecordSetWriter();
        runner.setProperty(GetWorkdayReport.WORKDAY_USERNAME, USER_NAME);
        runner.setProperty(GetWorkdayReport.WORKDAY_PASSWORD, PASSWORD);
        runner.setIncomingConnection(false);
        runner.setProperty(GetWorkdayReport.REPORT_URL, getMockWebServerUrl());

        String jsonContent = "{\"id\": 1, \"name\": \"test\"}";
        String csvContent = "id,name\n1,test\n";
        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(jsonContent).setHeader(CONTENT_TYPE, APPLICATION_JSON));

        runner.run();

        assertFalse(((MockProcessContext) runner.getProcessContext()).isYieldCalled());
        runner.assertTransferCount(ORIGINAL, 0);
        runner.assertTransferCount(SUCCESS, 1);
        runner.assertTransferCount(FAILURE, 0);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(SUCCESS).getFirst();
        flowFile.assertAttributeEquals(STATUS_CODE, OK_STATUS_CODE);
        flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), TEXT_CSV);

        flowFile.assertAttributeEquals(RECORD_COUNT, "1");
        flowFile.assertContentEquals(csvContent);
    }

    @Test
    void testOAuthAuthorization() throws InitializationException, InterruptedException {
        runner.setIncomingConnection(false);
        withWebClientService();
        runner.setProperty(GetWorkdayReport.REPORT_URL, getMockWebServerUrl());
        runner.setProperty(GetWorkdayReport.AUTH_TYPE, GetWorkdayReport.OAUTH_TYPE);
        withAccessTokenProvider();

        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setHeader(CONTENT_TYPE, APPLICATION_JSON));

        runner.run();

        RecordedRequest recordedRequest = mockWebServer.takeRequest(1, TimeUnit.SECONDS);
        String authorization = recordedRequest.getHeader(HEADER_AUTHORIZATION);
        assertNotNull(authorization, "Authorization Header not found");

        Pattern bearerPattern = Pattern.compile("^Bearer \\S+$");
        assertTrue(bearerPattern.matcher(authorization).matches(), "OAuth bearer not matched");
    }

    @Test
    void testBasicAuthentication() throws InitializationException, InterruptedException {
        runner.setIncomingConnection(false);
        runner.setProperty(GetWorkdayReport.WORKDAY_USERNAME, USER_NAME);
        runner.setProperty(GetWorkdayReport.WORKDAY_PASSWORD, PASSWORD);
        withWebClientService();
        runner.setProperty(GetWorkdayReport.REPORT_URL, getMockWebServerUrl());

        mockWebServer.enqueue(new MockResponse().setResponseCode(200).setHeader(CONTENT_TYPE, APPLICATION_JSON));

        runner.run();

        RecordedRequest recordedRequest = mockWebServer.takeRequest(1, TimeUnit.SECONDS);
        String authorization = recordedRequest.getHeader(HEADER_AUTHORIZATION);
        assertNotNull(authorization, "Authorization Header not found");

        Pattern basicAuthPattern = Pattern.compile("^Basic \\S+$");
        assertTrue(basicAuthPattern.matcher(authorization).matches(), "Basic Authentication not matched");
    }

    private String getMockWebServerUrl() {
        return mockWebServer.url("workdayReport").newBuilder().host(LOCALHOST).build().toString();
    }

    private void withAccessTokenProvider() throws InitializationException {
        String oauth2AccessTokenProviderId = "oauth2AccessTokenProviderId";
        String accessToken = "access_token";

        OAuth2AccessTokenProvider oauth2AccessTokenProvider = mock(OAuth2AccessTokenProvider.class, Answers.RETURNS_DEEP_STUBS);
        when(oauth2AccessTokenProvider.getIdentifier()).thenReturn(oauth2AccessTokenProviderId);
        when(oauth2AccessTokenProvider.getAccessDetails().getAccessToken()).thenReturn(accessToken);

        runner.addControllerService(oauth2AccessTokenProviderId, oauth2AccessTokenProvider);
        runner.enableControllerService(oauth2AccessTokenProvider);
        runner.setProperty(GetWorkdayReport.OAUTH2_ACCESS_TOKEN_PROVIDER, oauth2AccessTokenProviderId);
    }

    private void withWebClientService() throws InitializationException {
        String serviceIdentifier = StandardWebClientServiceProvider.class.getName();
        WebClientServiceProvider webClientServiceProvider = new StandardWebClientServiceProvider();

        runner.addControllerService(serviceIdentifier, webClientServiceProvider);
        runner.enableControllerService(webClientServiceProvider);
        runner.setProperty(WEB_CLIENT_SERVICE, serviceIdentifier);
    }

    private void withJsonRecordReader() throws InitializationException {
        String serviceIdentifier = JsonTreeReader.class.getName();

        RecordReaderFactory recordReaderFactory = new JsonTreeReader();

        runner.addControllerService(serviceIdentifier, recordReaderFactory);

        runner.enableControllerService(recordReaderFactory);
        runner.setProperty(RECORD_READER_FACTORY, serviceIdentifier);
    }

    private void withCsvRecordSetWriter() throws InitializationException {
        String serviceIdentifier = RecordSetWriterFactory.class.getName();

        RecordSetWriterFactory recordSetWriterFactory = new CSVRecordSetWriter();

        runner.addControllerService(serviceIdentifier, recordSetWriterFactory);

        runner.enableControllerService(recordSetWriterFactory);
        runner.setProperty(RECORD_WRITER_FACTORY, serviceIdentifier);
    }
}