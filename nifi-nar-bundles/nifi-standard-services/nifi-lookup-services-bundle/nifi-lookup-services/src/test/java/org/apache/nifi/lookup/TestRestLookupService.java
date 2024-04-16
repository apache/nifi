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
package org.apache.nifi.lookup;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Timeout(10)
@ExtendWith(MockitoExtension.class)
class TestRestLookupService {

    private static final String SERVICE_ID = RestLookupService.class.getSimpleName();

    private static final String READER_ID = RecordReaderFactory.class.getSimpleName();

    private static final String ROOT_PATH = "/";

    private static final String GET_METHOD = "GET";

    private static final String POST_METHOD = "POST";

    private static final String POST_BODY = "{}";

    private static final String APPLICATION_JSON = "application/json; charset=utf-8";

    private static final String CONTENT_TYPE_HEADER = "Content-Type";

    private static final String TIMEOUT = "5 s";

    private static final String SHORT_TIMEOUT = "100 ms";

    private MockWebServer mockWebServer;

    private TestRunner runner;

    @Mock
    private RecordReaderFactory recordReaderFactory;

    @Mock
    private RecordReader recordReader;

    @Mock
    private Record record;

    private RestLookupService restLookupService;

    @BeforeEach
    void setRunner() throws InitializationException {
        mockWebServer = new MockWebServer();

        runner = TestRunners.newTestRunner(NoOpProcessor.class);

        restLookupService = new RestLookupService();

        when(recordReaderFactory.getIdentifier()).thenReturn(READER_ID);
        runner.addControllerService(READER_ID, recordReaderFactory);
        runner.addControllerService(SERVICE_ID, restLookupService);

        final String url = mockWebServer.url(ROOT_PATH).toString();
        runner.setProperty(restLookupService, RestLookupService.URL, url);
        runner.setProperty(restLookupService, RestLookupService.RECORD_READER, READER_ID);
        runner.setProperty(restLookupService, RestLookupService.PROP_CONNECT_TIMEOUT, TIMEOUT);
        runner.setProperty(restLookupService, RestLookupService.PROP_READ_TIMEOUT, TIMEOUT);
    }

    @AfterEach
    void shutdownServer() throws IOException {
        mockWebServer.shutdown();
    }

    @Test
    void testLookupSocketTimeout() {
        runner.setProperty(restLookupService, RestLookupService.PROP_READ_TIMEOUT, SHORT_TIMEOUT);
        runner.enableControllerService(restLookupService);

        final LookupFailureException exception = assertThrows(LookupFailureException.class, () -> restLookupService.lookup(Collections.emptyMap()));
        assertInstanceOf(SocketTimeoutException.class, exception.getCause());
    }

    @Test
    void testLookupRecordNotPresent() throws Exception {
        runner.enableControllerService(restLookupService);

        when(recordReaderFactory.createRecordReader(any(), any(), anyLong(), any())).thenReturn(recordReader);
        mockWebServer.enqueue(new MockResponse().setResponseCode(HTTP_OK));

        final Optional<Record> recordFound = restLookupService.lookup(Collections.emptyMap());
        assertFalse(recordFound.isPresent());

        assertRecordedRequestFound();
    }

    @Test
    void testLookupRecordFound() throws Exception {
        runner.enableControllerService(restLookupService);

        when(recordReaderFactory.createRecordReader(any(), any(), anyLong(), any())).thenReturn(recordReader);
        when(recordReader.nextRecord()).thenReturn(record);
        mockWebServer.enqueue(new MockResponse().setResponseCode(HTTP_OK));

        final Optional<Record> recordFound = restLookupService.lookup(Collections.emptyMap());
        assertTrue(recordFound.isPresent());

        assertRecordedRequestFound();
    }

    @Test
    void testLookupRecordFoundPostMethod() throws Exception {
        runner.enableControllerService(restLookupService);

        when(recordReaderFactory.createRecordReader(any(), any(), anyLong(), any())).thenReturn(recordReader);
        when(recordReader.nextRecord()).thenReturn(record);
        mockWebServer.enqueue(new MockResponse().setResponseCode(HTTP_OK));

        final Map<String, Object> coordinates = new LinkedHashMap<>();
        coordinates.put(RestLookupService.METHOD_KEY, POST_METHOD);
        coordinates.put(RestLookupService.BODY_KEY, POST_BODY);
        coordinates.put(RestLookupService.MIME_TYPE_KEY, APPLICATION_JSON);

        final Optional<Record> recordFound = restLookupService.lookup(coordinates);
        assertTrue(recordFound.isPresent());

        assertPostRecordedRequestFound();
    }

    @Test
    void testLookupResponseHandlingStrategyReturned() throws Exception {
        runner.setProperty(restLookupService, RestLookupService.RESPONSE_HANDLING_STRATEGY, ResponseHandlingStrategy.RETURNED);
        runner.enableControllerService(restLookupService);

        when(recordReaderFactory.createRecordReader(any(), any(), anyLong(), any())).thenReturn(recordReader);
        when(recordReader.nextRecord()).thenReturn(record);

        mockWebServer.enqueue(new MockResponse().setResponseCode(HTTP_NOT_FOUND)
                .setHeader("Content-type", "application/json")
                .setBody("{\"error\": { \"code\": 404 } }"));

        final Optional<Record> recordFound = restLookupService.lookup(Collections.emptyMap());
        assertTrue(recordFound.isPresent());
    }

    @Test
    void testLookupResponseHandlingStrategyEvaluated() {
        runner.setProperty(restLookupService, RestLookupService.RESPONSE_HANDLING_STRATEGY, ResponseHandlingStrategy.EVALUATED);
        runner.enableControllerService(restLookupService);
        mockWebServer.enqueue(new MockResponse().setResponseCode(HTTP_NOT_FOUND));

        final LookupFailureException exception = assertThrows(LookupFailureException.class, () -> restLookupService.lookup(Collections.emptyMap()));
        assertInstanceOf(IOException.class, exception.getCause());
    }

    @Test
    void testOAuth2AuthorizationHeader() throws Exception {
        String accessToken = "access_token";
        String oauth2AccessTokenProviderId = "oauth2AccessTokenProviderId";

        OAuth2AccessTokenProvider oauth2AccessTokenProvider = mock(OAuth2AccessTokenProvider.class, Answers.RETURNS_DEEP_STUBS);
        when(oauth2AccessTokenProvider.getIdentifier()).thenReturn(oauth2AccessTokenProviderId);
        when(oauth2AccessTokenProvider.getAccessDetails().getAccessToken()).thenReturn(accessToken);
        runner.addControllerService(oauth2AccessTokenProviderId, oauth2AccessTokenProvider);
        runner.enableControllerService(oauth2AccessTokenProvider);

        runner.setProperty(RestLookupService.AUTHENTICATION_STRATEGY, AuthenticationStrategy.OAUTH2);
        runner.setProperty(restLookupService, RestLookupService.OAUTH2_ACCESS_TOKEN_PROVIDER, oauth2AccessTokenProvider.getIdentifier());
        runner.enableControllerService(restLookupService);

        when(recordReaderFactory.createRecordReader(any(), any(), anyLong(), any())).thenReturn(recordReader);
        when(recordReader.nextRecord()).thenReturn(record);
        mockWebServer.enqueue(new MockResponse());

        final Optional<Record> recordFound = restLookupService.lookup(Collections.emptyMap());
        assertTrue(recordFound.isPresent());

        RecordedRequest recordedRequest = mockWebServer.takeRequest();

        String actualAuthorizationHeader = recordedRequest.getHeader("Authorization");
        assertEquals("Bearer " + accessToken, actualAuthorizationHeader);

    }

    private void assertRecordedRequestFound() throws InterruptedException {
        final RecordedRequest request = mockWebServer.takeRequest();

        assertEquals(GET_METHOD, request.getMethod());
        assertEquals(ROOT_PATH, request.getPath());
    }

    private void assertPostRecordedRequestFound() throws InterruptedException {
        final RecordedRequest request = mockWebServer.takeRequest();

        assertEquals(POST_METHOD, request.getMethod());
        assertEquals(ROOT_PATH, request.getPath());
        assertEquals(APPLICATION_JSON, request.getHeader(CONTENT_TYPE_HEADER));

        final String body = request.getBody().readString(StandardCharsets.UTF_8);
        assertEquals(POST_BODY, body);
    }
}
