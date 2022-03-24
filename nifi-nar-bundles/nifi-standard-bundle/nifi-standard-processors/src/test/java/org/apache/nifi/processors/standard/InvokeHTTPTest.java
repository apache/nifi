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

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processors.standard.http.FlowFileNamingStrategy;
import org.apache.nifi.processors.standard.http.CookieStrategy;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.util.StandardTlsConfiguration;
import org.apache.nifi.security.util.TemporaryKeyStoreBuilder;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.security.util.TlsException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.web.util.ssl.SslContextUtils;
import org.mockito.Answers;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_MOVED_TEMP;
import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static org.apache.nifi.processors.standard.InvokeHTTP.GET_METHOD;
import static org.apache.nifi.processors.standard.InvokeHTTP.POST_METHOD;
import static org.apache.nifi.processors.standard.InvokeHTTP.PUT_METHOD;
import static org.apache.nifi.processors.standard.InvokeHTTP.PATCH_METHOD;
import static org.apache.nifi.processors.standard.InvokeHTTP.DELETE_METHOD;
import static org.apache.nifi.processors.standard.InvokeHTTP.HEAD_METHOD;
import static org.apache.nifi.processors.standard.InvokeHTTP.OPTIONS_METHOD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InvokeHTTPTest {
    private static final String HTTP_LOCALHOST_URL = "http://localhost";

    private static final String LOCALHOST = "localhost";

    private static final String BASE_PATH = "/";

    private static final String POST_FORM_PARAMETER_KEY = "post:form:parameter";

    private static final String DATE_HEADER = "Date";

    private static final String ACCEPT_HEADER = "Accept";

    private static final String AUTHORIZATION_HEADER = "Authorization";

    private static final String CONTENT_LENGTH_HEADER = "Content-Length";

    private static final String CONTENT_TYPE_HEADER = "Content-Type";

    private static final String LOCATION_HEADER = "Location";

    private static final String SET_COOKIE_HEADER = "Set-Cookie";

    private static final String COOKIE_HEADER = "Cookie";

    private static final String COOKIE_1 = "a=apple";

    private static final String COOKIE_2 = "b=banana";

    private static final String TRANSFER_ENCODING_HEADER = "Transfer-Encoding";

    private static final String USER_AGENT_HEADER = "User-Agent";

    private static final String AUTHENTICATE_HEADER = "WWW-Authenticate";

    private static final String REPEATED_HEADER = "Repeated";

    private static final String TEXT_PLAIN = "text/plain";

    private static final String FLOW_FILE_CONTENT = String.class.getName();

    private static final String FLOW_FILE_INITIAL_FILENAME = Double.class.getName();

    private static final int TAKE_REQUEST_COMPLETED_TIMEOUT = 1;

    private static final String TLS_CONNECTION_TIMEOUT = "60 s";

    private static TlsConfiguration generatedTlsConfiguration;

    private static TlsConfiguration truststoreTlsConfiguration;

    private MockWebServer mockWebServer;

    private TestRunner runner;

    @BeforeAll
    public static void setStores() {
        generatedTlsConfiguration = new TemporaryKeyStoreBuilder().build();
        truststoreTlsConfiguration = new StandardTlsConfiguration(
                null,
                null,
                null,
                generatedTlsConfiguration.getTruststorePath(),
                generatedTlsConfiguration.getTruststorePassword(),
                generatedTlsConfiguration.getTruststoreType()
        );
    }

    @BeforeEach
    public void setRunner() {
        mockWebServer = new MockWebServer();
        runner = TestRunners.newTestRunner(new InvokeHTTP());
        // Disable Connection Pooling
        runner.setProperty(InvokeHTTP.PROP_MAX_IDLE_CONNECTIONS, Integer.toString(0));
    }

    @AfterEach
    public void shutdownServer() throws IOException {
        mockWebServer.shutdown();
    }

    @Test
    public void testNotValidWithDefaultProperties() {
        runner.assertNotValid();
    }

    @Test
    public void testNotValidWithProxyTypeInvalid() {
        runner.setProperty(InvokeHTTP.PROP_URL, HTTP_LOCALHOST_URL);
        runner.setProperty(InvokeHTTP.PROP_PROXY_TYPE, String.class.getSimpleName());
        runner.assertNotValid();
    }

    @Test
    public void testNotValidWithProxyHostWithoutProxyPort() {
        runner.setProperty(InvokeHTTP.PROP_URL, HTTP_LOCALHOST_URL);
        runner.setProperty(InvokeHTTP.PROP_PROXY_HOST, String.class.getSimpleName());
        runner.assertNotValid();
    }

    @Test
    public void testNotValidWithProxyUserWithoutProxyPassword() {
        runner.setProperty(InvokeHTTP.PROP_URL, HTTP_LOCALHOST_URL);
        runner.setProperty(InvokeHTTP.PROP_PROXY_USER, String.class.getSimpleName());
        runner.assertNotValid();
    }

    @Test
    public void testNotValidWithProxyUserAndPasswordWithoutProxyHost() {
        runner.setProperty(InvokeHTTP.PROP_URL, HTTP_LOCALHOST_URL);
        runner.setProperty(InvokeHTTP.PROP_PROXY_USER, String.class.getSimpleName());
        runner.setProperty(InvokeHTTP.PROP_PROXY_PASSWORD, String.class.getSimpleName());
        runner.assertNotValid();
    }

    @Test
    public void testNotValidWithHttpsProxyTypeWithoutSslContextService() {
        runner.setProperty(InvokeHTTP.PROP_URL, HTTP_LOCALHOST_URL);
        runner.setProperty(InvokeHTTP.PROP_PROXY_TYPE, InvokeHTTP.HTTPS);
        runner.assertNotValid();
    }

    @Test
    public void testNotValidWithPostFormPropertyWithoutFormBodyFormName() {
        runner.setProperty(InvokeHTTP.PROP_URL, HTTP_LOCALHOST_URL);
        runner.setProperty(POST_FORM_PARAMETER_KEY, String.class.getSimpleName());
        runner.assertNotValid();
    }

    @Test
    public void testNotValidWithPostFormPropertyAndFormBodyFormNameWithoutSendBodyEnabled() {
        runner.setProperty(InvokeHTTP.PROP_URL, HTTP_LOCALHOST_URL);
        runner.setProperty(POST_FORM_PARAMETER_KEY, String.class.getSimpleName());
        runner.setProperty(InvokeHTTP.PROP_FORM_BODY_FORM_NAME, String.class.getSimpleName());
        runner.setProperty(InvokeHTTP.PROP_SEND_BODY, Boolean.FALSE.toString());
        runner.assertNotValid();
    }

    @Test
    public void testValidWithMinimumProperties() {
        runner.setProperty(InvokeHTTP.PROP_URL, HTTP_LOCALHOST_URL);
        runner.assertValid();
    }

    @Test
    public void testRunNoIncomingConnectionsWithNonLoopConnections() {
        runner.setIncomingConnection(false);
        runner.setNonLoopConnection(true);
        setUrlProperty();

        runner.run();
        runner.assertQueueEmpty();
    }

    @Test
    public void testRunNoIncomingConnectionsPostMethod() {
        runner.setIncomingConnection(false);
        runner.setNonLoopConnection(false);
        setUrlProperty();
        runner.setProperty(InvokeHTTP.PROP_METHOD, POST_METHOD);

        runner.run();
        runner.assertQueueEmpty();
    }

    @Test
    public void testRunGetMalformedUrlExceptionFailureNoIncomingConnections() {
        runner.setIncomingConnection(false);
        runner.setNonLoopConnection(false);

        runner.setProperty(InvokeHTTP.PROP_URL, "${file.name}");

        runner.run();

        final List<LogMessage> errorMessages = runner.getLogger().getErrorMessages();
        assertFalse(errorMessages.isEmpty());
    }

    @Test
    public void testRunGetMalformedUrlExceptionFailure() {
        final String urlAttributeKey = "request.url";
        runner.setProperty(InvokeHTTP.PROP_URL, String.format("${%s}", urlAttributeKey));

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(urlAttributeKey, String.class.getSimpleName());
        runner.enqueue(FLOW_FILE_CONTENT, attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(InvokeHTTP.REL_FAILURE);
        runner.assertPenalizeCount(1);

        final MockFlowFile flowFile = getFailureFlowFile();
        flowFile.assertAttributeEquals(InvokeHTTP.EXCEPTION_CLASS, MalformedURLException.class.getName());
        flowFile.assertAttributeExists(InvokeHTTP.EXCEPTION_MESSAGE);
    }

    @Test
    public void testRunGetMethodIllegalArgumentExceptionFailure() {
        setUrlProperty();
        final String methodAttributeKey = "request.method";
        runner.setProperty(InvokeHTTP.PROP_METHOD, String.format("${%s}", methodAttributeKey));

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(methodAttributeKey, null);
        runner.enqueue(FLOW_FILE_CONTENT, attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(InvokeHTTP.REL_FAILURE);
        runner.assertPenalizeCount(1);

        final MockFlowFile flowFile = getFailureFlowFile();
        flowFile.assertAttributeEquals(InvokeHTTP.EXCEPTION_CLASS, IllegalArgumentException.class.getName());
        flowFile.assertAttributeExists(InvokeHTTP.EXCEPTION_MESSAGE);
    }

    @Test
    public void testRunGetHttp200Success() throws InterruptedException {
        assertRequestMethodSuccess(GET_METHOD);
    }

    @Test
    public void testRunGetHttp200SuccessIgnoreResponseContentEnabled() throws InterruptedException {
        runner.setProperty(InvokeHTTP.IGNORE_RESPONSE_CONTENT, Boolean.TRUE.toString());
        assertRequestMethodSuccess(GET_METHOD);

        final MockFlowFile responseFlowFile = getResponseFlowFile();
        assertEquals(StringUtils.EMPTY, responseFlowFile.getContent());
    }

    @Test
    public void testRunGetHttp200SuccessOutputBodyAttribute() {
        final String outputAttributeKey = String.class.getSimpleName();
        runner.setProperty(InvokeHTTP.PROP_PUT_OUTPUT_IN_ATTRIBUTE, outputAttributeKey);
        setUrlProperty();

        final String body = String.class.getName();
        mockWebServer.enqueue(new MockResponse().setResponseCode(HTTP_OK).setBody(body));
        runner.enqueue(FLOW_FILE_CONTENT);
        runner.run();

        assertRelationshipStatusCodeEquals(InvokeHTTP.REL_SUCCESS_REQ, HTTP_OK);

        final MockFlowFile flowFile = getRequestFlowFile();
        flowFile.assertAttributeEquals(outputAttributeKey, body);
    }

    @Test
    public void testRunGetHttp200SuccessOutputBodyAttributeNoIncomingConnections() {
        final String outputAttributeKey = String.class.getSimpleName();
        runner.setProperty(InvokeHTTP.PROP_PUT_OUTPUT_IN_ATTRIBUTE, outputAttributeKey);
        setUrlProperty();
        runner.setIncomingConnection(false);
        runner.setNonLoopConnection(false);

        final String body = String.class.getName();
        mockWebServer.enqueue(new MockResponse().setResponseCode(HTTP_OK).setBody(body));
        runner.run();

        assertRelationshipStatusCodeEquals(InvokeHTTP.REL_SUCCESS_REQ, HTTP_OK);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(InvokeHTTP.REL_SUCCESS_REQ).iterator().next();
        flowFile.assertAttributeEquals(outputAttributeKey, body);
    }

    @Test
    public void testRunGetHttp200SuccessNoIncomingConnections() {
        runner.setIncomingConnection(false);
        runner.setNonLoopConnection(false);
        setUrlProperty();

        mockWebServer.enqueue(new MockResponse().setResponseCode(HTTP_OK));
        runner.run();

        assertRelationshipStatusCodeEquals(InvokeHTTP.REL_RESPONSE, HTTP_OK);
    }

    @Test
    public void testRunGetHttp200SuccessProxyHostPortConfigured() throws InterruptedException {
        final String mockWebServerUrl = getMockWebServerUrl();
        final URI uri = URI.create(mockWebServerUrl);

        runner.setProperty(InvokeHTTP.PROP_URL, mockWebServerUrl);
        runner.setProperty(InvokeHTTP.PROP_PROXY_HOST, uri.getHost());
        runner.setProperty(InvokeHTTP.PROP_PROXY_PORT, Integer.toString(uri.getPort()));

        mockWebServer.enqueue(new MockResponse().setResponseCode(HTTP_OK));
        runner.enqueue(FLOW_FILE_CONTENT);
        runner.run();

        assertRelationshipStatusCodeEquals(InvokeHTTP.REL_RESPONSE, HTTP_OK);
        final RecordedRequest request = takeRequestCompleted();
        final String requestLine = request.getRequestLine();

        final String proxyRequestLine = String.format("%s %s HTTP/1.1", GET_METHOD, mockWebServerUrl);
        assertEquals(proxyRequestLine, requestLine);
    }

    @Test
    public void testRunGetHttp200SuccessProxyHostPortUserPasswordConfigured() throws InterruptedException {
        final String mockWebServerUrl = getMockWebServerUrl();
        final URI uri = URI.create(mockWebServerUrl);

        runner.setProperty(InvokeHTTP.PROP_URL, mockWebServerUrl);
        runner.setProperty(InvokeHTTP.PROP_PROXY_HOST, uri.getHost());
        runner.setProperty(InvokeHTTP.PROP_PROXY_PORT, Integer.toString(uri.getPort()));
        runner.setProperty(InvokeHTTP.PROP_PROXY_USER, String.class.getSimpleName());
        runner.setProperty(InvokeHTTP.PROP_PROXY_PASSWORD, String.class.getName());

        mockWebServer.enqueue(new MockResponse().setResponseCode(HTTP_OK));
        runner.enqueue(FLOW_FILE_CONTENT);
        runner.run();

        assertRelationshipStatusCodeEquals(InvokeHTTP.REL_RESPONSE, HTTP_OK);
        final RecordedRequest request = takeRequestCompleted();
        final String requestLine = request.getRequestLine();

        final String proxyRequestLine = String.format("%s %s HTTP/1.1", GET_METHOD, mockWebServerUrl);
        assertEquals(proxyRequestLine, requestLine);
    }

    @Test
    public void testRunGetHttp200SuccessContentTypeHeaderMimeType() {
        final MockResponse response = new MockResponse().setResponseCode(HTTP_OK).setHeader(CONTENT_TYPE_HEADER, TEXT_PLAIN);
        mockWebServer.enqueue(response);

        setUrlProperty();
        runner.enqueue(FLOW_FILE_CONTENT);
        runner.run();

        assertResponseSuccessRelationships();
        assertRelationshipStatusCodeEquals(InvokeHTTP.REL_RESPONSE, HTTP_OK);

        final MockFlowFile responseFlowFile = getResponseFlowFile();
        responseFlowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), TEXT_PLAIN);
    }

    @Test
    public void testRunGetHttp200SuccessRequestDateHeader() throws InterruptedException {
        runner.setProperty(InvokeHTTP.PROP_DATE_HEADER, StringUtils.capitalize(Boolean.TRUE.toString()));

        enqueueResponseCodeAndRun(HTTP_OK);

        assertResponseSuccessRelationships();
        assertRelationshipStatusCodeEquals(InvokeHTTP.REL_RESPONSE, HTTP_OK);

        final RecordedRequest request = takeRequestCompleted();
        final String dateHeader = request.getHeader(DATE_HEADER);
        assertNotNull(dateHeader, "Request Date not found");

        final Pattern rfcDatePattern = Pattern.compile("^.+? \\d{4} \\d{2}:\\d{2}:\\d{2} GMT$");
        assertTrue(rfcDatePattern.matcher(dateHeader).matches(), "Request Date RFC 2616 not matched");

        final ZonedDateTime zonedDateTime = ZonedDateTime.parse(dateHeader, DateTimeFormatter.RFC_1123_DATE_TIME);
        assertNotNull(zonedDateTime, "Request Date Parsing Failed");
    }

    @Test
    public void testRunGetHttp200SuccessSendAttributesAndDynamicProperties() throws InterruptedException {
        runner.setProperty(InvokeHTTP.PROP_ATTRIBUTES_TO_SEND, String.format("^%s$", ACCEPT_HEADER));
        final String defaultContentTypeHeader = "Default-Content-Type";
        runner.setProperty(defaultContentTypeHeader, InvokeHTTP.DEFAULT_CONTENT_TYPE);
        setUrlProperty();
        mockWebServer.enqueue(new MockResponse().setResponseCode(HTTP_OK));

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(ACCEPT_HEADER, TEXT_PLAIN);
        runner.enqueue(FLOW_FILE_CONTENT, attributes);
        runner.run();

        assertResponseSuccessRelationships();
        assertRelationshipStatusCodeEquals(InvokeHTTP.REL_RESPONSE, HTTP_OK);

        final RecordedRequest request = takeRequestCompleted();
        final String acceptHeader = request.getHeader(ACCEPT_HEADER);
        assertEquals(TEXT_PLAIN, acceptHeader);

        final String contentType = request.getHeader(defaultContentTypeHeader);
        assertEquals(InvokeHTTP.DEFAULT_CONTENT_TYPE, contentType);

        runner.removeProperty(InvokeHTTP.PROP_ATTRIBUTES_TO_SEND);
        runner.removeProperty(defaultContentTypeHeader);
        mockWebServer.enqueue(new MockResponse().setResponseCode(HTTP_OK));
        runner.enqueue(FLOW_FILE_CONTENT, attributes);
        runner.run();

        final RecordedRequest secondRequest = takeRequestCompleted();
        assertNull(secondRequest.getHeader(ACCEPT_HEADER), "Accept Header found");
        assertNull(secondRequest.getHeader(defaultContentTypeHeader), "Default-Content-Type Header found");
    }

    @Test
    public void testRunGetHttp200SuccessResponseHeaderRequestFlowFileAttributes() {
        setUrlProperty();
        runner.setProperty(InvokeHTTP.PROP_ADD_HEADERS_TO_REQUEST, Boolean.TRUE.toString());

        final String firstHeader = String.class.getSimpleName();
        final String secondHeader = Integer.class.getSimpleName();
        final MockResponse response = new MockResponse()
                .setResponseCode(HTTP_OK)
                .addHeader(REPEATED_HEADER, firstHeader)
                .addHeader(REPEATED_HEADER, secondHeader);

        mockWebServer.enqueue(response);
        runner.enqueue(FLOW_FILE_CONTENT);
        runner.run();

        assertResponseSuccessRelationships();
        assertRelationshipStatusCodeEquals(InvokeHTTP.REL_RESPONSE, HTTP_OK);

        final MockFlowFile requestFlowFile = getRequestFlowFile();
        requestFlowFile.assertAttributeEquals(CONTENT_LENGTH_HEADER, Integer.toString(0));

        final String repeatedHeaders = String.format("%s, %s", firstHeader, secondHeader);
        requestFlowFile.assertAttributeEquals(REPEATED_HEADER, repeatedHeaders);
    }

    @Test
    public void testRunGetHttp200SuccessCacheTagEnabled() throws InterruptedException {
        runner.setProperty(InvokeHTTP.PROP_USE_ETAG, Boolean.TRUE.toString());

        assertRequestMethodSuccess(GET_METHOD);
    }

    @Test
    public void testRunGetHttp200SuccessBasicAuthentication() throws InterruptedException {
        runner.setProperty(InvokeHTTP.PROP_BASIC_AUTH_USERNAME, String.class.getSimpleName());
        runner.setProperty(InvokeHTTP.PROP_BASIC_AUTH_PASSWORD, String.class.getName());

        enqueueResponseCodeAndRun(HTTP_OK);

        assertResponseSuccessRelationships();
        assertRelationshipStatusCodeEquals(InvokeHTTP.REL_RESPONSE, HTTP_OK);

        final RecordedRequest request = takeRequestCompleted();
        final String authorization = request.getHeader(AUTHORIZATION_HEADER);
        assertNotNull(authorization, "Authorization Header not found");

        final Pattern basicAuthPattern = Pattern.compile("^Basic [^\\s]+$");
        assertTrue(basicAuthPattern.matcher(authorization).matches(), "Basic Authentication not matched");
    }

    @Test
    public void testRunGetHttp200SuccessDigestAuthentication() throws InterruptedException {
        runner.setProperty(InvokeHTTP.PROP_BASIC_AUTH_USERNAME, String.class.getSimpleName());
        runner.setProperty(InvokeHTTP.PROP_BASIC_AUTH_PASSWORD, String.class.getName());
        runner.setProperty(InvokeHTTP.PROP_DIGEST_AUTH, Boolean.TRUE.toString());

        final String realm = UUID.randomUUID().toString();
        final String nonce = UUID.randomUUID().toString();
        final String digestHeader = String.format("Digest realm=\"%s\", nonce=\"%s\"", realm, nonce);

        mockWebServer.enqueue(new MockResponse().setResponseCode(HTTP_UNAUTHORIZED).setHeader(AUTHENTICATE_HEADER, digestHeader));
        enqueueResponseCodeAndRun(HTTP_OK);

        assertResponseSuccessRelationships();
        assertRelationshipStatusCodeEquals(InvokeHTTP.REL_RESPONSE, HTTP_OK);

        final RecordedRequest request = takeRequestCompleted();
        assertNull(request.getHeader(AUTHORIZATION_HEADER), "Authorization Header found");

        final RecordedRequest authenticatedRequest = takeRequestCompleted();
        final String authorization = authenticatedRequest.getHeader(AUTHORIZATION_HEADER);
        assertNotNull(authorization, "Authorization Header not found");
        assertTrue(authorization.contains(realm), "Digest Realm not found");
        assertTrue(authorization.contains(nonce), "Digest Nonce not found");
    }

    @Test
    public void testRunGetHttp200SuccessSslContextServiceServerTrusted() throws InitializationException, GeneralSecurityException {
        assertResponseSuccessSslContextConfigured(generatedTlsConfiguration, truststoreTlsConfiguration);
    }

    @Test
    public void testRunGetHttp200SuccessSslContextServiceMutualTrusted() throws InitializationException, GeneralSecurityException {
        assertResponseSuccessSslContextConfigured(generatedTlsConfiguration, generatedTlsConfiguration);
    }

    @Test
    public void testRunGetSslContextServiceMutualTrustedClientCertificateMissing() throws InitializationException, GeneralSecurityException {
        runner.setProperty(InvokeHTTP.DISABLE_HTTP2_PROTOCOL, StringUtils.capitalize(Boolean.TRUE.toString()));
        setSslContextConfiguration(generatedTlsConfiguration, truststoreTlsConfiguration);
        mockWebServer.requireClientAuth();

        setUrlProperty();
        runner.enqueue(FLOW_FILE_CONTENT);
        runner.run();

        runner.assertAllFlowFilesTransferred(InvokeHTTP.REL_FAILURE);
        final MockFlowFile flowFile = getFailureFlowFile();
        flowFile.assertAttributeExists(InvokeHTTP.EXCEPTION_CLASS);
        flowFile.assertAttributeExists(InvokeHTTP.EXCEPTION_MESSAGE);
    }

    @Test
    public void testRunGetHttp200SuccessUserAgentConfigured() throws InterruptedException {
        final String userAgent = UUID.randomUUID().toString();
        runner.setProperty(InvokeHTTP.PROP_USERAGENT, userAgent);

        enqueueResponseCodeAndRun(HTTP_OK);

        assertResponseSuccessRelationships();
        assertRelationshipStatusCodeEquals(InvokeHTTP.REL_RESPONSE, HTTP_OK);

        final RecordedRequest request = takeRequestCompleted();
        final String userAgentHeader = request.getHeader(USER_AGENT_HEADER);
        assertEquals(userAgent, userAgentHeader);
    }

    @Test
    public void testRunGetHttp302NoRetryFollowRedirectsDefaultEnabled() {
        mockWebServer.enqueue(new MockResponse().setResponseCode(HTTP_MOVED_TEMP).setHeader(LOCATION_HEADER, getMockWebServerUrl()));
        enqueueResponseCodeAndRun(HTTP_OK);

        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 0);
        assertRelationshipStatusCodeEquals(InvokeHTTP.REL_RESPONSE, HTTP_OK);
    }

    @Test
    public void testRunGetHttp302NoRetryFollowRedirectsDisabled() {
        runner.setProperty(InvokeHTTP.PROP_FOLLOW_REDIRECTS, StringUtils.capitalize(Boolean.FALSE.toString()));
        enqueueResponseCodeAndRun(HTTP_MOVED_TEMP);

        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 0);
        assertRelationshipStatusCodeEquals(InvokeHTTP.REL_NO_RETRY, HTTP_MOVED_TEMP);
    }

    @Test
    public void testRunGetHttp302CookieStrategyAcceptAll() throws InterruptedException {
        runner.setProperty(InvokeHTTP.PROP_COOKIE_STRATEGY, CookieStrategy.ACCEPT_ALL.name());
        mockWebServer.enqueue(new MockResponse().setResponseCode(HTTP_MOVED_TEMP)
            .addHeader(SET_COOKIE_HEADER, COOKIE_1)
            .addHeader(SET_COOKIE_HEADER, COOKIE_2)
            .addHeader(LOCATION_HEADER, getMockWebServerUrl()));
        enqueueResponseCodeAndRun(HTTP_OK);

        RecordedRequest request1 = mockWebServer.takeRequest();
        assertNull(request1.getHeader(COOKIE_HEADER));

        RecordedRequest request2 = mockWebServer.takeRequest();
        final String expectedHeader = String.format("%s; %s", COOKIE_1, COOKIE_2);
        assertEquals(expectedHeader, request2.getHeader(COOKIE_HEADER));

        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 0);
        assertRelationshipStatusCodeEquals(InvokeHTTP.REL_RESPONSE, HTTP_OK);
    }

    @Test
    public void testRunGetHttp302CookieStrategyDefaultDisabled() throws InterruptedException {
        mockWebServer.enqueue(new MockResponse().setResponseCode(HTTP_MOVED_TEMP)
            .addHeader(SET_COOKIE_HEADER, COOKIE_1)
            .addHeader(SET_COOKIE_HEADER, COOKIE_2)
            .addHeader(LOCATION_HEADER, getMockWebServerUrl()));
        enqueueResponseCodeAndRun(HTTP_OK);

        RecordedRequest request1 = mockWebServer.takeRequest();
        assertNull(request1.getHeader(COOKIE_HEADER));

        RecordedRequest request2 = mockWebServer.takeRequest();
        assertNull(request2.getHeader(COOKIE_HEADER));

        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 0);
        assertRelationshipStatusCodeEquals(InvokeHTTP.REL_RESPONSE, HTTP_OK);
    }

    @Test
    public void testRunGetHttp400NoRetryMinimumProperties() {
        enqueueResponseCodeAndRun(HTTP_BAD_REQUEST);

        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 0);
        assertRelationshipStatusCodeEquals(InvokeHTTP.REL_NO_RETRY, HTTP_BAD_REQUEST);
    }

    @Test
    public void testRunGetHttp400NoRetryPenalizeNoRetry() {
        runner.setProperty(InvokeHTTP.PROP_PENALIZE_NO_RETRY, Boolean.TRUE.toString());

        enqueueResponseCodeAndRun(HTTP_BAD_REQUEST);

        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 0);
        runner.assertPenalizeCount(1);
        assertRelationshipStatusCodeEquals(InvokeHTTP.REL_NO_RETRY, HTTP_BAD_REQUEST);
    }

    @Test
    public void testRunGetHttp500RetryMinimumProperties() {
        enqueueResponseCodeAndRun(HTTP_INTERNAL_ERROR);

        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 0);
        assertRelationshipStatusCodeEquals(InvokeHTTP.REL_RETRY, HTTP_INTERNAL_ERROR);
    }

    @Test
    public void testRunGetHttp500RetryOutputResponseRegardless() {
        runner.setProperty(InvokeHTTP.PROP_OUTPUT_RESPONSE_REGARDLESS, Boolean.TRUE.toString());

        enqueueResponseCodeAndRun(HTTP_INTERNAL_ERROR);

        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
        assertRelationshipStatusCodeEquals(InvokeHTTP.REL_RETRY, HTTP_INTERNAL_ERROR);
        assertRelationshipStatusCodeEquals(InvokeHTTP.REL_RESPONSE, HTTP_INTERNAL_ERROR);
    }

    @Test
    public void testRunDeleteHttp200Success() throws InterruptedException {
        runner.setProperty(InvokeHTTP.PROP_METHOD, DELETE_METHOD);
        assertRequestMethodSuccess(DELETE_METHOD);
    }

    @Test
    public void testRunHeadHttp200Success() throws InterruptedException {
        runner.setProperty(InvokeHTTP.PROP_METHOD, HEAD_METHOD);
        assertRequestMethodSuccess(HEAD_METHOD);
    }

    @Test
    public void testRunOptionsHttp200Success() throws InterruptedException {
        runner.setProperty(InvokeHTTP.PROP_METHOD, OPTIONS_METHOD);
        assertRequestMethodSuccess(OPTIONS_METHOD);
    }

    @Test
    public void testRunPatchHttp200Success() throws InterruptedException {
        runner.setProperty(InvokeHTTP.PROP_METHOD, PATCH_METHOD);
        assertRequestMethodSuccess(PATCH_METHOD);
    }

    @Test
    public void testRunPostHttp200Success() throws InterruptedException {
        runner.setProperty(InvokeHTTP.PROP_METHOD, POST_METHOD);
        assertRequestMethodSuccess(POST_METHOD);
    }

    @Test
    public void testRunPostHttp200SuccessChunkedEncoding() throws InterruptedException {
        runner.setProperty(InvokeHTTP.PROP_METHOD, POST_METHOD);
        runner.setProperty(InvokeHTTP.PROP_USE_CHUNKED_ENCODING, Boolean.TRUE.toString());

        enqueueResponseCodeAndRun(HTTP_OK);

        assertResponseSuccessRelationships();
        assertRelationshipStatusCodeEquals(InvokeHTTP.REL_RESPONSE, HTTP_OK);

        final RecordedRequest request = takeRequestCompleted();
        final String contentLength = request.getHeader(CONTENT_LENGTH_HEADER);
        assertNull(contentLength, "Content-Length Request Header found");

        final String transferEncoding = request.getHeader(TRANSFER_ENCODING_HEADER);
        assertEquals("chunked", transferEncoding);
    }

    @Test
    public void testRunPostHttp200SuccessFormData() throws InterruptedException {
        runner.setProperty(InvokeHTTP.PROP_METHOD, POST_METHOD);

        final String formName = "multipart-form";
        runner.setProperty(InvokeHTTP.PROP_FORM_BODY_FORM_NAME, formName);

        final String formDataParameter = String.class.getName();
        final String formDataParameterName = "label";
        final String formDataPropertyName = String.format("%s:%s", InvokeHTTP.FORM_BASE, formDataParameterName);
        runner.setProperty(formDataPropertyName, formDataParameter);

        setUrlProperty();
        mockWebServer.enqueue(new MockResponse().setResponseCode(HTTP_OK));
        runner.enqueue(FLOW_FILE_CONTENT);
        runner.run();

        assertResponseSuccessRelationships();
        assertRelationshipStatusCodeEquals(InvokeHTTP.REL_RESPONSE, HTTP_OK);

        final RecordedRequest request = takeRequestCompleted();
        final String contentType = request.getHeader(CONTENT_TYPE_HEADER);
        assertNotNull(contentType, "Content Type not found");

        final Pattern multipartPattern = Pattern.compile("^multipart/form-data.+$");
        assertTrue(multipartPattern.matcher(contentType).matches(), "Content Type not matched");

        final String body = request.getBody().readUtf8();
        assertTrue(body.contains(formDataParameter), "Form Data Parameter not found");
    }

    @Test
    public void testRunPutHttp200Success() throws InterruptedException {
        runner.setProperty(InvokeHTTP.PROP_METHOD, PUT_METHOD);
        assertRequestMethodSuccess(PUT_METHOD);
    }

    @ParameterizedTest(name = "{index} => When {0} http://baseUrl/{1}, filename of the response FlowFile should be {2}")
    @MethodSource
    public void testResponseFlowFileFilenameExtractedFromRemoteUrl(String httpMethod, String inputUrl, String expectedFileName) throws MalformedURLException {
        URL baseUrl = new URL(getMockWebServerUrl());
        URL targetUrl = new URL(baseUrl, inputUrl);

        runner.setProperty(InvokeHTTP.PROP_METHOD, httpMethod);
        runner.setProperty(InvokeHTTP.PROP_URL, targetUrl.toString());
        runner.setProperty(InvokeHTTP.FLOW_FILE_NAMING_STRATEGY, FlowFileNamingStrategy.URL_PATH.name());

        Map<String, String> ffAttributes = new HashMap<>();
        ffAttributes.put(CoreAttributes.FILENAME.key(), FLOW_FILE_INITIAL_FILENAME);
        runner.enqueue(FLOW_FILE_CONTENT, ffAttributes);

        mockWebServer.enqueue(new MockResponse().setResponseCode(HTTP_OK));

        runner.run();

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(InvokeHTTP.REL_RESPONSE).iterator().next();
        flowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), expectedFileName);
    }

    private static Stream<Arguments> testResponseFlowFileFilenameExtractedFromRemoteUrl() {
        return Stream.of(
            Arguments.of(GET_METHOD, "file", "file"),
            Arguments.of(GET_METHOD, "file/", "file"),
            Arguments.of(GET_METHOD, "file.txt", "file.txt"),
            Arguments.of(GET_METHOD, "file.txt/", "file.txt"),
            Arguments.of(GET_METHOD, "file.txt/?qp=v", "file.txt"),
            Arguments.of(GET_METHOD, "f%69%6Cle.txt", "f%69%6Cle.txt"),
            Arguments.of(GET_METHOD, "path/to/file.txt", "file.txt"),
            Arguments.of(GET_METHOD, "", FLOW_FILE_INITIAL_FILENAME),
            Arguments.of(POST_METHOD, "has/path", FLOW_FILE_INITIAL_FILENAME),
            Arguments.of(POST_METHOD, "", FLOW_FILE_INITIAL_FILENAME),
            Arguments.of(PUT_METHOD, "has/path", FLOW_FILE_INITIAL_FILENAME),
            Arguments.of(PUT_METHOD, "", FLOW_FILE_INITIAL_FILENAME),
            Arguments.of(PATCH_METHOD, "", FLOW_FILE_INITIAL_FILENAME),
            Arguments.of(PATCH_METHOD, "has/path", FLOW_FILE_INITIAL_FILENAME),
            Arguments.of(DELETE_METHOD, "", FLOW_FILE_INITIAL_FILENAME),
            Arguments.of(DELETE_METHOD, "has/path", FLOW_FILE_INITIAL_FILENAME),
            Arguments.of(HEAD_METHOD, "", FLOW_FILE_INITIAL_FILENAME),
            Arguments.of(HEAD_METHOD, "has/path", FLOW_FILE_INITIAL_FILENAME),
            Arguments.of(OPTIONS_METHOD, "", FLOW_FILE_INITIAL_FILENAME),
            Arguments.of(OPTIONS_METHOD, "has/path", FLOW_FILE_INITIAL_FILENAME)
        );
    }

    @Test
    public void testValidWhenOAuth2Set() throws Exception {
        // GIVEN
        String oauth2AccessTokenProviderId = "oauth2AccessTokenProviderId";

        OAuth2AccessTokenProvider oauth2AccessTokenProvider = mock(OAuth2AccessTokenProvider.class, Answers.RETURNS_DEEP_STUBS);
        when(oauth2AccessTokenProvider.getIdentifier()).thenReturn(oauth2AccessTokenProviderId);

        runner.addControllerService(oauth2AccessTokenProviderId, oauth2AccessTokenProvider);
        runner.enableControllerService(oauth2AccessTokenProvider);

        setUrlProperty();

        // WHEN
        runner.setProperty(InvokeHTTP.OAUTH2_ACCESS_TOKEN_PROVIDER, oauth2AccessTokenProviderId);

        // THEN
        runner.assertValid();
    }

    @Test
    public void testInvalidWhenOAuth2AndUserNameSet() throws Exception {
        // GIVEN
        String oauth2AccessTokenProviderId = "oauth2AccessTokenProviderId";

        OAuth2AccessTokenProvider oauth2AccessTokenProvider = mock(OAuth2AccessTokenProvider.class, Answers.RETURNS_DEEP_STUBS);
        when(oauth2AccessTokenProvider.getIdentifier()).thenReturn(oauth2AccessTokenProviderId);

        runner.addControllerService(oauth2AccessTokenProviderId, oauth2AccessTokenProvider);
        runner.enableControllerService(oauth2AccessTokenProvider);

        setUrlProperty();

        // WHEN
        runner.setProperty(InvokeHTTP.OAUTH2_ACCESS_TOKEN_PROVIDER, oauth2AccessTokenProviderId);
        runner.setProperty(InvokeHTTP.PROP_BASIC_AUTH_USERNAME, "userName");

        // THEN
        runner.assertNotValid();
    }

    @Test
    public void testInvalidWhenOAuth2AndPasswordSet() throws Exception {
        // GIVEN
        String oauth2AccessTokenProviderId = "oauth2AccessTokenProviderId";

        OAuth2AccessTokenProvider oauth2AccessTokenProvider = mock(OAuth2AccessTokenProvider.class, Answers.RETURNS_DEEP_STUBS);
        when(oauth2AccessTokenProvider.getIdentifier()).thenReturn(oauth2AccessTokenProviderId);

        runner.addControllerService(oauth2AccessTokenProviderId, oauth2AccessTokenProvider);
        runner.enableControllerService(oauth2AccessTokenProvider);

        setUrlProperty();

        // WHEN
        runner.setProperty(InvokeHTTP.OAUTH2_ACCESS_TOKEN_PROVIDER, oauth2AccessTokenProviderId);
        runner.setProperty(InvokeHTTP.PROP_BASIC_AUTH_PASSWORD, "password");

        // THEN
        runner.assertNotValid();
    }

    @Test
    public void testOAuth2AuthorizationHeader() throws Exception {
        // GIVEN
        String accessToken = "access_token";

        String oauth2AccessTokenProviderId = "oauth2AccessTokenProviderId";

        OAuth2AccessTokenProvider oauth2AccessTokenProvider = mock(OAuth2AccessTokenProvider.class, Answers.RETURNS_DEEP_STUBS);
        when(oauth2AccessTokenProvider.getIdentifier()).thenReturn(oauth2AccessTokenProviderId);
        when(oauth2AccessTokenProvider.getAccessDetails().getAccessToken()).thenReturn(accessToken);

        runner.addControllerService(oauth2AccessTokenProviderId, oauth2AccessTokenProvider);
        runner.enableControllerService(oauth2AccessTokenProvider);

        setUrlProperty();

        mockWebServer.enqueue(new MockResponse());

        // WHEN
        runner.setProperty(InvokeHTTP.OAUTH2_ACCESS_TOKEN_PROVIDER, oauth2AccessTokenProviderId);
        runner.enqueue("unimportant");
        runner.run();

        // THEN
        RecordedRequest recordedRequest = mockWebServer.takeRequest();

        String actualAuthorizationHeader = recordedRequest.getHeader("Authorization");
        assertEquals("Bearer " + accessToken, actualAuthorizationHeader);

    }

    private void setUrlProperty() {
        runner.setProperty(InvokeHTTP.PROP_URL, getMockWebServerUrl());
    }

    private String getMockWebServerUrl() {
        return mockWebServer.url(BASE_PATH).newBuilder().host(LOCALHOST).build().toString();
    }

    private void enqueueResponseCodeAndRun(final int responseCode) {
        setUrlProperty();
        mockWebServer.enqueue(new MockResponse().setResponseCode(responseCode));
        runner.enqueue(FLOW_FILE_CONTENT);
        runner.run();
    }

    private RecordedRequest takeRequestCompleted() throws InterruptedException {
        final RecordedRequest request = mockWebServer.takeRequest(TAKE_REQUEST_COMPLETED_TIMEOUT, TimeUnit.SECONDS);
        assertNotNull(request, "Request not found");
        return request;
    }

    private MockFlowFile getFailureFlowFile() {
        return runner.getFlowFilesForRelationship(InvokeHTTP.REL_FAILURE).iterator().next();
    }

    private MockFlowFile getRequestFlowFile() {
        return runner.getFlowFilesForRelationship(InvokeHTTP.REL_SUCCESS_REQ).iterator().next();
    }

    private MockFlowFile getResponseFlowFile() {
        return runner.getFlowFilesForRelationship(InvokeHTTP.REL_RESPONSE).iterator().next();
    }

    private void assertRequestMethodSuccess(final String method) throws InterruptedException {
        enqueueResponseCodeAndRun(HTTP_OK);

        assertResponseSuccessRelationships();
        assertRelationshipStatusCodeEquals(InvokeHTTP.REL_RESPONSE, HTTP_OK);

        final RecordedRequest request = takeRequestCompleted();
        assertEquals(method, request.getMethod());
    }

    private void assertRelationshipStatusCodeEquals(final Relationship relationship, final int statusCode) {
        final List<MockFlowFile> responseFlowFiles = runner.getFlowFilesForRelationship(relationship);
        final String message = String.format("FlowFiles not found for Relationship [%s]", relationship);
        assertFalse(responseFlowFiles.isEmpty(), message);
        final MockFlowFile responseFlowFile = responseFlowFiles.iterator().next();
        assertStatusCodeEquals(responseFlowFile, statusCode);
    }

    private void assertStatusCodeEquals(final MockFlowFile flowFile, final int statusCode) {
        flowFile.assertAttributeEquals(InvokeHTTP.STATUS_CODE, Integer.toString(statusCode));
        flowFile.assertAttributeExists(InvokeHTTP.STATUS_MESSAGE);
        flowFile.assertAttributeExists(InvokeHTTP.TRANSACTION_ID);
        flowFile.assertAttributeExists(InvokeHTTP.REQUEST_URL);
        flowFile.assertAttributeExists(InvokeHTTP.REQUEST_DURATION);
        flowFile.assertAttributeExists(InvokeHTTP.RESPONSE_URL);
    }

    private void assertResponseSuccessRelationships() {
        final List<LogMessage> errorMessages = runner.getLogger().getErrorMessages();
        final Optional<LogMessage> errorMessage = errorMessages.stream().findFirst();
        if (errorMessage.isPresent()) {
            final String message = String.format("Error Message Logged: %s", errorMessage.get().getMsg());
            assertFalse(errorMessages.isEmpty(), message);
        }

        runner.assertTransferCount(InvokeHTTP.REL_RESPONSE, 1);
        runner.assertTransferCount(InvokeHTTP.REL_SUCCESS_REQ, 1);
        runner.assertTransferCount(InvokeHTTP.REL_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_NO_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.REL_FAILURE, 0);
    }

    private void assertResponseSuccessSslContextConfigured(final TlsConfiguration serverTlsConfiguration, final TlsConfiguration clientTlsConfiguration) throws InitializationException, TlsException {
        setSslContextConfiguration(serverTlsConfiguration, clientTlsConfiguration);
        enqueueResponseCodeAndRun(HTTP_OK);

        assertResponseSuccessRelationships();
        assertRelationshipStatusCodeEquals(InvokeHTTP.REL_RESPONSE, HTTP_OK);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(InvokeHTTP.REL_RESPONSE).iterator().next();
        flowFile.assertAttributeExists(InvokeHTTP.REMOTE_DN);
    }

    private void setSslContextConfiguration(final TlsConfiguration serverTlsConfiguration, final TlsConfiguration clientTlsConfiguration) throws InitializationException, TlsException {
        final SSLContextService sslContextService = setSslContextService();
        final SSLContext serverSslContext = SslContextUtils.createSslContext(serverTlsConfiguration);
        setMockWebServerSslSocketFactory(serverSslContext);

        final SSLContext clientSslContext = SslContextUtils.createSslContext(clientTlsConfiguration);
        when(sslContextService.createContext()).thenReturn(clientSslContext);
        when(sslContextService.createTlsConfiguration()).thenReturn(clientTlsConfiguration);
    }

    private SSLContextService setSslContextService() throws InitializationException {
        final String serviceIdentifier = SSLContextService.class.getName();
        final SSLContextService sslContextService = mock(SSLContextService.class);
        when(sslContextService.getIdentifier()).thenReturn(serviceIdentifier);

        runner.addControllerService(serviceIdentifier, sslContextService);
        runner.enableControllerService(sslContextService);
        runner.setProperty(InvokeHTTP.PROP_SSL_CONTEXT_SERVICE, serviceIdentifier);
        runner.setProperty(InvokeHTTP.PROP_READ_TIMEOUT, TLS_CONNECTION_TIMEOUT);
        runner.setProperty(InvokeHTTP.PROP_CONNECT_TIMEOUT, TLS_CONNECTION_TIMEOUT);
        return sslContextService;
    }

    private void setMockWebServerSslSocketFactory(final SSLContext sslContext) {
        final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();
        if (sslSocketFactory == null) {
            throw new IllegalArgumentException("Socket Factory not found");
        }
        mockWebServer.useHttps(sslSocketFactory, false);
    }
}
