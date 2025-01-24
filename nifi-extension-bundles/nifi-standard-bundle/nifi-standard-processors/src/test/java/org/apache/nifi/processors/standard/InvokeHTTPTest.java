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
import okio.Buffer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.URLValidator;
import org.apache.nifi.processors.standard.http.ContentEncodingStrategy;
import org.apache.nifi.processors.standard.http.CookieStrategy;
import org.apache.nifi.processors.standard.http.FlowFileNamingStrategy;
import org.apache.nifi.processors.standard.http.HttpHeader;
import org.apache.nifi.processors.standard.http.HttpMethod;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxyConfigurationService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.cert.builder.StandardCertificateBuilder;
import org.apache.nifi.security.ssl.EphemeralKeyStoreBuilder;
import org.apache.nifi.security.ssl.StandardSslContextBuilder;
import org.apache.nifi.security.ssl.StandardTrustManagerBuilder;
import org.apache.nifi.ssl.SSLContextProvider;
import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Answers;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.security.auth.x500.X500Principal;
import java.io.IOException;
import java.net.Proxy;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;
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
import java.util.zip.GZIPInputStream;

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_MOVED_TEMP;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static java.nio.file.Files.newDirectoryStream;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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

    private static final String CONTENT_ENCODING_HEADER = "Content-Encoding";

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

    private static SSLContext sslContext;

    private static SSLContext trustStoreSslContext;

    private static X509ExtendedTrustManager trustManager;

    private MockWebServer mockWebServer;

    private TestRunner runner;

    @BeforeAll
    public static void setStores() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        final X509Certificate certificate = new StandardCertificateBuilder(keyPair, new X500Principal("CN=localhost"), Duration.ofHours(1)).build();
        final KeyStore keyStore = new EphemeralKeyStoreBuilder()
                .addPrivateKeyEntry(new KeyStore.PrivateKeyEntry(keyPair.getPrivate(), new Certificate[]{certificate}))
                .build();
        final char[] protectionParameter = new char[]{};

        sslContext = new StandardSslContextBuilder()
                .trustStore(keyStore)
                .keyStore(keyStore)
                .keyPassword(protectionParameter)
                .build();

        trustStoreSslContext = new StandardSslContextBuilder()
                .trustStore(keyStore)
                .build();

        trustManager = new StandardTrustManagerBuilder().trustStore(keyStore).build();
    }

    @AfterAll
    public static void cleanUpAfterAll() {
        // Cleanup all the temporary InvokeHttp[0-9]* directories which are generated by InvokeHttp.
        final Path tempDir = Paths.get(System.getProperty("java.io.tmpdir"));
        try (DirectoryStream<Path> directoryStream = newDirectoryStream(tempDir, "InvokeHTTP[0-9]*")) {
            for (Path operationId : directoryStream) {
                FileUtils.deleteDirectory(operationId.toFile());
            }
        } catch (Exception ignored) {
        }
    }

    @BeforeEach
    public void setRunner() {
        mockWebServer = new MockWebServer();
        runner = TestRunners.newTestRunner(new InvokeHTTP());
        // Disable Connection Pooling
        runner.setProperty(InvokeHTTP.SOCKET_IDLE_CONNECTIONS, Integer.toString(0));
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
    public void testNotValidWithPostFormPropertyWithoutFormBodyFormName() {
        runner.setProperty(InvokeHTTP.HTTP_URL, HTTP_LOCALHOST_URL);
        runner.setProperty(POST_FORM_PARAMETER_KEY, String.class.getSimpleName());
        runner.assertNotValid();
    }

    @Test
    public void testNotValidWithPostFormPropertyAndFormBodyFormNameWithoutRequestBodyEnabled() {
        runner.setProperty(InvokeHTTP.HTTP_URL, HTTP_LOCALHOST_URL);
        runner.setProperty(POST_FORM_PARAMETER_KEY, String.class.getSimpleName());
        runner.setProperty(InvokeHTTP.REQUEST_FORM_DATA_NAME, String.class.getSimpleName());
        runner.setProperty(InvokeHTTP.REQUEST_BODY_ENABLED, Boolean.FALSE.toString());
        runner.assertNotValid();
    }

    @Test
    public void testValidWithMinimumProperties() {
        runner.setProperty(InvokeHTTP.HTTP_URL, HTTP_LOCALHOST_URL);
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
        runner.setProperty(InvokeHTTP.HTTP_METHOD, HttpMethod.POST.name());

        runner.run();
        runner.assertQueueEmpty();
    }

    @Test
    public void testRunGetMalformedUrlExceptionFailureNoIncomingConnections() {
        runner.setIncomingConnection(false);
        runner.setNonLoopConnection(false);

        runner.setProperty(InvokeHTTP.HTTP_URL, "${file.name}");

        runner.run();

        final List<LogMessage> errorMessages = runner.getLogger().getErrorMessages();
        assertFalse(errorMessages.isEmpty());
    }

    @Test
    public void testRunGetMalformedUrlExceptionFailure() {
        final String urlAttributeKey = "request.url";
        runner.setProperty(InvokeHTTP.HTTP_URL, String.format("${%s}", urlAttributeKey));

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(urlAttributeKey, String.class.getSimpleName());
        runner.enqueue(FLOW_FILE_CONTENT, attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(InvokeHTTP.FAILURE);
        runner.assertPenalizeCount(1);

        final MockFlowFile flowFile = getFailureFlowFile();
        flowFile.assertAttributeEquals(InvokeHTTP.EXCEPTION_CLASS, IllegalArgumentException.class.getName());
        flowFile.assertAttributeExists(InvokeHTTP.EXCEPTION_MESSAGE);
    }

    @Test
    public void testRunGetMethodIllegalArgumentExceptionFailure() {
        setUrlProperty();
        final String methodAttributeKey = "request.method";
        runner.setProperty(InvokeHTTP.HTTP_METHOD, String.format("${%s}", methodAttributeKey));

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(methodAttributeKey, null);
        runner.enqueue(FLOW_FILE_CONTENT, attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(InvokeHTTP.FAILURE);
        runner.assertPenalizeCount(1);

        final MockFlowFile flowFile = getFailureFlowFile();
        flowFile.assertAttributeEquals(InvokeHTTP.EXCEPTION_CLASS, IllegalArgumentException.class.getName());
        flowFile.assertAttributeExists(InvokeHTTP.EXCEPTION_MESSAGE);
    }

    @Test
    public void testRunGetHttp200Success() throws InterruptedException {
        assertRequestMethodSuccess(HttpMethod.GET);
    }

    @Test
    public void testRunGetHttp200SuccessResponseBodyIgnoredEnabled() throws InterruptedException {
        runner.setProperty(InvokeHTTP.RESPONSE_BODY_IGNORED, Boolean.TRUE.toString());
        assertRequestMethodSuccess(HttpMethod.GET);

        final MockFlowFile responseFlowFile = getResponseFlowFile();
        assertEquals(StringUtils.EMPTY, responseFlowFile.getContent());
    }

    @Test
    public void testRunGetHttp200SuccessResponseBodyAttributeName() {
        final String outputAttributeKey = String.class.getSimpleName();
        runner.setProperty(InvokeHTTP.RESPONSE_BODY_ATTRIBUTE_NAME, outputAttributeKey);
        setUrlProperty();

        final String body = String.class.getName();
        mockWebServer.enqueue(new MockResponse().setResponseCode(HTTP_OK).setBody(body));
        runner.enqueue(FLOW_FILE_CONTENT);
        runner.run();

        assertRelationshipStatusCodeEquals(InvokeHTTP.ORIGINAL, HTTP_OK);

        final MockFlowFile flowFile = getRequestFlowFile();
        flowFile.assertAttributeEquals(outputAttributeKey, body);
    }

    @Test
    public void testRunGetHttp200SuccessResponseBodyAttributeNameNoIncomingConnections() {
        final String outputAttributeKey = String.class.getSimpleName();
        runner.setProperty(InvokeHTTP.RESPONSE_BODY_ATTRIBUTE_NAME, outputAttributeKey);
        setUrlProperty();
        runner.setIncomingConnection(false);
        runner.setNonLoopConnection(false);

        final String body = String.class.getName();
        mockWebServer.enqueue(new MockResponse().setResponseCode(HTTP_OK).setBody(body));
        runner.run();

        assertRelationshipStatusCodeEquals(InvokeHTTP.ORIGINAL, HTTP_OK);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(InvokeHTTP.ORIGINAL).getFirst();
        flowFile.assertAttributeEquals(outputAttributeKey, body);
    }

    @Test
    public void testRunGetHttp200SuccessNoIncomingConnections() {
        runner.setIncomingConnection(false);
        runner.setNonLoopConnection(false);
        setUrlProperty();

        mockWebServer.enqueue(new MockResponse().setResponseCode(HTTP_OK));
        runner.run();

        assertRelationshipStatusCodeEquals(InvokeHTTP.RESPONSE, HTTP_OK);
    }

    @Test
    public void testRunGetHttp200SuccessProxyHostPortConfigured() throws InterruptedException, InitializationException {
        final String mockWebServerUrl = getMockWebServerUrl();
        final URI uri = URI.create(mockWebServerUrl);

        final String proxyConfigurationServiceId = ProxyConfigurationService.class.getSimpleName();
        final ProxyConfigurationService proxyConfigurationService = mock(ProxyConfigurationService.class);
        when(proxyConfigurationService.getIdentifier()).thenReturn(proxyConfigurationServiceId);
        runner.addControllerService(proxyConfigurationServiceId, proxyConfigurationService);
        runner.enableControllerService(proxyConfigurationService);

        final ProxyConfiguration proxyConfiguration = new ProxyConfiguration();
        proxyConfiguration.setProxyType(Proxy.Type.HTTP);
        proxyConfiguration.setProxyServerHost(uri.getHost());
        proxyConfiguration.setProxyServerPort(uri.getPort());

        when(proxyConfigurationService.getConfiguration()).thenReturn(proxyConfiguration);

        runner.setProperty(InvokeHTTP.HTTP_URL, mockWebServerUrl);
        runner.setProperty(ProxyConfigurationService.PROXY_CONFIGURATION_SERVICE, proxyConfigurationServiceId);

        mockWebServer.enqueue(new MockResponse().setResponseCode(HTTP_OK));
        runner.enqueue(FLOW_FILE_CONTENT);
        runner.run();

        assertRelationshipStatusCodeEquals(InvokeHTTP.RESPONSE, HTTP_OK);
        final RecordedRequest request = takeRequestCompleted();
        final String requestLine = request.getRequestLine();

        final String proxyRequestLine = String.format("%s %s HTTP/1.1", HttpMethod.GET.name(), mockWebServerUrl);
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
        assertRelationshipStatusCodeEquals(InvokeHTTP.RESPONSE, HTTP_OK);

        final MockFlowFile responseFlowFile = getResponseFlowFile();
        responseFlowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), TEXT_PLAIN);
    }

    @Test
    public void testRunGetHttp200SuccessRequestDateHeader() throws InterruptedException {
        runner.setProperty(InvokeHTTP.REQUEST_DATE_HEADER_ENABLED, StringUtils.capitalize(Boolean.TRUE.toString()));

        enqueueResponseCodeAndRun(HTTP_OK);

        assertResponseSuccessRelationships();
        assertRelationshipStatusCodeEquals(InvokeHTTP.RESPONSE, HTTP_OK);

        final RecordedRequest request = takeRequestCompleted();
        final String dateHeader = request.getHeader(DATE_HEADER);
        assertNotNull(dateHeader, "Request Date not found");

        final Pattern rfcDatePattern = Pattern.compile("^.+? \\d{4} \\d{2}:\\d{2}:\\d{2} GMT$");
        assertTrue(rfcDatePattern.matcher(dateHeader).matches(), "Request Date RFC 2616 not matched");

        final ZonedDateTime zonedDateTime = ZonedDateTime.parse(dateHeader, DateTimeFormatter.RFC_1123_DATE_TIME);
        assertNotNull(zonedDateTime, "Request Date Parsing Failed");
    }

    @Test
    public void testRunGetHttp200SuccessRequestHeaderAttributesAndDynamicProperties() throws InterruptedException {
        runner.setProperty(InvokeHTTP.REQUEST_HEADER_ATTRIBUTES_PATTERN, String.format("^%s$", ACCEPT_HEADER));
        final String defaultContentTypeHeader = "Default-Content-Type";
        runner.setProperty(defaultContentTypeHeader, InvokeHTTP.DEFAULT_CONTENT_TYPE);
        setUrlProperty();
        mockWebServer.enqueue(new MockResponse().setResponseCode(HTTP_OK));

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(ACCEPT_HEADER, TEXT_PLAIN);
        runner.enqueue(FLOW_FILE_CONTENT, attributes);
        runner.run();

        assertResponseSuccessRelationships();
        assertRelationshipStatusCodeEquals(InvokeHTTP.RESPONSE, HTTP_OK);

        final RecordedRequest request = takeRequestCompleted();
        final String acceptHeader = request.getHeader(ACCEPT_HEADER);
        assertEquals(TEXT_PLAIN, acceptHeader);

        final String contentType = request.getHeader(defaultContentTypeHeader);
        assertEquals(InvokeHTTP.DEFAULT_CONTENT_TYPE, contentType);

        runner.removeProperty(InvokeHTTP.REQUEST_HEADER_ATTRIBUTES_PATTERN);
        runner.removeProperty(defaultContentTypeHeader);
        mockWebServer.enqueue(new MockResponse().setResponseCode(HTTP_OK));
        runner.enqueue(FLOW_FILE_CONTENT, attributes);
        runner.run();

        final RecordedRequest secondRequest = takeRequestCompleted();
        assertNull(secondRequest.getHeader(ACCEPT_HEADER), "Accept Header found");
        assertNull(secondRequest.getHeader(defaultContentTypeHeader), "Default-Content-Type Header found");
    }

    @Test
    public void testRunGetHttp200SuccessResponseHeaderRequestAttributes() {
        final String prefix = "response.";
        setUrlProperty();
        runner.setProperty(InvokeHTTP.RESPONSE_HEADER_REQUEST_ATTRIBUTES_ENABLED, Boolean.TRUE.toString());
        runner.setProperty(InvokeHTTP.RESPONSE_HEADER_REQUEST_ATTRIBUTES_PREFIX, prefix);

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
        assertRelationshipStatusCodeEquals(InvokeHTTP.RESPONSE, HTTP_OK);

        final MockFlowFile requestFlowFile = getRequestFlowFile();
        requestFlowFile.assertAttributeEquals(prefix + CONTENT_LENGTH_HEADER, Integer.toString(0));
        requestFlowFile.assertAttributeNotExists(CONTENT_LENGTH_HEADER);

        final String repeatedHeaders = String.format("%s, %s", firstHeader, secondHeader);
        requestFlowFile.assertAttributeEquals(prefix + REPEATED_HEADER, repeatedHeaders);
        requestFlowFile.assertAttributeNotExists(REPEATED_HEADER);

        final MockFlowFile responseFlowFile = getResponseFlowFile();
        responseFlowFile.assertAttributeNotExists(prefix + CONTENT_LENGTH_HEADER);
        responseFlowFile.assertAttributeEquals(CONTENT_LENGTH_HEADER, Integer.toString(0));

        responseFlowFile.assertAttributeNotExists(prefix + REPEATED_HEADER);
        responseFlowFile.assertAttributeEquals(REPEATED_HEADER, repeatedHeaders);
    }

    @Test
    public void testRunGetHttp200SuccessResponseCacheEnabled() throws InterruptedException {
        runner.setProperty(InvokeHTTP.RESPONSE_CACHE_ENABLED, Boolean.TRUE.toString());

        assertRequestMethodSuccess(HttpMethod.GET);
    }

    @Test
    public void testRunGetHttp200SuccessBasicAuthentication() throws InterruptedException {
        runner.setProperty(InvokeHTTP.REQUEST_USERNAME, String.class.getSimpleName());
        runner.setProperty(InvokeHTTP.REQUEST_PASSWORD, String.class.getName());

        enqueueResponseCodeAndRun(HTTP_OK);

        assertResponseSuccessRelationships();
        assertRelationshipStatusCodeEquals(InvokeHTTP.RESPONSE, HTTP_OK);

        final RecordedRequest request = takeRequestCompleted();
        final String authorization = request.getHeader(AUTHORIZATION_HEADER);
        assertNotNull(authorization, "Authorization Header not found");

        final Pattern basicAuthPattern = Pattern.compile("^Basic \\S+$");
        assertTrue(basicAuthPattern.matcher(authorization).matches(), "Basic Authentication not matched");
    }

    @Test
    public void testRunGetHttp200SuccessDigestAuthentication() throws InterruptedException {
        runner.setProperty(InvokeHTTP.REQUEST_USERNAME, String.class.getSimpleName());
        runner.setProperty(InvokeHTTP.REQUEST_PASSWORD, String.class.getName());
        runner.setProperty(InvokeHTTP.REQUEST_DIGEST_AUTHENTICATION_ENABLED, Boolean.TRUE.toString());

        final String realm = UUID.randomUUID().toString();
        final String nonce = UUID.randomUUID().toString();
        final String digestHeader = String.format("Digest realm=\"%s\", nonce=\"%s\"", realm, nonce);

        mockWebServer.enqueue(new MockResponse().setResponseCode(HTTP_UNAUTHORIZED).setHeader(AUTHENTICATE_HEADER, digestHeader));
        enqueueResponseCodeAndRun(HTTP_OK);

        assertResponseSuccessRelationships();
        assertRelationshipStatusCodeEquals(InvokeHTTP.RESPONSE, HTTP_OK);

        final RecordedRequest request = takeRequestCompleted();
        assertNull(request.getHeader(AUTHORIZATION_HEADER), "Authorization Header found");

        final RecordedRequest authenticatedRequest = takeRequestCompleted();
        final String authorization = authenticatedRequest.getHeader(AUTHORIZATION_HEADER);
        assertNotNull(authorization, "Authorization Header not found");
        assertTrue(authorization.contains(realm), "Digest Realm not found");
        assertTrue(authorization.contains(nonce), "Digest Nonce not found");
    }

    @Test
    public void testRunGetHttp200SuccessSslContextServiceServerTrusted() throws InitializationException {
        assertResponseSuccessSslContextConfigured(trustStoreSslContext);
    }

    @Test
    public void testRunGetHttp200SuccessSslContextServiceMutualTrusted() throws InitializationException {
        assertResponseSuccessSslContextConfigured(sslContext);
    }

    @Test
    public void testRunGetSslContextServiceMutualTrustedClientCertificateMissing() throws InitializationException {
        runner.setProperty(InvokeHTTP.HTTP2_DISABLED, StringUtils.capitalize(Boolean.TRUE.toString()));
        setSslContextConfiguration(trustStoreSslContext);
        mockWebServer.requireClientAuth();

        setUrlProperty();
        runner.enqueue(FLOW_FILE_CONTENT);
        runner.run();

        runner.assertAllFlowFilesTransferred(InvokeHTTP.FAILURE);
        final MockFlowFile flowFile = getFailureFlowFile();
        flowFile.assertAttributeExists(InvokeHTTP.EXCEPTION_CLASS);
        flowFile.assertAttributeExists(InvokeHTTP.EXCEPTION_MESSAGE);
    }

    @Test
    public void testRunGetHttp200SuccessUserAgentConfigured() throws InterruptedException {
        final String userAgent = UUID.randomUUID().toString();
        runner.setProperty(InvokeHTTP.REQUEST_USER_AGENT, userAgent);

        enqueueResponseCodeAndRun(HTTP_OK);

        assertResponseSuccessRelationships();
        assertRelationshipStatusCodeEquals(InvokeHTTP.RESPONSE, HTTP_OK);

        final RecordedRequest request = takeRequestCompleted();
        final String userAgentHeader = request.getHeader(USER_AGENT_HEADER);
        assertEquals(userAgent, userAgentHeader);
    }

    @Test
    void testRunGetHttp200SuccessWithEncodableUrl() throws Exception {
        final String partialEncodableUrl = "/gitlab/ftp%2Fstage%2F15m%2FsomeFile.yaml/raw?ref=main";
        final String nonEncodedUrl = mockWebServer.url(partialEncodableUrl).newBuilder().host(LOCALHOST).build().toString();
        final String encodedUrl = URLValidator.createURL(nonEncodedUrl).toExternalForm();

        runner.setProperty(InvokeHTTP.HTTP_URL, nonEncodedUrl);
        mockWebServer.enqueue(new MockResponse().setResponseCode(HTTP_OK));
        runner.enqueue(FLOW_FILE_CONTENT);
        runner.run();

        assertResponseSuccessRelationships();
        assertRelationshipStatusCodeEquals(InvokeHTTP.RESPONSE, HTTP_OK);

        MockFlowFile flowFile = getResponseFlowFile();
        final String actualUrl = flowFile.getAttribute(InvokeHTTP.REQUEST_URL);
        assertNotEquals(encodedUrl, actualUrl);
        assertTrue(actualUrl.endsWith(partialEncodableUrl));

        final ProvenanceEventRecord event = runner.getProvenanceEvents().stream()
                .filter(record -> record.getEventType() == ProvenanceEventType.FETCH)
                .findFirst()
                .orElse(null);
        assertNotNull(event);
        final String transitUri = event.getTransitUri();
        assertNotEquals(encodedUrl, transitUri);
        assertTrue(transitUri.endsWith(partialEncodableUrl));
    }

    @Test
    public void testRunGetHttp302NoRetryResponseRedirectsDefaultEnabled() {
        mockWebServer.enqueue(new MockResponse().setResponseCode(HTTP_MOVED_TEMP).setHeader(LOCATION_HEADER, getMockWebServerUrl()));
        enqueueResponseCodeAndRun(HTTP_OK);

        runner.assertTransferCount(InvokeHTTP.FAILURE, 0);
        runner.assertTransferCount(InvokeHTTP.NO_RETRY, 0);
        assertRelationshipStatusCodeEquals(InvokeHTTP.RESPONSE, HTTP_OK);
    }

    @Test
    public void testRunGetHttp302NoRetryResponseRedirectsDisabled() {
        runner.setProperty(InvokeHTTP.RESPONSE_REDIRECTS_ENABLED, StringUtils.capitalize(Boolean.FALSE.toString()));
        enqueueResponseCodeAndRun(HTTP_MOVED_TEMP);

        runner.assertTransferCount(InvokeHTTP.FAILURE, 0);
        runner.assertTransferCount(InvokeHTTP.RESPONSE, 0);
        assertRelationshipStatusCodeEquals(InvokeHTTP.NO_RETRY, HTTP_MOVED_TEMP);
    }

    @Test
    public void testRunGetHttp302CookieStrategyAcceptAll() throws InterruptedException {
        runner.setProperty(InvokeHTTP.RESPONSE_COOKIE_STRATEGY, CookieStrategy.ACCEPT_ALL.name());
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

        runner.assertTransferCount(InvokeHTTP.FAILURE, 0);
        runner.assertTransferCount(InvokeHTTP.NO_RETRY, 0);
        assertRelationshipStatusCodeEquals(InvokeHTTP.RESPONSE, HTTP_OK);
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

        runner.assertTransferCount(InvokeHTTP.FAILURE, 0);
        runner.assertTransferCount(InvokeHTTP.NO_RETRY, 0);
        assertRelationshipStatusCodeEquals(InvokeHTTP.RESPONSE, HTTP_OK);
    }

    @Test
    public void testRunGetHttp400NoRetryMinimumProperties() {
        enqueueResponseCodeAndRun(HTTP_BAD_REQUEST);

        runner.assertTransferCount(InvokeHTTP.FAILURE, 0);
        runner.assertTransferCount(InvokeHTTP.RESPONSE, 0);
        assertRelationshipStatusCodeEquals(InvokeHTTP.NO_RETRY, HTTP_BAD_REQUEST);
    }

    @Test
    public void testRunGetHttp400NoRetryPenalizeNoRetry() {
        runner.setProperty(InvokeHTTP.REQUEST_FAILURE_PENALIZATION_ENABLED, Boolean.TRUE.toString());

        enqueueResponseCodeAndRun(HTTP_BAD_REQUEST);

        runner.assertTransferCount(InvokeHTTP.FAILURE, 0);
        runner.assertTransferCount(InvokeHTTP.RESPONSE, 0);
        runner.assertPenalizeCount(1);
        assertRelationshipStatusCodeEquals(InvokeHTTP.NO_RETRY, HTTP_BAD_REQUEST);
    }

    @Test
    public void testRunGetHttp500RetryMinimumProperties() {
        enqueueResponseCodeAndRun(HTTP_INTERNAL_ERROR);

        runner.assertTransferCount(InvokeHTTP.FAILURE, 0);
        runner.assertTransferCount(InvokeHTTP.RESPONSE, 0);
        assertRelationshipStatusCodeEquals(InvokeHTTP.RETRY, HTTP_INTERNAL_ERROR);
    }

    @Test
    public void testRunGetHttp500RetryResponseGeneratedRequired() {
        runner.setProperty(InvokeHTTP.RESPONSE_GENERATION_REQUIRED, Boolean.TRUE.toString());

        enqueueResponseCodeAndRun(HTTP_INTERNAL_ERROR);

        runner.assertTransferCount(InvokeHTTP.FAILURE, 0);
        assertRelationshipStatusCodeEquals(InvokeHTTP.RETRY, HTTP_INTERNAL_ERROR);
        assertRelationshipStatusCodeEquals(InvokeHTTP.RESPONSE, HTTP_INTERNAL_ERROR);
    }

    @Test
    public void testRunDeleteHttp200Success() throws InterruptedException {
        runner.setProperty(InvokeHTTP.HTTP_METHOD, HttpMethod.DELETE.name());
        assertRequestMethodSuccess(HttpMethod.DELETE);
    }

    @Test
    public void testRunHeadHttp200Success() throws InterruptedException {
        runner.setProperty(InvokeHTTP.HTTP_METHOD, HttpMethod.HEAD.name());
        assertRequestMethodSuccess(HttpMethod.HEAD);
    }

    @Test
    public void testRunOptionsHttp200Success() throws InterruptedException {
        runner.setProperty(InvokeHTTP.HTTP_METHOD, HttpMethod.OPTIONS.name());
        assertRequestMethodSuccess(HttpMethod.OPTIONS);
    }

    @Test
    public void testRunPatchHttp200Success() throws InterruptedException {
        runner.setProperty(InvokeHTTP.HTTP_METHOD, HttpMethod.PATCH.name());
        assertRequestMethodSuccess(HttpMethod.PATCH);
    }

    @Test
    public void testRunPostHttp200Success() throws InterruptedException {
        runner.setProperty(InvokeHTTP.HTTP_METHOD, HttpMethod.POST.name());
        assertRequestMethodSuccess(HttpMethod.POST);
    }

    @Test
    public void testRunPostHttp200SuccessContentEncodingGzip() throws InterruptedException, IOException {
        runner.setProperty(InvokeHTTP.HTTP_METHOD, HttpMethod.POST.name());
        runner.setProperty(InvokeHTTP.REQUEST_CONTENT_ENCODING, ContentEncodingStrategy.GZIP);
        runner.setProperty(InvokeHTTP.REQUEST_BODY_ENABLED, Boolean.TRUE.toString());

        enqueueResponseCodeAndRun(HTTP_OK);

        assertResponseSuccessRelationships();
        assertRelationshipStatusCodeEquals(InvokeHTTP.RESPONSE, HTTP_OK);

        final RecordedRequest request = takeRequestCompleted();
        final String contentLength = request.getHeader(CONTENT_LENGTH_HEADER);
        assertNull(contentLength, "Content-Length Request Header found");

        final String contentEncoding = request.getHeader(CONTENT_ENCODING_HEADER);
        assertEquals(ContentEncodingStrategy.GZIP.getValue().toLowerCase(), contentEncoding);

        final Buffer body = request.getBody();
        try (final GZIPInputStream gzipInputStream = new GZIPInputStream(body.inputStream())) {
            final String decompressed = IOUtils.toString(gzipInputStream, StandardCharsets.UTF_8);
            assertEquals(FLOW_FILE_CONTENT, decompressed);
        }
    }

    @Test
    public void testRunPostHttp200SuccessChunkedEncoding() throws InterruptedException {
        runner.setProperty(InvokeHTTP.HTTP_METHOD, HttpMethod.POST.name());
        runner.setProperty(InvokeHTTP.REQUEST_CHUNKED_TRANSFER_ENCODING_ENABLED, Boolean.TRUE.toString());

        enqueueResponseCodeAndRun(HTTP_OK);

        assertResponseSuccessRelationships();
        assertRelationshipStatusCodeEquals(InvokeHTTP.RESPONSE, HTTP_OK);

        final RecordedRequest request = takeRequestCompleted();
        final String contentLength = request.getHeader(CONTENT_LENGTH_HEADER);
        assertNull(contentLength, "Content-Length Request Header found");

        final String transferEncoding = request.getHeader(TRANSFER_ENCODING_HEADER);
        assertEquals("chunked", transferEncoding);
    }

    @Test
    public void testRunPostHttp200SuccessFormData() throws InterruptedException {
        runner.setProperty(InvokeHTTP.HTTP_METHOD, HttpMethod.POST.name());

        final String formName = "multipart-form";
        runner.setProperty(InvokeHTTP.REQUEST_FORM_DATA_NAME, formName);

        final String formDataParameter = String.class.getName();
        final String formDataParameterName = "label";
        final String formDataPropertyName = String.format("%s:%s", InvokeHTTP.FORM_DATA_NAME_BASE, formDataParameterName);
        runner.setProperty(formDataPropertyName, formDataParameter);

        setUrlProperty();
        mockWebServer.enqueue(new MockResponse().setResponseCode(HTTP_OK));
        runner.enqueue(FLOW_FILE_CONTENT);
        runner.run();

        assertResponseSuccessRelationships();
        assertRelationshipStatusCodeEquals(InvokeHTTP.RESPONSE, HTTP_OK);

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
        runner.setProperty(InvokeHTTP.HTTP_METHOD, HttpMethod.PUT.name());
        assertRequestMethodSuccess(HttpMethod.PUT);
    }

    @ParameterizedTest(name = "{index} => When {0} http://baseUrl/{1}, filename of the response FlowFile should be {2}")
    @MethodSource
    public void testResponseFlowFileFilenameExtractedFromRemoteUrl(String httpMethod, String relativePath, String expectedFileName) {
        //Build URL as a string to prevent double encoding
        final String targetUrl = String.format("http://%s:%d/%s", mockWebServer.getHostName(), mockWebServer.getPort(), relativePath);
        runner.setProperty(InvokeHTTP.HTTP_METHOD, httpMethod);
        runner.setProperty(InvokeHTTP.HTTP_URL, targetUrl);
        runner.setProperty(InvokeHTTP.RESPONSE_FLOW_FILE_NAMING_STRATEGY, FlowFileNamingStrategy.URL_PATH.name());

        Map<String, String> ffAttributes = new HashMap<>();
        ffAttributes.put(CoreAttributes.FILENAME.key(), FLOW_FILE_INITIAL_FILENAME);
        runner.enqueue(FLOW_FILE_CONTENT, ffAttributes);

        mockWebServer.enqueue(new MockResponse().setResponseCode(HTTP_OK));

        runner.run();

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(InvokeHTTP.RESPONSE).getFirst();
        flowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), expectedFileName);
    }

    private static Stream<Arguments> testResponseFlowFileFilenameExtractedFromRemoteUrl() {
        return Stream.of(
            Arguments.of(HttpMethod.GET.name(), "file", "file"),
            Arguments.of(HttpMethod.GET.name(), "file/", "file"),
            Arguments.of(HttpMethod.GET.name(), "file.txt", "file.txt"),
            Arguments.of(HttpMethod.GET.name(), "file.txt/", "file.txt"),
            Arguments.of(HttpMethod.GET.name(), "f%69%6Cle.txt", "f%2569%256Cle.txt"),
            Arguments.of(HttpMethod.GET.name(), "path/to/file.txt", "file.txt"),
            Arguments.of(HttpMethod.GET.name(), "", FLOW_FILE_INITIAL_FILENAME),
            Arguments.of(HttpMethod.POST.name(), "has/path", FLOW_FILE_INITIAL_FILENAME),
            Arguments.of(HttpMethod.POST.name(), "", FLOW_FILE_INITIAL_FILENAME),
            Arguments.of(HttpMethod.PUT.name(), "has/path", FLOW_FILE_INITIAL_FILENAME),
            Arguments.of(HttpMethod.PUT.name(), "", FLOW_FILE_INITIAL_FILENAME),
            Arguments.of(HttpMethod.PATCH.name(), "", FLOW_FILE_INITIAL_FILENAME),
            Arguments.of(HttpMethod.PATCH.name(), "has/path", FLOW_FILE_INITIAL_FILENAME),
            Arguments.of(HttpMethod.DELETE.name(), "", FLOW_FILE_INITIAL_FILENAME),
            Arguments.of(HttpMethod.DELETE.name(), "has/path", FLOW_FILE_INITIAL_FILENAME),
            Arguments.of(HttpMethod.HEAD.name(), "", FLOW_FILE_INITIAL_FILENAME),
            Arguments.of(HttpMethod.HEAD.name(), "has/path", FLOW_FILE_INITIAL_FILENAME),
            Arguments.of(HttpMethod.OPTIONS.name(), "", FLOW_FILE_INITIAL_FILENAME),
            Arguments.of(HttpMethod.OPTIONS.name(), "has/path", FLOW_FILE_INITIAL_FILENAME)
        );
    }

    @Test
    public void testValidWhenOAuth2Set() throws Exception {
        String oauth2AccessTokenProviderId = "oauth2AccessTokenProviderId";

        OAuth2AccessTokenProvider oauth2AccessTokenProvider = mock(OAuth2AccessTokenProvider.class, Answers.RETURNS_DEEP_STUBS);
        when(oauth2AccessTokenProvider.getIdentifier()).thenReturn(oauth2AccessTokenProviderId);

        runner.addControllerService(oauth2AccessTokenProviderId, oauth2AccessTokenProvider);
        runner.enableControllerService(oauth2AccessTokenProvider);

        setUrlProperty();

        runner.setProperty(InvokeHTTP.REQUEST_OAUTH2_ACCESS_TOKEN_PROVIDER, oauth2AccessTokenProviderId);

        runner.assertValid();
    }

    @Test
    public void testInvalidWhenOAuth2AndUserNameSet() throws Exception {
        String oauth2AccessTokenProviderId = "oauth2AccessTokenProviderId";

        OAuth2AccessTokenProvider oauth2AccessTokenProvider = mock(OAuth2AccessTokenProvider.class, Answers.RETURNS_DEEP_STUBS);
        when(oauth2AccessTokenProvider.getIdentifier()).thenReturn(oauth2AccessTokenProviderId);

        runner.addControllerService(oauth2AccessTokenProviderId, oauth2AccessTokenProvider);
        runner.enableControllerService(oauth2AccessTokenProvider);

        setUrlProperty();

        runner.setProperty(InvokeHTTP.REQUEST_OAUTH2_ACCESS_TOKEN_PROVIDER, oauth2AccessTokenProviderId);
        runner.setProperty(InvokeHTTP.REQUEST_USERNAME, "userName");

        runner.assertNotValid();
    }

    @Test
    public void testInvalidWhenOAuth2AndPasswordSet() throws Exception {
        String oauth2AccessTokenProviderId = "oauth2AccessTokenProviderId";

        OAuth2AccessTokenProvider oauth2AccessTokenProvider = mock(OAuth2AccessTokenProvider.class, Answers.RETURNS_DEEP_STUBS);
        when(oauth2AccessTokenProvider.getIdentifier()).thenReturn(oauth2AccessTokenProviderId);

        runner.addControllerService(oauth2AccessTokenProviderId, oauth2AccessTokenProvider);
        runner.enableControllerService(oauth2AccessTokenProvider);

        setUrlProperty();

        runner.setProperty(InvokeHTTP.REQUEST_OAUTH2_ACCESS_TOKEN_PROVIDER, oauth2AccessTokenProviderId);
        runner.setProperty(InvokeHTTP.REQUEST_PASSWORD, "password");

        runner.assertNotValid();
    }

    @Test
    public void testOAuth2AuthorizationHeader() throws Exception {
        String accessToken = "access_token";

        String oauth2AccessTokenProviderId = "oauth2AccessTokenProviderId";

        OAuth2AccessTokenProvider oauth2AccessTokenProvider = mock(OAuth2AccessTokenProvider.class, Answers.RETURNS_DEEP_STUBS);
        when(oauth2AccessTokenProvider.getIdentifier()).thenReturn(oauth2AccessTokenProviderId);
        when(oauth2AccessTokenProvider.getAccessDetails().getAccessToken()).thenReturn(accessToken);

        runner.addControllerService(oauth2AccessTokenProviderId, oauth2AccessTokenProvider);
        runner.enableControllerService(oauth2AccessTokenProvider);

        setUrlProperty();

        mockWebServer.enqueue(new MockResponse());

        runner.setProperty(InvokeHTTP.REQUEST_OAUTH2_ACCESS_TOKEN_PROVIDER, oauth2AccessTokenProviderId);
        runner.enqueue("unimportant");
        runner.run();

        RecordedRequest recordedRequest = mockWebServer.takeRequest();

        String actualAuthorizationHeader = recordedRequest.getHeader(HttpHeader.AUTHORIZATION.getHeader());
        assertEquals("Bearer " + accessToken, actualAuthorizationHeader);

    }

    private void setUrlProperty() {
        runner.setProperty(InvokeHTTP.HTTP_URL, getMockWebServerUrl());
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
        return runner.getFlowFilesForRelationship(InvokeHTTP.FAILURE).getFirst();
    }

    private MockFlowFile getRequestFlowFile() {
        return runner.getFlowFilesForRelationship(InvokeHTTP.ORIGINAL).getFirst();
    }

    private MockFlowFile getResponseFlowFile() {
        return runner.getFlowFilesForRelationship(InvokeHTTP.RESPONSE).getFirst();
    }

    private void assertRequestMethodSuccess(final HttpMethod httpMethod) throws InterruptedException {
        enqueueResponseCodeAndRun(HTTP_OK);

        assertResponseSuccessRelationships();
        assertRelationshipStatusCodeEquals(InvokeHTTP.RESPONSE, HTTP_OK);

        final RecordedRequest request = takeRequestCompleted();
        assertEquals(httpMethod.name(), request.getMethod());
    }

    private void assertRelationshipStatusCodeEquals(final Relationship relationship, final int statusCode) {
        final List<MockFlowFile> responseFlowFiles = runner.getFlowFilesForRelationship(relationship);
        final String message = String.format("FlowFiles not found for Relationship [%s]", relationship);
        assertFalse(responseFlowFiles.isEmpty(), message);
        final MockFlowFile responseFlowFile = responseFlowFiles.getFirst();
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

        runner.assertTransferCount(InvokeHTTP.RESPONSE, 1);
        runner.assertTransferCount(InvokeHTTP.ORIGINAL, 1);
        runner.assertTransferCount(InvokeHTTP.RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.NO_RETRY, 0);
        runner.assertTransferCount(InvokeHTTP.FAILURE, 0);
    }

    private void assertResponseSuccessSslContextConfigured(final SSLContext clientSslContext) throws InitializationException {
        setSslContextConfiguration(clientSslContext);
        enqueueResponseCodeAndRun(HTTP_OK);

        assertResponseSuccessRelationships();
        assertRelationshipStatusCodeEquals(InvokeHTTP.RESPONSE, HTTP_OK);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(InvokeHTTP.RESPONSE).getFirst();
        flowFile.assertAttributeExists(InvokeHTTP.REMOTE_DN);
    }

    private void setSslContextConfiguration(final SSLContext clientSslContext) throws InitializationException {
        setMockWebServerSslSocketFactory();

        final SSLContextProvider sslContextProvider = setSslContextProvider();
        when(sslContextProvider.createContext()).thenReturn(clientSslContext);
        when(sslContextProvider.createTrustManager()).thenReturn(trustManager);
    }

    private SSLContextProvider setSslContextProvider() throws InitializationException {
        final String serviceIdentifier = SSLContextProvider.class.getName();
        final SSLContextProvider sslContextProvider = mock(SSLContextProvider.class);
        when(sslContextProvider.getIdentifier()).thenReturn(serviceIdentifier);

        runner.addControllerService(serviceIdentifier, sslContextProvider);
        runner.enableControllerService(sslContextProvider);
        runner.setProperty(InvokeHTTP.SSL_CONTEXT_SERVICE, serviceIdentifier);
        runner.setProperty(InvokeHTTP.SOCKET_READ_TIMEOUT, TLS_CONNECTION_TIMEOUT);
        runner.setProperty(InvokeHTTP.SOCKET_CONNECT_TIMEOUT, TLS_CONNECTION_TIMEOUT);
        return sslContextProvider;
    }

    private void setMockWebServerSslSocketFactory() {
        final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();
        if (sslSocketFactory == null) {
            throw new IllegalArgumentException("Socket Factory not found");
        }
        mockWebServer.useHttps(sslSocketFactory, false);
    }
}
