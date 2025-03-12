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
package org.apache.nifi.web.client;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.nifi.web.client.api.HttpEntityHeaders;
import org.apache.nifi.web.client.api.HttpRequestMethod;
import org.apache.nifi.web.client.api.HttpRequestUriSpec;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.HttpResponseStatus;
import org.apache.nifi.web.client.api.StandardHttpRequestMethod;
import org.apache.nifi.web.client.api.WebClientServiceException;
import org.apache.nifi.web.client.proxy.ProxyContext;
import org.apache.nifi.web.client.redirect.RedirectHandling;
import org.apache.nifi.web.client.ssl.TlsContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.net.ssl.X509TrustManager;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Proxy;
import java.net.URI;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class StandardWebClientServiceTest {
    private static final String ROOT_PATH = "/";

    private static final String LOCALHOST = "localhost";

    private static final byte[] EMPTY_BODY = new byte[0];

    private static final String RESPONSE_BODY = String.class.getSimpleName();

    private static final byte[] TEXT_BODY = RESPONSE_BODY.getBytes(StandardCharsets.UTF_8);

    private static final Duration FAILURE_TIMEOUT = Duration.ofMillis(100);

    private static final String ACCEPT_HEADER = "Accept";

    private static final String ACCEPT_ANY_TYPE = "*/*";

    private static final String CONTENT_LENGTH_HEADER = "content-length";

    private static final String CONTENT_LENGTH_ZERO = "0";

    private static final String LOCATION_HEADER = "Location";

    private static final String PROXY_AUTHENTICATE_HEADER = "Proxy-Authenticate";

    private static final String PROXY_AUTHENTICATE_BASIC_REALM = "Basic realm=\"Authentication Required\"";

    private static final String PROXY_AUTHORIZATION_HEADER = "Proxy-Authorization";

    private static final String TLS_PROTOCOL = "TLS";

    private static final String TLS_PROTOCOL_UNSUPPORTED = "TLSv0";

    @Mock
    TlsContext tlsContext;

    @Mock
    ProxyContext proxyContext;

    @Mock
    X509TrustManager trustManager;

    MockWebServer mockWebServer;

    StandardWebClientService service;

    @BeforeEach
    void setServer() {
        mockWebServer = new MockWebServer();
        service = new StandardWebClientService();
    }

    @AfterEach
    void shutdownServer() throws IOException {
        mockWebServer.shutdown();
    }

    @Test
    void testSetTlsContext() {
        when(tlsContext.getProtocol()).thenReturn(TLS_PROTOCOL);
        when(tlsContext.getTrustManager()).thenReturn(trustManager);

        service.setTlsContext(tlsContext);
    }

    @Test
    void testSetTlsContextProtocolNotSupported() {
        when(tlsContext.getProtocol()).thenReturn(TLS_PROTOCOL_UNSUPPORTED);
        when(tlsContext.getTrustManager()).thenReturn(trustManager);

        assertThrows(IllegalArgumentException.class, () -> service.setTlsContext(tlsContext));
    }

    @Test
    void testHttpTimeoutException() throws IOException {
        mockWebServer.shutdown();

        service.setConnectTimeout(FAILURE_TIMEOUT);
        service.setReadTimeout(FAILURE_TIMEOUT);
        service.setWriteTimeout(FAILURE_TIMEOUT);

        when(proxyContext.getProxy()).thenReturn(Proxy.NO_PROXY);
        service.setProxyContext(proxyContext);

        final WebClientServiceException exception = assertThrows(WebClientServiceException.class, () ->
                service.method(StandardHttpRequestMethod.GET)
                        .uri(getRootUri())
                        .retrieve()
        );

        assertInstanceOf(HttpTimeoutException.class, exception.getCause());
    }

    @Test
    void testProxyAuthorization() throws IOException, InterruptedException {
        final Proxy proxy = mockWebServer.toProxyAddress();
        when(proxyContext.getProxy()).thenReturn(proxy);
        final String username = String.class.getSimpleName();
        final String password = String.class.getName();
        when(proxyContext.getUsername()).thenReturn(Optional.of(username));
        when(proxyContext.getPassword()).thenReturn(Optional.of(password));
        service.setProxyContext(proxyContext);

        mockWebServer.enqueue(new MockResponse()
                .setResponseCode(HttpResponseStatus.PROXY_AUTHENTICATION_REQUIRED.getCode())
                .setHeader(PROXY_AUTHENTICATE_HEADER, PROXY_AUTHENTICATE_BASIC_REALM)
        );

        runRequestMethod(service.get(), StandardHttpRequestMethod.GET, HttpResponseStatus.OK);

        final RecordedRequest proxyAuthorizationRequest = mockWebServer.takeRequest();
        final String proxyAuthorization = proxyAuthorizationRequest.getHeader(PROXY_AUTHORIZATION_HEADER);

        final String formatted = String.format("%s:%s", username, password);
        final String encoded = Base64.getEncoder().encodeToString(formatted.getBytes(StandardCharsets.UTF_8));
        final String credentials = String.format("Basic %s", encoded);
        assertEquals(credentials, proxyAuthorization);
    }

    @Test
    void testRedirectHandlingFollowed() throws InterruptedException {
        service.setRedirectHandling(RedirectHandling.FOLLOWED);

        final String location = mockWebServer.url(ROOT_PATH).newBuilder().host(LOCALHOST).build().toString();

        final MockResponse movedResponse = new MockResponse()
                .setResponseCode(HttpResponseStatus.MOVED_PERMANENTLY.getCode())
                .setHeader(LOCATION_HEADER, location);
        mockWebServer.enqueue(movedResponse);

        final HttpResponseStatus httpResponseStatus = HttpResponseStatus.OK;
        enqueueResponseStatus(httpResponseStatus);

        final HttpResponseEntity httpResponseEntity = service.get()
                .uri(getRootUri())
                .retrieve();

        assertRecordedRequestResponseStatus(httpResponseEntity, StandardHttpRequestMethod.GET, httpResponseStatus);
    }

    @Test
    void testRedirectHandlingIgnored() throws InterruptedException {
        service.setRedirectHandling(RedirectHandling.IGNORED);

        final HttpResponseStatus httpResponseStatus = HttpResponseStatus.MOVED_PERMANENTLY;

        enqueueResponseStatusBody(httpResponseStatus);

        final HttpResponseEntity httpResponseEntity = service.get()
                .uri(getRootUri())
                .retrieve();

        assertRecordedRequestResponseStatus(httpResponseEntity, StandardHttpRequestMethod.GET, httpResponseStatus);
    }

    @Test
    void testDelete() throws InterruptedException, IOException {
        runRequestMethod(service.delete(), StandardHttpRequestMethod.DELETE, HttpResponseStatus.NO_CONTENT);
    }

    @Test
    void testDeleteMethodNotAllowed() throws InterruptedException, IOException {
        runRequestMethod(service.delete(), StandardHttpRequestMethod.DELETE, HttpResponseStatus.METHOD_NOT_ALLOWED);
    }

    @Test
    void testGet() throws InterruptedException, IOException {
        runRequestMethod(service.get(), StandardHttpRequestMethod.GET, HttpResponseStatus.OK);
    }

    @Test
    void testGetNotFound() throws InterruptedException, IOException {
        runRequestMethod(service.get(), StandardHttpRequestMethod.GET, HttpResponseStatus.NOT_FOUND);
    }

    @Test
    void testGetInternalServerError() throws InterruptedException, IOException {
        runRequestMethod(service.get(), StandardHttpRequestMethod.GET, HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }

    @Test
    void testGetServiceUnavailable() throws InterruptedException, IOException {
        runRequestMethod(service.get(), StandardHttpRequestMethod.GET, HttpResponseStatus.SERVICE_UNAVAILABLE);
    }

    @Test
    void testHead() throws InterruptedException, IOException {
        runRequestMethod(service.head(), StandardHttpRequestMethod.HEAD, HttpResponseStatus.OK);
    }

    @Test
    void testHeadNoContent() throws InterruptedException, IOException {
        runRequestMethod(service.head(), StandardHttpRequestMethod.HEAD, HttpResponseStatus.NO_CONTENT);
    }

    @Test
    void testHeadNotFound() throws InterruptedException, IOException {
        runRequestMethod(service.head(), StandardHttpRequestMethod.HEAD, HttpResponseStatus.NOT_FOUND);
    }

    @Test
    void testPatch() throws InterruptedException, IOException {
        runRequestMethodRequestBody(service.patch(), StandardHttpRequestMethod.PATCH, HttpResponseStatus.OK);
    }

    @Test
    void testPatchBadRequest() throws InterruptedException, IOException {
        runRequestMethodRequestBody(service.patch(), StandardHttpRequestMethod.PATCH, HttpResponseStatus.BAD_REQUEST);
    }

    @Test
    void testPost() throws InterruptedException, IOException {
        runRequestMethodRequestBody(service.post(), StandardHttpRequestMethod.POST, HttpResponseStatus.CREATED);
    }

    @Test
    void testPostUnauthorized() throws InterruptedException, IOException {
        runRequestMethodRequestBody(service.post(), StandardHttpRequestMethod.POST, HttpResponseStatus.UNAUTHORIZED);
    }

    @Test
    void testPut() throws InterruptedException, IOException {
        runRequestMethodRequestBody(service.put(), StandardHttpRequestMethod.PUT, HttpResponseStatus.OK);
    }

    @Test
    void testPutForbidden() throws InterruptedException, IOException {
        runRequestMethodRequestBody(service.put(), StandardHttpRequestMethod.PUT, HttpResponseStatus.FORBIDDEN);
    }

    @ParameterizedTest
    @EnumSource(value = StandardHttpRequestMethod.class, names = {"DELETE", "GET"})
    void testHttpRequestMethod(final StandardHttpRequestMethod httpRequestMethod) throws InterruptedException, IOException {
        runRequestMethod(service.method(httpRequestMethod), httpRequestMethod, HttpResponseStatus.NO_CONTENT);
    }

    @ParameterizedTest
    @EnumSource(value = StandardHttpRequestMethod.class, names = {"DELETE", "GET"})
    void testHttpRequestMethodResponseBody(final StandardHttpRequestMethod httpRequestMethod) throws InterruptedException, IOException {
        final HttpResponseStatus httpResponseStatus = HttpResponseStatus.ACCEPTED;
        enqueueResponseStatusBody(httpResponseStatus);

        final HttpResponseEntity httpResponseEntity = service.method(httpRequestMethod)
                .uri(getRootUri())
                .retrieve();

        assertRecordedRequestResponseStatus(httpResponseEntity, httpRequestMethod, httpResponseStatus);

        try (final InputStream body = httpResponseEntity.body()) {
            final byte[] responseBody = new byte[TEXT_BODY.length];
            final int bytesRead = body.read(responseBody);
            assertEquals(TEXT_BODY.length, bytesRead);
            assertArrayEquals(TEXT_BODY, responseBody);
        }
    }

    @ParameterizedTest
    @EnumSource(value = StandardHttpRequestMethod.class, names = {"PATCH", "POST", "PUT"})
    void testHttpRequestMethodRequestBodyEmpty(final StandardHttpRequestMethod httpRequestMethod) throws InterruptedException, IOException {
        final HttpResponseStatus httpResponseStatus = HttpResponseStatus.ACCEPTED;
        enqueueResponseStatus(httpResponseStatus);

        final InputStream body = new ByteArrayInputStream(EMPTY_BODY);

        try (final HttpResponseEntity httpResponseEntity = service.method(httpRequestMethod)
                .uri(getRootUri())
                .body(body, OptionalLong.empty())
                .retrieve()
        ) {
            assertRecordedRequestResponseStatus(httpResponseEntity, httpRequestMethod, httpResponseStatus);
            assertContentLengthHeaderFound(httpResponseEntity.headers());
        }
    }

    @ParameterizedTest
    @EnumSource(value = StandardHttpRequestMethod.class, names = {"PATCH", "POST", "PUT"})
    void testHttpRequestMethodRequestBody(final StandardHttpRequestMethod httpRequestMethod) throws InterruptedException, IOException {
        final HttpResponseStatus httpResponseStatus = HttpResponseStatus.ACCEPTED;
        enqueueResponseStatus(httpResponseStatus);

        final InputStream body = new ByteArrayInputStream(TEXT_BODY);

        try (final HttpResponseEntity httpResponseEntity = service.method(httpRequestMethod)
                .uri(getRootUri())
                .header(ACCEPT_HEADER, ACCEPT_ANY_TYPE)
                .body(body, OptionalLong.of(TEXT_BODY.length))
                .retrieve()
        ) {

            final HttpEntityHeaders headers = httpResponseEntity.headers();
            assertContentLengthHeaderFound(headers);

            final RecordedRequest recordedRequest = assertRecordedRequestResponseStatus(httpResponseEntity, httpRequestMethod, httpResponseStatus);

            assertEquals(TEXT_BODY.length, recordedRequest.getBodySize());
            final byte[] requestBody = recordedRequest.getBody().readByteArray();
            assertArrayEquals(TEXT_BODY, requestBody);

            final String acceptHeader = recordedRequest.getHeader(ACCEPT_HEADER);
            assertEquals(ACCEPT_ANY_TYPE, acceptHeader);
        }
    }

    private void runRequestMethod(
            final HttpRequestUriSpec httpRequestUriSpec,
            final HttpRequestMethod httpRequestMethod,
            final HttpResponseStatus httpResponseStatus
    ) throws IOException, InterruptedException {
        enqueueResponseStatus(httpResponseStatus);

        try (final HttpResponseEntity httpResponseEntity = httpRequestUriSpec
                .uri(getRootUri())
                .retrieve()
        ) {
            assertRecordedRequestResponseStatus(httpResponseEntity, httpRequestMethod, httpResponseStatus);
            assertContentLengthHeaderFound(httpResponseEntity.headers());
        }
    }


    private void runRequestMethodRequestBody(
            final HttpRequestUriSpec httpRequestUriSpec,
            final HttpRequestMethod httpRequestMethod,
            final HttpResponseStatus httpResponseStatus
    ) throws IOException, InterruptedException {
        enqueueResponseStatus(httpResponseStatus);

        final InputStream body = new ByteArrayInputStream(EMPTY_BODY);

        try (final HttpResponseEntity httpResponseEntity = httpRequestUriSpec
                .uri(getRootUri())
                .body(body, OptionalLong.empty())
                .retrieve()
        ) {
            assertRecordedRequestResponseStatus(httpResponseEntity, httpRequestMethod, httpResponseStatus);
            assertContentLengthHeaderFound(httpResponseEntity.headers());
        }
    }

    private RecordedRequest assertRecordedRequestResponseStatus(
            final HttpResponseEntity httpResponseEntity,
            final HttpRequestMethod httpRequestMethod,
            final HttpResponseStatus httpResponseStatus
    ) throws InterruptedException {
        assertNotNull(httpResponseEntity);

        final RecordedRequest recordedRequest = mockWebServer.takeRequest();
        assertEquals(httpRequestMethod.getMethod(), recordedRequest.getMethod());

        assertEquals(httpResponseStatus.getCode(), httpResponseEntity.statusCode());

        return recordedRequest;
    }

    private void assertContentLengthHeaderFound(final HttpEntityHeaders headers) {
        final Optional<String> contentLengthHeader = headers.getFirstHeader(CONTENT_LENGTH_HEADER);
        assertTrue(contentLengthHeader.isPresent());
        assertEquals(CONTENT_LENGTH_ZERO, contentLengthHeader.get());

        final List<String> contentLengthHeaders = headers.getHeader(CONTENT_LENGTH_HEADER);
        assertFalse(contentLengthHeaders.isEmpty());
        assertEquals(Collections.singletonList(CONTENT_LENGTH_ZERO), contentLengthHeaders);

        final Collection<String> headerNames = headers.getHeaderNames();
        assertTrue(headerNames.contains(CONTENT_LENGTH_HEADER));
    }

    private void enqueueResponseStatus(final HttpResponseStatus httpResponseStatus) {
        mockWebServer.enqueue(new MockResponse().setResponseCode(httpResponseStatus.getCode()));
    }

    private void enqueueResponseStatusBody(final HttpResponseStatus httpResponseStatus) {
        mockWebServer.enqueue(new MockResponse()
                .setResponseCode(httpResponseStatus.getCode())
                .setBody(RESPONSE_BODY)
        );
    }

    private URI getRootUri() {
        return mockWebServer.url(ROOT_PATH).newBuilder().host(LOCALHOST).build().uri();
    }
}
