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

import org.apache.nifi.web.client.api.HttpEntityHeaders;
import org.apache.nifi.web.client.api.HttpRequestBodySpec;
import org.apache.nifi.web.client.api.HttpRequestHeadersSpec;
import org.apache.nifi.web.client.api.HttpRequestMethod;
import org.apache.nifi.web.client.api.HttpRequestUriSpec;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.StandardHttpRequestMethod;
import org.apache.nifi.web.client.api.WebClientService;
import org.apache.nifi.web.client.api.WebClientServiceException;
import org.apache.nifi.web.client.proxy.ProxyContext;
import org.apache.nifi.web.client.redirect.RedirectHandling;
import org.apache.nifi.web.client.ssl.SSLContextProvider;
import org.apache.nifi.web.client.ssl.StandardSSLContextProvider;
import org.apache.nifi.web.client.ssl.TlsContext;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.Authenticator;
import java.net.InetSocketAddress;
import java.net.PasswordAuthentication;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.SocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.Flow;

import javax.net.ssl.SSLContext;

/**
 * Standard implementation of Web Client Service using Java HttpClient
 */
public class StandardWebClientService implements WebClientService, Closeable {
    private static final byte[] EMPTY_BYTES = new byte[0];

    private static final SSLContextProvider sslContextProvider = new StandardSSLContextProvider();

    private HttpClient httpClient;

    private Duration connectTimeout;

    private Duration readTimeout;

    private Duration writeTimeout;

    private RedirectHandling redirectHandling;

    private ProxyContext proxyContext;

    private TlsContext tlsContext;

    /**
     * Standard Web Client Service constructor creates a Java HttpClient using default settings
     */
    public StandardWebClientService() {
        httpClient = HttpClient.newBuilder().build();
    }

    /**
     * Set timeout for initial socket connection
     *
     * @param connectTimeout Connect Timeout
     */
    public void setConnectTimeout(final Duration connectTimeout) {
        Objects.requireNonNull(connectTimeout, "Connect Timeout required");
        this.connectTimeout = connectTimeout;
        this.httpClient = buildHttpClient();
    }

    /**
     * Set timeout for reading responses from socket connection takes precedence over write timeout
     *
     * @param readTimeout Read Timeout
     */
    public void setReadTimeout(final Duration readTimeout) {
        Objects.requireNonNull(readTimeout, "Read Timeout required");
        this.readTimeout = readTimeout;
    }

    /**
     * Set timeout for writing requests to socket connection when read timeout is not specified
     *
     * @param writeTimeout Write Timeout
     */
    public void setWriteTimeout(final Duration writeTimeout) {
        Objects.requireNonNull(writeTimeout, "Write Timeout required");
        this.writeTimeout = writeTimeout;
    }

    /**
     * Set Proxy Context configuration for socket communication
     *
     * @param proxyContext Proxy Context configuration
     */
    public void setProxyContext(final ProxyContext proxyContext) {
        Objects.requireNonNull(proxyContext, "Proxy Context required");
        Objects.requireNonNull(proxyContext.getProxy(), "Proxy required");
        this.proxyContext = proxyContext;
        this.httpClient = buildHttpClient();
    }

    /**
     * Set Redirect Handling strategy
     *
     * @param redirectHandling Redirect Handling strategy
     */
    public void setRedirectHandling(final RedirectHandling redirectHandling) {
        Objects.requireNonNull(redirectHandling, "Redirect Handling required");
        this.redirectHandling = redirectHandling;
        this.httpClient = buildHttpClient();
    }

    /**
     * Set TLS Context overrides system default TLS settings for HTTPS communication
     *
     * @param tlsContext TLS Context
     */
    public void setTlsContext(final TlsContext tlsContext) {
        Objects.requireNonNull(tlsContext, "TLS Context required");
        Objects.requireNonNull(tlsContext.getTrustManager(), "Trust Manager required");
        this.tlsContext = tlsContext;
        this.httpClient = buildHttpClient();
    }

    /**
     * Create HTTP Request builder starting with specified HTTP Request Method
     *
     * @param httpRequestMethod HTTP Request Method required
     * @return HTTP Request URI Specification builder
     */
    @Override
    public HttpRequestUriSpec method(final HttpRequestMethod httpRequestMethod) {
        Objects.requireNonNull(httpRequestMethod, "HTTP Request Method required");
        return new StandardHttpRequestUriSpec(httpRequestMethod);
    }

    /**
     * Create HTTP Request builder starting with HTTP DELETE
     *
     * @return HTTP Request URI Specification builder
     */
    @Override
    public HttpRequestUriSpec delete() {
        return method(StandardHttpRequestMethod.DELETE);
    }

    /**
     * Create HTTP Request builder starting with HTTP GET
     *
     * @return HTTP Request URI Specification builder
     */
    @Override
    public HttpRequestUriSpec get() {
        return method(StandardHttpRequestMethod.GET);
    }

    /**
     * Create HTTP Request builder starting with HTTP HEAD
     *
     * @return HTTP Request URI Specification builder
     */
    @Override
    public HttpRequestUriSpec head() {
        return method(StandardHttpRequestMethod.HEAD);
    }

    /**
     * Create HTTP Request builder starting with HTTP PATCH
     *
     * @return HTTP Request URI Specification builder
     */
    @Override
    public HttpRequestUriSpec patch() {
        return method(StandardHttpRequestMethod.PATCH);
    }

    /**
     * Create HTTP Request builder starting with HTTP POST
     *
     * @return HTTP Request URI Specification builder
     */
    @Override
    public HttpRequestUriSpec post() {
        return method(StandardHttpRequestMethod.POST);
    }

    /**
     * Create HTTP Request builder starting with HTTP PUT
     *
     * @return HTTP Request URI Specification builder
     */
    @Override
    public HttpRequestUriSpec put() {
        return method(StandardHttpRequestMethod.PUT);
    }

    /**
     * Close configured HttpClient and shutdown executor resources
     */
    @Override
    public void close() {
        httpClient.close();
    }

    private HttpClient buildHttpClient() {
        final HttpClient.Builder builder = HttpClient.newBuilder();

        if (connectTimeout != null) {
            builder.connectTimeout(connectTimeout);
        }
        if (tlsContext != null) {
            final SSLContext sslContext = sslContextProvider.getSslContext(tlsContext);
            builder.sslContext(sslContext);
        }
        if (RedirectHandling.FOLLOWED == redirectHandling) {
            builder.followRedirects(HttpClient.Redirect.ALWAYS);
        } else if (RedirectHandling.IGNORED == redirectHandling) {
            builder.followRedirects(HttpClient.Redirect.NEVER);
        }
        if (proxyContext != null) {
            final Proxy proxy = proxyContext.getProxy();
            final SocketAddress proxyAddress = proxy.address();
            if (proxyAddress instanceof InetSocketAddress proxySocketAddress) {
                final ProxySelector proxySelector = ProxySelector.of(proxySocketAddress);
                builder.proxy(proxySelector);

                final Optional<String> proxyUsername = proxyContext.getUsername();
                if (proxyUsername.isPresent()) {
                    final ProxyPasswordAuthenticator passwordAuthenticator = getProxyPasswordAuthenticator(proxyUsername.get());
                    builder.authenticator(passwordAuthenticator);
                }
            }
        }

        return builder.build();
    }

    private ProxyPasswordAuthenticator getProxyPasswordAuthenticator(final String proxyUsername) {
        final char[] password;
        final Optional<String> proxyPassword = proxyContext.getPassword();
        if (proxyPassword.isPresent()) {
            password = proxyPassword.get().toCharArray();
        } else {
            throw new IllegalArgumentException("Proxy Password not configured");
        }
        final PasswordAuthentication passwordAuthentication = new PasswordAuthentication(proxyUsername, password);
        return new ProxyPasswordAuthenticator(passwordAuthentication);
    }

    static class ProxyPasswordAuthenticator extends Authenticator {
        private final PasswordAuthentication passwordAuthentication;

        ProxyPasswordAuthenticator(final PasswordAuthentication passwordAuthentication) {
            this.passwordAuthentication = passwordAuthentication;
        }

        @Override
        protected PasswordAuthentication getPasswordAuthentication() {
            return passwordAuthentication;
        }
    }

    class StandardHttpRequestUriSpec implements HttpRequestUriSpec {
        private final HttpRequestMethod httpRequestMethod;

        StandardHttpRequestUriSpec(final HttpRequestMethod httpRequestMethod) {
            this.httpRequestMethod = httpRequestMethod;
        }

        @Override
        public HttpRequestBodySpec uri(final URI uri) {
            Objects.requireNonNull(uri, "URI required");
            return new StandardHttpRequestBodySpec(httpRequestMethod, uri);
        }
    }

    class StandardHttpRequestBodySpec implements HttpRequestBodySpec {
        private static final long UNKNOWN_CONTENT_LENGTH = -1;

        private final HttpRequestMethod httpRequestMethod;

        private final URI uri;

        private final HttpRequest.Builder requestBuilder;

        private long contentLength = UNKNOWN_CONTENT_LENGTH;

        private InputStream body;

        StandardHttpRequestBodySpec(final HttpRequestMethod httpRequestMethod, final URI uri) {
            this.httpRequestMethod = httpRequestMethod;
            this.uri = uri;
            this.requestBuilder = HttpRequest.newBuilder();
        }

        @Override
        public HttpRequestHeadersSpec body(final InputStream body, final OptionalLong contentLength) {
            this.body = Objects.requireNonNull(body, "Body required");
            this.contentLength = Objects.requireNonNull(contentLength, "Content Length required").orElse(UNKNOWN_CONTENT_LENGTH);
            return this;
        }

        @Override
        public HttpRequestHeadersSpec body(final String body) {
            final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            return body(new ByteArrayInputStream(bytes), OptionalLong.of(bytes.length));
        }

        @Override
        public HttpRequestBodySpec header(final String headerName, final String headerValue) {
            Objects.requireNonNull(headerName, "Header Name required");
            Objects.requireNonNull(headerValue, "Header Value required");
            requestBuilder.header(headerName, headerValue);
            return this;
        }

        @Override
        public HttpResponseEntity retrieve() {
            final HttpRequest request = getRequest();
            final HttpResponse<InputStream> response = getResponse(request);

            final int code = response.statusCode();

            final HttpHeaders responseHeaders = response.headers();
            final HttpEntityHeaders headers = new StandardHttpEntityHeaders(responseHeaders.map());

            final InputStream responseBody = response.body();
            final InputStream body = responseBody == null ? new ByteArrayInputStream(EMPTY_BYTES) : responseBody;

            return new StandardHttpResponseEntity(code, headers, body, response.uri());
        }

        private HttpResponse<InputStream> getResponse(final HttpRequest request) {
            try {
                return httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());
            } catch (final IOException e) {
                throw new WebClientServiceException("Request execution failed", e, uri, httpRequestMethod);
            } catch (final InterruptedException e) {
                throw new WebClientServiceException("Request execution interrupted", e, uri, httpRequestMethod);
            }
        }

        private HttpRequest getRequest() {
            if (writeTimeout != null) {
                requestBuilder.timeout(writeTimeout);
            }
            // Prefer Read Timeout over Write Timeout when specified
            if (readTimeout != null) {
                requestBuilder.timeout(readTimeout);
            }

            final HttpRequest.BodyPublisher bodyPublisher = getBodyPublisher();

            return requestBuilder.method(httpRequestMethod.getMethod(), bodyPublisher)
                    .uri(uri)
                    .build();
        }

        private HttpRequest.BodyPublisher getBodyPublisher() {
            final HttpRequest.BodyPublisher bodyPublisher;

            if (body == null) {
                bodyPublisher = HttpRequest.BodyPublishers.noBody();
            } else {
                final HttpRequest.BodyPublisher inputStreamPublisher = HttpRequest.BodyPublishers.ofInputStream(() -> body);
                bodyPublisher = new HttpRequest.BodyPublisher() {
                    @Override
                    public long contentLength() {
                        return contentLength;
                    }

                    @Override
                    public void subscribe(Flow.Subscriber<? super ByteBuffer> subscriber) {
                        inputStreamPublisher.subscribe(subscriber);
                    }
                };
            }

            return bodyPublisher;
        }
    }
}
