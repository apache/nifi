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

import okhttp3.Call;
import okhttp3.Credentials;
import okhttp3.Headers;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;

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
import org.apache.nifi.web.client.ssl.SSLSocketFactoryProvider;
import org.apache.nifi.web.client.ssl.StandardSSLSocketFactoryProvider;
import org.apache.nifi.web.client.ssl.TlsContext;

import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.X509TrustManager;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Proxy;
import java.net.URI;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Standard implementation of Web Client Service using OkHttp
 */
public class StandardWebClientService implements WebClientService {
    private static final byte[] EMPTY_BYTES = new byte[0];

    private static final SSLSocketFactoryProvider sslSocketFactoryProvider = new StandardSSLSocketFactoryProvider();

    private OkHttpClient okHttpClient;

    /**
     * Standard Web Client Service constructor creates OkHttpClient using default settings
     */
    public StandardWebClientService() {
        okHttpClient = new OkHttpClient.Builder().build();
    }

    /**
     * Set timeout for initial socket connection
     *
     * @param connectTimeout Connect Timeout
     */
    public void setConnectTimeout(final Duration connectTimeout) {
        Objects.requireNonNull(connectTimeout, "Connect Timeout required");
        okHttpClient = okHttpClient.newBuilder().connectTimeout(connectTimeout).build();
    }

    /**
     * Set timeout for reading responses from socket connection
     *
     * @param readTimeout Read Timeout
     */
    public void setReadTimeout(final Duration readTimeout) {
        Objects.requireNonNull(readTimeout, "Read Timeout required");
        okHttpClient = okHttpClient.newBuilder().readTimeout(readTimeout).build();
    }

    /**
     * Set timeout for writing requests to socket connection
     *
     * @param writeTimeout Write Timeout
     */
    public void setWriteTimeout(final Duration writeTimeout) {
        Objects.requireNonNull(writeTimeout, "Write Timeout required");
        okHttpClient = okHttpClient.newBuilder().writeTimeout(writeTimeout).build();
    }

    /**
     * Set Proxy Context configuration for socket communication
     *
     * @param proxyContext Proxy Context configuration
     */
    public void setProxyContext(final ProxyContext proxyContext) {
        Objects.requireNonNull(proxyContext, "Proxy Context required");
        final Proxy proxy = Objects.requireNonNull(proxyContext.getProxy(), "Proxy required");
        okHttpClient = okHttpClient.newBuilder().proxy(proxy).build();

        final Optional<String> proxyUsername = proxyContext.getUsername();
        if (proxyUsername.isPresent()) {
            final String username = proxyUsername.get();
            final String password = proxyContext.getPassword().orElseThrow(() -> new IllegalArgumentException("Proxy password required"));
            final String credentials = Credentials.basic(username, password);
            final BasicProxyAuthenticator proxyAuthenticator = new BasicProxyAuthenticator(credentials);
            okHttpClient = okHttpClient.newBuilder().proxyAuthenticator(proxyAuthenticator).build();
        }
    }

    /**
     * Set Redirect Handling strategy
     *
     * @param redirectHandling Redirect Handling strategy
     */
    public void setRedirectHandling(final RedirectHandling redirectHandling) {
        Objects.requireNonNull(redirectHandling, "Redirect Handling required");
        final boolean followRedirects = RedirectHandling.FOLLOWED == redirectHandling;
        okHttpClient = okHttpClient.newBuilder().followRedirects(followRedirects).followSslRedirects(followRedirects).build();
    }

    /**
     * Set TLS Context overrides system default TLS settings for HTTPS communication
     *
     * @param tlsContext TLS Context
     */
    public void setTlsContext(final TlsContext tlsContext) {
        Objects.requireNonNull(tlsContext, "TLS Context required");
        final X509TrustManager trustManager = Objects.requireNonNull(tlsContext.getTrustManager(), "Trust Manager required");
        final SSLSocketFactory sslSocketFactory = sslSocketFactoryProvider.getSocketFactory(tlsContext);
        okHttpClient = okHttpClient.newBuilder().sslSocketFactory(sslSocketFactory, trustManager).build();
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
    public HttpRequestUriSpec post() {
        return method(StandardHttpRequestMethod.POST);
    }

    /**
     * Create HTTP Request builder starting with HTTP PUT
     *
     * @return HTTP Request URI Specification builder
     */
    public HttpRequestUriSpec put() {
        return method(StandardHttpRequestMethod.PUT);
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

        private final Headers.Builder headersBuilder;

        private long contentLength = UNKNOWN_CONTENT_LENGTH;

        private InputStream body;

        StandardHttpRequestBodySpec(final HttpRequestMethod httpRequestMethod, final URI uri) {
            this.httpRequestMethod = httpRequestMethod;
            this.uri = uri;
            this.headersBuilder = new Headers.Builder();
        }

        @Override
        public HttpRequestHeadersSpec body(final InputStream body, final OptionalLong contentLength) {
            this.body = Objects.requireNonNull(body, "Body required");
            this.contentLength = Objects.requireNonNull(contentLength, "Content Length required").orElse(UNKNOWN_CONTENT_LENGTH);
            return this;
        }

        @Override
        public HttpRequestBodySpec header(final String headerName, final String headerValue) {
            Objects.requireNonNull(headerName, "Header Name required");
            Objects.requireNonNull(headerValue, "Header Value required");
            headersBuilder.add(headerName, headerValue);
            return this;
        }

        @Override
        public HttpResponseEntity retrieve() {
            final Request request = getRequest();
            final Call call = okHttpClient.newCall(request);
            final Response response = execute(call);

            final int code = response.code();
            final Headers responseHeaders = response.headers();
            final HttpEntityHeaders headers = new StandardHttpEntityHeaders(responseHeaders.toMultimap());
            final ResponseBody responseBody = response.body();
            final InputStream body = responseBody == null ? new ByteArrayInputStream(EMPTY_BYTES) : responseBody.byteStream();

            return new StandardHttpResponseEntity(code, headers, body);
        }

        private Response execute(final Call call) {
            try {
                return call.execute();
            } catch (final IOException e) {
                throw new WebClientServiceException("Request execution failed", e, uri, httpRequestMethod);
            }
        }

        private Request getRequest() {
            final HttpUrl url = HttpUrl.get(uri);
            Objects.requireNonNull(url, "HTTP Request URI required");

            final Headers headers = headersBuilder.build();
            final RequestBody requestBody = getRequestBody();

            return new Request.Builder()
                    .method(httpRequestMethod.getMethod(), requestBody)
                    .url(url)
                    .headers(headers)
                    .build();
        }

        private RequestBody getRequestBody() {
            final RequestBody requestBody;

            if (body == null) {
                requestBody = null;
            } else {
                requestBody = new InputStreamRequestBody(body, contentLength);
            }

            return requestBody;
        }
    }
}
