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
package org.apache.nifi.web.servlet.shared;

import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.URI;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StandardRequestUriProviderTest {
    private static final String SCHEME = "http";

    private static final String HTTPS_SCHEME = "https";

    private static final String SERVER = "localhost";

    private static final String HOST = "localhost.local";

    private static final int PORT = 443;

    private static final int APPLICATION_PORT = 8443;

    private static final String HOST_WITH_PORT_FORMAT = "%s:%d";

    private static final String HOST_WITH_PORT = HOST_WITH_PORT_FORMAT.formatted(HOST, PORT);

    private static final String HOST_INVALID = "0000:0000:0000:0000:0000:0000:0000:0000";

    private static final String CONTEXT_PATH = "/context-path";

    private static final String ROOT_PATH = "/";

    private static final String EMPTY = "";

    @Mock
    private HttpServletRequest request;

    @BeforeEach
    void setRequest() {
        when(request.getScheme()).thenReturn(SCHEME);
        when(request.getServerName()).thenReturn(SERVER);
        when(request.getServerPort()).thenReturn(PORT);
    }

    @Test
    void testGetRequestUri() {
        final StandardRequestUriProvider provider = new StandardRequestUriProvider(Collections.emptyList());

        final URI requestUri = provider.getRequestUri(request);

        assertSchemeMatched(requestUri, SCHEME);
    }

    @Test
    void testGetRequestUriProxyScheme() {
        final StandardRequestUriProvider provider = new StandardRequestUriProvider(Collections.emptyList());

        when(request.getHeader(eq(ProxyHeader.PROXY_SCHEME.getHeader()))).thenReturn(HTTPS_SCHEME);
        final URI requestUri = provider.getRequestUri(request);

        assertSchemeMatched(requestUri, HTTPS_SCHEME);
    }

    @Test
    void testGetRequestUriForwardedProto() {
        final StandardRequestUriProvider provider = new StandardRequestUriProvider(Collections.emptyList());

        when(request.getHeader(eq(ProxyHeader.PROXY_SCHEME.getHeader()))).thenReturn(null);
        when(request.getHeader(eq(ProxyHeader.FORWARDED_PROTO.getHeader()))).thenReturn(HTTPS_SCHEME);
        final URI requestUri = provider.getRequestUri(request);

        assertSchemeMatched(requestUri, HTTPS_SCHEME);
    }

    @Test
    void testGetRequestUriProxySchemeInvalid() {
        final StandardRequestUriProvider provider = new StandardRequestUriProvider(Collections.emptyList());

        when(request.getHeader(eq(ProxyHeader.PROXY_SCHEME.getHeader()))).thenReturn(String.class.getSimpleName());
        final URI requestUri = provider.getRequestUri(request);

        assertSchemeMatched(requestUri, SCHEME);
    }

    @Test
    void testGetRequestUriProxyHost() {
        final StandardRequestUriProvider provider = new StandardRequestUriProvider(Collections.emptyList());

        when(request.getHeader(anyString())).thenReturn(null);
        when(request.getHeader(eq(ProxyHeader.PROXY_HOST.getHeader()))).thenReturn(HOST);
        final URI requestUri = provider.getRequestUri(request);

        assertHostMatched(requestUri, HOST);
    }

    @Test
    void testGetRequestUriForwardedHost() {
        final StandardRequestUriProvider provider = new StandardRequestUriProvider(Collections.emptyList());

        when(request.getHeader(anyString())).thenReturn(null);
        when(request.getHeader(eq(ProxyHeader.FORWARDED_HOST.getHeader()))).thenReturn(HOST);
        final URI requestUri = provider.getRequestUri(request);

        assertHostMatched(requestUri, HOST);
    }

    @Test
    void testGetRequestUriHost() {
        final StandardRequestUriProvider provider = new StandardRequestUriProvider(Collections.emptyList());

        when(request.getHeader(anyString())).thenReturn(null);
        when(request.getHeader(eq(ProxyHeader.HOST.getHeader()))).thenReturn(HOST);
        final URI requestUri = provider.getRequestUri(request);

        assertHostMatched(requestUri, HOST);
    }

    @Test
    void testGetRequestUriHostWithPort() {
        final StandardRequestUriProvider provider = new StandardRequestUriProvider(Collections.emptyList());

        when(request.getHeader(anyString())).thenReturn(null);
        when(request.getHeader(eq(ProxyHeader.HOST.getHeader()))).thenReturn(HOST_WITH_PORT);
        final URI requestUri = provider.getRequestUri(request);

        assertHostMatched(requestUri, HOST);
    }

    @Test
    void testGetRequestUriHostInvalid() {
        final StandardRequestUriProvider provider = new StandardRequestUriProvider(Collections.emptyList());

        when(request.getHeader(anyString())).thenReturn(null);
        when(request.getHeader(eq(ProxyHeader.HOST.getHeader()))).thenReturn(HOST_INVALID);
        final URI requestUri = provider.getRequestUri(request);

        assertHostMatched(requestUri, SERVER);
    }

    @Test
    void testGetRequestUriProxyHostPort() {
        final StandardRequestUriProvider provider = new StandardRequestUriProvider(Collections.emptyList());

        when(request.getHeader(anyString())).thenReturn(null);

        final String proxyHost = HOST_WITH_PORT_FORMAT.formatted(HOST, APPLICATION_PORT);

        when(request.getHeader(eq(ProxyHeader.PROXY_HOST.getHeader()))).thenReturn(proxyHost);
        final URI requestUri = provider.getRequestUri(request);

        assertHostPortMatched(requestUri, HOST, APPLICATION_PORT);
    }

    @Test
    void testGetRequestUriForwardedHostPort() {
        final StandardRequestUriProvider provider = new StandardRequestUriProvider(Collections.emptyList());

        when(request.getHeader(anyString())).thenReturn(null);

        final String proxyHost = HOST_WITH_PORT_FORMAT.formatted(HOST, APPLICATION_PORT);

        when(request.getHeader(eq(ProxyHeader.FORWARDED_HOST.getHeader()))).thenReturn(proxyHost);
        final URI requestUri = provider.getRequestUri(request);

        assertHostPortMatched(requestUri, HOST, APPLICATION_PORT);
    }

    @Test
    void testGetRequestUriForwardedHostPortInvalid() {
        final StandardRequestUriProvider provider = new StandardRequestUriProvider(Collections.emptyList());

        when(request.getHeader(anyString())).thenReturn(null);

        final String proxyHost = HOST_WITH_PORT_FORMAT.formatted(HOST, Integer.MAX_VALUE);

        when(request.getHeader(eq(ProxyHeader.FORWARDED_HOST.getHeader()))).thenReturn(proxyHost);
        final URI requestUri = provider.getRequestUri(request);

        assertHostPortMatched(requestUri, SERVER, PORT);
    }

    @Test
    void testGetRequestUriProxyPort() {
        final StandardRequestUriProvider provider = new StandardRequestUriProvider(Collections.emptyList());

        when(request.getHeader(anyString())).thenReturn(null);
        when(request.getHeader(eq(ProxyHeader.PROXY_PORT.getHeader()))).thenReturn(Integer.toString(APPLICATION_PORT));
        final URI requestUri = provider.getRequestUri(request);

        assertHostPortMatched(requestUri, SERVER, APPLICATION_PORT);
    }

    @Test
    void testGetRequestUriForwardedPort() {
        final StandardRequestUriProvider provider = new StandardRequestUriProvider(Collections.emptyList());

        when(request.getHeader(anyString())).thenReturn(null);
        when(request.getHeader(eq(ProxyHeader.FORWARDED_PORT.getHeader()))).thenReturn(Integer.toString(APPLICATION_PORT));
        final URI requestUri = provider.getRequestUri(request);

        assertHostPortMatched(requestUri, SERVER, APPLICATION_PORT);
    }

    @Test
    void testGetRequestUriProxyPortInvalid() {
        final StandardRequestUriProvider provider = new StandardRequestUriProvider(Collections.emptyList());

        when(request.getHeader(anyString())).thenReturn(null);
        when(request.getHeader(eq(ProxyHeader.PROXY_PORT.getHeader()))).thenReturn(Integer.toString(Integer.MAX_VALUE));
        final URI requestUri = provider.getRequestUri(request);

        assertHostPortMatched(requestUri, SERVER, PORT);
    }

    @Test
    void testGetRequestUriProxyPortInvalidString() {
        final StandardRequestUriProvider provider = new StandardRequestUriProvider(Collections.emptyList());

        when(request.getHeader(anyString())).thenReturn(null);
        when(request.getHeader(eq(ProxyHeader.PROXY_PORT.getHeader()))).thenReturn(String.class.getSimpleName());
        final URI requestUri = provider.getRequestUri(request);

        assertHostPortMatched(requestUri, SERVER, PORT);
    }

    @Test
    void testGetRequestUriRootPath() {
        final StandardRequestUriProvider provider = new StandardRequestUriProvider(Collections.emptyList());

        when(request.getHeader(anyString())).thenReturn(null);
        when(request.getHeader(eq(ProxyHeader.PROXY_CONTEXT_PATH.getHeader()))).thenReturn(ROOT_PATH);
        final URI requestUri = provider.getRequestUri(request);

        assertPathMatched(requestUri, ROOT_PATH);
    }

    @Test
    void testGetRequestUriContextPathNotAllowed() {
        final StandardRequestUriProvider provider = new StandardRequestUriProvider(Collections.emptyList());

        when(request.getHeader(anyString())).thenReturn(null);
        when(request.getHeader(eq(ProxyHeader.PROXY_CONTEXT_PATH.getHeader()))).thenReturn(CONTEXT_PATH);
        assertThrows(IllegalArgumentException.class, () -> provider.getRequestUri(request));
    }

    @Test
    void testGetRequestUriProxyContextPath() {
        final StandardRequestUriProvider provider = new StandardRequestUriProvider(List.of(CONTEXT_PATH));

        when(request.getHeader(anyString())).thenReturn(null);
        when(request.getHeader(eq(ProxyHeader.PROXY_CONTEXT_PATH.getHeader()))).thenReturn(CONTEXT_PATH);
        final URI requestUri = provider.getRequestUri(request);

        assertPathMatched(requestUri, CONTEXT_PATH);
    }

    @Test
    void testGetRequestUriProxyContextPathBlank() {
        final StandardRequestUriProvider provider = new StandardRequestUriProvider(List.of(CONTEXT_PATH));

        when(request.getHeader(anyString())).thenReturn(null);
        when(request.getHeader(eq(ProxyHeader.PROXY_CONTEXT_PATH.getHeader()))).thenReturn(EMPTY);
        final URI requestUri = provider.getRequestUri(request);

        assertPathMatched(requestUri, EMPTY);
    }

    @Test
    void testGetRequestUriForwardedContext() {
        final StandardRequestUriProvider provider = new StandardRequestUriProvider(List.of(CONTEXT_PATH));

        when(request.getHeader(anyString())).thenReturn(null);
        when(request.getHeader(eq(ProxyHeader.FORWARDED_CONTEXT.getHeader()))).thenReturn(CONTEXT_PATH);
        final URI requestUri = provider.getRequestUri(request);

        assertPathMatched(requestUri, CONTEXT_PATH);
    }

    @Test
    void testGetRequestUriForwardedPrefix() {
        final StandardRequestUriProvider provider = new StandardRequestUriProvider(List.of(CONTEXT_PATH));

        when(request.getHeader(anyString())).thenReturn(null);
        when(request.getHeader(eq(ProxyHeader.FORWARDED_PREFIX.getHeader()))).thenReturn(CONTEXT_PATH);
        final URI requestUri = provider.getRequestUri(request);

        assertPathMatched(requestUri, CONTEXT_PATH);
    }

    private void assertSchemeMatched(final URI requestUri, final String scheme) {
        assertNotNull(requestUri);
        assertEquals(scheme, requestUri.getScheme());
        assertEquals(SERVER, requestUri.getHost());
        assertEquals(PORT, requestUri.getPort());
        assertEquals(EMPTY, requestUri.getPath());
    }

    private void assertHostMatched(final URI requestUri, final String host) {
        assertNotNull(requestUri);
        assertEquals(SCHEME, requestUri.getScheme());
        assertEquals(host, requestUri.getHost());
        assertEquals(PORT, requestUri.getPort());
        assertEquals(EMPTY, requestUri.getPath());
    }

    private void assertHostPortMatched(final URI requestUri, final String host, final int port) {
        assertNotNull(requestUri);
        assertEquals(SCHEME, requestUri.getScheme());
        assertEquals(host, requestUri.getHost());
        assertEquals(port, requestUri.getPort());
        assertEquals(EMPTY, requestUri.getPath());
    }

    private void assertPathMatched(final URI requestUri, final String path) {
        assertNotNull(requestUri);
        assertEquals(SCHEME, requestUri.getScheme());
        assertEquals(SERVER, requestUri.getHost());
        assertEquals(PORT, requestUri.getPort());
        assertEquals(path, requestUri.getPath());
    }
}
