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

import jakarta.servlet.ServletContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import jakarta.servlet.http.HttpServletRequest;
import java.net.URI;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class RequestUriBuilderTest {
    private static final String HOST_HEADER = "Host";

    private static final String SCHEME = "https";

    private static final String HOST = "localhost.local";

    private static final String FRAGMENT = "section";

    private static final int PORT = 443;

    private static final String CONTEXT_PATH = "/context-path";

    private static final String EMPTY = "";

    private static final String ALLOWED_CONTEXT_PATHS = "allowedContextPaths";

    @Mock
    private HttpServletRequest httpServletRequest;

    @Mock
    private ServletContext servletContext;

    @Test
    public void testFromHttpServletRequestBuild() {
        when(httpServletRequest.getServletContext()).thenReturn(servletContext);
        when(httpServletRequest.getServerPort()).thenReturn(PORT);
        when(httpServletRequest.getScheme()).thenReturn(SCHEME);
        lenient().when(httpServletRequest.getHeader(eq(HOST_HEADER))).thenReturn(HOST);

        final RequestUriBuilder builder = RequestUriBuilder.fromHttpServletRequest(httpServletRequest);
        final URI uri = builder.build();

        assertNotNull(uri);
        assertEquals(SCHEME, uri.getScheme());
        assertEquals(HOST, uri.getHost());
        assertEquals(PORT, uri.getPort());
        assertEquals(EMPTY, uri.getPath());
    }

    @Test
    public void testFromHttpServletRequestPathBuild() {
        when(httpServletRequest.getServletContext()).thenReturn(servletContext);
        when(httpServletRequest.getServerPort()).thenReturn(PORT);
        when(httpServletRequest.getScheme()).thenReturn(SCHEME);
        lenient().when(httpServletRequest.getHeader(eq(HOST_HEADER))).thenReturn(HOST);

        final RequestUriBuilder builder = RequestUriBuilder.fromHttpServletRequest(httpServletRequest);
        builder.fragment(FRAGMENT).path(CONTEXT_PATH);
        final URI uri = builder.build();

        assertNotNull(uri);
        assertEquals(SCHEME, uri.getScheme());
        assertEquals(HOST, uri.getHost());
        assertEquals(PORT, uri.getPort());
        assertEquals(CONTEXT_PATH, uri.getPath());
        assertEquals(FRAGMENT, uri.getFragment());
    }

    @Test
    public void testFromHttpServletRequestProxyHeadersBuild() {
        when(httpServletRequest.getServletContext()).thenReturn(servletContext);
        when(servletContext.getInitParameter(eq(ALLOWED_CONTEXT_PATHS))).thenReturn(CONTEXT_PATH);
        when(httpServletRequest.getHeader(eq(ProxyHeader.PROXY_SCHEME.getHeader()))).thenReturn(SCHEME);
        when(httpServletRequest.getHeader(eq(ProxyHeader.PROXY_HOST.getHeader()))).thenReturn(HOST);
        when(httpServletRequest.getHeader(eq(ProxyHeader.PROXY_PORT.getHeader()))).thenReturn(Integer.toString(PORT));
        when(httpServletRequest.getHeader(eq(ProxyHeader.PROXY_CONTEXT_PATH.getHeader()))).thenReturn(CONTEXT_PATH);

        final RequestUriBuilder builder = RequestUriBuilder.fromHttpServletRequest(httpServletRequest);
        final URI uri = builder.build();

        assertNotNull(uri);
        assertEquals(SCHEME, uri.getScheme());
        assertEquals(HOST, uri.getHost());
        assertEquals(PORT, uri.getPort());
        assertEquals(CONTEXT_PATH, uri.getPath());
    }
}
