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
package org.apache.nifi.web.util;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.servlet.http.HttpServletRequest;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;

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

    private static final int PORT = 443;

    private static final String CONTEXT_PATH = "/context-path";

    @Mock
    private HttpServletRequest httpServletRequest;

    @Test
    public void testFromHttpServletRequestBuild() {
        when(httpServletRequest.getServerPort()).thenReturn(PORT);
        when(httpServletRequest.getScheme()).thenReturn(SCHEME);
        lenient().when(httpServletRequest.getHeader(eq(HOST_HEADER))).thenReturn(HOST);

        final RequestUriBuilder builder = RequestUriBuilder.fromHttpServletRequest(httpServletRequest, Collections.emptyList());
        final URI uri = builder.build();

        assertNotNull(uri);
        assertEquals(SCHEME, uri.getScheme());
        assertEquals(HOST, uri.getHost());
        assertEquals(PORT, uri.getPort());
        assertEquals(StringUtils.EMPTY, uri.getPath());
    }

    @Test
    public void testFromHttpServletRequestPathBuild() {
        when(httpServletRequest.getServerPort()).thenReturn(PORT);
        when(httpServletRequest.getScheme()).thenReturn(SCHEME);
        lenient().when(httpServletRequest.getHeader(eq(HOST_HEADER))).thenReturn(HOST);

        final RequestUriBuilder builder = RequestUriBuilder.fromHttpServletRequest(httpServletRequest, Collections.emptyList());
        builder.path(CONTEXT_PATH);
        final URI uri = builder.build();

        assertNotNull(uri);
        assertEquals(SCHEME, uri.getScheme());
        assertEquals(HOST, uri.getHost());
        assertEquals(PORT, uri.getPort());
        assertEquals(CONTEXT_PATH, uri.getPath());
    }

    @Test
    public void testFromHttpServletRequestProxyHeadersBuild() {
        when(httpServletRequest.getHeader(eq(WebUtils.PROXY_SCHEME_HTTP_HEADER))).thenReturn(SCHEME);
        when(httpServletRequest.getHeader(eq(WebUtils.PROXY_HOST_HTTP_HEADER))).thenReturn(HOST);
        when(httpServletRequest.getHeader(eq(WebUtils.PROXY_PORT_HTTP_HEADER))).thenReturn(Integer.toString(PORT));
        when(httpServletRequest.getHeader(eq(WebUtils.PROXY_CONTEXT_PATH_HTTP_HEADER))).thenReturn(CONTEXT_PATH);

        final RequestUriBuilder builder = RequestUriBuilder.fromHttpServletRequest(httpServletRequest, Arrays.asList(CONTEXT_PATH));
        final URI uri = builder.build();

        assertNotNull(uri);
        assertEquals(SCHEME, uri.getScheme());
        assertEquals(HOST, uri.getHost());
        assertEquals(PORT, uri.getPort());
        assertEquals(CONTEXT_PATH, uri.getPath());
    }
}
