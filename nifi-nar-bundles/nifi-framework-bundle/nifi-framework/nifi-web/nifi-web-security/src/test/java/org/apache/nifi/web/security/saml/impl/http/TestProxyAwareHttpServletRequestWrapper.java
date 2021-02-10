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
package org.apache.nifi.web.security.saml.impl.http;

import org.apache.nifi.web.util.WebUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestProxyAwareHttpServletRequestWrapper {

    @Mock
    private HttpServletRequest request;

    @Test
    public void testWhenNotProxied() {
        when(request.getScheme()).thenReturn("https");
        when(request.getServerName()).thenReturn("localhost");
        when(request.getServerPort()).thenReturn(8443);
        when(request.getContextPath()).thenReturn("/nifi-api");
        when(request.getServletPath()).thenReturn("/access/saml/metadata");
        when(request.getHeader(any())).thenReturn(null);

        final HttpServletRequestWrapper requestWrapper = new ProxyAwareHttpServletRequestWrapper(request);
        assertEquals("https://localhost:8443/nifi-api/access/saml/metadata", requestWrapper.getRequestURL().toString());
    }

    @Test
    public void testWhenProxied() {
        when(request.getHeader(eq(WebUtils.PROXY_SCHEME_HTTP_HEADER))).thenReturn("https");
        when(request.getHeader(eq(WebUtils.PROXY_HOST_HTTP_HEADER))).thenReturn("proxy-host");
        when(request.getHeader(eq(WebUtils.PROXY_PORT_HTTP_HEADER))).thenReturn("443");
        when(request.getHeader(eq(WebUtils.PROXY_CONTEXT_PATH_HTTP_HEADER))).thenReturn("/proxy-context");
        when(request.getContextPath()).thenReturn("/nifi-api");
        when(request.getServletPath()).thenReturn("/access/saml/metadata");

        final HttpServletRequestWrapper requestWrapper = new ProxyAwareHttpServletRequestWrapper(request);
        assertEquals("https://proxy-host:443/proxy-context/nifi-api/access/saml/metadata", requestWrapper.getRequestURL().toString());
    }

    @Test
    public void testWhenProxiedWithEmptyProxyContextPath() {
        when(request.getHeader(eq(WebUtils.PROXY_SCHEME_HTTP_HEADER))).thenReturn("https");
        when(request.getHeader(eq(WebUtils.PROXY_HOST_HTTP_HEADER))).thenReturn("proxy-host");
        when(request.getHeader(eq(WebUtils.PROXY_PORT_HTTP_HEADER))).thenReturn("443");
        when(request.getHeader(eq(WebUtils.PROXY_CONTEXT_PATH_HTTP_HEADER))).thenReturn("/");
        when(request.getContextPath()).thenReturn("/nifi-api");
        when(request.getServletPath()).thenReturn("/access/saml/metadata");

        final HttpServletRequestWrapper requestWrapper = new ProxyAwareHttpServletRequestWrapper(request);
        assertEquals("https://proxy-host:443/nifi-api/access/saml/metadata", requestWrapper.getRequestURL().toString());
    }
}
