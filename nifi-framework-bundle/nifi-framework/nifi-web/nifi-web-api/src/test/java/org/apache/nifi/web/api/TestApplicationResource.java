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
package org.apache.nifi.web.api;

import jakarta.servlet.ServletContext;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.servlet.shared.ProxyHeader;
import org.glassfish.jersey.uri.internal.JerseyUriBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.core.UriInfo;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestApplicationResource {
    private static final String PROXY_CONTEXT_PATH_PROP = NiFiProperties.WEB_PROXY_CONTEXT_PATH;
    private static final String SCHEME = "https";
    private static final String HOST = "nifi.apache.org";
    private static final int PORT = 8081;
    private static final String BASE_URI = SCHEME + "://" + HOST;
    private static final String ALLOWED_PATH = "/some/context/path";
    private static final String FORWARD_SLASH = "/";
    private static final String CUSTOM_UI_PATH = "/my-custom-ui-1.0.0";
    private static final String ACTUAL_RESOURCE = "actualResource";
    private static final String EXPECTED_URI = BASE_URI + ":" + PORT + ALLOWED_PATH + FORWARD_SLASH + ACTUAL_RESOURCE;
    private static final String MULTIPLE_ALLOWED_PATHS = String.join(",", ALLOWED_PATH, "another/path", "a/third/path");
    private static final String ALLOWED_CONTEXT_PATHS = "allowedContextPaths";

    @Mock
    private HttpServletRequest request;

    @Mock
    private ServletContext servletContext;

    private MockApplicationResource resource;

    @BeforeEach
    public void setUp(@Mock UriInfo uriInfo) throws Exception {
        // this stubbing is lenient because it is unnecessary in some tests
        lenient().when(uriInfo.getBaseUriBuilder()).thenReturn(new JerseyUriBuilder().uri(new URI(BASE_URI + FORWARD_SLASH)));

        when(request.getScheme()).thenReturn(SCHEME);
        when(request.getServerName()).thenReturn(HOST);
        when(request.getServerPort()).thenReturn(PORT);

        when(request.getServletContext()).thenReturn(servletContext);

        resource = new MockApplicationResource();
        resource.setHttpServletRequest(request);
        resource.setUriInfo(uriInfo);
        resource.setProperties(new NiFiProperties());
    }

    @Test
    public void testGenerateUriShouldBlockProxyContextPathHeaderIfNotInAllowList() {
        when(request.getHeader(anyString())).thenAnswer(new RequestAnswer());
        assertThrows(IllegalArgumentException.class, () -> resource.generateResourceUri(ACTUAL_RESOURCE));
    }

    @Test
    public void testGenerateUriShouldAllowProxyContextPathHeaderIfInAllowList() {
        when(request.getHeader(anyString())).thenAnswer(new RequestAnswer());
        setNiFiProperties(Collections.singletonMap(PROXY_CONTEXT_PATH_PROP, ALLOWED_PATH));

        assertEquals(EXPECTED_URI, resource.generateResourceUri(ACTUAL_RESOURCE));
    }

    @Test
    public void testGenerateUriShouldAllowProxyContextPathHeaderIfElementInMultipleAllowList() {
        when(request.getHeader(anyString())).thenAnswer(new RequestAnswer());
        setNiFiProperties(Collections.singletonMap(PROXY_CONTEXT_PATH_PROP, MULTIPLE_ALLOWED_PATHS));

        assertEquals(EXPECTED_URI, resource.generateResourceUri(ACTUAL_RESOURCE));
    }

    @Test
    public void testGenerateUriShouldBlockForwardedContextHeaderIfNotInAllowList() {
        when(request.getHeader(anyString())).thenAnswer(new RequestAnswer(ProxyHeader.FORWARDED_CONTEXT.getHeader()));
        assertThrows(IllegalArgumentException.class, () -> resource.generateResourceUri(ACTUAL_RESOURCE));
    }

    @Test
    public void testGenerateUriShouldBlockForwardedPrefixHeaderIfNotInAllowList() {
        when(request.getHeader(anyString())).thenAnswer(new RequestAnswer(ProxyHeader.FORWARDED_PREFIX.getHeader()));
        assertThrows(IllegalArgumentException.class, () -> resource.generateResourceUri(ACTUAL_RESOURCE));
    }

    @Test
    public void testGenerateUriShouldAllowForwardedContextHeaderIfInAllowList() {
        when(request.getHeader(anyString())).thenAnswer(new RequestAnswer(ProxyHeader.FORWARDED_CONTEXT.getHeader()));
        setNiFiProperties(Collections.singletonMap(PROXY_CONTEXT_PATH_PROP, ALLOWED_PATH));

        assertEquals(EXPECTED_URI, resource.generateResourceUri(ACTUAL_RESOURCE));
    }

    @Test
    public void testGenerateUriShouldAllowForwardedPrefixHeaderIfInAllowList() {
        when(request.getHeader(anyString())).thenAnswer(new RequestAnswer(ProxyHeader.FORWARDED_PREFIX.getHeader()));
        setNiFiProperties(Collections.singletonMap(PROXY_CONTEXT_PATH_PROP, ALLOWED_PATH));

        assertEquals(EXPECTED_URI, resource.generateResourceUri(ACTUAL_RESOURCE));
    }

    @Test
    public void testGenerateUriShouldAllowForwardedContextHeaderIfElementInMultipleAllowList() {
        when(request.getHeader(anyString())).thenAnswer(new RequestAnswer(ProxyHeader.FORWARDED_CONTEXT.getHeader()));
        setNiFiProperties(Collections.singletonMap(PROXY_CONTEXT_PATH_PROP, MULTIPLE_ALLOWED_PATHS));

        assertEquals(EXPECTED_URI, resource.generateResourceUri(ACTUAL_RESOURCE));
    }

    @Test
    public void testGenerateUriShouldAllowForwardedPrefixHeaderIfElementInMultipleAllowList() {
        when(request.getHeader(anyString())).thenAnswer(new RequestAnswer(ProxyHeader.FORWARDED_PREFIX.getHeader()));
        setNiFiProperties(Collections.singletonMap(PROXY_CONTEXT_PATH_PROP, MULTIPLE_ALLOWED_PATHS));

        assertEquals(EXPECTED_URI, resource.generateResourceUri(ACTUAL_RESOURCE));
    }

    @Test
    public void testGenerateExternalUiUri() {
        assertEquals(SCHEME + "://" + HOST + ":" + PORT + CUSTOM_UI_PATH, resource.generateExternalUiUri(CUSTOM_UI_PATH));
    }

    @Test
    public void testGenerateExternalUiUriWithProxy() {
        when(request.getHeader(anyString())).thenAnswer(new RequestAnswer(ProxyHeader.FORWARDED_CONTEXT.getHeader()));
        setNiFiProperties(Collections.singletonMap(PROXY_CONTEXT_PATH_PROP, ALLOWED_PATH));

        assertEquals(SCHEME + "://" + HOST + ":" + PORT + ALLOWED_PATH + CUSTOM_UI_PATH, resource.generateExternalUiUri(CUSTOM_UI_PATH));
    }

    private void setNiFiProperties(Map<String, String> props) {
        resource.properties = new NiFiProperties(props);
        when(servletContext.getInitParameter(eq(ALLOWED_CONTEXT_PATHS))).thenReturn(resource.properties.getAllowedContextPaths());
    }

    private static class MockApplicationResource extends ApplicationResource {
        void setHttpServletRequest(HttpServletRequest request) {
            super.httpServletRequest = request;
        }

        void setUriInfo(UriInfo uriInfo) {
            super.uriInfo = uriInfo;
        }
    }

    private static class RequestAnswer implements Answer<String> {
        private final List<String> proxyHeaders;

        public RequestAnswer() {
            this(ProxyHeader.FORWARDED_PREFIX.getHeader(), ProxyHeader.FORWARDED_CONTEXT.getHeader(), ProxyHeader.PROXY_CONTEXT_PATH.getHeader());
        }

        public RequestAnswer(String... proxyHeaders) {
            this(Arrays.asList(proxyHeaders));
        }

        public RequestAnswer(List<String> proxyHeaders) {
            this.proxyHeaders = proxyHeaders;
        }

        @Override
        public String answer(InvocationOnMock invocationOnMock) {
            String argument = invocationOnMock.getArgument(0);
            if (proxyHeaders.contains(argument)) {
                return ALLOWED_PATH;
            } else if (Arrays.asList(ProxyHeader.FORWARDED_PORT.getHeader(), ProxyHeader.FORWARDED_PREFIX.getHeader()).contains(argument)) {
                return "8081";
            } else if (Arrays.asList(ProxyHeader.FORWARDED_PROTO.getHeader(), ProxyHeader.PROXY_SCHEME.getHeader()).contains(argument)) {
                return "https";
            }  else if (Arrays.asList(ProxyHeader.PROXY_HOST.getHeader(), ProxyHeader.FORWARDED_HOST.getHeader()).contains(argument)) {
                return "nifi.apache.org:8081";
            } else {
                return "";
            }
        }
    }
}
