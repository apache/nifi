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

import org.apache.nifi.util.NiFiProperties;
import org.glassfish.jersey.uri.internal.JerseyUriBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.core.UriBuilderException;
import jakarta.ws.rs.core.UriInfo;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.web.util.WebUtils.FORWARDED_CONTEXT_HTTP_HEADER;
import static org.apache.nifi.web.util.WebUtils.FORWARDED_HOST_HTTP_HEADER;
import static org.apache.nifi.web.util.WebUtils.FORWARDED_PORT_HTTP_HEADER;
import static org.apache.nifi.web.util.WebUtils.FORWARDED_PREFIX_HTTP_HEADER;
import static org.apache.nifi.web.util.WebUtils.FORWARDED_PROTO_HTTP_HEADER;
import static org.apache.nifi.web.util.WebUtils.PROXY_CONTEXT_PATH_HTTP_HEADER;
import static org.apache.nifi.web.util.WebUtils.PROXY_HOST_HTTP_HEADER;
import static org.apache.nifi.web.util.WebUtils.PROXY_PORT_HTTP_HEADER;
import static org.apache.nifi.web.util.WebUtils.PROXY_SCHEME_HTTP_HEADER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestApplicationResource {
    private static final String PROXY_CONTEXT_PATH_PROP = NiFiProperties.WEB_PROXY_CONTEXT_PATH;
    private static final String BASE_URI = "https://nifi.apache.org";
    private static final String ALLOWED_PATH = "/some/context/path";
    private static final String FORWARD_SLASH = "/";
    private static final String ACTUAL_RESOURCE = "actualResource";
    private static final String EXPECTED_URI = BASE_URI + ":8081" + ALLOWED_PATH + FORWARD_SLASH + ACTUAL_RESOURCE;
    private static final String MULTIPLE_ALLOWED_PATHS = String.join(",", ALLOWED_PATH, "another/path", "a/third/path");

    @Mock
    private HttpServletRequest request;

    private MockApplicationResource resource;

    @BeforeEach
    public void setUp(@Mock UriInfo uriInfo) throws Exception {
        when(uriInfo.getBaseUriBuilder()).thenReturn(new JerseyUriBuilder().uri(new URI(BASE_URI + FORWARD_SLASH)));
        when(request.getScheme()).thenReturn("https");

        resource = new MockApplicationResource();
        resource.setHttpServletRequest(request);
        resource.setUriInfo(uriInfo);
        resource.setProperties(new NiFiProperties());
    }

    @Test
    public void testGenerateUriShouldBlockProxyContextPathHeaderIfNotInAllowList() {
        when(request.getHeader(anyString())).thenAnswer(new RequestAnswer());
        assertThrows(UriBuilderException.class, () -> resource.generateResourceUri(ACTUAL_RESOURCE));
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
        when(request.getHeader(anyString())).thenAnswer(new RequestAnswer(FORWARDED_CONTEXT_HTTP_HEADER));
        assertThrows(UriBuilderException.class, () -> resource.generateResourceUri(ACTUAL_RESOURCE));
    }

    @Test
    public void testGenerateUriShouldBlockForwardedPrefixHeaderIfNotInAllowList() {
        when(request.getHeader(anyString())).thenAnswer(new RequestAnswer(FORWARDED_PREFIX_HTTP_HEADER));
        assertThrows(UriBuilderException.class, () -> resource.generateResourceUri(ACTUAL_RESOURCE));
    }

    @Test
    public void testGenerateUriShouldAllowForwardedContextHeaderIfInAllowList() {
        when(request.getHeader(anyString())).thenAnswer(new RequestAnswer(FORWARDED_CONTEXT_HTTP_HEADER));
        setNiFiProperties(Collections.singletonMap(PROXY_CONTEXT_PATH_PROP, ALLOWED_PATH));

        assertEquals(EXPECTED_URI, resource.generateResourceUri(ACTUAL_RESOURCE));
    }

    @Test
    public void testGenerateUriShouldAllowForwardedPrefixHeaderIfInAllowList() {
        when(request.getHeader(anyString())).thenAnswer(new RequestAnswer(FORWARDED_PREFIX_HTTP_HEADER));
        setNiFiProperties(Collections.singletonMap(PROXY_CONTEXT_PATH_PROP, ALLOWED_PATH));

        assertEquals(EXPECTED_URI, resource.generateResourceUri(ACTUAL_RESOURCE));
    }

    @Test
    public void testGenerateUriShouldAllowForwardedContextHeaderIfElementInMultipleAllowList() {
        when(request.getHeader(anyString())).thenAnswer(new RequestAnswer(FORWARDED_CONTEXT_HTTP_HEADER));
        setNiFiProperties(Collections.singletonMap(PROXY_CONTEXT_PATH_PROP, MULTIPLE_ALLOWED_PATHS));

        assertEquals(EXPECTED_URI, resource.generateResourceUri(ACTUAL_RESOURCE));
    }

    @Test
    public void testGenerateUriShouldAllowForwardedPrefixHeaderIfElementInMultipleAllowList() {
        when(request.getHeader(anyString())).thenAnswer(new RequestAnswer(FORWARDED_PREFIX_HTTP_HEADER));
        setNiFiProperties(Collections.singletonMap(PROXY_CONTEXT_PATH_PROP, MULTIPLE_ALLOWED_PATHS));

        assertEquals(EXPECTED_URI, resource.generateResourceUri(ACTUAL_RESOURCE));
    }

    private void setNiFiProperties(Map<String, String> props) {
        resource.properties = new NiFiProperties(props);
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
            this(FORWARDED_PREFIX_HTTP_HEADER, FORWARDED_CONTEXT_HTTP_HEADER, PROXY_CONTEXT_PATH_HTTP_HEADER);
        }

        public RequestAnswer(String...proxyHeaders) {
            this(Arrays.asList(proxyHeaders));
        }

        public RequestAnswer(List<String> proxyHeaders) {
            this.proxyHeaders = proxyHeaders;
        }

        @Override
        public String answer(InvocationOnMock invocationOnMock) {
            String argument = invocationOnMock.getArgument(0);
            if(proxyHeaders.contains(argument)) {
                return ALLOWED_PATH;
            } else if(Arrays.asList(FORWARDED_PORT_HTTP_HEADER, PROXY_PORT_HTTP_HEADER).contains(argument)) {
                return "8081";
            } else if(Arrays.asList(FORWARDED_PROTO_HTTP_HEADER, PROXY_SCHEME_HTTP_HEADER).contains(argument)) {
                return "https";
            }  else if(Arrays.asList(PROXY_HOST_HTTP_HEADER, FORWARDED_HOST_HTTP_HEADER).contains(argument)) {
                return "nifi.apache.org:8081";
            } else {
                return "";
            }
        }
    }
}
