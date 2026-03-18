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
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.core.UriInfo;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.security.ProxiedEntitiesUtils;
import org.apache.nifi.web.servlet.shared.ProxyHeader;
import org.glassfish.jersey.uri.internal.JerseyUriBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;

import java.net.URI;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
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

        lenient().when(request.getScheme()).thenReturn(SCHEME);
        lenient().when(request.getServerName()).thenReturn(HOST);
        lenient().when(request.getServerPort()).thenReturn(PORT);

        lenient().when(request.getServletContext()).thenReturn(servletContext);

        resource = new MockApplicationResource();
        resource.setHttpServletRequest(request);
        resource.setUriInfo(uriInfo);
        resource.setProperties(new NiFiProperties());
    }

    @AfterEach
    public void tearDown() {
        SecurityContextHolder.clearContext();
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

    @Test
    public void testIsRequestFromClusterNodeExactMatch() {
        final MockApplicationResource testResource = createClusterNodeResource(
                Set.of("nifi-node1.example.com"),
                "nifi-node1.example.com",
                null
        );
        assertTrue(testResource.isRequestFromClusterNode());
    }

    @Test
    public void testIsRequestFromClusterNodeWildcardMatch() {
        final MockApplicationResource testResource = createClusterNodeResource(
                Set.of("*.nifi.svc.cluster.local"),
                "nifi-0.nifi.svc.cluster.local",
                null
        );
        assertTrue(testResource.isRequestFromClusterNode());
    }

    @Test
    public void testIsRequestFromClusterNodeWildcardNoMatchMultiLevel() {
        final MockApplicationResource testResource = createClusterNodeResource(
                Set.of("*.nifi.svc.cluster.local"),
                "extra-subdomain-level.nifi-0.nifi.svc.cluster.local",
                null
        );
        assertFalse(testResource.isRequestFromClusterNode());
    }

    @Test
    public void testIsRequestFromClusterNodeNoMatch() {
        final MockApplicationResource testResource = createClusterNodeResource(
                Set.of("other-node.nifi.apache.org"),
                "nifi-node1.nifi.apache.org",
                null
        );
        assertFalse(testResource.isRequestFromClusterNode());
    }

    @Test
    public void testIsRequestFromClusterNodeWithProxiedEntitiesChain() {
        final MockApplicationResource testResource = createClusterNodeResource(
                Set.of("nifi-node1.nifi.apache.org"),
                "nifi-node1.nifi.apache.org",
                "<user@example.com>"
        );
        assertFalse(testResource.isRequestFromClusterNode());
    }

    @Test
    public void testIsRequestFromClusterNodeNoCertificates() {
        final HttpServletRequest mockRequest = mock(HttpServletRequest.class);
        final ClusterCoordinator mockCoordinator = mock(ClusterCoordinator.class);

        final Authentication authentication = mock(Authentication.class);
        when(authentication.getCredentials()).thenReturn("bearer-token");
        final SecurityContext securityContext = mock(SecurityContext.class);
        when(securityContext.getAuthentication()).thenReturn(authentication);
        SecurityContextHolder.setContext(securityContext);

        final MockApplicationResource testResource = new MockApplicationResource();
        testResource.setHttpServletRequest(mockRequest);
        testResource.setClusterCoordinator(mockCoordinator);
        assertFalse(testResource.isRequestFromClusterNode());
    }

    private MockApplicationResource createClusterNodeResource(
            final Set<String> certDnsSans,
            final String nodeApiAddress,
            final String proxiedEntitiesChain) {

        final HttpServletRequest mockRequest = mock(HttpServletRequest.class);
        when(mockRequest.getHeader(eq(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN))).thenReturn(proxiedEntitiesChain);

        final ClusterCoordinator mockCoordinator = mock(ClusterCoordinator.class);
        final NodeIdentifier nodeId = new NodeIdentifier("node-1", nodeApiAddress, 8443,
                nodeApiAddress, 11443, nodeApiAddress, 6342,
                null, null, null, false, Set.of());
        lenient().when(mockCoordinator.getNodeIdentifiers()).thenReturn(Set.of(nodeId));

        final X509Certificate mockCert = mock(X509Certificate.class);
        final X509Certificate[] certs = new X509Certificate[]{mockCert};
        final Authentication authentication = mock(Authentication.class);
        when(authentication.getCredentials()).thenReturn(certs);
        final SecurityContext securityContext = mock(SecurityContext.class);
        when(securityContext.getAuthentication()).thenReturn(authentication);
        SecurityContextHolder.setContext(securityContext);

        final MockApplicationResource testResource = new MockApplicationResource();
        testResource.setHttpServletRequest(mockRequest);
        testResource.setClusterCoordinator(mockCoordinator);
        testResource.setCertDnsSans(certDnsSans);
        return testResource;
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

        void setCertDnsSans(Set<String> certDnsSans) {
            super.peerIdentityProvider = certs -> certDnsSans;
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
