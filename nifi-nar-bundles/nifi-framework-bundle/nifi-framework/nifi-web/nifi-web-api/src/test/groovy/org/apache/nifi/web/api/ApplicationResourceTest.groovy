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
package org.apache.nifi.web.api

import org.apache.nifi.util.NiFiProperties
import org.glassfish.jersey.uri.internal.JerseyUriBuilder
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

import javax.servlet.http.HttpServletRequest
import javax.ws.rs.core.UriBuilderException
import javax.ws.rs.core.UriInfo

import static org.apache.nifi.web.util.WebUtils.PROXY_CONTEXT_PATH_HTTP_HEADER
import static org.apache.nifi.web.util.WebUtils.PROXY_HOST_HTTP_HEADER
import static org.apache.nifi.web.util.WebUtils.PROXY_PORT_HTTP_HEADER
import static org.apache.nifi.web.util.WebUtils.PROXY_SCHEME_HTTP_HEADER
import static org.apache.nifi.web.util.WebUtils.FORWARDED_CONTEXT_HTTP_HEADER
import static org.apache.nifi.web.util.WebUtils.FORWARDED_HOST_HTTP_HEADER
import static org.apache.nifi.web.util.WebUtils.FORWARDED_PORT_HTTP_HEADER
import static org.apache.nifi.web.util.WebUtils.FORWARDED_PREFIX_HTTP_HEADER
import static org.apache.nifi.web.util.WebUtils.FORWARDED_PROTO_HTTP_HEADER

@RunWith(JUnit4.class)
class ApplicationResourceTest extends GroovyTestCase {
    static final String PROXY_CONTEXT_PATH_PROP = NiFiProperties.WEB_PROXY_CONTEXT_PATH
    static final String ALLOWED_PATH = "/some/context/path"

    class MockApplicationResource extends ApplicationResource {
        void setHttpServletRequest(HttpServletRequest request) {
            super.httpServletRequest = request
        }

        void setUriInfo(UriInfo uriInfo) {
            super.uriInfo = uriInfo
        }
    }

    private ApplicationResource buildApplicationResource() {
        buildApplicationResource([FORWARDED_PREFIX_HTTP_HEADER, FORWARDED_CONTEXT_HTTP_HEADER, PROXY_CONTEXT_PATH_HTTP_HEADER])
    }

    private ApplicationResource buildApplicationResource(List proxyHeaders) {
        ApplicationResource resource = new MockApplicationResource()
        String headerValue = ""
        HttpServletRequest mockRequest = [getHeader: { String k ->
            if (proxyHeaders.contains(k)) {
                headerValue = ALLOWED_PATH
            } else if ([FORWARDED_PORT_HTTP_HEADER, PROXY_PORT_HTTP_HEADER].contains(k)) {
                headerValue = "8081"
            } else if ([FORWARDED_PROTO_HTTP_HEADER, PROXY_SCHEME_HTTP_HEADER].contains(k)) {
                headerValue = "https"
            } else if ([PROXY_HOST_HTTP_HEADER, FORWARDED_HOST_HTTP_HEADER].contains(k)) {
                headerValue = "nifi.apache.org:8081"
            } else {
                headerValue = ""
            }
            headerValue
        }, getContextPath: { ->
            headerValue
        }, getScheme: { ->
            "https"
        }, getServerPort: { ->
            443
        }] as HttpServletRequest

        UriInfo mockUriInfo = [getBaseUriBuilder: { ->
            new JerseyUriBuilder().uri(new URI('https://nifi.apache.org/'))
        }] as UriInfo

        resource.setHttpServletRequest(mockRequest)
        resource.setUriInfo(mockUriInfo)
        resource.properties = new NiFiProperties()

        resource
    }

    @Test
    void testGenerateUriShouldBlockProxyContextPathHeaderIfNotInAllowList() throws Exception {
        ApplicationResource resource = buildApplicationResource()
        shouldFail(UriBuilderException) {
            resource.generateResourceUri('actualResource')
        }
    }

    @Test
    void testGenerateUriShouldAllowProxyContextPathHeaderIfInAllowList() throws Exception {
        ApplicationResource resource = buildApplicationResource()
        NiFiProperties niFiProperties = new NiFiProperties([(PROXY_CONTEXT_PATH_PROP): ALLOWED_PATH] as Properties)
        resource.properties = niFiProperties

        String generatedUri = resource.generateResourceUri('actualResource')

        assert generatedUri == "https://nifi.apache.org:8081${ALLOWED_PATH}/actualResource"
    }

    @Test
    void testGenerateUriShouldAllowProxyContextPathHeaderIfElementInMultipleAllowList() throws Exception {
        ApplicationResource resource = buildApplicationResource()
        String multipleAllowedPaths = [ALLOWED_PATH, "another/path", "a/third/path"].join(",")
        NiFiProperties niFiProperties = new NiFiProperties([(PROXY_CONTEXT_PATH_PROP): multipleAllowedPaths] as Properties)
        resource.properties = niFiProperties

        String generatedUri = resource.generateResourceUri('actualResource')

        assert generatedUri == "https://nifi.apache.org:8081${ALLOWED_PATH}/actualResource"
    }

    @Test
    void testGenerateUriShouldBlockForwardedContextHeaderIfNotInAllowList() throws Exception {
        ApplicationResource resource = buildApplicationResource([FORWARDED_CONTEXT_HTTP_HEADER])

        shouldFail(UriBuilderException) {
            resource.generateResourceUri('actualResource')
        }
    }

    @Test
    void testGenerateUriShouldBlockForwardedPrefixHeaderIfNotInAllowList() throws Exception {
        ApplicationResource resource = buildApplicationResource([FORWARDED_PREFIX_HTTP_HEADER])

        shouldFail(UriBuilderException) {
            resource.generateResourceUri('actualResource')
        }
    }

    @Test
    void testGenerateUriShouldAllowForwardedContextHeaderIfInAllowList() throws Exception {
        ApplicationResource resource = buildApplicationResource([FORWARDED_CONTEXT_HTTP_HEADER])
        NiFiProperties niFiProperties = new NiFiProperties([(PROXY_CONTEXT_PATH_PROP): ALLOWED_PATH] as Properties)
        resource.properties = niFiProperties

        String generatedUri = resource.generateResourceUri('actualResource')
        assert generatedUri == "https://nifi.apache.org:8081${ALLOWED_PATH}/actualResource"
    }

    @Test
    void testGenerateUriShouldAllowForwardedPrefixHeaderIfInAllowList() throws Exception {
        ApplicationResource resource = buildApplicationResource([FORWARDED_PREFIX_HTTP_HEADER])
        NiFiProperties niFiProperties = new NiFiProperties([(PROXY_CONTEXT_PATH_PROP): ALLOWED_PATH] as Properties)
        resource.properties = niFiProperties

        String generatedUri = resource.generateResourceUri('actualResource')
        assert generatedUri == "https://nifi.apache.org:8081${ALLOWED_PATH}/actualResource"
    }

    @Test
    void testGenerateUriShouldAllowForwardedContextHeaderIfElementInMultipleAllowList() throws Exception {
        ApplicationResource resource = buildApplicationResource([FORWARDED_CONTEXT_HTTP_HEADER])
        String multipleAllowedPaths = [ALLOWED_PATH, "another/path", "a/third/path"].join(",")
        NiFiProperties niFiProperties = new NiFiProperties([(PROXY_CONTEXT_PATH_PROP): multipleAllowedPaths] as Properties)
        resource.properties = niFiProperties

        String generatedUri = resource.generateResourceUri('actualResource')
        assert generatedUri == "https://nifi.apache.org:8081${ALLOWED_PATH}/actualResource"
    }

    @Test
    void testGenerateUriShouldAllowForwardedPrefixHeaderIfElementInMultipleAllowList() throws Exception {
        ApplicationResource resource = buildApplicationResource([FORWARDED_PREFIX_HTTP_HEADER])
        String multipleAllowedPaths = [ALLOWED_PATH, "another/path", "a/third/path"].join(",")
        NiFiProperties niFiProperties = new NiFiProperties([(PROXY_CONTEXT_PATH_PROP): multipleAllowedPaths] as Properties)
        resource.properties = niFiProperties

        String generatedUri = resource.generateResourceUri('actualResource')
        assert generatedUri == "https://nifi.apache.org:8081${ALLOWED_PATH}/actualResource"
    }
}
