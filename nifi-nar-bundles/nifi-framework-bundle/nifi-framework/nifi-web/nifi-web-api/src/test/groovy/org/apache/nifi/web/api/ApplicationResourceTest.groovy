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
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.servlet.http.HttpServletRequest
import javax.ws.rs.core.UriBuilderException
import javax.ws.rs.core.UriInfo

@RunWith(JUnit4.class)
class ApplicationResourceTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(ApplicationResourceTest.class)

    public static final String PROXY_HOST_HTTP_HEADER = "X-ProxyHost"
    public static final String FORWARDED_HOST_HTTP_HEADER = "X-Forwarded-Host"

    static final String PROXY_SCHEME_HTTP_HEADER = "X-ProxyScheme"
    static final String PROXY_PORT_HTTP_HEADER = "X-ProxyPort"
    static final String PROXY_CONTEXT_PATH_HTTP_HEADER = "X-ProxyContextPath"

    static final String FORWARDED_PROTO_HTTP_HEADER = "X-Forwarded-Proto"
    static final String FORWARDED_PORT_HTTP_HEADER = "X-Forwarded-Port"
    static final String FORWARDED_CONTEXT_HTTP_HEADER = "X-Forwarded-Context"
    static final String FORWARDED_PREFIX_HTTP_HEADER = "X-Forwarded-Prefix"

    static final String PROXY_CONTEXT_PATH_PROP = NiFiProperties.WEB_PROXY_CONTEXT_PATH
    static final String ALLOWED_PATH = "/some/context/path"

    @BeforeClass
    static void setUpOnce() throws Exception {
        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Before
    void setUp() throws Exception {
    }

    @After
    void tearDown() throws Exception {
    }

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
            logger.mock("Request.getHeader($k) -> \"$headerValue\"")
            headerValue
        }, getContextPath: { ->
            logger.mock("Request.getContextPath() -> \"$headerValue\"")
            headerValue
        }] as HttpServletRequest

        UriInfo mockUriInfo = [getBaseUriBuilder: { ->
            logger.mock("Returning mock UriBuilder")
            new JerseyUriBuilder().uri(new URI('https://nifi.apache.org/'))
        }] as UriInfo

        resource.setHttpServletRequest(mockRequest)
        resource.setUriInfo(mockUriInfo)
        resource.properties = new NiFiProperties()

        resource
    }

    @Test
    void testGenerateUriShouldBlockProxyContextPathHeaderIfNotInAllowList() throws Exception {
        // Arrange
        ApplicationResource resource = buildApplicationResource()
        logger.info("Allowed path(s): ")

        // Act
        def msg = shouldFail(UriBuilderException) {
            String generatedUri = resource.generateResourceUri('actualResource')
            logger.unexpected("Generated URI: ${generatedUri}")
        }

        // Assert
        logger.expected(msg)
        assert msg =~ "The provided context path \\[.*\\] was not registered as allowed \\[\\]"
    }

    @Test
    void testGenerateUriShouldAllowProxyContextPathHeaderIfInAllowList() throws Exception {
        // Arrange
        ApplicationResource resource = buildApplicationResource()
        logger.info("Allowed path(s): ${ALLOWED_PATH}")
        NiFiProperties niFiProperties = new NiFiProperties([(PROXY_CONTEXT_PATH_PROP): ALLOWED_PATH] as Properties)
        resource.properties = niFiProperties

        // Act
        String generatedUri = resource.generateResourceUri('actualResource')
        logger.info("Generated URI: ${generatedUri}")

        // Assert
        assert generatedUri == "https://nifi.apache.org:8081${ALLOWED_PATH}/actualResource"
    }

    @Test
    void testGenerateUriShouldAllowProxyContextPathHeaderIfElementInMultipleAllowList() throws Exception {
        // Arrange
        ApplicationResource resource = buildApplicationResource()
        String multipleAllowedPaths = [ALLOWED_PATH, "another/path", "a/third/path"].join(",")
        logger.info("Allowed path(s): ${multipleAllowedPaths}")
        NiFiProperties niFiProperties = new NiFiProperties([(PROXY_CONTEXT_PATH_PROP): multipleAllowedPaths] as Properties)
        resource.properties = niFiProperties

        // Act
        String generatedUri = resource.generateResourceUri('actualResource')
        logger.info("Generated URI: ${generatedUri}")

        // Assert
        assert generatedUri == "https://nifi.apache.org:8081${ALLOWED_PATH}/actualResource"
    }

    @Test
    void testGenerateUriShouldBlockForwardedContextHeaderIfNotInAllowList() throws Exception {
        // Arrange
        ApplicationResource resource = buildApplicationResource([FORWARDED_CONTEXT_HTTP_HEADER])
        logger.info("Allowed path(s): ")

        // Act
        def msg = shouldFail(UriBuilderException) {
            String generatedUri = resource.generateResourceUri('actualResource')
            logger.unexpected("Generated URI: ${generatedUri}")
        }

        // Assert
        logger.expected(msg)
        assert msg =~ "The provided context path \\[.*\\] was not registered as allowed \\[\\]"
    }

    @Test
    void testGenerateUriShouldBlockForwardedPrefixHeaderIfNotInAllowList() throws Exception {
        // Arrange
        ApplicationResource resource = buildApplicationResource([FORWARDED_PREFIX_HTTP_HEADER])
        logger.info("Allowed path(s): ")

        // Act
        def msg = shouldFail(UriBuilderException) {
            String generatedUri = resource.generateResourceUri('actualResource')
            logger.unexpected("Generated URI: ${generatedUri}")
        }

        // Assert
        logger.expected(msg)
        assert msg =~ "The provided context path \\[.*\\] was not registered as allowed \\[\\]"
    }

    @Test
    void testGenerateUriShouldAllowForwardedContextHeaderIfInAllowList() throws Exception {
        // Arrange
        ApplicationResource resource = buildApplicationResource([FORWARDED_CONTEXT_HTTP_HEADER])
        logger.info("Allowed path(s): ${ALLOWED_PATH}")
        NiFiProperties niFiProperties = new NiFiProperties([(PROXY_CONTEXT_PATH_PROP): ALLOWED_PATH] as Properties)
        resource.properties = niFiProperties

        // Act
        String generatedUri = resource.generateResourceUri('actualResource')
        logger.info("Generated URI: ${generatedUri}")

        // Assert
        assert generatedUri == "https://nifi.apache.org:8081${ALLOWED_PATH}/actualResource"
    }

    @Test
    void testGenerateUriShouldAllowForwardedPrefixHeaderIfInAllowList() throws Exception {
        // Arrange
        ApplicationResource resource = buildApplicationResource([FORWARDED_PREFIX_HTTP_HEADER])
        logger.info("Allowed path(s): ${ALLOWED_PATH}")
        NiFiProperties niFiProperties = new NiFiProperties([(PROXY_CONTEXT_PATH_PROP): ALLOWED_PATH] as Properties)
        resource.properties = niFiProperties

        // Act
        String generatedUri = resource.generateResourceUri('actualResource')
        logger.info("Generated URI: ${generatedUri}")

        // Assert
        assert generatedUri == "https://nifi.apache.org:8081${ALLOWED_PATH}/actualResource"
    }

    @Test
    void testGenerateUriShouldAllowForwardedContextHeaderIfElementInMultipleAllowList() throws Exception {
        // Arrange
        ApplicationResource resource = buildApplicationResource([FORWARDED_CONTEXT_HTTP_HEADER])
        String multipleAllowedPaths = [ALLOWED_PATH, "another/path", "a/third/path"].join(",")
        logger.info("Allowed path(s): ${multipleAllowedPaths}")
        NiFiProperties niFiProperties = new NiFiProperties([(PROXY_CONTEXT_PATH_PROP): multipleAllowedPaths] as Properties)
        resource.properties = niFiProperties

        // Act
        String generatedUri = resource.generateResourceUri('actualResource')
        logger.info("Generated URI: ${generatedUri}")

        // Assert
        assert generatedUri == "https://nifi.apache.org:8081${ALLOWED_PATH}/actualResource"
    }

    @Test
    void testGenerateUriShouldAllowForwardedPrefixHeaderIfElementInMultipleAllowList() throws Exception {
        // Arrange
        ApplicationResource resource = buildApplicationResource([FORWARDED_PREFIX_HTTP_HEADER])
        String multipleAllowedPaths = [ALLOWED_PATH, "another/path", "a/third/path"].join(",")
        logger.info("Allowed path(s): ${multipleAllowedPaths}")
        NiFiProperties niFiProperties = new NiFiProperties([(PROXY_CONTEXT_PATH_PROP): multipleAllowedPaths] as Properties)
        resource.properties = niFiProperties

        // Act
        String generatedUri = resource.generateResourceUri('actualResource')
        logger.info("Generated URI: ${generatedUri}")

        // Assert
        assert generatedUri == "https://nifi.apache.org:8081${ALLOWED_PATH}/actualResource"
    }
}
