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
package org.apache.nifi.web.util

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

@RunWith(JUnit4.class)
class WebUtilsTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(WebUtilsTest.class)

    static final String PCP_HEADER = "X-ProxyContextPath"
    static final String FC_HEADER = "X-Forwarded-Context"

    static final String WHITELISTED_PATH = "/some/context/path"

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

    HttpServletRequest mockRequest(Map keys) {
        HttpServletRequest mockRequest = [
                getContextPath: { ->
                    logger.mock("Request.getContextPath() -> default/path")
                    "default/path"
                },
                getHeader     : { String k ->
                    logger.mock("Request.getHeader($k) -> ${keys}")
                    switch (k) {
                        case PCP_HEADER:
                            return keys["proxy"]
                            break
                        case FC_HEADER:
                            return keys["forward"]
                            break
                        default:
                            return ""
                    }
                }] as HttpServletRequest
        mockRequest
    }

    @Test
    void testShouldDetermineCorrectContextPathWhenPresent() throws Exception {
        // Arrange
        final String CORRECT_CONTEXT_PATH = WHITELISTED_PATH
        final String WRONG_CONTEXT_PATH = "this/is/a/bad/path"

        // Variety of requests with different ordering of context paths (the correct one is always "some/context/path"
        HttpServletRequest proxyRequest = mockRequest([proxy: CORRECT_CONTEXT_PATH])
        HttpServletRequest forwardedRequest = mockRequest([forward: CORRECT_CONTEXT_PATH])
        HttpServletRequest proxyBeforeForwardedRequest = mockRequest([proxy: CORRECT_CONTEXT_PATH, forward: WRONG_CONTEXT_PATH])
        List<HttpServletRequest> requests = [proxyRequest, forwardedRequest, proxyBeforeForwardedRequest]

        // Act
        requests.each { HttpServletRequest request ->
            String determinedContextPath = WebUtils.determineContextPath(request)
            logger.info("Determined context path: ${determinedContextPath}")

            // Assert
            assert determinedContextPath == CORRECT_CONTEXT_PATH
        }
    }

    @Test
    void testShouldDetermineCorrectContextPathWhenAbsent() throws Exception {
        // Arrange
        final String CORRECT_CONTEXT_PATH = ""

        // Variety of requests with different ordering of non-existent context paths (the correct one is always ""
        HttpServletRequest proxyRequest = mockRequest([proxy: ""])
        HttpServletRequest proxySpacesRequest = mockRequest([proxy: "   "])
        HttpServletRequest forwardedRequest = mockRequest([forward: ""])
        HttpServletRequest forwardedSpacesRequest = mockRequest([forward: "   "])
        HttpServletRequest proxyBeforeForwardedRequest = mockRequest([proxy: "", forward: ""])
        List<HttpServletRequest> requests = [proxyRequest, proxySpacesRequest, forwardedRequest, forwardedSpacesRequest, proxyBeforeForwardedRequest]

        // Act
        requests.each { HttpServletRequest request ->
            String determinedContextPath = WebUtils.determineContextPath(request)
            logger.info("Determined context path: ${determinedContextPath}")

            // Assert
            assert determinedContextPath == CORRECT_CONTEXT_PATH
        }
    }

    @Test
    void testShouldNormalizeContextPath() throws Exception {
        // Arrange
        final String CORRECT_CONTEXT_PATH = WHITELISTED_PATH
        final String TRIMMED_PATH = WHITELISTED_PATH[1..-1] // Trims leading /

        // Variety of different context paths (the correct one is always "/some/context/path")
        List<String> contextPaths = ["/$TRIMMED_PATH", "/" + TRIMMED_PATH, TRIMMED_PATH, TRIMMED_PATH + "/"]

        // Act
        contextPaths.each { String contextPath ->
            String normalizedContextPath = WebUtils.normalizeContextPath(contextPath)
            logger.info("Normalized context path: ${normalizedContextPath} <- ${contextPath}")

            // Assert
            assert normalizedContextPath == CORRECT_CONTEXT_PATH
        }
    }

    @Test
    void testGetResourcePathShouldBlockContextPathHeaderIfNotInWhitelist() throws Exception {
        // Arrange
        logger.info("Whitelisted path(s): ")

        HttpServletRequest requestWithProxyHeader = mockRequest([proxy: "any/context/path"])
        HttpServletRequest requestWithProxyAndForwardHeader = mockRequest([proxy: "any/context/path", forward: "any/other/context/path"])
        List<HttpServletRequest> requests = [requestWithProxyHeader, requestWithProxyAndForwardHeader]

        // Act
        requests.each { HttpServletRequest request ->
            def msg = shouldFail(UriBuilderException) {
                String generatedResourcePath = WebUtils.getResourcePath(new URI('https://nifi.apache.org/actualResource'), request, "")
                logger.unexpected("Generated Resource Path: ${generatedResourcePath}")
            }

            // Assert
            logger.expected(msg)
            assert msg =~ "The provided context path \\[.*\\] was not whitelisted \\[\\]"
        }
    }

    @Test
    void testGetResourcePathShouldAllowContextPathHeaderIfInWhitelist() throws Exception {
        // Arrange
        logger.info("Whitelisted path(s): ${WHITELISTED_PATH}")

        HttpServletRequest requestWithProxyHeader = mockRequest([proxy: "some/context/path"])
        HttpServletRequest requestWithForwardHeader = mockRequest([forward: "some/context/path"])
        HttpServletRequest requestWithProxyAndForwardHeader = mockRequest([proxy: "some/context/path", forward: "any/other/context/path"])
        List<HttpServletRequest> requests = [requestWithProxyHeader, requestWithForwardHeader, requestWithProxyAndForwardHeader]

        // Act
        requests.each { HttpServletRequest request ->
            String generatedResourcePath = WebUtils.getResourcePath(new URI('https://nifi.apache.org/actualResource'), request, WHITELISTED_PATH)
            logger.info("Generated Resource Path: ${generatedResourcePath}")

            // Assert
            assert generatedResourcePath == "${WHITELISTED_PATH}/actualResource"
        }
    }

    @Test
    void testGetResourcePathShouldAllowContextPathHeaderIfElementInMultipleWhitelist() throws Exception {
        // Arrange
        String multipleWhitelistedPaths = [WHITELISTED_PATH, "/another/path", "/a/third/path"].join(",")
        logger.info("Whitelisted path(s): ${multipleWhitelistedPaths}")

        final List<String> VALID_RESOURCE_PATHS = multipleWhitelistedPaths.split(",").collect { "$it/actualResource" }

        HttpServletRequest requestWithProxyHeader = mockRequest([proxy: "some/context/path"])
        HttpServletRequest requestWithForwardHeader = mockRequest([forward: "another/path"])
        HttpServletRequest requestWithProxyAndForwardHeader = mockRequest([proxy: "a/third/path", forward: "any/other/context/path"])
        List<HttpServletRequest> requests = [requestWithProxyHeader, requestWithForwardHeader, requestWithProxyAndForwardHeader]

        // Act
        requests.each { HttpServletRequest request ->
            String generatedResourcePath = WebUtils.getResourcePath(new URI('https://nifi.apache.org/actualResource'), request, multipleWhitelistedPaths)
            logger.info("Generated Resource Path: ${generatedResourcePath}")

            // Assert
            assert VALID_RESOURCE_PATHS.any { it == generatedResourcePath }
        }
    }

    @Test
    void testVerifyContextPathShouldAllowContextPathHeaderIfInWhitelist() throws Exception {
        // Arrange
        logger.info("Whitelisted path(s): ${WHITELISTED_PATH}")
        String contextPath = WHITELISTED_PATH

        // Act
        logger.info("Testing [${contextPath}] against ${WHITELISTED_PATH}")
        WebUtils.verifyContextPath(WHITELISTED_PATH, contextPath)
        logger.info("Verified [${contextPath}]")

        // Assert
        // Would throw exception if invalid
    }

    @Test
    void testVerifyContextPathShouldAllowContextPathHeaderIfInMultipleWhitelist() throws Exception {
        // Arrange
        String multipleWhitelist = [WHITELISTED_PATH, WebUtils.normalizeContextPath(WHITELISTED_PATH.reverse())].join(",")
        logger.info("Whitelisted path(s): ${multipleWhitelist}")
        String contextPath = WHITELISTED_PATH

        // Act
        logger.info("Testing [${contextPath}] against ${multipleWhitelist}")
        WebUtils.verifyContextPath(multipleWhitelist, contextPath)
        logger.info("Verified [${contextPath}]")

        // Assert
        // Would throw exception if invalid
    }

    @Test
    void testVerifyContextPathShouldAllowContextPathHeaderIfBlank() throws Exception {
        // Arrange
        logger.info("Whitelisted path(s): ${WHITELISTED_PATH}")

        def emptyContextPaths = ["", "  ", "\t", null]

        // Act
        emptyContextPaths.each { String contextPath ->
            logger.info("Testing [${contextPath}] against ${WHITELISTED_PATH}")
            WebUtils.verifyContextPath(WHITELISTED_PATH, contextPath)
            logger.info("Verified [${contextPath}]")

            // Assert
            // Would throw exception if invalid
        }
    }

    @Test
    void testVerifyContextPathShouldBlockContextPathHeaderIfNotAllowed() throws Exception {
        // Arrange
        logger.info("Whitelisted path(s): ${WHITELISTED_PATH}")

        def invalidContextPaths = ["/other/path", "somesite.com", "/../trying/to/escape"]

        // Act
        invalidContextPaths.each { String contextPath ->
            logger.info("Testing [${contextPath}] against ${WHITELISTED_PATH}")
            def msg = shouldFail(UriBuilderException) {
                WebUtils.verifyContextPath(WHITELISTED_PATH, contextPath)
                logger.info("Verified [${contextPath}]")
            }

            // Assert
            logger.expected(msg)
            assert msg =~ " was not whitelisted "
        }
    }
}
