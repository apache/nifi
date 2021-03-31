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
package org.apache.nifi.web.filter

import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.servlet.FilterConfig
import javax.servlet.ServletContext
import javax.servlet.http.HttpServletRequest

@RunWith(JUnit4.class)
class SanitizeContextPathFilterTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(SanitizeContextPathFilterTest.class)

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

    private static String getValue(String parameterName, Map<String, String> params = [:]) {
        params.containsKey(parameterName) ? params[parameterName] : ""
    }

    @Test
    void testInitShouldExtractAllowedContextPaths() {
        // Arrange
        def EXPECTED_ALLOWED_CONTEXT_PATHS = ["/path1", "/path2"].join(", ")
        def parameters = [allowedContextPaths: EXPECTED_ALLOWED_CONTEXT_PATHS]
        FilterConfig mockFilterConfig = [
                getInitParameter : { String parameterName -> getValue(parameterName, parameters) },
                getServletContext: { ->
                    [getInitParameter: { String parameterName -> return getValue(parameterName, parameters) }] as ServletContext
                }] as FilterConfig

        SanitizeContextPathFilter scpf = new SanitizeContextPathFilter()

        // Act
        scpf.init(mockFilterConfig)
        logger.info("Allowed context paths: ${scpf.getAllowedContextPaths()}")

        // Assert
        assert scpf.getAllowedContextPaths() == EXPECTED_ALLOWED_CONTEXT_PATHS
    }

    @Test
    void testInitShouldHandleBlankAllowedContextPaths() {
        // Arrange
        def EXPECTED_ALLOWED_CONTEXT_PATHS = ""
        FilterConfig mockFilterConfig = [
                getInitParameter : { String parameterName -> "" },
                getServletContext: { ->
                    [getInitParameter: { String parameterName -> "" }] as ServletContext
                }] as FilterConfig

        SanitizeContextPathFilter scpf = new SanitizeContextPathFilter()

        // Act
        scpf.init(mockFilterConfig)
        logger.info("Allowed context paths: ${scpf.getAllowedContextPaths()}")

        // Assert
        assert scpf.getAllowedContextPaths() == EXPECTED_ALLOWED_CONTEXT_PATHS
    }

    @Test
    void testShouldInjectContextPathAttribute() {
        // Arrange
        final String EXPECTED_ALLOWED_CONTEXT_PATHS = ["/path1", "/path2"].join(", ")
        final String EXPECTED_FORWARD_PATH = "index.jsp"
        final Map PARAMETERS = [
                allowedContextPaths: EXPECTED_ALLOWED_CONTEXT_PATHS,
                forwardPath        : EXPECTED_FORWARD_PATH
        ]

        final String EXPECTED_CONTEXT_PATH = "/path1"

        // Mock collaborators
        FilterConfig mockFilterConfig = [
                getInitParameter : { String parameterName ->
                    return getValue(parameterName, PARAMETERS)
                },
                getServletContext: { ->
                    [getInitParameter: { String parameterName ->
                        return getValue(parameterName, PARAMETERS)
                    }] as ServletContext
                }] as FilterConfig

        // Local map to store request attributes
        def requestAttributes = [:]

        final Map HEADERS = [
                "X-ProxyContextPath" : "path1",
                "X-Forwarded-Context": "",
                "X-Forwarded-Prefix" : ""]

        HttpServletRequest mockRequest = [
                getContextPath      : { -> EXPECTED_CONTEXT_PATH },
                getHeader           : { String headerName -> getValue(headerName, HEADERS) },
                setAttribute        : { String attr, String value ->
                    requestAttributes[attr] = value
                    logger.mock("Set request attribute ${attr} to ${value}")
                },
        ] as HttpServletRequest

        SanitizeContextPathFilter scpf = new SanitizeContextPathFilter()
        scpf.init(mockFilterConfig)
        logger.info("Allowed context paths: ${scpf.getAllowedContextPaths()}")

        // Act
        scpf.injectContextPathAttribute(mockRequest)

        // Assert
        assert requestAttributes["contextPath"] == EXPECTED_CONTEXT_PATH
    }
}
