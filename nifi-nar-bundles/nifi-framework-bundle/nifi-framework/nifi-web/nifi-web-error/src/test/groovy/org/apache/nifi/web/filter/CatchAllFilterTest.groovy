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

import javax.servlet.FilterChain
import javax.servlet.FilterConfig
import javax.servlet.RequestDispatcher
import javax.servlet.ServletContext
import javax.servlet.ServletRequest
import javax.servlet.ServletResponse
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

@RunWith(JUnit4.class)
class CatchAllFilterTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(CatchAllFilterTest.class)

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
    void testInitShouldCallSuper() {
        // Arrange
        def EXPECTED_ALLOWED_CONTEXT_PATHS = ["/path1", "/path2"].join(", ")
        def parameters = [allowedContextPaths: EXPECTED_ALLOWED_CONTEXT_PATHS]
        FilterConfig mockFilterConfig = [
                getInitParameter : { String parameterName ->
                    return getValue(parameterName, parameters)
                },
                getServletContext: { ->
                    [getInitParameter: { String parameterName ->
                        return getValue(parameterName, parameters)
                    }] as ServletContext
                }] as FilterConfig

        CatchAllFilter caf = new CatchAllFilter()

        // Act
        caf.init(mockFilterConfig)
        logger.info("Allowed context paths: ${caf.getAllowedContextPaths()}")

        // Assert
        assert caf.getAllowedContextPaths() == EXPECTED_ALLOWED_CONTEXT_PATHS
    }

    @Test
    void testShouldDoFilter() {
        // Arrange
        final String EXPECTED_ALLOWED_CONTEXT_PATHS = ["/path1", "/path2"].join(", ")
        final String EXPECTED_FORWARD_PATH = "index.jsp"
        final Map PARAMETERS = [
                allowedContextPaths: EXPECTED_ALLOWED_CONTEXT_PATHS,
                forwardPath        : EXPECTED_FORWARD_PATH
        ]

        final String EXPECTED_CONTEXT_PATH = ""

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

        // Local string to store resulting path
        String forwardedRequestTo = ""

        final Map HEADERS = [
                "X-ProxyContextPath" : "",
                "X-Forwarded-Context": "",
                "X-Forwarded-Prefix" : ""]

        HttpServletRequest mockRequest = [
                getContextPath      : { -> EXPECTED_CONTEXT_PATH },
                getHeader           : { String headerName -> getValue(headerName, HEADERS) },
                setAttribute        : { String attr, String value ->
                    requestAttributes[attr] = value
                    logger.mock("Set request attribute ${attr} to ${value}")
                },
                getRequestDispatcher: { String path ->
                    [forward: { ServletRequest request, ServletResponse response ->
                        forwardedRequestTo = path
                        logger.mock("Forwarded request to ${path}")
                    }] as RequestDispatcher
                }] as HttpServletRequest
        HttpServletResponse mockResponse = [:] as HttpServletResponse
        FilterChain mockFilterChain = [:] as FilterChain

        CatchAllFilter caf = new CatchAllFilter()
        caf.init(mockFilterConfig)
        logger.info("Allowed context paths: ${caf.getAllowedContextPaths()}")

        // Act
        caf.doFilter(mockRequest, mockResponse, mockFilterChain)

        // Assert
        assert forwardedRequestTo == EXPECTED_FORWARD_PATH
    }
}
