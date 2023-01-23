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

import org.junit.jupiter.api.Test

import javax.servlet.FilterConfig
import javax.servlet.ServletContext
import javax.servlet.http.HttpServletRequest

import static org.junit.jupiter.api.Assertions.assertEquals

class SanitizeContextPathFilterTest {

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

        // Assert
        assertEquals(EXPECTED_ALLOWED_CONTEXT_PATHS, scpf.getAllowedContextPaths())
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

        // Assert
        assertEquals(EXPECTED_ALLOWED_CONTEXT_PATHS, scpf.getAllowedContextPaths())
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
                },
        ] as HttpServletRequest

        SanitizeContextPathFilter scpf = new SanitizeContextPathFilter()
        scpf.init(mockFilterConfig)

        // Act
        scpf.injectContextPathAttribute(mockRequest)

        // Assert
        assertEquals(EXPECTED_CONTEXT_PATH, requestAttributes["contextPath"])
    }
}
