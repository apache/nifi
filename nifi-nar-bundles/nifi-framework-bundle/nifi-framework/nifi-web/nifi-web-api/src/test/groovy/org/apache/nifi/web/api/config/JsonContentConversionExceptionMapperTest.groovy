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
package org.apache.nifi.web.api.config

import com.fasterxml.jackson.core.JsonLocation
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.exc.InvalidFormatException
import org.apache.nifi.web.api.ProcessGroupResourceTest
import org.junit.Rule
import org.junit.rules.TestName
import org.junit.BeforeClass
import org.junit.Before
import org.junit.After
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.ws.rs.core.Response

@RunWith(JUnit4.class)
class JsonContentConversionExceptionMapperTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(ProcessGroupResourceTest.class)

    @Rule
    public TestName testName = new TestName()

    @BeforeClass
    static void setUpOnce() throws Exception {
        logger.metaClass.methodMissing = { String name, args ->
            logger.debug("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Before
    void setUp() throws Exception {
    }

    @After
    void tearDown() throws Exception {
    }

    @Test
    void testShouldThrowExceptionWithStringPortValue() throws Exception{
        // Arrange
        JsonContentConversionExceptionMapper jsonCCEM = new JsonContentConversionExceptionMapper()

        // Using real exception
        Class<?> instClass = Integer.class
        def mockParser = [getTokenLocation: { -> return new JsonLocation(null, 100, 1, 1)}] as JsonParser
        String message = "Some message"
        String value = "thisIsAnInvalidPort"
        InvalidFormatException ife = InvalidFormatException.from(mockParser, message, value, instClass)
        JsonMappingException.wrapWithPath(ife, new JsonMappingException.Reference("RemoteProcessGroupDTO", "proxyPort"))
        JsonMappingException.wrapWithPath(ife, new JsonMappingException.Reference("RemoteProcessGroupEntity", "component"))

        // Act
        Response response = jsonCCEM.toResponse(ife)
        logger.info(response.toString())

        // Assert
        assert response.status == Response.Status.BAD_REQUEST.statusCode
        assert response.entity == "The provided proxyPort value \'thisIsAnInvalidPort\' is not of required type class java.lang.Integer"
    }

    @Test
    void testShouldSanitizeScriptInInput() throws Exception{
        // Arrange
        JsonContentConversionExceptionMapper jsonCCEM = new JsonContentConversionExceptionMapper();

        // Using real exception
        Class<?> instClass = Integer.class
        def mockParser = [getTokenLocation: { -> return new JsonLocation(null, 100, 1, 1)}] as JsonParser
        String message = "Some message"
        String value = "<script>alert(1);</script>"
        InvalidFormatException ife = InvalidFormatException.from(mockParser, message, value, instClass)
        JsonMappingException.wrapWithPath(ife, new JsonMappingException.Reference("RemoteProcessGroupDTO", "proxyPort"))
        JsonMappingException.wrapWithPath(ife, new JsonMappingException.Reference("RemoteProcessGroupEntity", "component"))

        // Act
        Response response = jsonCCEM.toResponse(ife)
        logger.info(response.toString())

        // Assert
        assert response.status == Response.Status.BAD_REQUEST.statusCode
        assert !(response.entity =~ /<script.*>/)
    }
}
