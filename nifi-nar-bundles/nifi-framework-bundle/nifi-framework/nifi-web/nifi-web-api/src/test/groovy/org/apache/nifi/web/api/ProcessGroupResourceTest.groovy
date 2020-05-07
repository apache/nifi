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

import org.apache.nifi.authorization.AuthorizeAccess
import org.apache.nifi.util.NiFiProperties
import org.apache.nifi.web.NiFiServiceFacade
import org.apache.nifi.web.api.dto.FlowSnippetDTO
import org.apache.nifi.web.api.dto.TemplateDTO
import org.apache.nifi.web.api.entity.TemplateEntity
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestName
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.servlet.http.HttpServletRequest
import javax.ws.rs.core.Response
import javax.ws.rs.core.UriInfo

@RunWith(JUnit4.class)
class ProcessGroupResourceTest extends GroovyTestCase {
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

    /** This test creates a malformed template upload request to exercise error handling and sanitization */
    @Test
    void testUploadShouldHandleMalformedTemplate() {
        // Arrange
        ProcessGroupResource pgResource = new ProcessGroupResource()

        // Mocking the returned template object to throw a specific exception would be nice
        final String TEMPLATE_WITH_XSS_PLAIN = "<?xml version=\"1.0\" encoding='><script xmlns=\"http://www.w3.org/1999/xhtml\">alert(JSON.stringify(localstorage));</script><errorResponse test='?>"
        logger.info("Malformed template XML: ${TEMPLATE_WITH_XSS_PLAIN}")
        InputStream contentInputStream = new ByteArrayInputStream(TEMPLATE_WITH_XSS_PLAIN.bytes)

        HttpServletRequest mockRequest = [:] as HttpServletRequest
        UriInfo mockUriInfo = [:] as UriInfo
        String groupId = "1"

        // Build a malformed template object which can be unmarshalled from XML

        // Act

        // Try to submit the malformed template
        Response response = pgResource.uploadTemplate(mockRequest, mockUriInfo, groupId, false, contentInputStream)
        logger.info("Response: ${response}")

        // Assert

        // Assert that the expected error response was returned
        assert response.status == Response.Status.OK.statusCode

        // Assert that the error response is sanitized
        String responseEntity = response.entity as String
        logger.info("Error response: ${responseEntity}")
        assert !(responseEntity =~ /<script.*>/)
    }

    /** This test creates a malformed template import request to exercise error handling and sanitization */
    @Test
    void testImportShouldHandleMalformedTemplate() {
        // Arrange
        ProcessGroupResource pgResource = new ProcessGroupResource()

        // Configure parent fields for write lock process
        pgResource.properties = [isNode: { -> return false }] as NiFiProperties
        pgResource.serviceFacade = [
                authorizeAccess     : { AuthorizeAccess a -> },
                verifyCanAddTemplate: { String gid, String templateName -> },
                importTemplate      : { TemplateDTO template, String gid, Optional<String> seedId ->
                    logger.mock("Called importTemplate;")
                    template
                }
        ] as NiFiServiceFacade
        pgResource.templateResource = [
                populateRemainingTemplateContent: { TemplateDTO td -> }
        ] as TemplateResource

        final String TEMPLATE_WITH_XSS_PLAIN = "<?xml version=\"1.0\" encoding='><script xmlns=\"http://www.w3.org/1999/xhtml\">alert(JSON.stringify(localstorage));</script><errorResponse test='?>"
        logger.info("Malformed template XML: ${TEMPLATE_WITH_XSS_PLAIN}")

        TemplateDTO mockIAETemplate = [
                getName   : { -> "mockIAETemplate" },
                getUri    : { ->
                    throw new IllegalArgumentException("Expected exception with <script> element")
                },
                getSnippet: { -> new FlowSnippetDTO() }
        ] as TemplateDTO

        TemplateDTO mockExceptionTemplate = [
                getName   : { -> "mockExceptionTemplate" },
                getUri    : { ->
                    throw new RuntimeException("Expected exception with <script> element")
                },
                getSnippet: { -> new FlowSnippetDTO() }
        ] as TemplateDTO

        TemplateEntity mockIAETemplateEntity = [getTemplate: { ->
            mockIAETemplate
        }] as TemplateEntity

        TemplateEntity mockExceptionTemplateEntity = [getTemplate: { ->
            mockExceptionTemplate
        }] as TemplateEntity

        // Override the request object and store it for ApplicationResource#withWriteLock
        HttpServletRequest mockRequest = [getHeader: { String headerName ->
            logger.mock("Requesting header ${headerName}; returning null")
            null
        }] as HttpServletRequest

        // Set the persisted request object so the parent ApplicationResource can use it
        pgResource.httpServletRequest = mockRequest
        String groupId = "1"

        // Act
        List<Response> responses = [mockIAETemplateEntity, mockExceptionTemplateEntity].collect { TemplateEntity te ->
            // Try to submit the malformed template which throws some kind of exception
            Response response = pgResource.importTemplate(mockRequest, groupId, te)
            logger.info("Response: ${response}")
            response
        }

        // Assert
        responses.each { Response r ->
            // Assert that the expected error response was returned
            assert r.status == Response.Status.OK.statusCode

            // Assert that the error response is sanitized
            String entity = r.entity as String
            logger.info("Error response: ${entity}")
            assert !(entity =~ /<script.*>/)
        }
    }
}
