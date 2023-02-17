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
package org.apache.nifi.web.security.requests

import org.apache.commons.lang3.StringUtils
import org.apache.nifi.stream.io.StreamUtils
import org.eclipse.jetty.server.LocalConnector
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.FilterHolder
import org.eclipse.jetty.servlet.ServletContextHandler
import org.eclipse.jetty.servlet.ServletHolder
import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.servlet.DispatcherType
import javax.servlet.ServletException
import javax.servlet.ServletInputStream
import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import java.util.concurrent.TimeUnit

@RunWith(JUnit4.class)
class ContentLengthFilterTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(ContentLengthFilterTest.class)

    private static final int MAX_CONTENT_LENGTH = 1000
    private static final int SERVER_IDLE_TIMEOUT = 2500 // only one request needed + value large enough for slow systems
    private static final String POST_REQUEST = "POST / HTTP/1.1\r\nContent-Length: %d\r\nHost: h\r\n\r\n%s"
    private static final String FORM_REQUEST = "POST / HTTP/1.1\r\nContent-Length: %d\r\nHost: h\r\nContent-Type: application/x-www-form-urlencoded\r\nAccept-Charset: UTF-8\r\n\r\n%s"
    public static final int FORM_CONTENT_SIZE = 128

    // These variables hold data for content small enough to be allowed
    private static final int SMALL_CLAIM_SIZE_BYTES = 150
    private static final String SMALL_PAYLOAD = "1" * SMALL_CLAIM_SIZE_BYTES

    // These variables hold data for content too large to be allowed
    private static final int LARGE_CLAIM_SIZE_BYTES = 2000
    private static final String LARGE_PAYLOAD = "1" * LARGE_CLAIM_SIZE_BYTES

    private Server serverUnderTest
    private LocalConnector localConnector
    private ServletContextHandler contextUnderTest

    @BeforeClass
    static void setUpOnce() {
        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Before
    void setUp() {

    }

    @After
    void tearDown() {
        stopServer()
    }

    void stopServer() throws Exception {
        if (serverUnderTest && serverUnderTest.isRunning()) {
            serverUnderTest.stop()
        }
    }

    private void configureAndStartServer(HttpServlet servlet, int maxFormContentSize) throws Exception {
        serverUnderTest = new Server()
        localConnector = new LocalConnector(serverUnderTest)
        localConnector.setIdleTimeout(SERVER_IDLE_TIMEOUT)
        serverUnderTest.addConnector(localConnector)

        contextUnderTest = new ServletContextHandler(serverUnderTest, "/")
        if (maxFormContentSize > 0) {
            contextUnderTest.setMaxFormContentSize(maxFormContentSize)
        }
        contextUnderTest.addServlet(new ServletHolder(servlet), "/*")

        // This only adds the ContentLengthFilter if a valid maxFormContentSize is not provided
        if (maxFormContentSize < 0) {
            FilterHolder holder = contextUnderTest.addFilter(ContentLengthFilter.class, "/*", EnumSet.of(DispatcherType.REQUEST) as EnumSet<DispatcherType>)
            holder.setInitParameter(ContentLengthFilter.MAX_LENGTH_INIT_PARAM, String.valueOf(MAX_CONTENT_LENGTH))
        }
        serverUnderTest.start()
    }

    /**
     * Initializes a server which consumes any provided request input stream and returns HTTP 200. It has no
     * {@code maxFormContentSize}, so the {@link ContentLengthFilter} is applied. The response contains a header and the
     * response body indicating the total number of request content bytes read.
     *
     * @throws Exception if there is a problem setting up the server
     */
    private void createSimpleReadServer() throws Exception {
        HttpServlet mockServlet = [
                doPost: { HttpServletRequest req, HttpServletResponse resp ->
                    byte[] byteBuffer = new byte[2048]
                    int bytesRead = StreamUtils.fillBuffer(req.getInputStream(), byteBuffer, false)
                    resp.setHeader("Bytes-Read", bytesRead as String)
                    resp.setStatus(HttpServletResponse.SC_OK)
                    resp.getWriter().write("Read ${bytesRead} bytes of request input")
                }
        ] as HttpServlet
        configureAndStartServer(mockServlet, -1)
    }

    private static void logResponse(String response, String s = "Response: ") {
        String responseId = String.valueOf(System.currentTimeMillis() % 100)
        final String delimiterLine = "\n-----" + responseId + "-----\n"
        String formattedResponse = s + delimiterLine + response + delimiterLine
        logger.info(formattedResponse)
    }

    @Test
    void testRequestsWithMissingContentLengthHeader() throws Exception {
        createSimpleReadServer()

        // This shows that the ContentLengthFilter allows a request that does not have a content-length header.
        String response = localConnector.getResponse("POST / HTTP/1.0\r\n\r\n")
        Assert.assertFalse(StringUtils.containsIgnoreCase(response, "411 Length Required"))
    }

    /**
     * This shows that the ContentLengthFilter rejects a request when the client claims more than the max + sends more than
     * the max.
     */
    @Test
    void testShouldRejectRequestWithLongContentLengthHeader() throws Exception {
        // Arrange
        createSimpleReadServer()
        final String requestBody = String.format(POST_REQUEST, LARGE_CLAIM_SIZE_BYTES, LARGE_PAYLOAD)
        logger.info("Making request with CL: ${LARGE_CLAIM_SIZE_BYTES} and actual length: ${LARGE_PAYLOAD.length()}")

        // Act
        String response = localConnector.getResponse(requestBody)
        logResponse(response)

        // Assert
        assert response =~ "413 Payload Too Large"
    }

    /**
     * This shows that the ContentLengthFilter rejects a request when the client claims more than the max + sends less than
     * the claim.
     */
    @Test
    void testShouldRejectRequestWithLongContentLengthHeaderAndSmallPayload() throws Exception {
        // Arrange
        createSimpleReadServer()

        String incompletePayload = "1" * (SMALL_CLAIM_SIZE_BYTES / 2)
        final String requestBody = String.format(POST_REQUEST, LARGE_CLAIM_SIZE_BYTES, incompletePayload)
        logger.info("Making request with CL: ${LARGE_CLAIM_SIZE_BYTES} and actual length: ${incompletePayload.length()}")

        // Act
        String response = localConnector.getResponse(requestBody)
        logResponse(response)

        // Assert
        assert response =~ "413 Payload Too Large"
    }

    /**
     * This shows that the ContentLengthFilter <em>allows</em> a request when the client claims less
     * than the max + sends more than the max, but restricts the request body to the stated content
     * length size.
     */
    @Test
    void testShouldRejectRequestWithSmallContentLengthHeaderAndLargePayload() throws Exception {
        // Arrange
        createSimpleReadServer()
        final String requestBody = String.format(POST_REQUEST, SMALL_CLAIM_SIZE_BYTES, LARGE_PAYLOAD)
        logger.info("Making request with CL: ${SMALL_CLAIM_SIZE_BYTES} and actual length: ${LARGE_PAYLOAD.length()}")

        // Act
        String response = localConnector.getResponse(requestBody)
        logResponse(response)

        // Assert
        assert response =~ "200"
        assert response =~ "Bytes-Read: ${SMALL_CLAIM_SIZE_BYTES}"
        assert response =~ "Read ${SMALL_CLAIM_SIZE_BYTES} bytes"

    }

    /**
     * This shows that the server times out when the client claims less than the max + sends less than the max + sends
     * less than it claims to send.
     */
    @Test
    void testShouldTimeoutRequestWithSmallContentLengthHeaderAndSmallerPayload() throws Exception {
        // Arrange
        createSimpleReadServer()

        String smallerPayload = SMALL_PAYLOAD[0..(SMALL_PAYLOAD.length() / 2)]
        final String requestBody = String.format(POST_REQUEST, SMALL_CLAIM_SIZE_BYTES, smallerPayload)
        logger.info("Making request with CL: ${SMALL_CLAIM_SIZE_BYTES} and actual length: ${smallerPayload.length()}")

        // Act
        String response = localConnector.getResponse(requestBody, 500, TimeUnit.MILLISECONDS)
        logResponse(response)

        // Assert
        assert response =~ "500 Server Error"
        assert response =~ "Timeout"
    }

    @Test
    void testFilterShouldAllowSiteToSiteTransfer() throws Exception {
        // Arrange
        createSimpleReadServer()

        final String SITE_TO_SITE_POST_REQUEST = "POST /nifi-api/data-transfer/input-ports HTTP/1.1\r\nContent-Length: %d\r\nHost: h\r\n\r\n%s"

        final String siteToSiteRequest = String.format(SITE_TO_SITE_POST_REQUEST, LARGE_CLAIM_SIZE_BYTES, LARGE_PAYLOAD)
        logResponse(siteToSiteRequest, "Request: ")

        // Act
        String response = localConnector.getResponse(siteToSiteRequest)
        logResponse(response)

        // Assert
        assert response =~ "200 OK"
    }

    @Test
    void testJettyMaxFormSize() throws Exception {
        // This shows that the jetty server option for 'maxFormContentSize' is insufficient for our needs because it
        // catches requests like this:

        // Configure the server but do not apply the CLF because the FORM_CONTENT_SIZE > 0
        configureAndStartServer(new HttpServlet() {
            @Override
            protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
                try {
                    req.getParameterMap()
                    ServletInputStream input = req.getInputStream()
                    int count = 0
                    while (!input.isFinished()) {
                        input.read()
                        count += 1
                    }
                    final int FORM_LIMIT_BYTES = FORM_CONTENT_SIZE + "a=\n".length()
                    if (count > FORM_LIMIT_BYTES) {
                        logger.warn("Bytes read ({}) is larger than the limit ({})", count, FORM_LIMIT_BYTES)
                        resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Should not reach this code.")
                    } else {
                        logger.warn("Bytes read ({}) is less than or equal to the limit ({})", count, FORM_LIMIT_BYTES)
                        resp.sendError(HttpServletResponse.SC_EXPECTATION_FAILED, "Read Too Many Bytes")
                    }
                } catch (final Exception e) {
                    // This is the jetty context returning a 400 from the maxFormContentSize setting:
                    if (StringUtils.containsIgnoreCase(e.getCause().toString(), "Form is larger than max length " + FORM_CONTENT_SIZE)) {
                        logger.warn("Exception thrown by input stream: ", e)
                        resp.sendError(HttpServletResponse.SC_REQUEST_ENTITY_TOO_LARGE, "Payload Too Large")
                    } else {
                        logger.warn("Exception thrown by input stream: ", e)
                        resp.sendError(HttpServletResponse.SC_FORBIDDEN, "Should not reach this code, either.")
                    }
                }
            }
        }, FORM_CONTENT_SIZE)

        // Test to catch a form submission that exceeds the FORM_CONTENT_SIZE limit
        String form = "a=" + "1" * FORM_CONTENT_SIZE
        String response = localConnector.getResponse(String.format(FORM_REQUEST, form.length(), form))
        logResponse(response)
        assert response =~ "413 Payload Too Large"


        // But it does not catch requests like this:
        response = localConnector.getResponse(String.format(POST_REQUEST, form.length(), form + form))
        assert response =~ "417 Read Too Many Bytes"
    }
}