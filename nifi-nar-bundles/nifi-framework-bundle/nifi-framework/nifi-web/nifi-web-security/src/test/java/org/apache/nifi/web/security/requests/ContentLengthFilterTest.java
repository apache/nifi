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
package org.apache.nifi.web.security.requests;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.stream.io.StreamUtils;
import org.eclipse.jetty.server.LocalConnector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.servlet.DispatcherType;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
class ContentLengthFilterTest {
    private static final String POST_REQUEST = "POST / HTTP/1.1\r\nContent-Length: %d\r\nHost: h\r\n\r\n%s";
    public static final int FORM_CONTENT_SIZE = 128;

    // These variables hold data for content small enough to be allowed
    private static final int SMALL_CLAIM_SIZE_BYTES = 150;
    private static final String SMALL_PAYLOAD = StringUtils.repeat("1", SMALL_CLAIM_SIZE_BYTES);

    // These variables hold data for content too large to be allowed
    private static final int LARGE_CLAIM_SIZE_BYTES = 2000;
    private static final String LARGE_PAYLOAD = StringUtils.repeat("1", LARGE_CLAIM_SIZE_BYTES);

    private Server serverUnderTest;
    private LocalConnector localConnector;

    @BeforeEach
    public void setUp() throws Exception {
        createSimpleReadServer();
    }

    @AfterEach
    public void tearDown() throws Exception {
        stopServer();
    }

    @Test
    public void testRequestsWithMissingContentLengthHeader() throws Exception {
        // This shows that the ContentLengthFilter allows a request that does not have a content-length header.
        String response = localConnector.getResponse("POST / HTTP/1.0\r\n\r\n");
        assertFalse(StringUtils.containsIgnoreCase(response, "411 Length Required"));
    }

    /**
     * This shows that the ContentLengthFilter rejects a request when the client claims more than the max + sends more than
     * the max.
     */
    @Test
    public void testShouldRejectRequestWithLongContentLengthHeader() throws Exception {
        final String requestBody = String.format(POST_REQUEST, LARGE_CLAIM_SIZE_BYTES, LARGE_PAYLOAD);
        String response = localConnector.getResponse(requestBody);

        assertTrue(response.contains("413 Payload Too Large"));
    }

    /**
     * This shows that the ContentLengthFilter rejects a request when the client claims more than the max + sends less than
     * the claim.
     */
    @Test
    public void testShouldRejectRequestWithLongContentLengthHeaderAndSmallPayload() throws Exception {
        String incompletePayload = StringUtils.repeat("1", SMALL_CLAIM_SIZE_BYTES / 2);
        final String requestBody = String.format(POST_REQUEST, LARGE_CLAIM_SIZE_BYTES, incompletePayload);
        String response = localConnector.getResponse(requestBody);

        assertTrue(response.contains("413 Payload Too Large"));
    }

    /**
     * This shows that the ContentLengthFilter <em>allows</em> a request when the client claims less
     * than the max + sends more than the max, but restricts the request body to the stated content
     * length size.
     */
    @Test
    public void testShouldRejectRequestWithSmallContentLengthHeaderAndLargePayload() throws Exception {
        final String requestBody = String.format(POST_REQUEST, SMALL_CLAIM_SIZE_BYTES, LARGE_PAYLOAD);
        String response = localConnector.getResponse(requestBody);

        assertTrue(response.contains("200"));
        assertTrue(response.contains("Bytes-Read: " + SMALL_CLAIM_SIZE_BYTES));
        assertTrue(response.contains("Read " + SMALL_CLAIM_SIZE_BYTES + " bytes"));
    }

    /**
     * This shows that the server times out when the client claims less than the max + sends less than the max + sends
     * less than it claims to send.
     */
    @Test
    public void testShouldTimeoutRequestWithSmallContentLengthHeaderAndSmallerPayload() throws Exception {
        String smallerPayload = SMALL_PAYLOAD.substring(0, SMALL_PAYLOAD.length() / 2);
        final String requestBody = String.format(POST_REQUEST, SMALL_CLAIM_SIZE_BYTES, smallerPayload);
        String response = localConnector.getResponse(requestBody, 500, TimeUnit.MILLISECONDS);

        assertTrue(response.contains("500 Server Error"));
        assertTrue(response.contains("Timeout"));
    }

    @Test
    public void testFilterShouldAllowSiteToSiteTransfer() throws Exception {
        final String siteToSitePostRequest = "POST /nifi-api/data-transfer/input-ports HTTP/1.1\r\nContent-Length: %d\r\nHost: h\r\n\r\n%s";
        final String siteToSiteRequest = String.format(siteToSitePostRequest, LARGE_CLAIM_SIZE_BYTES, LARGE_PAYLOAD);
        String response = localConnector.getResponse(siteToSiteRequest);

        assertTrue(response.contains("200 OK"));
    }

    @Test
    void testJettyMaxFormSize() throws Exception {
        // This shows that the jetty server option for 'maxFormContentSize' is insufficient for our needs because it
        // catches requests like this:

        // Configure the server but do not apply the CLF because the FORM_CONTENT_SIZE > 0
        configureAndStartServer(new HttpServlet() {
            @Override
            protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
                try {
                    req.getParameterMap();
                    ServletInputStream input = req.getInputStream();
                    int count = 0;
                    while (!input.isFinished()) {
                        input.read();
                        count += 1;
                    }
                    final int formLimitBytes = FORM_CONTENT_SIZE + "a=\n".length();
                    if (count > formLimitBytes) {
                        resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Should not reach this code.");
                    } else {
                        resp.sendError(HttpServletResponse.SC_EXPECTATION_FAILED, "Read Too Many Bytes");
                    }
                } catch (final Exception e) {
                    // This is the jetty context returning a 400 from the maxFormContentSize setting:
                    if (StringUtils.containsIgnoreCase(e.getCause().toString(), "Form is larger than max length " + FORM_CONTENT_SIZE)) {
                        resp.sendError(HttpServletResponse.SC_REQUEST_ENTITY_TOO_LARGE, "Payload Too Large");
                    } else {
                        resp.sendError(HttpServletResponse.SC_FORBIDDEN, "Should not reach this code, either.");
                    }
                }
            }
        }, FORM_CONTENT_SIZE);

        // Test to catch a form submission that exceeds the FORM_CONTENT_SIZE limit
        String form = "a=" + StringUtils.repeat("1", FORM_CONTENT_SIZE);
        final String formRequest = "POST / HTTP/1.1\r\nContent-Length: %d\r\nHost: h\r\nContent-Type: application/x-www-form-urlencoded\r\nAccept-Charset: UTF-8\r\n\r\n%s";
        String response = localConnector.getResponse(String.format(formRequest, form.length(), form));

        assertTrue(response.contains("413 Payload Too Large"));

        // But it does not catch requests like this:
        response = localConnector.getResponse(String.format(POST_REQUEST, form.length(), form + form));
        assertTrue(response.contains("417 Read Too Many Bytes"));
    }

    /**
     * Initializes a server which consumes any provided request input stream and returns HTTP 200. It has no
     * {@code maxFormContentSize}, so the {@link ContentLengthFilter} is applied. The response contains a header and the
     * response body indicating the total number of request content bytes read.
     *
     * @throws Exception if there is a problem setting up the server
     */
    private void createSimpleReadServer() throws Exception {
        HttpServlet mockServlet = new HttpServlet() {
            @Override
            protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
                byte[] byteBuffer = new byte[2048];
                int bytesRead = StreamUtils.fillBuffer(req.getInputStream(), byteBuffer, false);
                resp.setHeader("Bytes-Read", Integer.toString(bytesRead));
                resp.setStatus(HttpServletResponse.SC_OK);
                resp.getWriter().write("Read " + bytesRead + " bytes of request input");
            }
        };
        configureAndStartServer(mockServlet, -1);
    }

    private void configureAndStartServer(HttpServlet servlet, int maxFormContentSize) throws Exception {
        serverUnderTest = new Server();
        localConnector = new LocalConnector(serverUnderTest);
        localConnector.setIdleTimeout(2500);  // only one request needed + value large enough for slow systems
        serverUnderTest.addConnector(localConnector);

        ServletContextHandler contextUnderTest = new ServletContextHandler(serverUnderTest, "/");
        if (maxFormContentSize > 0) {
            contextUnderTest.setMaxFormContentSize(maxFormContentSize);
        }
        contextUnderTest.addServlet(new ServletHolder(servlet), "/*");

        // This only adds the ContentLengthFilter if a valid maxFormContentSize is not provided
        if (maxFormContentSize < 0) {
            FilterHolder holder = contextUnderTest.addFilter(ContentLengthFilter.class, "/*", EnumSet.of(DispatcherType.REQUEST));
            holder.setInitParameter(ContentLengthFilter.MAX_LENGTH_INIT_PARAM, String.valueOf(1000));
        }
        serverUnderTest.start();
    }

    void stopServer() throws Exception {
        if (serverUnderTest != null && serverUnderTest.isRunning()) {
            serverUnderTest.stop();
        }
    }
}
