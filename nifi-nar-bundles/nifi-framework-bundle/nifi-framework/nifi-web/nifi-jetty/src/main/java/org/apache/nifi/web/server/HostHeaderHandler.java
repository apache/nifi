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
package org.apache.nifi.web.server;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.lang3.StringEscapeUtils;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.ScopedHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HostHeaderHandler extends ScopedHandler {
    private static final Logger logger = LoggerFactory.getLogger(HostHeaderHandler.class);

    private final String serverName;
    private final int serverPort;
    private final List<String> validHosts;

    /**
     * @param serverName the {@code serverName} to set on the request (the {@code serverPort} will not be set)
     */
    public HostHeaderHandler(String serverName) {
        this(serverName, 0);
    }

    /**
     * @param serverName the {@code serverName} to set on the request
     * @param serverPort the {@code serverPort} to set on the request
     */
    public HostHeaderHandler(String serverName, int serverPort) {
        this.serverName = Objects.requireNonNull(serverName);
        this.serverPort = serverPort;

        validHosts = new ArrayList<>();
        validHosts.add(serverName.toLowerCase());
        validHosts.add(serverName.toLowerCase() + ":" + serverPort);
        // Sometimes the hostname is left empty but the port is always populated
        validHosts.add("localhost");
        validHosts.add("localhost:" + serverPort);
        // Different from customizer -- empty is ok here
        validHosts.add("");

        logger.info("Created " + this.toString());
    }

    @Override
    public void doScope(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
        logger.debug("HostHeaderHandler#doScope on " + request.getRequestURI());
        nextScope(target, baseRequest, request, response);
    }

    private boolean hostHeaderIsValid(String hostHeader) {
        return validHosts.contains(hostHeader.toLowerCase().trim());
    }

    @Override
    public String toString() {
        return "HostHeaderHandler for " + serverName + ":" + serverPort;
    }

    /**
     * Returns an error message to the response and marks the request as handled if the host header is not valid.
     * Otherwise passes the request on to the next scoped handler.
     *
     * @param target the target (not relevant here)
     * @param baseRequest the original request object
     * @param request the request as an HttpServletRequest
     * @param response the current response
     */
    @Override
    public void doHandle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        final String hostHeader = request.getHeader("Host");
        logger.debug("Received request [" + request.getRequestURI() + "] with host header: " + hostHeader);
        if (!hostHeaderIsValid(hostHeader)) {
            logger.warn("Request host header [" + hostHeader + "] different from web hostname [" +
                    serverName + "(:" + serverPort + ")]. Overriding to [" + serverName + ":" +
                    serverPort + request.getRequestURI() + "]");

            response.setContentType("text/html; charset=utf-8");
            response.setStatus(HttpServletResponse.SC_OK);

            PrintWriter out = response.getWriter();

            out.println("<h1>System Error</h1>");
            out.println("<h2>The request contained an invalid host header [" + StringEscapeUtils.escapeHtml4(hostHeader) +
                    "] in the request [" + StringEscapeUtils.escapeHtml4(request.getRequestURI()) +
                    "]. Check for request manipulation or third-party intercept. </h2>");

            baseRequest.setHandled(true);
        }
    }
}
