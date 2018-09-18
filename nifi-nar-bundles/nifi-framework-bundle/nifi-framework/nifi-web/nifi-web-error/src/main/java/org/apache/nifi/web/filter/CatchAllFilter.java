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
package org.apache.nifi.web.filter;

import java.io.IOException;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This filter catches all requests and explicitly forwards them to a JSP ({@code index.jsp} injected in
 * the filter configuration. This is used to handle all errors (this module is only used for errors). It
 * extends {@link SanitizeContextPathFilter} which sanitizes the context path and injects it as a request
 * attribute to be used on the page for linking resources without XSS vulnerabilities.
 */
public class CatchAllFilter extends SanitizeContextPathFilter {
    private static final Logger logger = LoggerFactory.getLogger(CatchAllFilter.class);

    private String forwardPath = "";
    private String displayPath = "";

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        // TODO: Perform path validation (against what set of rules)?
        forwardPath = filterConfig.getInitParameter("forwardPath");
        displayPath = filterConfig.getInitParameter("displayPath");

        logger.debug("CatchAllFilter  [" + displayPath + "] received provided whitelisted context paths from NiFi properties: " + getWhitelistedContextPaths());
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain) throws IOException, ServletException {
        // Inject the contextPath attribute into the request
        injectContextPathAttribute(request);

        // Forward all requests to index.jsp
        request.getRequestDispatcher(forwardPath).forward(request, response);
    }

    @Override
    public void destroy() {
    }
}
