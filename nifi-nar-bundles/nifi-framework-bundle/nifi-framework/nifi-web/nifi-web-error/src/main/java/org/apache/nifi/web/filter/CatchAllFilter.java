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
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.UriBuilderException;
import org.apache.nifi.web.util.WebUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filter for forward all requests to index.jsp.
 */
public class CatchAllFilter implements Filter {
    private static final Logger logger = LoggerFactory.getLogger(CatchAllFilter.class);

    private static String whitelistedContextPaths = "";

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        String providedWhitelist = filterConfig.getServletContext().getInitParameter("whitelistedContextPaths");
        logger.debug("CatchAllFilter received provided whitelisted context paths from NiFi properties: " + providedWhitelist);
        if (providedWhitelist != null) {
            whitelistedContextPaths = providedWhitelist;
        }
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain) throws IOException, ServletException {
        // Capture the provided context path headers and sanitize them before using in the response
        String contextPath = getSanitizedContextPath(request);
        request.setAttribute("contextPath", contextPath);

        // for all requests to index.jsp
        request.getRequestDispatcher("/index.jsp").forward(request, response);
    }

    @Override
    public void destroy() {
    }

    /**
     * Returns a "safe" context path value from the request headers to use in a proxy environment.
     * This is used on the JSP to build the resource paths for the external resources (CSS, JS, etc.).
     * If no headers are present specifying this value, it is an empty string.
     *
     * @param request the HTTP request
     * @return the context path safe to be printed to the page
     */
    private String getSanitizedContextPath(ServletRequest request) {
        String contextPath = WebUtils.normalizeContextPath(WebUtils.determineContextPath((HttpServletRequest) request));
        try {
            WebUtils.verifyContextPath(whitelistedContextPaths, contextPath);
            return contextPath;
        } catch (UriBuilderException e) {
            logger.error("Error determining context path on index.jsp: " + e.getMessage());
            return "";
        }
    }
}
