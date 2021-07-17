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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.web.util.WebUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This filter intercepts a request and populates the {@code contextPath} attribute on the request with a sanitized value (originally) retrieved from {@code nifi.properties}.
 */
public class SanitizeContextPathFilter implements Filter {
    private static final Logger logger = LoggerFactory.getLogger(SanitizeContextPathFilter.class);
    private static final String ALLOWED_CONTEXT_PATHS_PARAMETER_NAME = "allowedContextPaths";

    private String allowedContextPaths = "";

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        String providedAllowedList = filterConfig.getServletContext().getInitParameter(ALLOWED_CONTEXT_PATHS_PARAMETER_NAME);

        logger.debug("SanitizeContextPathFilter received provided allowed context paths from NiFi properties: " + providedAllowedList);
        if (StringUtils.isNotBlank(providedAllowedList)) {
            allowedContextPaths = providedAllowedList;
        }
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain) throws IOException, ServletException {
        // Inject the contextPath attribute into the request
        injectContextPathAttribute(request);

        // Pass execution to the next filter in the chain
        filterChain.doFilter(request, response);
    }

    /**
     * Determines, sanitizes, and injects the {@code contextPath} attribute into the {@code request}. If not present, an empty string {@code ""} is injected.
     *
     * @param request the request
     */
    protected void injectContextPathAttribute(ServletRequest request) {
        // Capture the provided context path headers and sanitize them before using in the response
        String contextPath = WebUtils.sanitizeContextPath(request, allowedContextPaths, "");
        request.setAttribute("contextPath", contextPath);

        logger.debug("SanitizeContextPathFilter set contextPath: " + contextPath);
    }

    @Override
    public void destroy() {
    }

    /**
     * Getter for allowed context paths. Cannot be package-private because of an issue where the package is scoped per classloader.
     *
     * @return the allowed context path(s)
     */
    protected String getAllowedContextPaths() {
        return allowedContextPaths;
    }
}
