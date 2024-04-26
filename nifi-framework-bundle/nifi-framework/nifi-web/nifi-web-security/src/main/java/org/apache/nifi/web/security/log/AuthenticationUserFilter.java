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
package org.apache.nifi.web.security.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.OncePerRequestFilter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Authentication User Filter sets authentication username in request attribute for later retrieval and logging
 */
public class AuthenticationUserFilter extends OncePerRequestFilter {
    private static final Logger logger = LoggerFactory.getLogger(AuthenticationUserFilter.class);

    /**
     * Read Authentication username from Spring Security Context and set username request attribute when found
     *
     * @param request     HTTP Servlet Request
     * @param response    HTTP Servlet Response
     * @param filterChain Filter Chain
     * @throws ServletException Thrown on FilterChain.doFilter()
     * @throws IOException      Thrown on FilterChain.doFilter()
     */
    @Override
    protected void doFilterInternal(final HttpServletRequest request, final HttpServletResponse response, final FilterChain filterChain) throws ServletException, IOException {
        final SecurityContext securityContext = SecurityContextHolder.getContext();
        final Authentication authentication = securityContext.getAuthentication();
        if (authentication == null) {
            logger.debug("Authentication not found in Security Context for Remote Address [{}]", request.getRemoteAddr());
        } else {
            final String username = authentication.getName();
            request.setAttribute(AuthenticationUserAttribute.USERNAME.getName(), username);
        }
        filterChain.doFilter(request, response);
    }
}
