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
package org.apache.nifi.web.security.logout;

import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.authentication.logout.LogoutSuccessHandler;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;
import org.springframework.web.filter.OncePerRequestFilter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Standard Logout Filter completes application Logout Requests
 */
public class StandardLogoutFilter extends OncePerRequestFilter {
    private final AntPathRequestMatcher requestMatcher;

    private final LogoutSuccessHandler logoutSuccessHandler;

    public StandardLogoutFilter(
            final AntPathRequestMatcher requestMatcher,
            final LogoutSuccessHandler logoutSuccessHandler
    ) {
        this.requestMatcher = requestMatcher;
        this.logoutSuccessHandler = logoutSuccessHandler;
    }

    /**
     * Call Logout Success Handler when request path matches
     *
     * @param request HTTP Servlet Request
     * @param response HTTP Servlet Response
     * @param filterChain Filter Chain
     * @throws ServletException Thrown on FilterChain.doFilter() failures
     * @throws IOException Thrown on FilterChain.doFilter() failures
     */
    @Override
    protected void doFilterInternal(final HttpServletRequest request, final HttpServletResponse response, final FilterChain filterChain) throws ServletException, IOException {
        if (requestMatcher.matches(request)) {
            final SecurityContext securityContext = SecurityContextHolder.getContext();
            final Authentication authentication = securityContext.getAuthentication();
            logoutSuccessHandler.onLogoutSuccess(request, response, authentication);
        } else {
            filterChain.doFilter(request, response);
        }
    }
}
