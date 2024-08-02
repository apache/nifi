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
package org.apache.nifi.web.server.filter;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.nifi.web.servlet.shared.RequestUriBuilder;

import java.io.IOException;
import java.net.URI;

/**
 * Logout Complete Redirect Filter for web user interface integration with fragment-based routing
 */
public class LogoutCompleteRedirectFilter implements Filter {
    private static final String LOGOUT_COMPLETE_PATH = "/nifi/logout-complete";

    private static final String USER_INTERFACE_PATH = "/nifi/";

    private static final String FRAGMENT_PATH = "/logout-complete";

    @Override
    public void doFilter(final ServletRequest servletRequest, final ServletResponse servletResponse, final FilterChain filterChain) throws IOException, ServletException {
        if (servletRequest instanceof HttpServletRequest httpServletRequest) {
            final String requestUri = httpServletRequest.getRequestURI();
            if (requestUri.endsWith(LOGOUT_COMPLETE_PATH)) {
                final HttpServletResponse httpServletResponse = (HttpServletResponse) servletResponse;
                doRedirect(httpServletRequest, httpServletResponse);
            } else {
                filterChain.doFilter(servletRequest, servletResponse);
            }
        } else {
            filterChain.doFilter(servletRequest, servletResponse);
        }
    }

    private void doRedirect(final HttpServletRequest httpServletRequest, final HttpServletResponse httpServletResponse) throws IOException {
        final URI redirectUri = RequestUriBuilder.fromHttpServletRequest(httpServletRequest)
                .path(USER_INTERFACE_PATH)
                .fragment(FRAGMENT_PATH)
                .build();
        final String redirectLocation = redirectUri.toString();
        httpServletResponse.sendRedirect(redirectLocation);
    }
}
