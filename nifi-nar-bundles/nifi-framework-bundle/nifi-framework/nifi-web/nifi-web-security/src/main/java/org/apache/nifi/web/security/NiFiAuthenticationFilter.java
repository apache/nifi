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
package org.apache.nifi.web.security;

import java.io.IOException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.web.security.user.NiFiUserUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.context.SecurityContextHolder;

/**
 *
 */
public abstract class NiFiAuthenticationFilter implements Filter {

    private static final Logger logger = LoggerFactory.getLogger(NiFiAuthenticationFilter.class);

    private AuthenticationManager authenticationManager;

    @Override
    public void init(final FilterConfig filterConfig) throws ServletException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void doFilter(final ServletRequest request, final ServletResponse response, final FilterChain chain) throws IOException, ServletException {
        if (logger.isDebugEnabled()) {
            logger.debug("Checking secure context token: " + SecurityContextHolder.getContext().getAuthentication());
        }

        if (requiresAuthentication((HttpServletRequest) request)) {
            authenticate((HttpServletRequest) request, (HttpServletResponse) response);
        }

        chain.doFilter(request, response);
    }

    private boolean requiresAuthentication(final HttpServletRequest request) {
        return NiFiUserUtils.getNiFiUser() == null && NiFiUserUtils.getNewAccountRequest() == null;
    }

    private void authenticate(final HttpServletRequest request, final HttpServletResponse response) {
        try {
            final Authentication authenticated = attemptAuthentication(request, response);
            if (authenticated != null) {
                final Authentication authorized = authenticationManager.authenticate(authenticated);
                successfulAuthorization(request, response, authorized);
            }
        } catch (final AuthenticationException ae) {
            unsuccessfulAuthorization(request, response, ae);
        }
    }

    public abstract Authentication attemptAuthentication(HttpServletRequest request, HttpServletResponse response);

    protected void successfulAuthorization(HttpServletRequest request, HttpServletResponse response, Authentication authResult) {
        if (logger.isDebugEnabled()) {
            logger.debug("Authentication success: " + authResult);
        }

        SecurityContextHolder.getContext().setAuthentication(authResult);
        ProxiedEntitiesUtils.successfulAuthorization(request, response, authResult);
    }

    protected void unsuccessfulAuthorization(HttpServletRequest request, HttpServletResponse response, AuthenticationException failed) {
        ProxiedEntitiesUtils.unsuccessfulAuthorization(request, response, failed);
    }

    /**
     * Determines if the specified request is attempting to register a new user account.
     *
     * @param request http request
     * @return true if new user
     */
    protected final boolean isNewAccountRequest(HttpServletRequest request) {
        if ("POST".equalsIgnoreCase(request.getMethod())) {
            String path = request.getPathInfo();
            if (StringUtils.isNotBlank(path)) {
                if ("/controller/users".equals(path)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Extracts the justification from the specified request.
     *
     * @param request The request
     * @return The justification
     */
    protected final String getJustification(HttpServletRequest request) {
        // get the justification
        String justification = request.getParameter("justification");
        if (justification == null) {
            justification = StringUtils.EMPTY;
        }
        return justification;
    }

    @Override
    public void destroy() {
    }

    public void setAuthenticationManager(AuthenticationManager authenticationManager) {
        this.authenticationManager = authenticationManager;
    }

}
