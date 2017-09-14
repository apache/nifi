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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.AuthenticationServiceException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.GenericFilterBean;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

/**
 *
 */
public abstract class NiFiAuthenticationFilter extends GenericFilterBean {

    private static final Logger log = LoggerFactory.getLogger(NiFiAuthenticationFilter.class);

    private AuthenticationManager authenticationManager;
    private NiFiProperties properties;

    @Override
    public void doFilter(final ServletRequest request, final ServletResponse response, final FilterChain chain) throws IOException, ServletException {
        final Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (log.isDebugEnabled()) {
            log.debug("Checking secure context token: " + authentication);
        }

        if (requiresAuthentication((HttpServletRequest) request)) {
            authenticate((HttpServletRequest) request, (HttpServletResponse) response, chain);
        } else {
            chain.doFilter(request, response);
        }

    }

    private boolean requiresAuthentication(final HttpServletRequest request) {
        return NiFiUserUtils.getNiFiUser() == null;
    }

    private void authenticate(final HttpServletRequest request, final HttpServletResponse response, final FilterChain chain) throws IOException, ServletException {
        String dnChain = null;
        try {
            final Authentication authenticationRequest = attemptAuthentication(request);
            if (authenticationRequest != null) {
                // log the request attempt - response details will be logged later
                log.info(String.format("Attempting request for (%s) %s %s (source ip: %s)", authenticationRequest.toString(), request.getMethod(),
                        request.getRequestURL().toString(), request.getRemoteAddr()));

                // attempt to authorize the user
                final Authentication authenticated = authenticationManager.authenticate(authenticationRequest);
                successfulAuthorization(request, response, authenticated);
            }

            // continue
            chain.doFilter(request, response);
        } catch (final AuthenticationException ae) {
            // invalid authentication - always error out
            unsuccessfulAuthorization(request, response, ae);
        }
    }

    /**
     * Attempt to extract an authentication attempt from the specified request.
     *
     * @param request The request
     * @return The authentication attempt or null if none is found int he request
     */
    public abstract Authentication attemptAuthentication(HttpServletRequest request);

    protected void successfulAuthorization(HttpServletRequest request, HttpServletResponse response, Authentication authResult) {
        log.info("Authentication success for " + authResult);

        SecurityContextHolder.getContext().setAuthentication(authResult);
        ProxiedEntitiesUtils.successfulAuthorization(request, response, authResult);
    }

    protected void unsuccessfulAuthorization(HttpServletRequest request, HttpServletResponse response, AuthenticationException ae) throws IOException {
        // populate the response
        ProxiedEntitiesUtils.unsuccessfulAuthorization(request, response, ae);

        // set the response status
        response.setContentType("text/plain");

        // write the response message
        PrintWriter out = response.getWriter();

        // use the type of authentication exception to determine the response code
        if (ae instanceof InvalidAuthenticationException) {
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            out.println(ae.getMessage());
        } else if (ae instanceof UntrustedProxyException) {
            response.setStatus(HttpServletResponse.SC_FORBIDDEN);
            out.println(ae.getMessage());
        } else if (ae instanceof AuthenticationServiceException) {
            log.error(String.format("Unable to authorize: %s", ae.getMessage()), ae);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            out.println(String.format("Unable to authorize: %s", ae.getMessage()));
        } else {
            log.error(String.format("Unable to authorize: %s", ae.getMessage()), ae);
            response.setStatus(HttpServletResponse.SC_FORBIDDEN);
            out.println("Access is denied.");
        }

        // log the failure
        log.warn(String.format("Rejecting access to web api: %s", ae.getMessage()));

        // optionally log the stack trace
        if (log.isDebugEnabled()) {
            log.debug(StringUtils.EMPTY, ae);
        }
    }

    @Override
    public void destroy() {
    }

    public void setAuthenticationManager(AuthenticationManager authenticationManager) {
        this.authenticationManager = authenticationManager;
    }

    public void setProperties(NiFiProperties properties) {
        this.properties = properties;
    }

    public NiFiProperties getProperties() {
        return properties;
    }
}
