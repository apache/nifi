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

import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.AuthenticationDetailsSource;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.AuthenticationServiceException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.GenericFilterBean;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * The NiFiAuthenticationFilter abstract class defines the base methods for NiFi's various existing and future
 * authentication mechanisms. The subclassed filters are applied in NiFiWebApiSecurityConfiguration.
 */
public abstract class NiFiAuthenticationFilter extends GenericFilterBean {

    private static final Logger log = LoggerFactory.getLogger(NiFiAuthenticationFilter.class);

    protected AuthenticationDetailsSource<HttpServletRequest, NiFiWebAuthenticationDetails> authenticationDetailsSource;
    private AuthenticationManager authenticationManager;
    private NiFiProperties properties;

    @Override
    public void doFilter(final ServletRequest request, final ServletResponse response, final FilterChain chain) throws IOException, ServletException {
        final Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        log.debug("Authenticating [{}]", authentication);

        if (requiresAuthentication()) {
            authenticate((HttpServletRequest) request, (HttpServletResponse) response, chain);
        } else {
            chain.doFilter(request, response);
        }
    }

    private boolean requiresAuthentication() {
        return NiFiUserUtils.getNiFiUser() == null;
    }

    private void authenticate(final HttpServletRequest request, final HttpServletResponse response, final FilterChain chain) throws IOException, ServletException {
        try {
            final Authentication authenticationRequest = attemptAuthentication(request);
            if (authenticationRequest != null) {
                log.debug("Authentication Started {} [{}] {} {}", request.getRemoteAddr(), authenticationRequest, request.getMethod(), request.getRequestURL());

                // attempt to authenticate the user
                final Authentication authenticated = authenticationManager.authenticate(authenticationRequest);
                successfulAuthentication(request, response, authenticated);
            }
        } catch (final AuthenticationException ae) {
            // invalid authentication - always error out
            unsuccessfulAuthentication(request, response, ae);
            return;
        } catch (final Exception e) {
            log.error("Authentication Failed [{}]", e.getMessage(), e);

            // set the response status
            response.setContentType("text/plain");
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);

            // other exception - always error out
            PrintWriter out = response.getWriter();
            out.println("Failed to authenticate request. Please contact the system administrator.");
            return;
        }

        // continue
        chain.doFilter(request, response);
    }

    /**
     * Attempt to extract an authentication attempt from the specified request.
     *
     * @param request The request
     * @return The authentication attempt or null if none is found int he request
     */
    public abstract Authentication attemptAuthentication(HttpServletRequest request);

    /**
     * If authentication was successful, apply the successful authentication result to the security context and add
     * proxy headers to the response if the request was made via a proxy.
     *
     * @param request The original client request that was successfully authenticated.
     * @param response Servlet response to the client containing the successful authentication details.
     * @param authResult The Authentication 'token'/object created by one of the various NiFiAuthenticationFilter subclasses.
     */
    protected void successfulAuthentication(HttpServletRequest request, HttpServletResponse response, Authentication authResult) {
        log.debug("Authentication Success [{}] {} {} {}", authResult, request.getRemoteAddr(), request.getMethod(), request.getRequestURL());

        SecurityContextHolder.getContext().setAuthentication(authResult);
        ProxiedEntitiesUtils.successfulAuthentication(request, response);
    }

    /**
     * If authentication was unsuccessful, update the response with the appropriate status and give the reason for why
     * the user was not able to be authenticated. Update the response with proxy headers if the request was made via a proxy.
     *
     * @param request The original client request that failed to be authenticated.
     * @param response Servlet response to the client containing the unsuccessful authentication attempt details.
     * @param ae The related exception thrown and explanation for the unsuccessful authentication attempt.
     */
    protected void unsuccessfulAuthentication(HttpServletRequest request, HttpServletResponse response, AuthenticationException ae) throws IOException {
        // populate the response
        ProxiedEntitiesUtils.unsuccessfulAuthentication(request, response, ae);

        // set the response status
        response.setContentType("text/plain");

        // write the response message
        PrintWriter out = response.getWriter();

        // use the type of authentication exception to determine the response code
        if (ae instanceof InvalidAuthenticationException) {
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            out.println("Authentication credentials invalid");
        } else if (ae instanceof UntrustedProxyException) {
            response.setStatus(HttpServletResponse.SC_FORBIDDEN);
            out.println("Authentication Proxy Server not trusted");
        } else if (ae instanceof AuthenticationServiceException) {
            log.error("Authentication Service Failed: {}", ae.getMessage(), ae);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            out.println("Authentication service processing failed");
        } else {
            log.error("Authentication Exception: {}", ae.getMessage(), ae);
            response.setStatus(HttpServletResponse.SC_FORBIDDEN);
            out.println("Access is denied.");
        }

        // log the failure
        log.warn("Authentication Failed {} {} {} [{}]", request.getRemoteAddr(), request.getMethod(), request.getRequestURL(), ae.getMessage());

        // optionally log the stack trace
        log.debug("Authentication Failed", ae);
    }

    public void setAuthenticationManager(AuthenticationManager authenticationManager) {
        this.authenticationManager = authenticationManager;
    }

    public void setAuthenticationDetailsSource(final AuthenticationDetailsSource<HttpServletRequest, NiFiWebAuthenticationDetails> authenticationDetailsSource) {
        this.authenticationDetailsSource = authenticationDetailsSource;
    }

    public void setProperties(NiFiProperties properties) {
        this.properties = properties;
    }

    public NiFiProperties getProperties() {
        return properties;
    }
}
