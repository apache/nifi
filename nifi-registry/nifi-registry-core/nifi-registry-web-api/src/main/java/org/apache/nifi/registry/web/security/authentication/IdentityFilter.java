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
package org.apache.nifi.registry.web.security.authentication;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import org.apache.nifi.registry.security.authentication.AuthenticationRequest;
import org.apache.nifi.registry.security.authentication.IdentityProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.GenericFilterBean;

import java.io.IOException;

/**
 * A class that will extract an identity / credentials claim from an HttpServlet Request using an injected IdentityProvider.
 *
 * This class is designed to be used in collaboration with an {@link IdentityAuthenticationProvider}. The identity/credentials will be
 * extracted by this filter and validated by the {@link IdentityAuthenticationProvider} via the {@link AuthenticationManager}.
 */
public class IdentityFilter extends GenericFilterBean {

    private static final Logger logger = LoggerFactory.getLogger(IdentityFilter.class);

    private final IdentityProvider identityProvider;
    private final AuthenticationManager authenticationManager;

    public IdentityFilter(IdentityProvider identityProvider, AuthenticationManager authenticationManager) {
        this.identityProvider = identityProvider;
        this.authenticationManager = authenticationManager;
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {

        // Only require authentication from an identity provider if the NiFi registry is running securely.
        if (!servletRequest.isSecure()) {
            // Otherwise, requests will be "authenticated" by the AnonymousIdentityFilter
            filterChain.doFilter(servletRequest, servletResponse);
            return;
        }

        if (identityProvider == null) {
            logger.warn("Identity Filter configured with NULL identity provider. Credentials will not be extracted.");
            filterChain.doFilter(servletRequest, servletResponse);
            return;
        }

        if (credentialsAlreadyPresent()) {
            logger.debug("Credentials already extracted for [{}], skipping credentials extraction filter using {}",
                    SecurityContextHolder.getContext().getAuthentication().getPrincipal(),
                    identityProvider.getClass().getSimpleName());
            filterChain.doFilter(servletRequest, servletResponse);
            return;
        }

        logger.debug("Attempting to extract user credentials using {}", identityProvider.getClass().getSimpleName());

        try {
            AuthenticationRequest authenticationRequest = identityProvider.extractCredentials((HttpServletRequest) servletRequest);
            if (authenticationRequest != null) {
                Authentication authenticationRequestToken = new AuthenticationRequestToken(
                        authenticationRequest,
                        identityProvider.getClass(),
                        servletRequest.getRemoteAddr());
                logger.debug("Attempting to authenticate credentials extracted by {}: {}",
                        identityProvider.getClass().getSimpleName(),
                        authenticationRequest);

                // Authenticate the request token using the AuthenticationManager,
                // which will try all the configured AuthenticationProviders until it finds one that supports authenticating the type of extracted credentials
                final Authentication authenticated = authenticationManager.authenticate(authenticationRequestToken);
                if (authenticated != null && authenticated.isAuthenticated()) {
                    SecurityContextHolder.getContext().setAuthentication(authenticated);
                    logger.debug("Authentication successful for {}", authenticated.getName());
                }
            } else {
                logger.debug("The {} did not find credentials it supports on the servlet request. " +
                        "Allowing this request to continue through the filter chain where another filter might authenticate it.",
                    identityProvider.getClass().getSimpleName());
            }
        } catch (final AuthenticationException e) {
            logger.info("Authentication failed for credentials extracted by {}: {}", identityProvider.getClass().getSimpleName(), e.getMessage());
            logger.debug("Authentication failure details", e);
            // Allow request to continue, where it will result in a 401 error for paths that require an authenticated request
        } catch (final Exception e) {
            logger.debug("Exception occurred while extracting credentials:", e);
        }

        filterChain.doFilter(servletRequest, servletResponse);
    }

    private boolean credentialsAlreadyPresent() {
        final Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        return authentication != null && authentication.isAuthenticated();
    }
}
