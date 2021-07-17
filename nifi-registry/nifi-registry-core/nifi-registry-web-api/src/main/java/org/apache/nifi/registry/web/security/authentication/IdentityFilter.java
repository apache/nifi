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

import org.apache.nifi.registry.security.authentication.AuthenticationRequest;
import org.apache.nifi.registry.security.authentication.IdentityProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.GenericFilterBean;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

/**
 * A class that will extract an identity / credentials claim from an HttpServlet Request using an injected IdentityProvider.
 *
 * This class is designed to be used in collaboration with an {@link IdentityAuthenticationProvider}. The identity/credentials will be
 * extracted by this filter and later validated by the {@link IdentityAuthenticationProvider} in the default SecurityInterceptorFilter.
 */
public class IdentityFilter extends GenericFilterBean {

    private static final Logger logger = LoggerFactory.getLogger(IdentityFilter.class);

    private final IdentityProvider identityProvider;

    public IdentityFilter(IdentityProvider identityProvider) {
        this.identityProvider = identityProvider;
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
                    SecurityContextHolder.getContext().getAuthentication().getPrincipal().toString(),
                    identityProvider.getClass().getSimpleName());
            filterChain.doFilter(servletRequest, servletResponse);
            return;
        }

        logger.debug("Attempting to extract user credentials using {}", identityProvider.getClass().getSimpleName());

        try {
            AuthenticationRequest authenticationRequest = identityProvider.extractCredentials((HttpServletRequest)servletRequest);
            if (authenticationRequest != null) {
                Authentication authentication = new AuthenticationRequestToken(authenticationRequest, identityProvider.getClass(), servletRequest.getRemoteAddr());
                logger.debug("Adding credentials claim to SecurityContext to be authenticated. Credentials extracted by {}: {}",
                        identityProvider.getClass().getSimpleName(),
                        authenticationRequest);
                SecurityContextHolder.getContext().setAuthentication(authentication);
                // This filter's job, which is merely to search for and extract an identity claim, is done.
                // The actual authentication of the identity claim will be handled by a corresponding IdentityAuthenticationProvider
            }
        } catch (Exception e) {
            logger.debug("Exception occurred while extracting credentials:", e);
        }

        filterChain.doFilter(servletRequest, servletResponse);
    }

    private boolean credentialsAlreadyPresent() {
        return SecurityContextHolder.getContext().getAuthentication() != null;
    }
}
