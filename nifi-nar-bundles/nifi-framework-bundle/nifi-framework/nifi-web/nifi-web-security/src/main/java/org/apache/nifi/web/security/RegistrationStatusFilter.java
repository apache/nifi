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
import java.io.PrintWriter;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.nifi.authentication.LoginCredentials;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.web.security.token.NiFiAuthenticationRequestToken;
import org.apache.nifi.web.security.x509.X509CertificateExtractor;
import org.apache.nifi.web.security.x509.X509CertificateValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.authentication.AccountStatusException;
import org.springframework.security.authentication.AuthenticationServiceException;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.userdetails.AuthenticationUserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.web.authentication.AbstractAuthenticationProcessingFilter;
import org.springframework.security.web.authentication.preauth.x509.X509PrincipalExtractor;

/**
 * Exchanges a successful login with the configured provider for a ID token for accessing the API.
 */
public class RegistrationStatusFilter extends AbstractAuthenticationProcessingFilter {

    private static final Logger logger = LoggerFactory.getLogger(RegistrationStatusFilter.class);

    private NiFiProperties properties;
    private AuthenticationUserDetailsService<NiFiAuthenticationRequestToken> userDetailsService;
    private X509CertificateValidator certificateValidator;
    private X509CertificateExtractor certificateExtractor;
    private X509PrincipalExtractor principalExtractor;

    public RegistrationStatusFilter(final String defaultFilterProcessesUrl) {
        super(defaultFilterProcessesUrl);

        // do not continue filter chain... simply exchaning authentication for token
        setContinueChainBeforeSuccessfulAuthentication(false);
    }

    @Override
    public Authentication attemptAuthentication(final HttpServletRequest request, final HttpServletResponse response) throws AuthenticationException, IOException, ServletException {
        // only suppport login when running securely
        if (!request.isSecure()) {
            return null;
        }

        // look for a certificate
        final X509Certificate certificate = certificateExtractor.extractClientCertificate(request);

        // if no certificate, just check the credentials
        if (certificate == null) {
            final LoginCredentials credentials = getLoginCredentials(request);

            // ensure we have something we can work with (certificate or crendentials)
            if (credentials == null) {
                throw new BadCredentialsException("Unable to check registration status as no credentials were included with the request.");
            }

            // without a certificate, this is not a proxied request
            final List<String> chain = Arrays.asList(credentials.getUsername());

            // check authorization for this user
            checkAuthorization(chain);

            // no issues with authorization
            return new RegistrationStatusAuthenticationToken(credentials);
        } else {
            // we have a certificate so let's consider a proxy chain
            final String principal = extractPrincipal(certificate);

            try {
                // validate the certificate
                certificateValidator.validateClientCertificate(request, certificate);
            } catch (CertificateExpiredException cee) {
                final String message = String.format("Client certificate for (%s) is expired.", principal);
                logger.info(message, cee);
                if (logger.isDebugEnabled()) {
                    logger.debug("", cee);
                }
                return null;
            } catch (CertificateNotYetValidException cnyve) {
                final String message = String.format("Client certificate for (%s) is not yet valid.", principal);
                logger.info(message, cnyve);
                if (logger.isDebugEnabled()) {
                    logger.debug("", cnyve);
                }
                return null;
            } catch (final Exception e) {
                logger.info(e.getMessage());
                if (logger.isDebugEnabled()) {
                    logger.debug("", e);
                }
                return null;
            }

            // ensure the proxy chain is authorized
            checkAuthorization(ProxiedEntitiesUtils.buildProxyChain(request, principal));

            // no issues with authorization
            final LoginCredentials preAuthenticatedCredentials = new LoginCredentials(principal, null);
            return new RegistrationStatusAuthenticationToken(preAuthenticatedCredentials);
        }
    }

    /**
     * Checks the status of the proxy.
     *
     * @param proxyChain the proxy chain
     * @throws AuthenticationException if the proxy chain is not authorized
     */
    private void checkAuthorization(final List<String> proxyChain) throws AuthenticationException {
        userDetailsService.loadUserDetails(new NiFiAuthenticationRequestToken(proxyChain));
    }

    private String extractPrincipal(final X509Certificate certificate) {
        // extract the principal
        final Object certificatePrincipal = principalExtractor.extractPrincipal(certificate);
        return ProxiedEntitiesUtils.formatProxyDn(certificatePrincipal.toString());
    }

    private LoginCredentials getLoginCredentials(HttpServletRequest request) {
        final String username = request.getParameter("username");
        final String password = request.getParameter("password");

        if (StringUtils.isBlank(username) || StringUtils.isBlank(password)) {
            return null;
        } else {
            return new LoginCredentials(username, password);
        }
    }

    @Override
    protected void successfulAuthentication(final HttpServletRequest request, final HttpServletResponse response, final FilterChain chain, final Authentication authentication)
            throws IOException, ServletException {

        // mark as successful
        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType("text/plain");
        response.setContentLength(0);
    }

    @Override
    protected void unsuccessfulAuthentication(final HttpServletRequest request, final HttpServletResponse response, final AuthenticationException ae) throws IOException, ServletException {
        // set the response status
        response.setContentType("text/plain");

        // write the response message
        PrintWriter out = response.getWriter();

        // use the type of authentication exception to determine the response code
        if (ae instanceof UsernameNotFoundException) {
            if (properties.getSupportNewAccountRequests()) {
                response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                out.println("Not authorized.");
            } else {
                response.setStatus(HttpServletResponse.SC_FORBIDDEN);
                out.println("Access is denied.");
            }
        } else if (ae instanceof AccountStatusException) {
            response.setStatus(HttpServletResponse.SC_FORBIDDEN);
            out.println(ae.getMessage());
        } else if (ae instanceof UntrustedProxyException) {
            response.setStatus(HttpServletResponse.SC_FORBIDDEN);
            out.println(ae.getMessage());
        } else if (ae instanceof AuthenticationServiceException) {
            logger.error(String.format("Unable to authorize: %s", ae.getMessage()), ae);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            out.println(String.format("Unable to authorize: %s", ae.getMessage()));
        } else {
            logger.error(String.format("Unable to authorize: %s", ae.getMessage()), ae);
            response.setStatus(HttpServletResponse.SC_FORBIDDEN);
            out.println("Access is denied.");
        }

        // log the failure
        logger.info(String.format("Rejecting access to web api: %s", ae.getMessage()));

        // optionally log the stack trace
        if (logger.isDebugEnabled()) {
            logger.debug(StringUtils.EMPTY, ae);
        }
    }

    /**
     * This is an Authentication Token for logging in. Once a user is authenticated, they can be issues an ID token.
     */
    public static class RegistrationStatusAuthenticationToken extends AbstractAuthenticationToken {

        final LoginCredentials credentials;

        public RegistrationStatusAuthenticationToken(final LoginCredentials credentials) {
            super(null);
            setAuthenticated(true);
            this.credentials = credentials;
        }

        public LoginCredentials getLoginCredentials() {
            return credentials;
        }

        @Override
        public Object getCredentials() {
            return credentials.getPassword();
        }

        @Override
        public Object getPrincipal() {
            return credentials.getUsername();
        }
    }

    public void setCertificateValidator(X509CertificateValidator certificateValidator) {
        this.certificateValidator = certificateValidator;
    }

    public void setCertificateExtractor(X509CertificateExtractor certificateExtractor) {
        this.certificateExtractor = certificateExtractor;
    }

    public void setPrincipalExtractor(X509PrincipalExtractor principalExtractor) {
        this.principalExtractor = principalExtractor;
    }

    public void setUserDetailsService(AuthenticationUserDetailsService<NiFiAuthenticationRequestToken> userDetailsService) {
        this.userDetailsService = userDetailsService;
    }

    public void setProperties(NiFiProperties properties) {
        this.properties = properties;
    }

}
