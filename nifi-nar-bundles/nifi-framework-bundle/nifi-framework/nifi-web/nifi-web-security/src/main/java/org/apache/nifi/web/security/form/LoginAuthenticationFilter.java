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
package org.apache.nifi.web.security.form;

import java.io.IOException;
import java.io.PrintWriter;
import java.security.cert.X509Certificate;
import java.util.List;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.nifi.authentication.LoginCredentials;
import org.apache.nifi.authentication.LoginIdentityProvider;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.web.security.ProxiedEntitiesUtils;
import org.apache.nifi.web.security.jwt.JwtService;
import org.apache.nifi.web.security.token.NiFiAuthenticationRequestToken;
import org.apache.nifi.web.security.x509.X509CertificateExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.userdetails.AuthenticationUserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.web.authentication.AbstractAuthenticationProcessingFilter;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedCredentialsNotFoundException;
import org.springframework.security.web.authentication.preauth.x509.X509PrincipalExtractor;

/**
 * Exchanges a successful login with the configured provider for a ID token for accessing the API.
 */
public class LoginAuthenticationFilter extends AbstractAuthenticationProcessingFilter {

    private static final Logger logger = LoggerFactory.getLogger(LoginAuthenticationFilter.class);

    private AuthenticationUserDetailsService<NiFiAuthenticationRequestToken> userDetailsService;

    private X509CertificateExtractor certificateExtractor;
    private X509PrincipalExtractor principalExtractor;

    private LoginIdentityProvider loginIdentityProvider;
    private JwtService jwtService;

    public LoginAuthenticationFilter(final String defaultFilterProcessesUrl) {
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

        // look for the credentials in the request
        final LoginCredentials credentials = getLoginCredentials(request);

        // if the credentials were not part of the request, attempt to log in with the certificate in the request
        if (credentials == null) {
            // look for a certificate
            final X509Certificate certificate = certificateExtractor.extractClientCertificate(request);

            if (certificate == null) {
                throw new PreAuthenticatedCredentialsNotFoundException("Unable to extract client certificate after processing request with no login credentials specified.");
            }

            // authorize the proxy if necessary
            final String principal = extractPrincipal(certificate);
            authorizeProxyIfNecessary(ProxiedEntitiesUtils.buildProxyChain(request, principal));

            final LoginCredentials preAuthenticatedCredentials = new LoginCredentials(principal, null);
            return new LoginAuthenticationToken(preAuthenticatedCredentials);
        } else {
            // look for a certificate
            final X509Certificate certificate = certificateExtractor.extractClientCertificate(request);

            // if there was a certificate with this request see if it was proxying an end user request
            if (certificate != null) {
                // authorize the proxy if necessary
                final String principal = extractPrincipal(certificate);
                authorizeProxyIfNecessary(ProxiedEntitiesUtils.buildProxyChain(request, principal));
            }

            if (loginIdentityProvider.authenticate(credentials)) {
                return new LoginAuthenticationToken(credentials);
            } else {
                throw new BadCredentialsException("User could not be authenticated with the configured identity provider.");
            }
        }
    }

    /**
     * Ensures the proxyChain is authorized before allowing the user to be authenticated.
     *
     * @param proxyChain the proxy chain
     * @throws AuthenticationException if the proxy chain is not authorized
     */
    private void authorizeProxyIfNecessary(final List<String> proxyChain) throws AuthenticationException {
        if (proxyChain.size() > 1) {
            try {
                userDetailsService.loadUserDetails(new NiFiAuthenticationRequestToken(proxyChain));
            } catch (final UsernameNotFoundException unfe) {
                // if a username not found exception was thrown, the proxies were authorized and now
                // we can issue a new ID token to the end user
            }
        }
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

        // generate JWT for response
        jwtService.addToken(response, authentication);

        // mark as successful
        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType("text/plain");
        response.setContentLength(0);
    }

    @Override
    protected void unsuccessfulAuthentication(final HttpServletRequest request, final HttpServletResponse response, final AuthenticationException failed) throws IOException, ServletException {
        // set the response status
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        response.setContentType("text/plain");

        final PrintWriter out = response.getWriter();
        out.println("Unable to authenticate.");
    }

    /**
     * This is an Authentication Token for logging in. Once a user is authenticated, they can be issues an ID token.
     */
    public static class LoginAuthenticationToken extends AbstractAuthenticationToken {

        final LoginCredentials credentials;

        public LoginAuthenticationToken(final LoginCredentials credentials) {
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

    public void setJwtService(JwtService jwtService) {
        this.jwtService = jwtService;
    }

    public void setLoginIdentityProvider(LoginIdentityProvider loginIdentityProvider) {
        this.loginIdentityProvider = loginIdentityProvider;
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

}
