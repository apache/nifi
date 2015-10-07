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
import org.apache.nifi.web.security.ProxiedEntitiesUtils;
import org.apache.nifi.web.security.jwt.JwtService;
import org.apache.nifi.web.security.token.NiFiAuthenticationRequestToken;
import org.apache.nifi.web.security.token.LoginAuthenticationRequestToken;
import org.apache.nifi.web.security.x509.X509CertificateExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.authentication.AbstractAuthenticationProcessingFilter;
import org.springframework.security.web.authentication.preauth.x509.X509PrincipalExtractor;

/**
 * Exchanges a successful login with the configured provider for a ID token for accessing the API.
 */
public class LoginAuthenticationFilter extends AbstractAuthenticationProcessingFilter {

    private static final Logger logger = LoggerFactory.getLogger(LoginAuthenticationFilter.class);

    private X509CertificateExtractor certificateExtractor;
    private X509PrincipalExtractor principalExtractor;

    private JwtService jwtService;
    private LoginIdentityProvider loginIdentityProvider;

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
        
        // look for a certificate
        final X509Certificate certificate = certificateExtractor.extractClientCertificate(request);

        // ensure the cert was found
        if (certificate != null) {
            // extract the principal
            Object certificatePrincipal = principalExtractor.extractPrincipal(certificate);
            final String principal = ProxiedEntitiesUtils.formatProxyDn(certificatePrincipal.toString());

            final List<String> proxyChain = ProxiedEntitiesUtils.buildProxyChain(request, principal);
            return new NiFiAuthenticationRequestToken(proxyChain);
        } else {
            // if there were no certificate or no principal, defer to the login provider
            final LoginCredentials credentials = getLoginCredentials(request);

            // if unable to authenticate return null
            if (credentials == null) {
                return null;
            }

            final List<String> proxyChain = ProxiedEntitiesUtils.buildProxyChain(request, credentials.getUsername());
            return new LoginAuthenticationRequestToken(proxyChain, credentials);
        }
    }

    private LoginCredentials getLoginCredentials(HttpServletRequest request) {
        return new LoginCredentials(request.getParameter("username"), request.getParameter("password"));
    }

    @Override
    protected void successfulAuthentication(final HttpServletRequest request, final HttpServletResponse response, final FilterChain chain, final Authentication authentication)
            throws IOException, ServletException {

        // generate JWT for response
        jwtService.addToken(response, authentication);
    }

    @Override
    protected void unsuccessfulAuthentication(final HttpServletRequest request, final HttpServletResponse response, final AuthenticationException failed) throws IOException, ServletException {
        // set the response status
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        response.setContentType("text/plain");

        final PrintWriter out = response.getWriter();
        out.println("Invalid username/password");
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

}
