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

import org.apache.nifi.web.security.token.LoginAuthenticationToken;
import java.io.IOException;
import java.io.PrintWriter;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
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
import org.apache.nifi.web.security.x509.X509CertificateValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.AuthenticationCredentialsNotFoundException;
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
public class LoginAuthenticationFilter extends AbstractAuthenticationProcessingFilter {

    private static final Logger logger = LoggerFactory.getLogger(LoginAuthenticationFilter.class);

    private AuthenticationUserDetailsService<NiFiAuthenticationRequestToken> userDetailsService;

    private X509CertificateValidator certificateValidator;
    private X509CertificateExtractor certificateExtractor;
    private X509PrincipalExtractor principalExtractor;

    private LoginIdentityProvider loginIdentityProvider;
    private JwtService jwtService;

    public LoginAuthenticationFilter(final String defaultFilterProcessesUrl) {
        super(defaultFilterProcessesUrl);

        // do not continue filter chain... simply exchanging authentication for token
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

            // if there is no certificate, look for an existing token
            if (certificate == null) {
                // if not configured for login, don't consider existing tokens
                if (loginIdentityProvider == null) {
                    throw new BadCredentialsException("Login not supported.");
                }

                final String principal = jwtService.getAuthentication(request);

                if (principal == null) {
                    throw new AuthenticationCredentialsNotFoundException("Unable to issue token as issue token as no credentials were found in the request.");
                }

                final LoginCredentials tokenCredentials = new LoginCredentials(principal, null);
                return new LoginAuthenticationToken(tokenCredentials);
            } else {
                // extract the principal
                final String principal = principalExtractor.extractPrincipal(certificate).toString();

                try {
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

                // authorize the proxy if necessary
                authorizeProxyIfNecessary(ProxiedEntitiesUtils.buildProxyChain(request, principal));

                final LoginCredentials preAuthenticatedCredentials = new LoginCredentials(principal, null);
                return new LoginAuthenticationToken(preAuthenticatedCredentials);
            }
        } else {
            // if not configuration for login, don't consider credentials 
            if (loginIdentityProvider == null) {
                throw new BadCredentialsException("Login not supported.");
            }

            if (loginIdentityProvider.authenticate(credentials)) {
                return new LoginAuthenticationToken(credentials);
            } else {
                throw new BadCredentialsException("The supplied username and password are not valid.");
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
            } catch (final Exception e) {
                // any other issue we're going to treat as an authentication exception which will return 401
                throw new AuthenticationException(e.getMessage(), e) {
                };
            }
        }
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
    }

    @Override
    protected void unsuccessfulAuthentication(final HttpServletRequest request, final HttpServletResponse response, final AuthenticationException failed) throws IOException, ServletException {
        response.setContentType("text/plain");

        final PrintWriter out = response.getWriter();
        out.println(failed.getMessage());

        if (failed instanceof BadCredentialsException || failed instanceof AuthenticationCredentialsNotFoundException) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        } else {
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        }
    }

    public void setJwtService(JwtService jwtService) {
        this.jwtService = jwtService;
    }

    public void setLoginIdentityProvider(LoginIdentityProvider loginIdentityProvider) {
        this.loginIdentityProvider = loginIdentityProvider;
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

}
