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
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.nifi.admin.service.AccountDisabledException;
import org.apache.nifi.admin.service.AccountNotFoundException;
import org.apache.nifi.admin.service.AccountPendingException;
import org.apache.nifi.admin.service.AdministrationException;
import org.apache.nifi.admin.service.UserService;
import org.apache.nifi.authentication.LoginCredentials;
import org.apache.nifi.authentication.LoginIdentityProvider;
import org.apache.nifi.authorization.exception.IdentityAlreadyExistsException;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.web.security.jwt.JwtService;
import org.apache.nifi.web.security.token.LoginAuthenticationToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.AccountStatusException;
import org.springframework.security.authentication.AuthenticationServiceException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.web.authentication.AbstractAuthenticationProcessingFilter;

/**
 * Exchanges a successful login with the configured provider for a ID token for accessing the API.
 */
public class RegistrationFilter extends AbstractAuthenticationProcessingFilter {

    private static final Logger logger = LoggerFactory.getLogger(RegistrationFilter.class);

    private LoginIdentityProvider loginIdentityProvider;
    private JwtService jwtService;
    private UserService userService;

    public RegistrationFilter(final String defaultFilterProcessesUrl) {
        super(defaultFilterProcessesUrl);

        // do not continue filter chain... simply exchanging authentication for token
        setContinueChainBeforeSuccessfulAuthentication(false);
    }

    @Override
    public Authentication attemptAuthentication(final HttpServletRequest request, final HttpServletResponse response) throws AuthenticationException, IOException, ServletException {
        // only suppport registration when running securely
        if (!request.isSecure()) {
            return null;
        }

        // look for the credentials in the request
        final LoginCredentials credentials = getLoginCredentials(request);

        // if the credentials were not part of the request, attempt to log in with the certificate in the request
        if (credentials == null) {
            throw new UsernameNotFoundException("User login credentials not found in request.");
        } else {
            try {
                // attempt to register the user
                loginIdentityProvider.register(credentials);
            } catch (final IdentityAlreadyExistsException iaee) {
                // if the identity already exists, try to create the nifi account request
            }

            try {
                // see if the account already exists so we're able to return the current status
                userService.checkAuthorization(credentials.getUsername());

                // account exists and is valid
                throw new AccountStatusException(String.format("An account for %s already exists.", credentials.getUsername())) {
                };
            } catch (AdministrationException ase) {
                throw new AuthenticationServiceException(ase.getMessage(), ase);
            } catch (AccountDisabledException | AccountPendingException e) {
                throw new AccountStatusException(e.getMessage(), e) {
                };
            } catch (AccountNotFoundException anfe) {
                // create the pending user account
                userService.createPendingUserAccount(credentials.getUsername(), request.getParameter("justification"));

                // create the login token
                return new LoginAuthenticationToken(credentials);
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

        // mark as successful
        response.setStatus(HttpServletResponse.SC_CREATED);
        response.setContentType("text/plain");
        response.setContentLength(0);
    }

    @Override
    protected void unsuccessfulAuthentication(final HttpServletRequest request, final HttpServletResponse response, final AuthenticationException failed) throws IOException, ServletException {
        response.setContentType("text/plain");

        final PrintWriter out = response.getWriter();
        out.println(failed.getMessage());

        // set the appropriate response status
        if (failed instanceof UsernameNotFoundException) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        } else if (failed instanceof AccountStatusException) {
            // account exists (maybe valid, pending, revoked)
            response.setStatus(HttpServletResponse.SC_FORBIDDEN);
        } else {
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }

    public void setJwtService(JwtService jwtService) {
        this.jwtService = jwtService;
    }

    public void setLoginIdentityProvider(LoginIdentityProvider loginIdentityProvider) {
        this.loginIdentityProvider = loginIdentityProvider;
    }

    public void setUserService(UserService userService) {
        this.userService = userService;
    }

}
