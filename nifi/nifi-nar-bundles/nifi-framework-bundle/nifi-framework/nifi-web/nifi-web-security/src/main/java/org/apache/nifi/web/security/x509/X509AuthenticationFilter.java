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
package org.apache.nifi.web.security.x509;

import org.apache.nifi.web.security.x509.ocsp.OcspCertificateValidator;
import java.io.IOException;
import java.io.PrintWriter;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.X509Certificate;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.nifi.admin.service.AdministrationException;
import org.apache.nifi.admin.service.UserService;
import org.apache.nifi.web.security.DnUtils;
import org.apache.nifi.web.security.UntrustedProxyException;
import org.apache.nifi.util.NiFiProperties;
import org.apache.commons.lang3.StringUtils;
import org.springframework.security.authentication.AccountStatusException;
import org.springframework.security.authentication.AuthenticationServiceException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.web.authentication.preauth.AbstractPreAuthenticatedProcessingFilter;
import org.springframework.security.web.authentication.preauth.x509.X509PrincipalExtractor;

/**
 * Custom X509 filter that will inspect the HTTP headers for a proxied user
 * before extracting the user details from the client certificate.
 */
public class X509AuthenticationFilter extends AbstractPreAuthenticatedProcessingFilter {

    public static final String PROXY_ENTITIES_CHAIN = "X-ProxiedEntitiesChain";
    public static final String PROXY_ENTITIES_ACCEPTED = "X-ProxiedEntitiesAccepted";
    public static final String PROXY_ENTITIES_DETAILS = "X-ProxiedEntitiesDetails";

    private final X509CertificateExtractor certificateExtractor = new X509CertificateExtractor();
    private final X509PrincipalExtractor principalExtractor = new SubjectDnX509PrincipalExtractor();
    private OcspCertificateValidator certificateValidator;
    private NiFiProperties properties;
    private UserService userService;

    /**
     * Override doFilter in order to properly handle when users could not be
     * authenticated.
     *
     * @param request
     * @param response
     * @param chain
     * @throws IOException
     * @throws ServletException
     */
    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        final HttpServletResponse httpResponse = (HttpServletResponse) response;

        // determine if this request is attempting to create a new account
        if (isNewAccountRequest((HttpServletRequest) request)) {
            // determine if this nifi supports new account requests
            if (properties.getSupportNewAccountRequests()) {
                // ensure there is a certificate in the request
                X509Certificate certificate = certificateExtractor.extractClientCertificate((HttpServletRequest) request);
                if (certificate != null) {
                    // extract the principal from the certificate
                    Object certificatePrincipal = principalExtractor.extractPrincipal(certificate);
                    String principal = certificatePrincipal.toString();

                    // log the new user account request
                    logger.info("Requesting new user account for " + principal);

                    try {
                        // get the justification
                        String justification = request.getParameter("justification");
                        if (justification == null) {
                            justification = StringUtils.EMPTY;
                        }

                        // create the pending user account
                        userService.createPendingUserAccount(principal, justification);

                        // generate a response
                        httpResponse.setStatus(HttpServletResponse.SC_CREATED);
                        httpResponse.setContentType("text/plain");

                        // write the response message
                        PrintWriter out = response.getWriter();
                        out.println("Not authorized. User account created. Authorization pending.");
                    } catch (IllegalArgumentException iae) {
                        handleUserServiceError((HttpServletRequest) request, httpResponse, HttpServletResponse.SC_BAD_REQUEST, iae.getMessage());
                    } catch (AdministrationException ae) {
                        handleUserServiceError((HttpServletRequest) request, httpResponse, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, ae.getMessage());
                    }
                } else {
                    // can this really happen?
                    handleMissingCertificate((HttpServletRequest) request, httpResponse);
                }
            } else {
                handleUserServiceError((HttpServletRequest) request, httpResponse, HttpServletResponse.SC_NOT_FOUND, "This NiFi does not support new account requests.");
            }
        } else {
            try {
                // this not a request to create a user account - try to authorize
                super.doFilter(request, response, chain);
            } catch (AuthenticationException ae) {
                // continue the filter chain since anonymous access should be supported
                if (!properties.getNeedClientAuth()) {
                    chain.doFilter(request, response);
                } else {
                    // create an appropriate response for the given exception
                    handleUnsuccessfulAuthentication((HttpServletRequest) request, httpResponse, ae);
                }
            }
        }
    }

    @Override
    protected Object getPreAuthenticatedPrincipal(HttpServletRequest request) {
        String principal;

        // extract the cert
        X509Certificate certificate = certificateExtractor.extractClientCertificate(request);

        // ensure the cert was found
        if (certificate == null) {
            return null;
        }

        // extract the principal
        Object certificatePrincipal = principalExtractor.extractPrincipal(certificate);
        principal = DnUtils.formatProxyDn(certificatePrincipal.toString());

        try {
            // ensure the cert is valid
            certificate.checkValidity();
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
        }

        // validate the certificate in question
        try {
            certificateValidator.validate(request);
        } catch (final Exception e) {
            logger.info(e.getMessage());
            if (logger.isDebugEnabled()) {
                logger.debug("", e);
            }
            return null;
        }

        // look for a proxied user
        if (StringUtils.isNotBlank(request.getHeader(PROXY_ENTITIES_CHAIN))) {
            principal = request.getHeader(PROXY_ENTITIES_CHAIN) + principal;
        }

        // log the request attempt - response details will be logged later
        logger.info(String.format("Attempting request for (%s) %s %s (source ip: %s)", principal, request.getMethod(),
                request.getRequestURL().toString(), request.getRemoteAddr()));

        return principal;
    }

    @Override
    protected Object getPreAuthenticatedCredentials(HttpServletRequest request) {
        return certificateExtractor.extractClientCertificate(request);
    }

    /**
     * Sets the response headers for successful proxied requests.
     *
     * @param request
     * @param response
     * @param authResult
     */
    @Override
    protected void successfulAuthentication(HttpServletRequest request, HttpServletResponse response, Authentication authResult) {
        if (StringUtils.isNotBlank(request.getHeader(PROXY_ENTITIES_CHAIN))) {
            response.setHeader(PROXY_ENTITIES_ACCEPTED, Boolean.TRUE.toString());
        }
        super.successfulAuthentication(request, response, authResult);
    }

    /**
     * Sets the response headers for unsuccessful proxied requests.
     *
     * @param request
     * @param response
     * @param failed
     */
    @Override
    protected void unsuccessfulAuthentication(HttpServletRequest request, HttpServletResponse response, AuthenticationException failed) {
        if (StringUtils.isNotBlank(request.getHeader(PROXY_ENTITIES_CHAIN))) {
            response.setHeader(PROXY_ENTITIES_DETAILS, failed.getMessage());
        }
        super.unsuccessfulAuthentication(request, response, failed);
    }

    /**
     * Determines if the specified request is attempting to register a new user
     * account.
     *
     * @param request
     * @return
     */
    private boolean isNewAccountRequest(HttpServletRequest request) {
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
     * Handles requests that were unable to be authorized.
     *
     * @param request
     * @param response
     * @param ae
     * @throws IOException
     */
    private void handleUnsuccessfulAuthentication(HttpServletRequest request, HttpServletResponse response, AuthenticationException ae) throws IOException {
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
     * Handles requests that failed because of a user service error.
     *
     * @param request
     * @param response
     * @param e
     * @throws IOException
     */
    private void handleUserServiceError(HttpServletRequest request, HttpServletResponse response, int responseCode, String message) throws IOException {
        // set the response status
        response.setContentType("text/plain");
        response.setStatus(responseCode);

        // write the response message
        PrintWriter out = response.getWriter();
        out.println(message);

        // log the failure
        logger.info(String.format("Unable to process request because %s", message));
    }

    /**
     * Handles requests that failed because they were bad input.
     *
     * @param request
     * @param response
     * @throws IOException
     */
    private void handleMissingCertificate(HttpServletRequest request, HttpServletResponse response) throws IOException {
        // set the response status
        response.setContentType("text/plain");
        response.setStatus(HttpServletResponse.SC_BAD_REQUEST);

        // write the response message
        PrintWriter out = response.getWriter();
        out.println("Unable to process request because the user certificate was not specified.");

        // log the failure
        logger.info("Unable to process request because the user certificate was not specified.");
    }

    /* setters */
    public void setProperties(NiFiProperties properties) {
        this.properties = properties;
    }

    public void setUserService(UserService userService) {
        this.userService = userService;
    }

    public void setCertificateValidator(OcspCertificateValidator certificateValidator) {
        this.certificateValidator = certificateValidator;
    }

}
