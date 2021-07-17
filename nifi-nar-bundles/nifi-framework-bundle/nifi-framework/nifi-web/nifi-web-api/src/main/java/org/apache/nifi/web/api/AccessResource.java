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
package org.apache.nifi.web.api;

import io.jsonwebtoken.JwtException;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.admin.service.AdministrationException;
import org.apache.nifi.authentication.AuthenticationResponse;
import org.apache.nifi.authentication.LoginCredentials;
import org.apache.nifi.authentication.LoginIdentityProvider;
import org.apache.nifi.authentication.exception.AuthenticationNotSupportedException;
import org.apache.nifi.authentication.exception.IdentityAccessException;
import org.apache.nifi.authentication.exception.InvalidLoginCredentialsException;
import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserDetails;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.authorization.util.IdentityMappingUtil;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.web.api.dto.AccessConfigurationDTO;
import org.apache.nifi.web.api.dto.AccessStatusDTO;
import org.apache.nifi.web.api.entity.AccessConfigurationEntity;
import org.apache.nifi.web.api.entity.AccessStatusEntity;
import org.apache.nifi.web.security.InvalidAuthenticationException;
import org.apache.nifi.web.security.LogoutException;
import org.apache.nifi.web.security.ProxiedEntitiesUtils;
import org.apache.nifi.web.security.UntrustedProxyException;
import org.apache.nifi.web.security.jwt.JwtAuthenticationProvider;
import org.apache.nifi.web.security.jwt.JwtAuthenticationRequestToken;
import org.apache.nifi.web.security.jwt.JwtService;
import org.apache.nifi.web.security.jwt.NiFiBearerTokenResolver;
import org.apache.nifi.web.security.kerberos.KerberosService;
import org.apache.nifi.web.security.knox.KnoxService;
import org.apache.nifi.web.security.logout.LogoutRequest;
import org.apache.nifi.web.security.logout.LogoutRequestManager;
import org.apache.nifi.web.security.otp.OtpService;
import org.apache.nifi.web.security.token.LoginAuthenticationToken;
import org.apache.nifi.web.security.token.NiFiAuthenticationToken;
import org.apache.nifi.web.security.token.OtpAuthenticationToken;
import org.apache.nifi.web.security.x509.X509AuthenticationProvider;
import org.apache.nifi.web.security.x509.X509AuthenticationRequestToken;
import org.apache.nifi.web.security.x509.X509CertificateExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.AuthenticationServiceException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.authentication.preauth.x509.X509PrincipalExtractor;
import org.springframework.web.util.WebUtils;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.security.cert.X509Certificate;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * RESTful endpoint for managing access.
 */
@Path("/access")
@Api(
        value = "/access",
        description = "Endpoints for obtaining an access token or checking access status."
)
public class AccessResource extends ApplicationResource {

    private static final Logger logger = LoggerFactory.getLogger(AccessResource.class);
    protected static final String AUTHENTICATION_NOT_ENABLED_MSG = "User authentication/authorization is only supported when running over HTTPS.";
    static final String LOGOUT_REQUEST_IDENTIFIER = "nifi-logout-request-identifier";

    private X509CertificateExtractor certificateExtractor;
    private X509AuthenticationProvider x509AuthenticationProvider;
    private X509PrincipalExtractor principalExtractor;

    private LoginIdentityProvider loginIdentityProvider;
    private JwtAuthenticationProvider jwtAuthenticationProvider;
    private JwtService jwtService;
    private OtpService otpService;
    private KnoxService knoxService;
    private KerberosService kerberosService;
    protected LogoutRequestManager logoutRequestManager;

    /**
     * Retrieves the access configuration for this NiFi.
     *
     * @param httpServletRequest the servlet request
     * @return A accessConfigurationEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("config")
    @ApiOperation(
            value = "Retrieves the access configuration for this NiFi",
            response = AccessConfigurationEntity.class
    )
    public Response getLoginConfig(@Context HttpServletRequest httpServletRequest) {

        final AccessConfigurationDTO accessConfiguration = new AccessConfigurationDTO();

        // specify whether login should be supported and only support for secure requests
        accessConfiguration.setSupportsLogin(loginIdentityProvider != null && httpServletRequest.isSecure());

        // create the response entity
        final AccessConfigurationEntity entity = new AccessConfigurationEntity();
        entity.setConfig(accessConfiguration);

        // generate the response
        return generateOkResponse(entity).build();
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path("knox/request")
    @ApiOperation(
            value = "Initiates a request to authenticate through Apache Knox.",
            notes = NON_GUARANTEED_ENDPOINT
    )
    public void knoxRequest(@Context HttpServletRequest httpServletRequest, @Context HttpServletResponse httpServletResponse) throws Exception {
        // only consider user specific access over https
        if (!httpServletRequest.isSecure()) {
            forwardToLoginMessagePage(httpServletRequest, httpServletResponse, AUTHENTICATION_NOT_ENABLED_MSG);
            return;
        }

        // ensure knox is enabled
        if (!knoxService.isKnoxEnabled()) {
            forwardToLoginMessagePage(httpServletRequest, httpServletResponse, "Apache Knox SSO support is not configured.");
            return;
        }

        // build the originalUri, and direct back to the ui
        final String originalUri = generateResourceUri("access", "knox", "callback");

        // build the authorization uri
        final URI authorizationUri = UriBuilder.fromUri(knoxService.getKnoxUrl())
                .queryParam("originalUrl", originalUri)
                .build();

        // generate the response
        httpServletResponse.sendRedirect(authorizationUri.toString());
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path("knox/callback")
    @ApiOperation(
            value = "Redirect/callback URI for processing the result of the Apache Knox login sequence.",
            notes = NON_GUARANTEED_ENDPOINT
    )
    public void knoxCallback(@Context HttpServletRequest httpServletRequest, @Context HttpServletResponse httpServletResponse) throws Exception {
        // only consider user specific access over https
        if (!httpServletRequest.isSecure()) {
            forwardToLoginMessagePage(httpServletRequest, httpServletResponse, AUTHENTICATION_NOT_ENABLED_MSG);
            return;
        }

        // ensure knox is enabled
        if (!knoxService.isKnoxEnabled()) {
            forwardToLoginMessagePage(httpServletRequest, httpServletResponse, "Apache Knox SSO support is not configured.");
            return;
        }

        httpServletResponse.sendRedirect(getNiFiUri());
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path("knox/logout")
    @ApiOperation(
            value = "Performs a logout in the Apache Knox.",
            notes = NON_GUARANTEED_ENDPOINT
    )
    public void knoxLogout(@Context HttpServletRequest httpServletRequest, @Context HttpServletResponse httpServletResponse) throws Exception {
        String redirectPath = generateResourceUri("..", "nifi", "login");
        httpServletResponse.sendRedirect(redirectPath);
    }

    /**
     * Gets the status the client's access.
     *
     * @param httpServletRequest the servlet request
     * @return A accessStatusEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("")
    @ApiOperation(
            value = "Gets the status the client's access",
            notes = NON_GUARANTEED_ENDPOINT,
            response = AccessStatusEntity.class
    )
    @ApiResponses(
            value = {
                    @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(code = 401, message = "Unable to determine access status because the client could not be authenticated."),
                    @ApiResponse(code = 403, message = "Unable to determine access status because the client is not authorized to make this request."),
                    @ApiResponse(code = 409, message = "Unable to determine access status because NiFi is not in the appropriate state."),
                    @ApiResponse(code = 500, message = "Unable to determine access status because an unexpected error occurred.")
            }
    )
    public Response getAccessStatus(@Context HttpServletRequest httpServletRequest, @Context HttpServletResponse httpServletResponse) {

        // only consider user specific access over https
        if (!httpServletRequest.isSecure()) {
            throw new AuthenticationNotSupportedException(AUTHENTICATION_NOT_ENABLED_MSG);
        }

        final AccessStatusDTO accessStatus = new AccessStatusDTO();

        try {
            final X509Certificate[] certificates = certificateExtractor.extractClientCertificate(httpServletRequest);

            // if there is not certificate, consider a token
            if (certificates == null) {
                // look for an authorization token in header or cookie
                final String authorization = new NiFiBearerTokenResolver().resolve(httpServletRequest);

                // if there is no authorization header, we don't know the user
                if (authorization == null) {
                    accessStatus.setStatus(AccessStatusDTO.Status.UNKNOWN.name());
                    accessStatus.setMessage("No credentials supplied, unknown user.");
                } else {
                    try {
                        // authenticate the token
                        final JwtAuthenticationRequestToken jwtRequest = new JwtAuthenticationRequestToken(authorization, httpServletRequest.getRemoteAddr());
                        final NiFiAuthenticationToken authenticationResponse = (NiFiAuthenticationToken) jwtAuthenticationProvider.authenticate(jwtRequest);
                        final NiFiUser nifiUser = ((NiFiUserDetails) authenticationResponse.getDetails()).getNiFiUser();

                        // set the user identity
                        accessStatus.setIdentity(nifiUser.getIdentity());

                        // attempt authorize to /flow
                        accessStatus.setStatus(AccessStatusDTO.Status.ACTIVE.name());
                        accessStatus.setMessage("You are already logged in.");
                    } catch (final InvalidAuthenticationException iae) {
                        if (WebUtils.getCookie(httpServletRequest, NiFiBearerTokenResolver.JWT_COOKIE_NAME) != null) {
                            removeCookie(httpServletResponse, NiFiBearerTokenResolver.JWT_COOKIE_NAME);
                        }

                        throw iae;
                    }
                }
            } else {
                try {
                    final String proxiedEntitiesChain = httpServletRequest.getHeader(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN);
                    final String proxiedEntityGroups = httpServletRequest.getHeader(ProxiedEntitiesUtils.PROXY_ENTITY_GROUPS);

                    final X509AuthenticationRequestToken x509Request = new X509AuthenticationRequestToken(
                            proxiedEntitiesChain, proxiedEntityGroups, principalExtractor, certificates, httpServletRequest.getRemoteAddr());

                    final NiFiAuthenticationToken authenticationResponse = (NiFiAuthenticationToken) x509AuthenticationProvider.authenticate(x509Request);
                    final NiFiUser nifiUser = ((NiFiUserDetails) authenticationResponse.getDetails()).getNiFiUser();

                    // set the user identity
                    accessStatus.setIdentity(nifiUser.getIdentity());

                    // attempt authorize to /flow
                    accessStatus.setStatus(AccessStatusDTO.Status.ACTIVE.name());
                    accessStatus.setMessage("You are already logged in.");
                } catch (final IllegalArgumentException iae) {
                    throw new InvalidAuthenticationException(iae.getMessage(), iae);
                }
            }
        } catch (final UntrustedProxyException upe) {
            throw new AccessDeniedException(upe.getMessage(), upe);
        } catch (final AuthenticationServiceException ase) {
            throw new AdministrationException(ase.getMessage(), ase);
        }

        // create the entity
        final AccessStatusEntity entity = new AccessStatusEntity();
        entity.setAccessStatus(accessStatus);

        return generateOkResponse(entity).build();
    }

    /**
     * Creates a single use access token for downloading FlowFile content.
     *
     * @param httpServletRequest the servlet request
     * @return A token (string)
     */
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/download-token")
    @ApiOperation(
            value = "Creates a single use access token for downloading FlowFile content.",
            notes = "The token returned is a base64 encoded string. It is valid for a single request up to five minutes from being issued. " +
                    "It is used as a query parameter name 'access_token'.",
            response = String.class
    )
    @ApiResponses(
            value = {
                    @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                    @ApiResponse(code = 409, message = "Unable to create the download token because NiFi is not in the appropriate state. " +
                            "(i.e. may not have any tokens to grant or be configured to support username/password login)"),
                    @ApiResponse(code = 500, message = "Unable to create download token because an unexpected error occurred.")
            }
    )
    public Response createDownloadToken(@Context HttpServletRequest httpServletRequest) {
        // only support access tokens when communicating over HTTPS
        if (!httpServletRequest.isSecure()) {
            throw new IllegalStateException("Download tokens are only issued over HTTPS.");
        }

        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        if (user == null) {
            throw new AccessDeniedException("No user authenticated in the request.");
        }

        final OtpAuthenticationToken authenticationToken = new OtpAuthenticationToken(user.getIdentity());

        // generate otp for response
        final String token = otpService.generateDownloadToken(authenticationToken);

        // build the response
        final URI uri = URI.create(generateResourceUri("access", "download-token"));
        return generateCreatedResponse(uri, token).build();
    }

    /**
     * Creates a single use access token for accessing a NiFi UI extension.
     *
     * @param httpServletRequest the servlet request
     * @return A token (string)
     */
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/ui-extension-token")
    @ApiOperation(
            value = "Creates a single use access token for accessing a NiFi UI extension.",
            notes = "The token returned is a base64 encoded string. It is valid for a single request up to five minutes from being issued. " +
                    "It is used as a query parameter name 'access_token'.",
            response = String.class
    )
    @ApiResponses(
            value = {
                    @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                    @ApiResponse(code = 409, message = "Unable to create the download token because NiFi is not in the appropriate state. " +
                            "(i.e. may not have any tokens to grant or be configured to support username/password login)"),
                    @ApiResponse(code = 500, message = "Unable to create download token because an unexpected error occurred.")
            }
    )
    public Response createUiExtensionToken(@Context HttpServletRequest httpServletRequest) {
        // only support access tokens when communicating over HTTPS
        if (!httpServletRequest.isSecure()) {
            throw new AuthenticationNotSupportedException("UI extension access tokens are only issued over HTTPS.");
        }

        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        if (user == null) {
            throw new AccessDeniedException("No user authenticated in the request.");
        }

        final OtpAuthenticationToken authenticationToken = new OtpAuthenticationToken(user.getIdentity());

        // generate otp for response
        final String token = otpService.generateUiExtensionToken(authenticationToken);

        // build the response
        final URI uri = URI.create(generateResourceUri("access", "ui-extension-token"));
        return generateCreatedResponse(uri, token).build();
    }

    /**
     * Creates a token for accessing the REST API via Kerberos ticket exchange / SPNEGO negotiation.
     *
     * @param httpServletRequest the servlet request
     * @return A JWT (string)
     */
    @POST
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/kerberos")
    @ApiOperation(
            value = "Creates a token for accessing the REST API via Kerberos ticket exchange / SPNEGO negotiation",
            notes = "The token returned is formatted as a JSON Web Token (JWT). The token is base64 encoded and comprised of three parts. The header, " +
                    "the body, and the signature. The expiration of the token is a contained within the body. The token can be used in the Authorization header " +
                    "in the format 'Authorization: Bearer <token>'. It is also stored in the browser as a cookie.",
            response = String.class
    )
    @ApiResponses(
            value = {
                    @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(code = 401, message = "NiFi was unable to complete the request because it did not contain a valid Kerberos " +
                            "ticket in the Authorization header. Retry this request after initializing a ticket with kinit and " +
                            "ensuring your browser is configured to support SPNEGO."),
                    @ApiResponse(code = 409, message = "Unable to create access token because NiFi is not in the appropriate state. (i.e. may not be configured to support Kerberos login."),
                    @ApiResponse(code = 500, message = "Unable to create access token because an unexpected error occurred.")
            }
    )
    public Response createAccessTokenFromTicket(@Context HttpServletRequest httpServletRequest) {

        // only support access tokens when communicating over HTTPS
        if (!httpServletRequest.isSecure()) {
            throw new AuthenticationNotSupportedException("Access tokens are only issued over HTTPS.");
        }

        // If Kerberos Service Principal and keytab location not configured, throws exception
        if (!properties.isKerberosSpnegoSupportEnabled() || kerberosService == null) {
            final String message = "Kerberos ticket login not supported by this NiFi.";
            logger.debug(message);
            return Response.status(Response.Status.CONFLICT).entity(message).build();
        }

        String authorizationHeaderValue = httpServletRequest.getHeader(KerberosService.AUTHORIZATION_HEADER_NAME);

        if (!kerberosService.isValidKerberosHeader(authorizationHeaderValue)) {
            final Response response = generateNotAuthorizedResponse().header(KerberosService.AUTHENTICATION_CHALLENGE_HEADER_NAME, KerberosService.AUTHORIZATION_NEGOTIATE).build();
            return response;
        } else {
            try {
                // attempt to authenticate
                Authentication authentication = kerberosService.validateKerberosTicket(httpServletRequest);

                if (authentication == null) {
                    throw new IllegalArgumentException("Request is not HTTPS or Kerberos ticket missing or malformed");
                }

                final String expirationFromProperties = properties.getKerberosAuthenticationExpiration();
                long expiration = FormatUtils.getTimeDuration(expirationFromProperties, TimeUnit.MILLISECONDS);
                final String rawIdentity = authentication.getName();
                String mappedIdentity = IdentityMappingUtil.mapIdentity(rawIdentity, IdentityMappingUtil.getIdentityMappings(properties));
                expiration = validateTokenExpiration(expiration, mappedIdentity);

                // create the authentication token
                final LoginAuthenticationToken loginAuthenticationToken = new LoginAuthenticationToken(mappedIdentity, expiration, "KerberosService");

                // generate JWT for response
                final String token = jwtService.generateSignedToken(loginAuthenticationToken);

                // build the response
                final URI uri = URI.create(generateResourceUri("access", "kerberos"));
                return generateTokenResponse(generateCreatedResponse(uri, token), token);
            } catch (final AuthenticationException e) {
                throw new AccessDeniedException(e.getMessage(), e);
            }
        }
    }

    /**
     * Creates a token for accessing the REST API via username/password stored as a cookie in the browser.
     *
     * @param httpServletRequest the servlet request
     * @param username           the username
     * @param password           the password
     * @return A JWT (string) in a cookie and as the body
     */
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/token")
    @ApiOperation(
            value = "Creates a token for accessing the REST API via username/password",
            notes = "The token returned is formatted as a JSON Web Token (JWT). The token is base64 encoded and comprised of three parts. The header, " +
                    "the body, and the signature. The expiration of the token is a contained within the body. It is stored in the browser as a cookie, but also returned in" +
                    "the response body to be stored/used by third party client scripts.",
            response = String.class
    )
    @ApiResponses(
            value = {
                    @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                    @ApiResponse(code = 409, message = "Unable to create access token because NiFi is not in the appropriate state. (i.e. may not be configured to support username/password login."),
                    @ApiResponse(code = 500, message = "Unable to create access token because an unexpected error occurred.")
            }
    )
    public Response createAccessToken(
            @Context HttpServletRequest httpServletRequest,
            @FormParam("username") String username,
            @FormParam("password") String password) {

        // only support access tokens when communicating over HTTPS
        if (!httpServletRequest.isSecure()) {
            throw new AuthenticationNotSupportedException("Access tokens are only issued over HTTPS.");
        }

        // if not configuration for login, don't consider credentials
        if (loginIdentityProvider == null) {
            throw new IllegalStateException("Username/Password login not supported by this NiFi.");
        }

        final LoginAuthenticationToken loginAuthenticationToken;

        // ensure we have login credentials
        if (StringUtils.isBlank(username) || StringUtils.isBlank(password)) {
            throw new IllegalArgumentException("The username and password must be specified.");
        }

        try {
            // attempt to authenticate
            final AuthenticationResponse authenticationResponse = loginIdentityProvider.authenticate(new LoginCredentials(username, password));
            final String rawIdentity = authenticationResponse.getIdentity();
            String mappedIdentity = IdentityMappingUtil.mapIdentity(rawIdentity, IdentityMappingUtil.getIdentityMappings(properties));
            long expiration = validateTokenExpiration(authenticationResponse.getExpiration(), mappedIdentity);

            // create the authentication token
            loginAuthenticationToken = new LoginAuthenticationToken(mappedIdentity, expiration, authenticationResponse.getIssuer());
        } catch (final InvalidLoginCredentialsException ilce) {
            throw new IllegalArgumentException("The supplied username and password are not valid.", ilce);
        } catch (final IdentityAccessException iae) {
            throw new AdministrationException(iae.getMessage(), iae);
        }

        // generate JWT for response
        final String token = jwtService.generateSignedToken(loginAuthenticationToken);

        // build the response
        final URI uri = URI.create(generateResourceUri("access", "token"));
        return generateTokenResponse(generateCreatedResponse(uri, token), token);
    }

    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path("/logout")
    @ApiOperation(
            value = "Performs a logout for other providers that have been issued a JWT.",
            notes = NON_GUARANTEED_ENDPOINT
    )
    @ApiResponses(
            value = {
                    @ApiResponse(code = 200, message = "User was logged out successfully."),
                    @ApiResponse(code = 401, message = "Authentication token provided was empty or not in the correct JWT format."),
                    @ApiResponse(code = 500, message = "Client failed to log out."),
            }
    )
    public Response logOut(@Context HttpServletRequest httpServletRequest, @Context HttpServletResponse httpServletResponse) {
        if (!httpServletRequest.isSecure()) {
            throw new IllegalStateException(AUTHENTICATION_NOT_ENABLED_MSG);
        }

        final String mappedUserIdentity = NiFiUserUtils.getNiFiUserIdentity();
        if (StringUtils.isBlank(mappedUserIdentity)) {
            return Response.status(Response.Status.UNAUTHORIZED)
                    .entity("Authentication token provided was empty or not in the correct JWT format.").build();
        }

        try {
            logger.info("Logging out " + mappedUserIdentity);
            logOutUser(httpServletRequest);
            removeCookie(httpServletResponse, NiFiBearerTokenResolver.JWT_COOKIE_NAME);
            logger.debug("Invalidated JWT for user [{}]", mappedUserIdentity);

            // create a LogoutRequest and tell the LogoutRequestManager about it for later retrieval
            final LogoutRequest logoutRequest = new LogoutRequest(UUID.randomUUID().toString(), mappedUserIdentity);
            logoutRequestManager.start(logoutRequest);

            // generate a cookie to store the logout request identifier
            final Cookie cookie = new Cookie(LOGOUT_REQUEST_IDENTIFIER, logoutRequest.getRequestIdentifier());
            cookie.setPath("/");
            cookie.setHttpOnly(true);
            cookie.setMaxAge(60);
            cookie.setSecure(true);
            httpServletResponse.addCookie(cookie);

            return generateOkResponse().build();
        } catch (final JwtException e) {
            logger.error("JWT processing failed for [{}], due to: ", mappedUserIdentity, e.getMessage(), e);
            return Response.serverError().build();
        } catch (final LogoutException e) {
            logger.error("Logout failed for user [{}] due to: ", mappedUserIdentity, e.getMessage(), e);
            return Response.serverError().build();
        }
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path("/logout/complete")
    @ApiOperation(
            value = "Completes the logout sequence by removing the cached Logout Request and Cookie if they existed and redirects to /nifi/login.",
            notes = NON_GUARANTEED_ENDPOINT
    )
    @ApiResponses(
            value = {
                    @ApiResponse(code = 200, message = "User was logged out successfully."),
                    @ApiResponse(code = 401, message = "Authentication token provided was empty or not in the correct JWT format."),
                    @ApiResponse(code = 500, message = "Client failed to log out."),
            }
    )
    public void logOutComplete(@Context HttpServletRequest httpServletRequest, @Context HttpServletResponse httpServletResponse) throws Exception {
        if (!httpServletRequest.isSecure()) {
            throw new IllegalStateException("User authentication/authorization is only supported when running over HTTPS.");
        }

        // complete the logout request by removing the cookie and cached request, if they were present
        completeLogoutRequest(httpServletResponse);

        // redirect to logout landing page
        httpServletResponse.sendRedirect(getNiFiLogoutCompleteUri());
    }

    LogoutRequest completeLogoutRequest(final HttpServletResponse httpServletResponse) {
        LogoutRequest logoutRequest = null;

        // check if a logout request identifier is present and if so complete the request
        final Cookie cookie = WebUtils.getCookie(httpServletRequest, LOGOUT_REQUEST_IDENTIFIER);
        final String logoutRequestIdentifier = cookie == null ? null : cookie.getValue();
        if (logoutRequestIdentifier != null) {
            logoutRequest = logoutRequestManager.complete(logoutRequestIdentifier);
        }

        if (logoutRequest == null) {
            logger.warn("Logout request did not exist for identifier: " + logoutRequestIdentifier);
        } else {
            logger.info("Completed logout request for " + logoutRequest.getMappedUserIdentity());
        }

        // remove the cookie if it existed
        removeLogoutRequestCookie(httpServletResponse);

        return logoutRequest;
    }

    long validateTokenExpiration(long proposedTokenExpiration, String identity) {
        final long maxExpiration = TimeUnit.MILLISECONDS.convert(12, TimeUnit.HOURS);
        final long minExpiration = TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES);

        if (proposedTokenExpiration > maxExpiration) {
            logger.warn(String.format("Max token expiration exceeded. Setting expiration to %s from %s for %s", maxExpiration,
                    proposedTokenExpiration, identity));
            proposedTokenExpiration = maxExpiration;
        } else if (proposedTokenExpiration < minExpiration) {
            logger.warn(String.format("Min token expiration not met. Setting expiration to %s from %s for %s", minExpiration,
                    proposedTokenExpiration, identity));
            proposedTokenExpiration = minExpiration;
        }

        return proposedTokenExpiration;
    }

    String getNiFiLogoutCompleteUri() {
        return getNiFiUri() + "logout-complete";
    }

    void removeLogoutRequestCookie(final HttpServletResponse httpServletResponse) {
        removeCookie(httpServletResponse, LOGOUT_REQUEST_IDENTIFIER);
    }

    // setters
    public void setLoginIdentityProvider(LoginIdentityProvider loginIdentityProvider) {
        this.loginIdentityProvider = loginIdentityProvider;
    }

    public void setJwtService(JwtService jwtService) {
        this.jwtService = jwtService;
    }

    public void setJwtAuthenticationProvider(JwtAuthenticationProvider jwtAuthenticationProvider) {
        this.jwtAuthenticationProvider = jwtAuthenticationProvider;
    }

    public void setKerberosService(KerberosService kerberosService) {
        this.kerberosService = kerberosService;
    }

    public void setX509AuthenticationProvider(X509AuthenticationProvider x509AuthenticationProvider) {
        this.x509AuthenticationProvider = x509AuthenticationProvider;
    }

    public void setPrincipalExtractor(X509PrincipalExtractor principalExtractor) {
        this.principalExtractor = principalExtractor;
    }

    public void setCertificateExtractor(X509CertificateExtractor certificateExtractor) {
        this.certificateExtractor = certificateExtractor;
    }

    public void setOtpService(OtpService otpService) {
        this.otpService = otpService;
    }

    public void setKnoxService(KnoxService knoxService) {
        this.knoxService = knoxService;
    }

    private void logOutUser(HttpServletRequest httpServletRequest) {
        final String jwt = new NiFiBearerTokenResolver().resolve(httpServletRequest);
        jwtService.logOut(jwt);
    }

    public void setLogoutRequestManager(LogoutRequestManager logoutRequestManager) {
        this.logoutRequestManager = logoutRequestManager;
    }
}
