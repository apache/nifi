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

import java.net.HttpURLConnection;
import java.net.URI;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.FormParam;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriBuilder;
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
import org.apache.nifi.web.api.dto.AccessTokenExpirationDTO;
import org.apache.nifi.web.api.entity.AccessConfigurationEntity;
import org.apache.nifi.web.api.entity.AccessStatusEntity;
import org.apache.nifi.web.api.entity.AccessTokenExpirationEntity;
import org.apache.nifi.web.security.InvalidAuthenticationException;
import org.apache.nifi.web.security.LogoutException;
import org.apache.nifi.web.security.ProxiedEntitiesUtils;
import org.apache.nifi.web.security.UntrustedProxyException;
import org.apache.nifi.web.security.cookie.ApplicationCookieName;
import org.apache.nifi.web.security.jwt.provider.BearerTokenProvider;
import org.apache.nifi.web.security.jwt.revocation.JwtLogoutListener;
import org.apache.nifi.web.security.kerberos.KerberosService;
import org.apache.nifi.web.security.knox.KnoxService;
import org.apache.nifi.web.security.logout.LogoutRequest;
import org.apache.nifi.web.security.logout.LogoutRequestManager;
import org.apache.nifi.web.security.token.LoginAuthenticationToken;
import org.apache.nifi.web.security.x509.X509AuthenticationProvider;
import org.apache.nifi.web.security.x509.X509AuthenticationRequestToken;
import org.apache.nifi.web.security.x509.X509CertificateExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.AuthenticationServiceException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.server.resource.authentication.BearerTokenAuthenticationToken;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationProvider;
import org.springframework.security.oauth2.server.resource.web.BearerTokenResolver;
import org.springframework.security.web.authentication.preauth.x509.X509PrincipalExtractor;

/**
 * RESTful endpoint for managing access.
 */
@Path("/access")
@Tag(name = "Access")
public class AccessResource extends ApplicationResource {

    private static final Logger logger = LoggerFactory.getLogger(AccessResource.class);
    protected static final String AUTHENTICATION_NOT_ENABLED_MSG = "User authentication/authorization is only supported when running over HTTPS.";

    private X509CertificateExtractor certificateExtractor;
    private X509AuthenticationProvider x509AuthenticationProvider;
    private X509PrincipalExtractor principalExtractor;

    private LoginIdentityProvider loginIdentityProvider;
    private JwtAuthenticationProvider jwtAuthenticationProvider;
    private JwtLogoutListener jwtLogoutListener;
    private JwtDecoder jwtDecoder;
    private BearerTokenProvider bearerTokenProvider;
    private BearerTokenResolver bearerTokenResolver;
    private KnoxService knoxService;
    private KerberosService kerberosService;
    private LogoutRequestManager logoutRequestManager;

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
    @Operation(
            summary = "Retrieves the access configuration for this NiFi",
            responses = @ApiResponse(content = @Content(schema = @Schema(implementation = AccessConfigurationEntity.class)))
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
    @Operation(
            summary = "Initiates a request to authenticate through Apache Knox."
    )
    public void knoxRequest(@Context HttpServletRequest httpServletRequest, @Context HttpServletResponse httpServletResponse) throws Exception {
        // only consider user specific access over https
        if (!httpServletRequest.isSecure()) {
            httpServletResponse.sendError(HttpURLConnection.HTTP_NOT_ACCEPTABLE, AUTHENTICATION_NOT_ENABLED_MSG);
            return;
        }

        // ensure knox is enabled
        if (!knoxService.isKnoxEnabled()) {
            httpServletResponse.sendError(HttpURLConnection.HTTP_NOT_ACCEPTABLE, "Apache Knox SSO support is not configured.");
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
    @Operation(
            summary = "Redirect/callback URI for processing the result of the Apache Knox login sequence."
    )
    public void knoxCallback(@Context HttpServletRequest httpServletRequest, @Context HttpServletResponse httpServletResponse) throws Exception {
        // only consider user specific access over https
        if (!httpServletRequest.isSecure()) {
            httpServletResponse.sendError(HttpURLConnection.HTTP_NOT_ACCEPTABLE, AUTHENTICATION_NOT_ENABLED_MSG);
            return;
        }

        // ensure knox is enabled
        if (!knoxService.isKnoxEnabled()) {
            httpServletResponse.sendError(HttpURLConnection.HTTP_NOT_ACCEPTABLE, "Apache Knox SSO support is not configured.");
            return;
        }

        httpServletResponse.sendRedirect(getNiFiUri());
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path("knox/logout")
    @Operation(
            summary = "Performs a logout in the Apache Knox.",
            description = NON_GUARANTEED_ENDPOINT
    )
    public void knoxLogout(@Context HttpServletResponse httpServletResponse) throws Exception {
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
    @Operation(
            summary = "Gets the status the client's access",
            description = NON_GUARANTEED_ENDPOINT,
            responses = @ApiResponse(content = @Content(schema = @Schema(implementation = AccessStatusEntity.class)))
    )
    @ApiResponses(
            value = {
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Unable to determine access status because the client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Unable to determine access status because the client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it."),
                    @ApiResponse(responseCode = "500", description = "Unable to determine access status because an unexpected error occurred.")
            }
    )
    public Response getAccessStatus(@Context HttpServletRequest httpServletRequest, @Context HttpServletResponse httpServletResponse) {
        if (!httpServletRequest.isSecure()) {
            throw new AuthenticationNotSupportedException(AUTHENTICATION_NOT_ENABLED_MSG);
        }

        final AccessStatusDTO accessStatus = new AccessStatusDTO();

        try {
            final X509Certificate[] certificates = certificateExtractor.extractClientCertificate(httpServletRequest);

            if (certificates == null) {
                final String bearerToken = bearerTokenResolver.resolve(httpServletRequest);
                if (bearerToken == null) {
                    accessStatus.setStatus(AccessStatusDTO.Status.UNKNOWN.name());
                    accessStatus.setMessage("Access Unknown: Certificate and Token not found.");
                } else {
                    try {
                        final BearerTokenAuthenticationToken authenticationToken = new BearerTokenAuthenticationToken(bearerToken);
                        final Authentication authentication = jwtAuthenticationProvider.authenticate(authenticationToken);
                        final NiFiUserDetails userDetails = (NiFiUserDetails) authentication.getPrincipal();
                        final String identity = userDetails.getUsername();

                        accessStatus.setIdentity(identity);
                        accessStatus.setStatus(AccessStatusDTO.Status.ACTIVE.name());
                        accessStatus.setMessage("Access Granted: Token authenticated.");
                    } catch (final AuthenticationException iae) {
                        applicationCookieService.removeCookie(getCookieResourceUri(), httpServletResponse, ApplicationCookieName.AUTHORIZATION_BEARER);
                        throw iae;
                    }
                }
            } else {
                try {
                    final String proxiedEntitiesChain = httpServletRequest.getHeader(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN);
                    final String proxiedEntityGroups = httpServletRequest.getHeader(ProxiedEntitiesUtils.PROXY_ENTITY_GROUPS);

                    final X509AuthenticationRequestToken x509Request = new X509AuthenticationRequestToken(
                            proxiedEntitiesChain, proxiedEntityGroups, principalExtractor, certificates, httpServletRequest.getRemoteAddr());

                    final Authentication authenticationResponse = x509AuthenticationProvider.authenticate(x509Request);
                    final NiFiUser nifiUser = ((NiFiUserDetails) authenticationResponse.getDetails()).getNiFiUser();

                    accessStatus.setIdentity(nifiUser.getIdentity());
                    accessStatus.setStatus(AccessStatusDTO.Status.ACTIVE.name());
                    accessStatus.setMessage("Access Granted: Certificate authenticated.");
                } catch (final IllegalArgumentException iae) {
                    throw new InvalidAuthenticationException(iae.getMessage(), iae);
                }
            }
        } catch (final UntrustedProxyException upe) {
            throw new AccessDeniedException(upe.getMessage(), upe);
        } catch (final AuthenticationServiceException ase) {
            throw new AdministrationException(ase.getMessage(), ase);
        }

        final AccessStatusEntity entity = new AccessStatusEntity();
        entity.setAccessStatus(accessStatus);

        return generateOkResponse(entity).build();
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
    @Operation(
            summary = "Creates a token for accessing the REST API via Kerberos ticket exchange / SPNEGO negotiation",
            description = "The token returned is formatted as a JSON Web Token (JWT). The token is base64 encoded and comprised of three parts. The header, " +
                    "the body, and the signature. The expiration of the token is a contained within the body. The token can be used in the Authorization header " +
                    "in the format 'Authorization: Bearer <token>'. It is also stored in the browser as a cookie.",
            responses = @ApiResponse(content = @Content(schema = @Schema(implementation = String.class)))
    )
    @ApiResponses(
            value = {
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(
                            responseCode = "401", description = "NiFi was unable to complete the request because it did not contain a valid Kerberos " +
                            "ticket in the Authorization header. Retry this request after initializing a ticket with kinit and " +
                            "ensuring your browser is configured to support SPNEGO."
                    ),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it."),
                    @ApiResponse(responseCode = "500", description = "Unable to create access token because an unexpected error occurred.")
            }
    )
    public Response createAccessTokenFromTicket(@Context final HttpServletRequest httpServletRequest, @Context final HttpServletResponse httpServletResponse) {

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
            return generateNotAuthorizedResponse().header(KerberosService.AUTHENTICATION_CHALLENGE_HEADER_NAME, KerberosService.AUTHORIZATION_NEGOTIATE).build();
        } else {
            try {
                // attempt to authenticate
                Authentication authentication = kerberosService.validateKerberosTicket(httpServletRequest);

                if (authentication == null) {
                    throw new IllegalArgumentException("Request is not HTTPS or Kerberos ticket missing or malformed");
                }

                final String expirationFromProperties = properties.getKerberosAuthenticationExpiration();
                final long expirationDuration = Math.round(FormatUtils.getPreciseTimeDuration(expirationFromProperties, TimeUnit.MILLISECONDS));
                final Instant expiration = Instant.now().plusMillis(expirationDuration);

                final String rawIdentity = authentication.getName();
                final String mappedIdentity = IdentityMappingUtil.mapIdentity(rawIdentity, IdentityMappingUtil.getIdentityMappings(properties));

                final LoginAuthenticationToken loginAuthenticationToken = new LoginAuthenticationToken(mappedIdentity, expiration, Collections.emptySet());
                final String token = bearerTokenProvider.getBearerToken(loginAuthenticationToken);
                final URI uri = URI.create(generateResourceUri("access", "kerberos"));
                setBearerToken(httpServletResponse, token);
                return generateCreatedResponse(uri, token).build();
            } catch (final AuthenticationException e) {
                throw new AccessDeniedException(e.getMessage(), e);
            }
        }
    }

    /**
     * Creates a token for accessing the REST API via username/password stored as a cookie in the browser.
     *
     * @param httpServletRequest the servlet request
     * @param username the username
     * @param password the password
     * @return A JWT (string) in a cookie and as the body
     */
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/token")
    @Operation(
            summary = "Creates a token for accessing the REST API via username/password",
            description = "The token returned is formatted as a JSON Web Token (JWT). The token is base64 encoded and comprised of three parts. The header, " +
                    "the body, and the signature. The expiration of the token is a contained within the body. It is stored in the browser as a cookie, but also returned in" +
                    "the response body to be stored/used by third party client scripts.",
            responses = @ApiResponse(content = @Content(schema = @Schema(implementation = String.class)))
    )
    @ApiResponses(
            value = {
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it."),
                    @ApiResponse(responseCode = "500", description = "Unable to create access token because an unexpected error occurred.")
            }
    )
    public Response createAccessToken(
            @Context final HttpServletRequest httpServletRequest,
            @Context final HttpServletResponse httpServletResponse,
            @FormParam("username") final String username,
            @FormParam("password") final String password) {

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
            final String mappedIdentity = IdentityMappingUtil.mapIdentity(rawIdentity, IdentityMappingUtil.getIdentityMappings(properties));
            final long expirationDuration = authenticationResponse.getExpiration();
            final Instant expiration = Instant.now().plusMillis(expirationDuration);

            // create the authentication token
            loginAuthenticationToken = new LoginAuthenticationToken(mappedIdentity, expiration, Collections.emptySet());
        } catch (final InvalidLoginCredentialsException ilce) {
            throw new IllegalArgumentException("The supplied username and password are not valid.", ilce);
        } catch (final IdentityAccessException iae) {
            throw new AdministrationException(iae.getMessage(), iae);
        }

        final String bearerToken = bearerTokenProvider.getBearerToken(loginAuthenticationToken);
        final URI uri = URI.create(generateResourceUri("access", "token"));
        setBearerToken(httpServletResponse, bearerToken);
        return generateCreatedResponse(uri, bearerToken).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/token/expiration")
    @Operation(
            summary = "Get expiration for current Access Token",
            description = NON_GUARANTEED_ENDPOINT,
            responses = @ApiResponse(content = @Content(schema = @Schema(implementation = AccessTokenExpirationEntity.class)))
    )
    @ApiResponses(
            value = {
                    @ApiResponse(responseCode = "200", description = "Access Token Expiration found"),
                    @ApiResponse(responseCode = "401", description = "Access Token not authorized"),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            }
    )
    public Response getAccessTokenExpiration() {
        final String bearerToken = bearerTokenResolver.resolve(httpServletRequest);
        if (bearerToken == null) {
            throw new IllegalStateException("Access Token not found");
        } else {
            final Jwt jwt = jwtDecoder.decode(bearerToken);
            final Instant expiration = jwt.getExpiresAt();
            final AccessTokenExpirationDTO accessTokenExpiration = new AccessTokenExpirationDTO();
            accessTokenExpiration.setExpiration(expiration);
            final AccessTokenExpirationEntity accessTokenExpirationEntity = new AccessTokenExpirationEntity();
            accessTokenExpirationEntity.setAccessTokenExpiration(accessTokenExpiration);
            return Response.ok(accessTokenExpirationEntity).build();
        }
    }

    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path("/logout")
    @Operation(
            summary = "Performs a logout for other providers that have been issued a JWT.",
            description = NON_GUARANTEED_ENDPOINT
    )
    @ApiResponses(
            value = {
                    @ApiResponse(responseCode = "200", description = "User was logged out successfully."),
                    @ApiResponse(responseCode = "401", description = "Authentication token provided was empty or not in the correct JWT format."),
                    @ApiResponse(responseCode = "500", description = "Client failed to log out."),
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
            final String requestIdentifier = UUID.randomUUID().toString();
            logger.info("Logout Request [{}] Identity [{}] started", requestIdentifier, mappedUserIdentity);
            applicationCookieService.removeCookie(getCookieResourceUri(), httpServletResponse, ApplicationCookieName.AUTHORIZATION_BEARER);

            final String bearerToken = bearerTokenResolver.resolve(httpServletRequest);
            jwtLogoutListener.logout(bearerToken);

            // create a LogoutRequest and tell the LogoutRequestManager about it for later retrieval
            final LogoutRequest logoutRequest = new LogoutRequest(requestIdentifier, mappedUserIdentity);
            logoutRequestManager.start(logoutRequest);

            applicationCookieService.addCookie(getCookieResourceUri(), httpServletResponse, ApplicationCookieName.LOGOUT_REQUEST_IDENTIFIER, logoutRequest.getRequestIdentifier());
            return generateOkResponse().build();
        } catch (final LogoutException e) {
            logger.error("Logout Failed Identity [{}]", mappedUserIdentity, e);
            return Response.serverError().build();
        }
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path("/logout/complete")
    @Operation(
            summary = "Completes the logout sequence by removing the cached Logout Request and Cookie if they existed and redirects to /nifi/login.",
            description = NON_GUARANTEED_ENDPOINT
    )
    @ApiResponses(
            value = {
                    @ApiResponse(responseCode = "200", description = "User was logged out successfully."),
                    @ApiResponse(responseCode = "401", description = "Authentication token provided was empty or not in the correct JWT format."),
                    @ApiResponse(responseCode = "500", description = "Client failed to log out."),
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

    private LogoutRequest completeLogoutRequest(final HttpServletResponse httpServletResponse) {
        LogoutRequest logoutRequest = null;

        final Optional<String> cookieValue = getLogoutRequestIdentifier();
        if (cookieValue.isPresent()) {
            final String logoutRequestIdentifier = cookieValue.get();
            logoutRequest = logoutRequestManager.complete(logoutRequestIdentifier);
            logger.info("Logout Request [{}] Completed [{}]", logoutRequestIdentifier, logoutRequest.getMappedUserIdentity());
        } else {
            logger.warn("Logout Request Cookie [{}] not found", ApplicationCookieName.LOGOUT_REQUEST_IDENTIFIER.getCookieName());
        }

        removeLogoutRequestCookie(httpServletResponse);
        return logoutRequest;
    }

    private String getNiFiLogoutCompleteUri() {
        return getNiFiUri() + "logout-complete";
    }

    /**
     * Send Set-Cookie header to remove Logout Request Identifier cookie from client
     *
     * @param httpServletResponse HTTP Servlet Response
     */
    private void removeLogoutRequestCookie(final HttpServletResponse httpServletResponse) {
        applicationCookieService.removeCookie(getCookieResourceUri(), httpServletResponse, ApplicationCookieName.LOGOUT_REQUEST_IDENTIFIER);
    }

    /**
     * Get Logout Request Identifier from current HTTP Request Cookie header
     *
     * @return Optional Logout Request Identifier
     */
    private Optional<String> getLogoutRequestIdentifier() {
        return applicationCookieService.getCookieValue(httpServletRequest, ApplicationCookieName.LOGOUT_REQUEST_IDENTIFIER);
    }

    // setters
    public void setLoginIdentityProvider(LoginIdentityProvider loginIdentityProvider) {
        this.loginIdentityProvider = loginIdentityProvider;
    }

    public void setBearerTokenProvider(final BearerTokenProvider bearerTokenProvider) {
        this.bearerTokenProvider = bearerTokenProvider;
    }

    public void setBearerTokenResolver(final BearerTokenResolver bearerTokenResolver) {
        this.bearerTokenResolver = bearerTokenResolver;
    }

    public void setJwtAuthenticationProvider(JwtAuthenticationProvider jwtAuthenticationProvider) {
        this.jwtAuthenticationProvider = jwtAuthenticationProvider;
    }

    public void setJwtDecoder(final JwtDecoder jwtDecoder) {
        this.jwtDecoder = jwtDecoder;
    }

    public void setJwtLogoutListener(final JwtLogoutListener jwtLogoutListener) {
        this.jwtLogoutListener = jwtLogoutListener;
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

    public void setKnoxService(KnoxService knoxService) {
        this.knoxService = knoxService;
    }

    public void setLogoutRequestManager(LogoutRequestManager logoutRequestManager) {
        this.logoutRequestManager = logoutRequestManager;
    }
}
