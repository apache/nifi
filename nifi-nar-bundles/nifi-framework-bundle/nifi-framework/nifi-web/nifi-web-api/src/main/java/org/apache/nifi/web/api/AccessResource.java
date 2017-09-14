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

import com.nimbusds.oauth2.sdk.AuthorizationCode;
import com.nimbusds.oauth2.sdk.AuthorizationCodeGrant;
import com.nimbusds.oauth2.sdk.AuthorizationGrant;
import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.oauth2.sdk.id.State;
import com.nimbusds.openid.connect.sdk.AuthenticationErrorResponse;
import com.nimbusds.openid.connect.sdk.AuthenticationResponseParser;
import com.nimbusds.openid.connect.sdk.AuthenticationSuccessResponse;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;
import io.jsonwebtoken.JwtException;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.admin.service.AdministrationException;
import org.apache.nifi.authentication.AuthenticationResponse;
import org.apache.nifi.authentication.LoginCredentials;
import org.apache.nifi.authentication.LoginIdentityProvider;
import org.apache.nifi.authentication.exception.IdentityAccessException;
import org.apache.nifi.authentication.exception.InvalidLoginCredentialsException;
import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserDetails;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.web.api.dto.AccessConfigurationDTO;
import org.apache.nifi.web.api.dto.AccessStatusDTO;
import org.apache.nifi.web.api.entity.AccessConfigurationEntity;
import org.apache.nifi.web.api.entity.AccessStatusEntity;
import org.apache.nifi.web.security.InvalidAuthenticationException;
import org.apache.nifi.web.security.ProxiedEntitiesUtils;
import org.apache.nifi.web.security.UntrustedProxyException;
import org.apache.nifi.web.security.jwt.JwtAuthenticationFilter;
import org.apache.nifi.web.security.jwt.JwtAuthenticationProvider;
import org.apache.nifi.web.security.jwt.JwtAuthenticationRequestToken;
import org.apache.nifi.web.security.jwt.JwtService;
import org.apache.nifi.web.security.kerberos.KerberosService;
import org.apache.nifi.web.security.knox.KnoxService;
import org.apache.nifi.web.security.oidc.OidcService;
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

import javax.servlet.ServletContext;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
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

    private static final String OIDC_REQUEST_IDENTIFIER = "oidc-request-identifier";
    private static final String OIDC_ERROR_TITLE = "Unable to continue login sequence";

    private X509CertificateExtractor certificateExtractor;
    private X509AuthenticationProvider x509AuthenticationProvider;
    private X509PrincipalExtractor principalExtractor;

    private LoginIdentityProvider loginIdentityProvider;
    private JwtAuthenticationProvider jwtAuthenticationProvider;
    private JwtService jwtService;
    private OtpService otpService;
    private OidcService oidcService;
    private KnoxService knoxService;

    private KerberosService kerberosService;

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
    @Path("oidc/request")
    @ApiOperation(
            value = "Initiates a request to authenticate through the configured OpenId Connect provider.",
            notes = NON_GUARANTEED_ENDPOINT
    )
    public void oidcRequest(@Context HttpServletRequest httpServletRequest, @Context HttpServletResponse httpServletResponse) throws Exception {
        // only consider user specific access over https
        if (!httpServletRequest.isSecure()) {
            forwardToMessagePage(httpServletRequest, httpServletResponse, "User authentication/authorization is only supported when running over HTTPS.");
            return;
        }

        // ensure oidc is enabled
        if (!oidcService.isOidcEnabled()) {
            forwardToMessagePage(httpServletRequest, httpServletResponse, "OpenId Connect is not configured.");
            return;
        }

        final String oidcRequestIdentifier = UUID.randomUUID().toString();

        // generate a cookie to associate this login sequence
        final Cookie cookie = new Cookie(OIDC_REQUEST_IDENTIFIER, oidcRequestIdentifier);
        cookie.setPath("/");
        cookie.setHttpOnly(true);
        cookie.setMaxAge(60);
        cookie.setSecure(true);
        httpServletResponse.addCookie(cookie);

        // get the state for this request
        final State state = oidcService.createState(oidcRequestIdentifier);

        // build the authorization uri
        final URI authorizationUri = UriBuilder.fromUri(oidcService.getAuthorizationEndpoint())
                .queryParam("client_id", oidcService.getClientId())
                .queryParam("response_type", "code")
                .queryParam("scope", oidcService.getScope().toString())
                .queryParam("state", state.getValue())
                .queryParam("redirect_uri", getOidcCallback())
                .build();

        // generate the response
        httpServletResponse.sendRedirect(authorizationUri.toString());
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path("oidc/callback")
    @ApiOperation(
            value = "Redirect/callback URI for processing the result of the OpenId Connect login sequence.",
            notes = NON_GUARANTEED_ENDPOINT
    )
    public void oidcCallback(@Context HttpServletRequest httpServletRequest, @Context HttpServletResponse httpServletResponse) throws Exception {
        // only consider user specific access over https
        if (!httpServletRequest.isSecure()) {
            forwardToMessagePage(httpServletRequest, httpServletResponse, "User authentication/authorization is only supported when running over HTTPS.");
            return;
        }

        // ensure oidc is enabled
        if (!oidcService.isOidcEnabled()) {
            forwardToMessagePage(httpServletRequest, httpServletResponse, "OpenId Connect is not configured.");
            return;
        }

        final String oidcRequestIdentifier = getCookieValue(httpServletRequest.getCookies(), OIDC_REQUEST_IDENTIFIER);
        if (oidcRequestIdentifier == null) {
            forwardToMessagePage(httpServletRequest, httpServletResponse, "The login request identifier was not found in the request. Unable to continue.");
            return;
        }

        final com.nimbusds.openid.connect.sdk.AuthenticationResponse oidcResponse;
        try {
            oidcResponse = AuthenticationResponseParser.parse(getRequestUri());
        } catch (final ParseException e) {
            logger.error("Unable to parse the redirect URI from the OpenId Connect Provider. Unable to continue login process.");

            // remove the oidc request cookie
            removeOidcRequestCookie(httpServletResponse);

            // forward to the error page
            forwardToMessagePage(httpServletRequest, httpServletResponse, "Unable to parse the redirect URI from the OpenId Connect Provider. Unable to continue login process.");
            return;
        }

        if (oidcResponse.indicatesSuccess()) {
            final AuthenticationSuccessResponse successfulOidcResponse = (AuthenticationSuccessResponse) oidcResponse;

            // confirm state
            final State state = successfulOidcResponse.getState();
            if (state == null || !oidcService.isStateValid(oidcRequestIdentifier, state)) {
                logger.error("The state value returned by the OpenId Connect Provider does not match the stored state. Unable to continue login process.");

                // remove the oidc request cookie
                removeOidcRequestCookie(httpServletResponse);

                // forward to the error page
                forwardToMessagePage(httpServletRequest, httpServletResponse, "Purposed state does not match the stored state. Unable to continue login process.");
                return;
            }

            try {
                // exchange authorization code for id token
                final AuthorizationCode authorizationCode = successfulOidcResponse.getAuthorizationCode();
                final AuthorizationGrant authorizationGrant = new AuthorizationCodeGrant(authorizationCode, URI.create(getOidcCallback()));
                oidcService.exchangeAuthorizationCode(oidcRequestIdentifier, authorizationGrant);
            } catch (final Exception e) {
                logger.error("Unable to exchange authorization for ID token: " + e.getMessage(), e);

                // remove the oidc request cookie
                removeOidcRequestCookie(httpServletResponse);

                // forward to the error page
                forwardToMessagePage(httpServletRequest, httpServletResponse, "Unable to exchange authorization for ID token: " + e.getMessage());
                return;
            }

            // redirect to the name page
            httpServletResponse.sendRedirect("../../../nifi");
        } else {
            // remove the oidc request cookie
            removeOidcRequestCookie(httpServletResponse);

            // report the unsuccessful login
            final AuthenticationErrorResponse errorOidcResponse = (AuthenticationErrorResponse) oidcResponse;
            forwardToMessagePage(httpServletRequest, httpServletResponse, "Unsuccessful login attempt: " + errorOidcResponse.getErrorObject().getDescription());
        }
    }

    @POST
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("oidc/exchange")
    @ApiOperation(
            value = "Retrieves a JWT following a successful login sequence using the configured OpenId Connect provider.",
            response = String.class,
            notes = NON_GUARANTEED_ENDPOINT
    )
    public Response oidcExchange(@Context HttpServletRequest httpServletRequest, @Context HttpServletResponse httpServletResponse) throws Exception {
        // only consider user specific access over https
        if (!httpServletRequest.isSecure()) {
            throw new IllegalStateException("User authentication/authorization is only supported when running over HTTPS.");
        }

        // ensure oidc is enabled
        if (!oidcService.isOidcEnabled()) {
            throw new IllegalStateException("OpenId Connect is not configured.");
        }

        final String oidcRequestIdentifier = getCookieValue(httpServletRequest.getCookies(), OIDC_REQUEST_IDENTIFIER);
        if (oidcRequestIdentifier == null) {
            throw new IllegalArgumentException("The login request identifier was not found in the request. Unable to continue.");
        }

        // remove the oidc request cookie
        removeOidcRequestCookie(httpServletResponse);

        // get the jwt
        final String jwt = oidcService.getJwt(oidcRequestIdentifier);
        if (jwt == null) {
            throw new IllegalArgumentException("A JWT for this login request identifier could not be found. Unable to continue.");
        }

        // generate the response
        return generateOkResponse(jwt).build();
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
            forwardToMessagePage(httpServletRequest, httpServletResponse, "User authentication/authorization is only supported when running over HTTPS.");
            return;
        }

        // ensure knox is enabled
        if (!knoxService.isKnoxEnabled()) {
            forwardToMessagePage(httpServletRequest, httpServletResponse, "Apache Knox SSO support is not configured.");
            return;
        }

        // build the originalUri, and direct back to the ui
        final String originalUri = generateResourceUri("access", "knox", "callback");

        // build the authorization uri
        final URI authorizationUri = UriBuilder.fromUri(knoxService.getKnoxUrl())
                .queryParam("originalUrl", originalUri.toString())
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
            forwardToMessagePage(httpServletRequest, httpServletResponse, "User authentication/authorization is only supported when running over HTTPS.");
            return;
        }

        // ensure knox is enabled
        if (!knoxService.isKnoxEnabled()) {
            forwardToMessagePage(httpServletRequest, httpServletResponse, "Apache Knox SSO support is not configured.");
            return;
        }

        httpServletResponse.sendRedirect("../../../nifi");
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
    public Response getAccessStatus(@Context HttpServletRequest httpServletRequest) {

        // only consider user specific access over https
        if (!httpServletRequest.isSecure()) {
            throw new IllegalStateException("User authentication/authorization is only supported when running over HTTPS.");
        }

        final AccessStatusDTO accessStatus = new AccessStatusDTO();

        try {
            final X509Certificate[] certificates = certificateExtractor.extractClientCertificate(httpServletRequest);

            // if there is not certificate, consider a token
            if (certificates == null) {
                // look for an authorization token
                final String authorization = httpServletRequest.getHeader(JwtAuthenticationFilter.AUTHORIZATION);

                // if there is no authorization header, we don't know the user
                if (authorization == null) {
                    accessStatus.setStatus(AccessStatusDTO.Status.UNKNOWN.name());
                    accessStatus.setMessage("No credentials supplied, unknown user.");
                } else {
                    try {
                        // Extract the Base64 encoded token from the Authorization header
                        final String token = StringUtils.substringAfterLast(authorization, " ");

                        final JwtAuthenticationRequestToken jwtRequest = new JwtAuthenticationRequestToken(token, httpServletRequest.getRemoteAddr());
                        final NiFiAuthenticationToken authenticationResponse = (NiFiAuthenticationToken) jwtAuthenticationProvider.authenticate(jwtRequest);
                        final NiFiUser nifiUser = ((NiFiUserDetails) authenticationResponse.getDetails()).getNiFiUser();

                        // set the user identity
                        accessStatus.setIdentity(nifiUser.getIdentity());

                        // attempt authorize to /flow
                        accessStatus.setStatus(AccessStatusDTO.Status.ACTIVE.name());
                        accessStatus.setMessage("You are already logged in.");
                    } catch (JwtException e) {
                        throw new InvalidAuthenticationException(e.getMessage(), e);
                    }
                }
            } else {
                try {
                    final X509AuthenticationRequestToken x509Request = new X509AuthenticationRequestToken(
                            httpServletRequest.getHeader(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN), principalExtractor, certificates, httpServletRequest.getRemoteAddr());

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
            throw new IllegalStateException("UI extension access tokens are only issued over HTTPS.");
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
                    "in the format 'Authorization: Bearer <token>'.",
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
            throw new IllegalStateException("Access tokens are only issued over HTTPS.");
        }

        // If Kerberos Service Principal and keytab location not configured, throws exception
        if (!properties.isKerberosSpnegoSupportEnabled() || kerberosService == null) {
            throw new IllegalStateException("Kerberos ticket login not supported by this NiFi.");
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
                final String identity = authentication.getName();
                expiration = validateTokenExpiration(expiration, identity);

                // create the authentication token
                final LoginAuthenticationToken loginAuthenticationToken = new LoginAuthenticationToken(identity, expiration, "KerberosService");

                // generate JWT for response
                final String token = jwtService.generateSignedToken(loginAuthenticationToken);

                // build the response
                final URI uri = URI.create(generateResourceUri("access", "kerberos"));
                return generateCreatedResponse(uri, token).build();
            } catch (final AuthenticationException e) {
                throw new AccessDeniedException(e.getMessage(), e);
            }
        }
    }

    /**
     * Creates a token for accessing the REST API via username/password.
     *
     * @param httpServletRequest the servlet request
     * @param username           the username
     * @param password           the password
     * @return A JWT (string)
     */
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/token")
    @ApiOperation(
            value = "Creates a token for accessing the REST API via username/password",
            notes = "The token returned is formatted as a JSON Web Token (JWT). The token is base64 encoded and comprised of three parts. The header, " +
                    "the body, and the signature. The expiration of the token is a contained within the body. The token can be used in the Authorization header " +
                    "in the format 'Authorization: Bearer <token>'.",
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
            throw new IllegalStateException("Access tokens are only issued over HTTPS.");
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
            long expiration = validateTokenExpiration(authenticationResponse.getExpiration(), authenticationResponse.getIdentity());

            // create the authentication token
            loginAuthenticationToken = new LoginAuthenticationToken(authenticationResponse.getIdentity(), expiration, authenticationResponse.getIssuer());
        } catch (final InvalidLoginCredentialsException ilce) {
            throw new IllegalArgumentException("The supplied username and password are not valid.", ilce);
        } catch (final IdentityAccessException iae) {
            throw new AdministrationException(iae.getMessage(), iae);
        }

        // generate JWT for response
        final String token = jwtService.generateSignedToken(loginAuthenticationToken);

        // build the response
        final URI uri = URI.create(generateResourceUri("access", "token"));
        return generateCreatedResponse(uri, token).build();
    }

    private long validateTokenExpiration(long proposedTokenExpiration, String identity) {
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

    /**
     * Gets the value of a cookie matching the specified name. If no cookie with that name exists, null is returned.
     *
     * @param cookies the cookies
     * @param name the name of the cookie
     * @return the value of the corresponding cookie, or null if the cookie does not exist
     */
    private String getCookieValue(final Cookie[] cookies, final String name) {
        if (cookies != null) {
            for (final Cookie cookie : cookies) {
                if (name.equals(cookie.getName())) {
                    return cookie.getValue();
                }
            }
        }

        return null;
    }

    private String getOidcCallback() {
        return generateResourceUri("access", "oidc", "callback");
    }

    private void removeOidcRequestCookie(final HttpServletResponse httpServletResponse) {
        final Cookie cookie = new Cookie(OIDC_REQUEST_IDENTIFIER, null);
        cookie.setPath("/");
        cookie.setHttpOnly(true);
        cookie.setMaxAge(0);
        cookie.setSecure(true);
        httpServletResponse.addCookie(cookie);
    }

    private void forwardToMessagePage(final HttpServletRequest httpServletRequest, final HttpServletResponse httpServletResponse, final String message) throws Exception {
        httpServletRequest.setAttribute("title", OIDC_ERROR_TITLE);
        httpServletRequest.setAttribute("messages", message);

        final ServletContext uiContext = httpServletRequest.getServletContext().getContext("/nifi");
        uiContext.getRequestDispatcher("/WEB-INF/pages/message-page.jsp").forward(httpServletRequest, httpServletResponse);
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

    public void setOidcService(OidcService oidcService) {
        this.oidcService = oidcService;
    }

    public void setKnoxService(KnoxService knoxService) {
        this.knoxService = knoxService;
    }
}
