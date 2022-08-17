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
package org.apache.nifi.registry.web.api;

import com.nimbusds.oauth2.sdk.AuthorizationCode;
import com.nimbusds.oauth2.sdk.AuthorizationCodeGrant;
import com.nimbusds.oauth2.sdk.AuthorizationGrant;
import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.oauth2.sdk.id.State;
import com.nimbusds.openid.connect.sdk.AuthenticationErrorResponse;
import com.nimbusds.openid.connect.sdk.AuthenticationResponseParser;
import com.nimbusds.openid.connect.sdk.AuthenticationSuccessResponse;
import io.jsonwebtoken.JwtException;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.authorization.CurrentUser;
import org.apache.nifi.registry.event.EventService;
import org.apache.nifi.registry.exception.AdministrationException;
import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.apache.nifi.registry.security.authentication.AuthenticationRequest;
import org.apache.nifi.registry.security.authentication.AuthenticationResponse;
import org.apache.nifi.registry.security.authentication.BasicAuthIdentityProvider;
import org.apache.nifi.registry.security.authentication.IdentityProvider;
import org.apache.nifi.registry.security.authentication.IdentityProviderUsage;
import org.apache.nifi.registry.security.authentication.exception.IdentityAccessException;
import org.apache.nifi.registry.security.authentication.exception.InvalidCredentialsException;
import org.apache.nifi.registry.security.authorization.user.NiFiUser;
import org.apache.nifi.registry.security.authorization.user.NiFiUserUtils;
import org.apache.nifi.registry.web.exception.UnauthorizedException;
import org.apache.nifi.registry.web.security.authentication.jwt.JwtService;
import org.apache.nifi.registry.web.security.authentication.kerberos.KerberosSpnegoIdentityProvider;
import org.apache.nifi.registry.web.security.authentication.oidc.OidcService;
import org.apache.nifi.registry.web.security.authentication.x509.X509IdentityProvider;
import org.apache.nifi.registry.web.service.ServiceFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import javax.servlet.ServletContext;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

@Component
@Path("/access")
@Api(
        value = "access",
        description = "Endpoints for obtaining an access token or checking access status."
)
public class AccessResource extends ApplicationResource {

    private static final Logger logger = LoggerFactory.getLogger(AccessResource.class);

    private static final String OIDC_REQUEST_IDENTIFIER = "oidc-request-identifier";
    private static final String OIDC_ERROR_TITLE = "Unable to continue login sequence";

    private NiFiRegistryProperties properties;
    private JwtService jwtService;
    private OidcService oidcService;
    private X509IdentityProvider x509IdentityProvider;
    private KerberosSpnegoIdentityProvider kerberosSpnegoIdentityProvider;
    private IdentityProvider identityProvider;

    @Context
    protected UriInfo uriInfo;

    @Autowired
    public AccessResource(
            NiFiRegistryProperties properties,
            JwtService jwtService,
            X509IdentityProvider x509IdentityProvider,
            OidcService oidcService,
            @Nullable KerberosSpnegoIdentityProvider kerberosSpnegoIdentityProvider,
            @Nullable IdentityProvider identityProvider,
            ServiceFacade serviceFacade,
            EventService eventService) {
        super(serviceFacade, eventService);
        this.properties = properties;
        this.jwtService = jwtService;
        this.x509IdentityProvider = x509IdentityProvider;
        this.oidcService = oidcService;
        this.kerberosSpnegoIdentityProvider = kerberosSpnegoIdentityProvider;
        this.identityProvider = identityProvider;
    }

    /**
     * Gets the current client's identity and authorized permissions.
     *
     * @param httpServletRequest the servlet request
     * @return An object describing the current client identity, as determined by the server, and it's permissions.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(
            value = "Get access status",
            notes = "Returns the current client's authenticated identity and permissions to top-level resources",
            response = CurrentUser.class,
            authorizations = {@Authorization(value = "Authorization")}
    )
    @ApiResponses({
            @ApiResponse(code = 409, message = HttpStatusMessages.MESSAGE_409 + " The NiFi Registry might be running unsecured.") })
    public Response getAccessStatus(@Context HttpServletRequest httpServletRequest) {

        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        if (user == null) {
            // Not expected to happen unless the nifi registry server has been seriously misconfigured.
            throw new WebApplicationException(new Throwable("Unable to access details for current user."));
        }

        final CurrentUser currentUser = serviceFacade.getCurrentUser();
        currentUser.setLoginSupported(isBasicLoginSupported(httpServletRequest));
        currentUser.setOIDCLoginSupported(isOIDCLoginSupported(httpServletRequest));

        return generateOkResponse(currentUser).build();
    }

    /**
     * Creates a token for accessing the REST API.
     *
     * @param httpServletRequest the servlet request
     * @return A JWT (string)
     */
    @POST
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/token")
    @ApiOperation(
            value = "Create token trying all providers",
            notes = "Creates a token for accessing the REST API via auto-detected method of verifying client identity claim credentials. " +
                    "The token returned is formatted as a JSON Web Token (JWT). The token is base64 encoded and comprised of three parts. The header, " +
                    "the body, and the signature. The expiration of the token is a contained within the body. The token can be used in the Authorization header " +
                    "in the format 'Authorization: Bearer <token>'.",
            response = String.class
    )
    @ApiResponses({
            @ApiResponse(code = 400, message = HttpStatusMessages.MESSAGE_400),
            @ApiResponse(code = 401, message = HttpStatusMessages.MESSAGE_401),
            @ApiResponse(code = 409, message = HttpStatusMessages.MESSAGE_409 + " The NiFi Registry may not be configured to support login with username/password."),
            @ApiResponse(code = 500, message = HttpStatusMessages.MESSAGE_500) })
    public Response createAccessTokenByTryingAllProviders(@Context HttpServletRequest httpServletRequest) {

        // only support access tokens when communicating over HTTPS
        if (!httpServletRequest.isSecure()) {
            throw new IllegalStateException("Access tokens are only issued over HTTPS");
        }

        List<IdentityProvider> identityProviderWaterfall = generateIdentityProviderWaterfall();

        String token = null;
        for (IdentityProvider provider : identityProviderWaterfall) {

            AuthenticationRequest authenticationRequest = provider.extractCredentials(httpServletRequest);
            if (authenticationRequest == null) {
                continue;
            }
            try {
                token = createAccessToken(provider, authenticationRequest);
                break;
            } catch (final InvalidCredentialsException ice){
                logger.debug("{}: the supplied client credentials are invalid.", provider.getClass().getSimpleName());
                logger.debug("", ice);
            }

        }

        if (StringUtils.isEmpty(token)) {
            List<IdentityProviderUsage.AuthType> acceptableAuthTypes = identityProviderWaterfall.stream()
                    .map(IdentityProvider::getUsageInstructions)
                    .map(IdentityProviderUsage::getAuthType)
                    .filter(Objects::nonNull)
                    .distinct()
                    .collect(Collectors.toList());

            throw new UnauthorizedException("Client credentials are missing or invalid according to all configured identity providers.")
                    .withAuthenticateChallenge(acceptableAuthTypes);
        }

        // build the response
        final URI uri = URI.create(generateResourceUri("access", "token"));
        return generateCreatedResponse(uri, token).build();
    }

    /**
     * Creates a token for accessing the REST API.
     *
     * @param httpServletRequest the servlet request
     * @return A JWT (string)
     */
    @POST
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/token/login")
    @ApiOperation(
            value = "Create token using basic auth",
            notes = "Creates a token for accessing the REST API via username/password. The user credentials must be passed in standard HTTP Basic Auth format. " +
                    "That is: 'Authorization: Basic <credentials>', where <credentials> is the base64 encoded value of '<username>:<password>'. " +
                    "The token returned is formatted as a JSON Web Token (JWT). The token is base64 encoded and comprised of three parts. The header, " +
                    "the body, and the signature. The expiration of the token is a contained within the body. The token can be used in the Authorization header " +
                    "in the format 'Authorization: Bearer <token>'.",
            response = String.class,
            authorizations = { @Authorization("BasicAuth") }
    )
    @ApiResponses({
            @ApiResponse(code = 400, message = HttpStatusMessages.MESSAGE_400),
            @ApiResponse(code = 401, message = HttpStatusMessages.MESSAGE_401),
            @ApiResponse(code = 409, message = HttpStatusMessages.MESSAGE_409 + " The NiFi Registry may not be configured to support login with username/password."),
            @ApiResponse(code = 500, message = HttpStatusMessages.MESSAGE_500) })
    public Response createAccessTokenUsingBasicAuthCredentials(@Context HttpServletRequest httpServletRequest) {

        // only support access tokens when communicating over HTTPS
        if (!httpServletRequest.isSecure()) {
            throw new IllegalStateException("Access tokens are only issued over HTTPS");
        }

        // if not configured with custom identity provider, or if provider doesn't support HTTP Basic Auth, don't consider credentials
        if (identityProvider == null) {
            logger.debug("An Identity Provider must be configured to use this endpoint. Please consult the administration guide.");
            throw new IllegalStateException("Username/Password login not supported by this NiFi. Contact System Administrator.");
        }
        if (!(identityProvider instanceof BasicAuthIdentityProvider)) {
            logger.debug("An Identity Provider is configured, but it does not support HTTP Basic Auth authentication. " +
                    "The configured Identity Provider must extend {}", BasicAuthIdentityProvider.class);
            throw new IllegalStateException("Username/Password login not supported by this NiFi. Contact System Administrator.");
        }

        // generate JWT for response
        AuthenticationRequest authenticationRequest = identityProvider.extractCredentials(httpServletRequest);

        if (authenticationRequest == null) {
            throw new UnauthorizedException("The client credentials are missing from the request.")
                    .withAuthenticateChallenge(IdentityProviderUsage.AuthType.OTHER);
        }

        final String token;
        try {
             token = createAccessToken(identityProvider, authenticationRequest);
        } catch (final InvalidCredentialsException ice){
            throw new UnauthorizedException("The supplied client credentials are not valid.", ice)
                    .withAuthenticateChallenge(IdentityProviderUsage.AuthType.OTHER);
        }

        // form the response
        final URI uri = URI.create(generateResourceUri("access", "token"));
        return generateCreatedResponse(uri, token).build();
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
            throw new IllegalStateException("User authentication/authorization is only supported when running over HTTPS.");
        }

        String userIdentity = NiFiUserUtils.getNiFiUserIdentity();

        if(userIdentity != null && !userIdentity.isEmpty()) {
            try {
                logger.info("Logging out user " + userIdentity);
                jwtService.logOut(userIdentity);
                return generateOkResponse().build();
            } catch (final JwtException e) {
                logger.error("Logout of user " + userIdentity + " failed due to: " + e.getMessage());
                return Response.serverError().build();
            }
        } else {
            return Response.status(401, "Authentication token provided was empty or not in the correct JWT format.").build();
        }
    }

    @POST
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/token/kerberos")
    @ApiOperation(
            value = "Create token using kerberos",
            notes = "Creates a token for accessing the REST API via Kerberos Service Tickets or SPNEGO Tokens (which includes Kerberos Service Tickets). " +
                    "The token returned is formatted as a JSON Web Token (JWT). The token is base64 encoded and comprised of three parts. The header, " +
                    "the body, and the signature. The expiration of the token is a contained within the body. The token can be used in the Authorization header " +
                    "in the format 'Authorization: Bearer <token>'.",
            response = String.class
    )
    @ApiResponses({
            @ApiResponse(code = 400, message = HttpStatusMessages.MESSAGE_400),
            @ApiResponse(code = 401, message = HttpStatusMessages.MESSAGE_401),
            @ApiResponse(code = 409, message = HttpStatusMessages.MESSAGE_409 + " The NiFi Registry may not be configured to support login Kerberos credentials."),
            @ApiResponse(code = 500, message = HttpStatusMessages.MESSAGE_500) })
    public Response createAccessTokenUsingKerberosTicket(@Context HttpServletRequest httpServletRequest) {

        // only support access tokens when communicating over HTTPS
        if (!httpServletRequest.isSecure()) {
            throw new IllegalStateException("Access tokens are only issued over HTTPS");
        }

        // if not configured with custom identity provider, don't consider credentials
        if (!properties.isKerberosSpnegoSupportEnabled() || kerberosSpnegoIdentityProvider == null) {
            throw new IllegalStateException("Kerberos service ticket login not supported by this NiFi Registry");
        }

        AuthenticationRequest authenticationRequest = kerberosSpnegoIdentityProvider.extractCredentials(httpServletRequest);

        if (authenticationRequest == null) {
            throw new UnauthorizedException("The client credentials are missing from the request.")
                    .withAuthenticateChallenge(kerberosSpnegoIdentityProvider.getUsageInstructions().getAuthType());
        }

        final String token;
        try {
            token = createAccessToken(kerberosSpnegoIdentityProvider, authenticationRequest);
        } catch (final InvalidCredentialsException ice){
            throw new UnauthorizedException("The supplied client credentials are not valid.", ice)
                    .withAuthenticateChallenge(kerberosSpnegoIdentityProvider.getUsageInstructions().getAuthType());
        }

        // build the response
        final URI uri = URI.create(generateResourceUri("access", "token"));
        return generateCreatedResponse(uri, token).build();

    }

    /**
     * Creates a token for accessing the REST API using a custom identity provider configured using NiFi Registry extensions.
     *
     * @param httpServletRequest the servlet request
     * @return A JWT (string)
     */
    @POST
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/token/identity-provider")
    @ApiOperation(
            value = "Create token using identity provider",
            notes = "Creates a token for accessing the REST API via a custom identity provider. " +
                    "The user credentials must be passed in a format understood by the custom identity provider, e.g., a third-party auth token in an HTTP header. " +
                    "The exact format of the user credentials expected by the custom identity provider can be discovered by 'GET /access/token/identity-provider/usage'. " +
                    "The token returned is formatted as a JSON Web Token (JWT). The token is base64 encoded and comprised of three parts. The header, " +
                    "the body, and the signature. The expiration of the token is a contained within the body. The token can be used in the Authorization header " +
                    "in the format 'Authorization: Bearer <token>'.",
            response = String.class
    )
    @ApiResponses({
            @ApiResponse(code = 400, message = HttpStatusMessages.MESSAGE_400),
            @ApiResponse(code = 401, message = HttpStatusMessages.MESSAGE_401),
            @ApiResponse(code = 409, message = HttpStatusMessages.MESSAGE_409 + " The NiFi Registry may not be configured to support login with customized credentials."),
            @ApiResponse(code = 500, message = HttpStatusMessages.MESSAGE_500) })
    public Response createAccessTokenUsingIdentityProviderCredentials(@Context HttpServletRequest httpServletRequest) {

        // only support access tokens when communicating over HTTPS
        if (!httpServletRequest.isSecure()) {
            throw new IllegalStateException("Access tokens are only issued over HTTPS");
        }

        // if not configured with custom identity provider, don't consider credentials
        if (identityProvider == null) {
            throw new IllegalStateException("Custom login not supported by this NiFi Registry");
        }

        AuthenticationRequest authenticationRequest = identityProvider.extractCredentials(httpServletRequest);

        if (authenticationRequest == null) {
            throw new UnauthorizedException("The client credentials are missing from the request.")
                    .withAuthenticateChallenge(identityProvider.getUsageInstructions().getAuthType());
        }

        final String token;
        try {
            token = createAccessToken(identityProvider, authenticationRequest);
        } catch (InvalidCredentialsException ice) {
            throw new UnauthorizedException("The supplied client credentials are not valid.", ice)
                    .withAuthenticateChallenge(identityProvider.getUsageInstructions().getAuthType());
        }

        // build the response
        final URI uri = URI.create(generateResourceUri("access", "token"));
        return generateCreatedResponse(uri, token).build();

    }

    /**
     * Creates a token for accessing the REST API using a custom identity provider configured using NiFi Registry extensions.
     *
     * @param httpServletRequest the servlet request
     * @return A JWT (string)
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/token/identity-provider/usage")
    @ApiOperation(
            value = "Get identity provider usage",
            notes = "Provides a description of how the currently configured identity provider expects credentials to be passed to POST /access/token/identity-provider",
            response = String.class
    )
    @ApiResponses({
            @ApiResponse(code = 400, message = HttpStatusMessages.MESSAGE_400),
            @ApiResponse(code = 409, message = HttpStatusMessages.MESSAGE_409 + " The NiFi Registry may not be configured to support login with customized credentials."),
            @ApiResponse(code = 500, message = HttpStatusMessages.MESSAGE_500) })
    public Response getIdentityProviderUsageInstructions(@Context HttpServletRequest httpServletRequest) {

        // if not configuration for login, don't consider credentials
        if (identityProvider == null) {
            throw new IllegalStateException("Custom login not supported by this NiFi Registry");
        }

        Class ipClazz = identityProvider.getClass();
        String identityProviderName = StringUtils.isNotEmpty(ipClazz.getSimpleName()) ? ipClazz.getSimpleName() : ipClazz.getName();

        try {
            String usageInstructions = "Usage Instructions for '" + identityProviderName + "': ";
            usageInstructions += identityProvider.getUsageInstructions().getText();
            return generateOkResponse(usageInstructions).build();

        } catch (Exception e) {
            // If, for any reason, this identity provider does not support getUsageInstructions(), e.g., returns null or throws NotImplementedException.
            return Response.status(Response.Status.NOT_IMPLEMENTED)
                    .entity("The currently configured identity provider, '" + identityProvider.getClass().getName() + "' does not provide usage instructions.")
                    .build();
        }

    }

    /**
     * Creates a token for accessing the REST API using a custom identity provider configured using NiFi Registry extensions.
     *
     * @param httpServletRequest the servlet request
     * @return A JWT (string)
     */
    @POST
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/token/identity-provider/test")
    @ApiOperation(
            value = "Test identity provider",
            notes = "Tests the format of the credentials against this identity provider without preforming authentication on the credentials to validate them. " +
                    "The user credentials should be passed in a format understood by the custom identity provider as defined by 'GET /access/token/identity-provider/usage'.",
            response = String.class
    )
    @ApiResponses({
            @ApiResponse(code = 400, message = HttpStatusMessages.MESSAGE_400),
            @ApiResponse(code = 401, message = "The format of the credentials were not recognized by the currently configured identity provider."),
            @ApiResponse(code = 409, message = HttpStatusMessages.MESSAGE_409 + " The NiFi Registry may not be configured to support login with customized credentials."),
            @ApiResponse(code = 500, message = HttpStatusMessages.MESSAGE_500) })
    public Response testIdentityProviderRecognizesCredentialsFormat(@Context HttpServletRequest httpServletRequest) {

        // only support access tokens when communicating over HTTPS
        if (!httpServletRequest.isSecure()) {
            throw new IllegalStateException("Access tokens are only issued over HTTPS");
        }

        // if not configured with custom identity provider, don't consider credentials
        if (identityProvider == null) {
            throw new IllegalStateException("Custom login not supported by this NiFi Registry");
        }

        final Class ipClazz = identityProvider.getClass();
        final String identityProviderName = StringUtils.isNotEmpty(ipClazz.getSimpleName()) ? ipClazz.getSimpleName() : ipClazz.getName();

        // attempt to extract client credentials without authenticating them
        AuthenticationRequest authenticationRequest = identityProvider.extractCredentials(httpServletRequest);

        if (authenticationRequest == null) {
            throw new UnauthorizedException("The format of the credentials were not recognized by the currently configured identity provider " +
                    "'" + identityProviderName + "'. " + identityProvider.getUsageInstructions().getText())
                    .withAuthenticateChallenge(identityProvider.getUsageInstructions().getAuthType());
        }


        final String successMessage = identityProviderName + " recognized the format of the credentials in the HTTP request.";
        return generateOkResponse(successMessage).build();

    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path("/oidc/request")
    @ApiOperation(
            value = "Initiates a request to authenticate through the configured OpenId Connect provider.",
            notes = NON_GUARANTEED_ENDPOINT
    )
    public void oidcRequest(@Context HttpServletRequest httpServletRequest, @Context HttpServletResponse httpServletResponse) throws Exception {
        // only consider user specific access over https
        if (!httpServletRequest.isSecure()) {
            //forwardToMessagePage(httpServletRequest, httpServletResponse, "User authentication/authorization is only supported when running over HTTPS.");
            throw new IllegalStateException("User authentication/authorization is only supported when running over HTTPS.");
        }

        // ensure oidc is enabled
        if (!oidcService.isOidcEnabled()) {
            //forwardToMessagePage(httpServletRequest, httpServletResponse, "OpenId Connect is not configured.");
            throw new IllegalStateException("OpenId Connect is not configured.");
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
    @Path("/oidc/callback")
    @ApiOperation(
            value = "Redirect/callback URI for processing the result of the OpenId Connect login sequence.",
            notes = NON_GUARANTEED_ENDPOINT
    )
    public void oidcCallback(@Context HttpServletRequest httpServletRequest, @Context HttpServletResponse httpServletResponse) throws Exception {
        // only consider user specific access over https
        if (!httpServletRequest.isSecure()) {
            //forwardToMessagePage(httpServletRequest, httpServletResponse, "User authentication/authorization is only supported when running over HTTPS.");
            throw new IllegalStateException("User authentication/authorization is only supported when running over HTTPS.");
        }

        // ensure oidc is enabled
        if (!oidcService.isOidcEnabled()) {
            //forwardToMessagePage(httpServletRequest, httpServletResponse, "OpenId Connect is not configured.");
            throw new IllegalStateException("OpenId Connect is not configured.");
        }

        final String oidcRequestIdentifier = getCookieValue(httpServletRequest.getCookies(), OIDC_REQUEST_IDENTIFIER);
        if (oidcRequestIdentifier == null) {
            throw new IllegalStateException("The login request identifier was not found in the request. Unable to continue.");
        }

        final com.nimbusds.openid.connect.sdk.AuthenticationResponse oidcResponse;
        try {
            oidcResponse = AuthenticationResponseParser.parse(getRequestUri());
        } catch (final ParseException e) {
            logger.error("Unable to parse the redirect URI from the OpenId Connect Provider. Unable to continue login process.");

            // remove the oidc request cookie
            removeOidcRequestCookie(httpServletResponse);

            // forward to the error page
            throw new IllegalStateException("Unable to parse the redirect URI from the OpenId Connect Provider. Unable to continue login process.");
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
                throw new IllegalStateException("Purposed state does not match the stored state. Unable to continue login process.");
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
                throw new IllegalStateException("Unable to exchange authorization for ID token: " + e.getMessage());
            }

            // redirect to the name page
            httpServletResponse.sendRedirect(getNiFiRegistryUri());
        } else {
            // remove the oidc request cookie
            removeOidcRequestCookie(httpServletResponse);

            // report the unsuccessful login
            final AuthenticationErrorResponse errorOidcResponse = (AuthenticationErrorResponse) oidcResponse;
            throw new IllegalStateException("Unsuccessful login attempt: " + errorOidcResponse.getErrorObject().getDescription());
        }
    }

    @POST
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/oidc/exchange")
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

    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path("/oidc/logout")
    @ApiOperation(
            value = "Performs a logout in the OpenId Provider.",
            notes = NON_GUARANTEED_ENDPOINT
    )
    public void oidcLogout(@Context HttpServletRequest httpServletRequest, @Context HttpServletResponse httpServletResponse) throws Exception {
        if (!httpServletRequest.isSecure()) {
            throw new IllegalStateException("User authentication/authorization is only supported when running over HTTPS.");
        }

        if (!oidcService.isOidcEnabled()) {
            throw new IllegalStateException("OpenId Connect is not configured.");
        }

        final String tokenHeader = httpServletRequest.getHeader(JwtService.AUTHORIZATION);
        jwtService.logOutUsingAuthHeader(tokenHeader);

        URI endSessionEndpoint = oidcService.getEndSessionEndpoint();
        String postLogoutRedirectUri = generateResourceUri("..", "nifi-registry");

        if (endSessionEndpoint == null) {
            // handle the case, where the OpenID Provider does not have an end session endpoint
            //httpServletResponse.sendRedirect(postLogoutRedirectUri);
        } else {
            URI logoutUri = UriBuilder.fromUri(endSessionEndpoint)
                    .queryParam("post_logout_redirect_uri", postLogoutRedirectUri)
                    .build();
            httpServletResponse.sendRedirect(logoutUri.toString());
        }
    }

    /**
     * Gets the value of a cookie matching the specified name. If no cookie with that name exists, null is returned.
     *
     * @param cookies the cookies
     * @param name    the name of the cookie
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

    public void setOidcService(OidcService oidcService) {
        this.oidcService = oidcService;
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

    protected URI getRequestUri() {
        return uriInfo.getRequestUri();
    }

    private String getNiFiRegistryUri() {
        final String nifiRegistryApiUrl = generateResourceUri();
        final String baseUrl = StringUtils.substringBeforeLast(nifiRegistryApiUrl, "/nifi-registry-api");
        return baseUrl + "/nifi-registry";
    }

    private void forwardToMessagePage(final HttpServletRequest httpServletRequest, final HttpServletResponse httpServletResponse, final String message) throws Exception {
        httpServletRequest.setAttribute("title", OIDC_ERROR_TITLE);
        httpServletRequest.setAttribute("messages", message);

        final ServletContext uiContext = httpServletRequest.getServletContext().getContext("/nifi-registry");
        uiContext.getRequestDispatcher("/WEB-INF/pages/message-page.jsp").forward(httpServletRequest, httpServletResponse);
    }

    private String createAccessToken(IdentityProvider identityProvider, AuthenticationRequest authenticationRequest)
            throws InvalidCredentialsException, AdministrationException {

        final AuthenticationResponse authenticationResponse;

        try {
            authenticationResponse = identityProvider.authenticate(authenticationRequest);
            final String token = jwtService.generateSignedToken(authenticationResponse);
            return token;
        } catch (final IdentityAccessException | JwtException e) {
            throw new AdministrationException(e.getMessage());
        }

    }

    /**
     * A helper function that generates a prioritized list of IdentityProviders to use to
     * attempt client authentication.
     *
     * Note: This is currently a hard-coded list order consisting of:
     *
     * - X509IdentityProvider (if available)
     * - KerberosProvider (if available)
     * - User-defined IdentityProvider (if available)
     *
     * However, in the future it could be entirely user-configurable
     *
     * @return a list of providers to use in order to authenticate the client.
     */
    private List<IdentityProvider> generateIdentityProviderWaterfall() {
        List<IdentityProvider> identityProviderWaterfall = new ArrayList<>();

        // if configured with an X509IdentityProvider, add it to the list of providers to try
        if (x509IdentityProvider != null) {
            identityProviderWaterfall.add(x509IdentityProvider);
        }

        // if configured with an KerberosSpnegoIdentityProvider, add it to the end of the list of providers to try
        if (kerberosSpnegoIdentityProvider != null) {
            identityProviderWaterfall.add(kerberosSpnegoIdentityProvider);
        }

        // if configured with custom identity provider, add it to the end of the list of providers to try
        if (identityProvider != null) {
            identityProviderWaterfall.add(identityProvider);
        }

        return identityProviderWaterfall;
    }

    private boolean isBasicLoginSupported(HttpServletRequest request) {
        return request.isSecure() && identityProvider != null;
    }

    private boolean isOIDCLoginSupported(HttpServletRequest request) {
        return request.isSecure() && oidcService != null && oidcService.isOidcEnabled();
    }
}
