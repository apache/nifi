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
import com.nimbusds.oauth2.sdk.http.HTTPResponse;
import com.nimbusds.oauth2.sdk.id.State;
import com.nimbusds.openid.connect.sdk.AuthenticationErrorResponse;
import com.nimbusds.openid.connect.sdk.AuthenticationResponse;
import com.nimbusds.openid.connect.sdk.AuthenticationResponseParser;
import com.nimbusds.openid.connect.sdk.AuthenticationSuccessResponse;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.nifi.authentication.exception.AuthenticationNotSupportedException;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.security.jwt.JwtService;
import org.apache.nifi.web.security.jwt.NiFiBearerTokenResolver;
import org.apache.nifi.web.security.oidc.OIDCEndpoints;
import org.apache.nifi.web.security.oidc.OidcService;
import org.apache.nifi.web.security.token.LoginAuthenticationToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.util.WebUtils;

import javax.annotation.PreDestroy;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


@Path(OIDCEndpoints.OIDC_ACCESS_ROOT)
@Api(
        value = OIDCEndpoints.OIDC_ACCESS_ROOT,
        description = "Endpoints for obtaining an access token or checking access status."
)
public class OIDCAccessResource extends AccessResource {

    private static final Logger logger = LoggerFactory.getLogger(OIDCAccessResource.class);
    private static final String OIDC_REQUEST_IDENTIFIER = "oidc-request-identifier";
    private static final String OIDC_ID_TOKEN_AUTHN_ERROR = "Unable to exchange authorization for ID token: ";
    private static final String OPEN_ID_CONNECT_SUPPORT_IS_NOT_CONFIGURED_MSG = "OpenId Connect support is not configured";
    private static final String REVOKE_ACCESS_TOKEN_LOGOUT = "oidc_access_token_logout";
    private static final String ID_TOKEN_LOGOUT = "oidc_id_token_logout";
    private static final String STANDARD_LOGOUT = "oidc_standard_logout";
    private static final Pattern REVOKE_ACCESS_TOKEN_LOGOUT_FORMAT = Pattern.compile("(\\.google\\.com)");
    private static final Pattern ID_TOKEN_LOGOUT_FORMAT = Pattern.compile("(\\.okta)");
    private static final int msTimeout = 30_000;
    private static final boolean LOGGING_IN = true;

    private OidcService oidcService;
    private JwtService jwtService;
    private CloseableHttpClient httpClient;

    public OIDCAccessResource() {
        RequestConfig config = RequestConfig.custom()
                .setConnectTimeout(msTimeout)
                .setConnectionRequestTimeout(msTimeout)
                .setSocketTimeout(msTimeout)
                .build();

        httpClient = HttpClientBuilder
                .create()
                .setDefaultRequestConfig(config)
                .build();
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path(OIDCEndpoints.LOGIN_REQUEST_RELATIVE)
    @ApiOperation(
            value = "Initiates a request to authenticate through the configured OpenId Connect provider.",
            notes = NON_GUARANTEED_ENDPOINT
    )
    public void oidcRequest(@Context HttpServletRequest httpServletRequest, @Context HttpServletResponse httpServletResponse) throws Exception {
        // only consider user specific access over https
        if (!httpServletRequest.isSecure()) {
            forwardToLoginMessagePage(httpServletRequest, httpServletResponse, AUTHENTICATION_NOT_ENABLED_MSG);
            return;
        }

        // ensure oidc is enabled
        if (!oidcService.isOidcEnabled()) {
            forwardToLoginMessagePage(httpServletRequest, httpServletResponse, OPEN_ID_CONNECT_SUPPORT_IS_NOT_CONFIGURED_MSG);
            return;
        }

        // generate the authorization uri
        URI authorizationURI = oidcRequestAuthorizationCode(httpServletResponse, getOidcCallback());

        // generate the response
        httpServletResponse.sendRedirect(authorizationURI.toString());
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path(OIDCEndpoints.LOGIN_CALLBACK_RELATIVE)
    @ApiOperation(
            value = "Redirect/callback URI for processing the result of the OpenId Connect login sequence.",
            notes = NON_GUARANTEED_ENDPOINT
    )
    public void oidcCallback(@Context HttpServletRequest httpServletRequest, @Context HttpServletResponse httpServletResponse) throws Exception {
        final AuthenticationResponse oidcResponse = parseOidcResponse(httpServletRequest, httpServletResponse, LOGGING_IN);

        final String oidcRequestIdentifier = WebUtils.getCookie(httpServletRequest, OIDC_REQUEST_IDENTIFIER).getValue();

        if (oidcResponse != null && oidcResponse.indicatesSuccess()) {
            final AuthenticationSuccessResponse successfulOidcResponse = (AuthenticationSuccessResponse) oidcResponse;

            checkOidcState(httpServletResponse, oidcRequestIdentifier, successfulOidcResponse, LOGGING_IN);

            try {
                // exchange authorization code for id token
                final AuthorizationCode authorizationCode = successfulOidcResponse.getAuthorizationCode();
                final AuthorizationGrant authorizationGrant = new AuthorizationCodeGrant(authorizationCode, URI.create(getOidcCallback()));

                // get the oidc token
                LoginAuthenticationToken oidcToken = oidcService.exchangeAuthorizationCodeForLoginAuthenticationToken(authorizationGrant);

                // exchange the oidc token for the NiFi token
                String nifiJwt = jwtService.generateSignedToken(oidcToken);

                // store the NiFi token
                oidcService.storeJwt(oidcRequestIdentifier, nifiJwt);
            } catch (final Exception e) {
                logger.error(OIDC_ID_TOKEN_AUTHN_ERROR + e.getMessage(), e);

                // remove the oidc request cookie
                removeOidcRequestCookie(httpServletResponse);

                // forward to the error page
                forwardToLoginMessagePage(httpServletRequest, httpServletResponse, OIDC_ID_TOKEN_AUTHN_ERROR + e.getMessage());
                return;
            }

            // redirect to the name page
            httpServletResponse.sendRedirect(getNiFiUri());
        } else {
            // remove the oidc request cookie
            removeOidcRequestCookie(httpServletResponse);

            // report the unsuccessful login
            final AuthenticationErrorResponse errorOidcResponse = (AuthenticationErrorResponse) oidcResponse;
            forwardToLoginMessagePage(httpServletRequest, httpServletResponse, "Unsuccessful login attempt: "
                    + errorOidcResponse.getErrorObject().getDescription());
        }
    }

    @POST
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.TEXT_PLAIN)
    @Path(OIDCEndpoints.TOKEN_EXCHANGE_RELATIVE)
    @ApiOperation(
            value = "Retrieves a JWT following a successful login sequence using the configured OpenId Connect provider.",
            response = String.class,
            notes = NON_GUARANTEED_ENDPOINT
    )
    public Response oidcExchange(@Context HttpServletRequest httpServletRequest, @Context HttpServletResponse httpServletResponse) {
        // only consider user specific access over https
        if (!httpServletRequest.isSecure()) {
            throw new AuthenticationNotSupportedException(AUTHENTICATION_NOT_ENABLED_MSG);
        }

        // ensure oidc is enabled
        if (!oidcService.isOidcEnabled()) {
            logger.debug(OPEN_ID_CONNECT_SUPPORT_IS_NOT_CONFIGURED_MSG);
            return Response.status(Response.Status.CONFLICT).entity(OPEN_ID_CONNECT_SUPPORT_IS_NOT_CONFIGURED_MSG).build();
        }

        final String oidcRequestIdentifier = WebUtils.getCookie(httpServletRequest, OIDC_REQUEST_IDENTIFIER).getValue();
        if (oidcRequestIdentifier == null) {
            final String message = "The login request identifier was not found in the request. Unable to continue.";
            logger.warn(message);
            return Response.status(Response.Status.BAD_REQUEST).entity(message).build();
        }

        // remove the oidc request cookie
        removeOidcRequestCookie(httpServletResponse);

        // get the jwt
        final String jwt = oidcService.getJwt(oidcRequestIdentifier);
        if (jwt == null) {
            throw new IllegalArgumentException("A JWT for this login request identifier could not be found. Unable to continue.");
        }

        return generateTokenResponse(generateOkResponse(jwt), jwt);
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path(OIDCEndpoints.LOGOUT_REQUEST_RELATIVE)
    @ApiOperation(
            value = "Performs a logout in the OpenId Provider.",
            notes = NON_GUARANTEED_ENDPOINT
    )
    public void oidcLogout(@Context HttpServletRequest httpServletRequest, @Context HttpServletResponse httpServletResponse) throws Exception {
        if (!httpServletRequest.isSecure()) {
            throw new IllegalStateException(AUTHENTICATION_NOT_ENABLED_MSG);
        }

        if (!oidcService.isOidcEnabled()) {
            throw new IllegalStateException(OPEN_ID_CONNECT_SUPPORT_IS_NOT_CONFIGURED_MSG);
        }

        final String mappedUserIdentity = NiFiUserUtils.getNiFiUserIdentity();
        removeCookie(httpServletResponse, NiFiBearerTokenResolver.JWT_COOKIE_NAME);
        logger.debug("Invalidated JWT for user [{}]", mappedUserIdentity);

        // Get the oidc discovery url
        String oidcDiscoveryUrl = properties.getOidcDiscoveryUrl();

        // Determine the logout method
        String logoutMethod = determineLogoutMethod(oidcDiscoveryUrl);

        switch (logoutMethod) {
            case REVOKE_ACCESS_TOKEN_LOGOUT:
            case ID_TOKEN_LOGOUT:
                // Make a request to the IdP
                URI authorizationURI = oidcRequestAuthorizationCode(httpServletResponse, getOidcLogoutCallback());
                httpServletResponse.sendRedirect(authorizationURI.toString());
                break;
            case STANDARD_LOGOUT:
            default:
                // Get the OIDC end session endpoint
                URI endSessionEndpoint = oidcService.getEndSessionEndpoint();
                String postLogoutRedirectUri = generateResourceUri( "..", "nifi", "logout-complete");

                if (endSessionEndpoint == null) {
                    httpServletResponse.sendRedirect(postLogoutRedirectUri);
                } else {
                    URI logoutUri = UriBuilder.fromUri(endSessionEndpoint)
                            .queryParam("post_logout_redirect_uri", postLogoutRedirectUri)
                            .build();
                    httpServletResponse.sendRedirect(logoutUri.toString());
                }
                break;
        }
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path(OIDCEndpoints.LOGOUT_CALLBACK_RELATIVE)
    @ApiOperation(
            value = "Redirect/callback URI for processing the result of the OpenId Connect logout sequence.",
            notes = NON_GUARANTEED_ENDPOINT
    )
    public void oidcLogoutCallback(@Context HttpServletRequest httpServletRequest, @Context HttpServletResponse httpServletResponse) throws Exception {
        final AuthenticationResponse oidcResponse = parseOidcResponse(httpServletRequest, httpServletResponse, !LOGGING_IN);

        final String oidcRequestIdentifier = WebUtils.getCookie(httpServletRequest, OIDC_REQUEST_IDENTIFIER).getValue();

        if (oidcResponse != null && oidcResponse.indicatesSuccess()) {
            final AuthenticationSuccessResponse successfulOidcResponse = (AuthenticationSuccessResponse) oidcResponse;

            // confirm state
            checkOidcState(httpServletResponse, oidcRequestIdentifier, successfulOidcResponse, false);

            // Get the oidc discovery url
            String oidcDiscoveryUrl = properties.getOidcDiscoveryUrl();

            // Determine which logout method to use
            String logoutMethod = determineLogoutMethod(oidcDiscoveryUrl);

            // Get the authorization code and grant
            final AuthorizationCode authorizationCode = successfulOidcResponse.getAuthorizationCode();
            final AuthorizationGrant authorizationGrant = new AuthorizationCodeGrant(authorizationCode, URI.create(getOidcLogoutCallback()));

            switch (logoutMethod) {
                case REVOKE_ACCESS_TOKEN_LOGOUT:
                    // Use the Revocation endpoint + access token
                    final String accessToken;
                    try {
                        // Return the access token
                        accessToken = oidcService.exchangeAuthorizationCodeForAccessToken(authorizationGrant);
                    } catch (final Exception e) {
                        logger.error("Unable to exchange authorization for the Access token: " + e.getMessage(), e);

                        // Remove the oidc request cookie
                        removeOidcRequestCookie(httpServletResponse);

                        // Forward to the error page
                        forwardToLogoutMessagePage(httpServletRequest, httpServletResponse, OIDC_ID_TOKEN_AUTHN_ERROR + e.getMessage());
                        return;
                    }

                    // Build the revoke URI and send the POST request
                    URI revokeEndpoint = getRevokeEndpoint();

                    if (revokeEndpoint != null) {
                        try {
                            // Logout with the revoke endpoint
                            revokeEndpointRequest(httpServletResponse, accessToken, revokeEndpoint);

                        } catch (final IOException e) {
                            logger.error("There was an error logging out of the OpenId Connect Provider: "
                                    + e.getMessage(), e);

                            // Remove the oidc request cookie
                            removeOidcRequestCookie(httpServletResponse);

                            // Forward to the error page
                            forwardToLogoutMessagePage(httpServletRequest, httpServletResponse,
                                    "There was an error logging out of the OpenId Connect Provider: "
                                            + e.getMessage());
                        }
                    }
                    break;
                case ID_TOKEN_LOGOUT:
                    // Use the end session endpoint + ID Token
                    final String idToken;
                    try {
                        // Return the ID Token
                        idToken = oidcService.exchangeAuthorizationCodeForIdToken(authorizationGrant);
                    } catch (final Exception e) {
                        logger.error(OIDC_ID_TOKEN_AUTHN_ERROR + e.getMessage(), e);

                        // Remove the oidc request cookie
                        removeOidcRequestCookie(httpServletResponse);

                        // Forward to the error page
                        forwardToLogoutMessagePage(httpServletRequest, httpServletResponse, OIDC_ID_TOKEN_AUTHN_ERROR + e.getMessage());
                        return;
                    }

                    // Get the OIDC end session endpoint
                    URI endSessionEndpoint = oidcService.getEndSessionEndpoint();
                    String postLogoutRedirectUri = generateResourceUri("..", "nifi", "logout-complete");

                    if (endSessionEndpoint == null) {
                        logger.debug("Unable to log out of the OpenId Connect Provider. The end session endpoint is: null." +
                                " Redirecting to the logout page.");
                        httpServletResponse.sendRedirect(postLogoutRedirectUri);
                    } else {
                        URI logoutUri = UriBuilder.fromUri(endSessionEndpoint)
                                .queryParam("id_token_hint", idToken)
                                .queryParam("post_logout_redirect_uri", postLogoutRedirectUri)
                                .build();
                        httpServletResponse.sendRedirect(logoutUri.toString());
                    }
                    break;
            }
        } else {
            // remove the oidc request cookie
            removeOidcRequestCookie(httpServletResponse);

            // report the unsuccessful logout
            final AuthenticationErrorResponse errorOidcResponse = (AuthenticationErrorResponse) oidcResponse;
            forwardToLogoutMessagePage(httpServletRequest, httpServletResponse, "Unsuccessful logout attempt: "
                    + errorOidcResponse.getErrorObject().getDescription());
        }
    }

    /**
     * Generates the request Authorization URI for the OpenID Connect Provider. Returns an authorization
     * URI using the provided callback URI.
     *
     * @param httpServletResponse the servlet response
     * @param callback the OIDC callback URI
     * @return the authorization URI
     */
    private URI oidcRequestAuthorizationCode(@Context HttpServletResponse httpServletResponse, String callback) {

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
                .queryParam("redirect_uri", callback)
                .build();

        // return Authorization URI
        return authorizationUri;
    }

    private String determineLogoutMethod(String oidcDiscoveryUrl) {
        Matcher accessTokenMatcher = REVOKE_ACCESS_TOKEN_LOGOUT_FORMAT.matcher(oidcDiscoveryUrl);
        Matcher idTokenMatcher = ID_TOKEN_LOGOUT_FORMAT.matcher(oidcDiscoveryUrl);

        if (accessTokenMatcher.find()) {
            return REVOKE_ACCESS_TOKEN_LOGOUT;
        } else if (idTokenMatcher.find()) {
            return ID_TOKEN_LOGOUT;
        } else {
            return STANDARD_LOGOUT;
        }
    }

    /**
     * Sends a POST request to the revoke endpoint to log out of the ID Provider.
     *
     * @param httpServletResponse the servlet response
     * @param accessToken the OpenID Connect Provider access token
     * @param revokeEndpoint the name of the cookie
     * @throws IOException exceptional case for communication error with the OpenId Connect Provider
     */
    private void revokeEndpointRequest(@Context HttpServletResponse httpServletResponse, String accessToken, URI revokeEndpoint) throws IOException {

        HttpPost httpPost = new HttpPost(revokeEndpoint);

        List<NameValuePair> params = new ArrayList<>();
        // Append a query param with the access token
        params.add(new BasicNameValuePair("token", accessToken));
        httpPost.setEntity(new UrlEncodedFormEntity(params));

        try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
            if (response.getStatusLine().getStatusCode() == HTTPResponse.SC_OK) {
                // Redirect to logout page
                logger.debug("You are logged out of the OpenId Connect Provider.");
                String postLogoutRedirectUri = generateResourceUri("..", "nifi", "logout-complete");
                httpServletResponse.sendRedirect(postLogoutRedirectUri);
            } else {
                logger.error("There was an error logging out of the OpenId Connect Provider. " +
                        "Response status: " + response.getStatusLine().getStatusCode());
            }
        }
    }

    @PreDestroy
    private final void closeClient() throws IOException {
        httpClient.close();
    }

    private AuthenticationResponse parseOidcResponse(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse, boolean isLogin) throws Exception {
        final String pageTitle = getForwardPageTitle(isLogin);

        // only consider user specific access over https
        if (!httpServletRequest.isSecure()) {
            forwardToMessagePage(httpServletRequest, httpServletResponse, pageTitle, AUTHENTICATION_NOT_ENABLED_MSG);
            return null;
        }

        // ensure oidc is enabled
        if (!oidcService.isOidcEnabled()) {
            forwardToMessagePage(httpServletRequest, httpServletResponse, pageTitle, OPEN_ID_CONNECT_SUPPORT_IS_NOT_CONFIGURED_MSG);
            return null;
        }

        final String oidcRequestIdentifier = WebUtils.getCookie(httpServletRequest, OIDC_REQUEST_IDENTIFIER).getValue();
        if (oidcRequestIdentifier == null) {
            forwardToMessagePage(httpServletRequest, httpServletResponse, pageTitle,"The request identifier was " +
                    "not found in the request. Unable to continue.");
            return null;
        }

        final com.nimbusds.openid.connect.sdk.AuthenticationResponse oidcResponse;
        try {
            oidcResponse = AuthenticationResponseParser.parse(getRequestUri());
            return oidcResponse;
        } catch (final ParseException e) {
            logger.error("Unable to parse the redirect URI from the OpenId Connect Provider. Unable to continue login/logout process.");

            // remove the oidc request cookie
            removeOidcRequestCookie(httpServletResponse);

            // forward to the error page
            forwardToMessagePage(httpServletRequest, httpServletResponse, pageTitle,"Unable to parse the redirect URI " +
                    "from the OpenId Connect Provider. Unable to continue login/logout process.");
            return null;
        }
    }

    private void checkOidcState(HttpServletResponse httpServletResponse, final String oidcRequestIdentifier, AuthenticationSuccessResponse successfulOidcResponse, boolean isLogin) throws Exception {
        // confirm state
        final State state = successfulOidcResponse.getState();
        if (state == null || !oidcService.isStateValid(oidcRequestIdentifier, state)) {
            logger.error("The state value returned by the OpenId Connect Provider does not match the stored " +
                    "state. Unable to continue login/logout process.");

            // remove the oidc request cookie
            removeOidcRequestCookie(httpServletResponse);

            // forward to the error page
            forwardToMessagePage(httpServletRequest, httpServletResponse, getForwardPageTitle(isLogin), "Purposed state does not match " +
                    "the stored state. Unable to continue login/logout process.");
            return;
        }
    }

    private String getForwardPageTitle(boolean isLogin) {
        return isLogin ? ApplicationResource.LOGIN_ERROR_TITLE : ApplicationResource.LOGOUT_ERROR_TITLE;
    }

    private String getOidcCallback() {
        return generateResourceUri("access", "oidc", "callback");
    }

    private String getOidcLogoutCallback() {
        return generateResourceUri("access", "oidc", "logoutCallback");
    }

    private URI getRevokeEndpoint() {
        return oidcService.getRevocationEndpoint();
    }

    private void removeOidcRequestCookie(final HttpServletResponse httpServletResponse) {
        removeCookie(httpServletResponse, OIDC_REQUEST_IDENTIFIER);
    }

    public void setOidcService(OidcService oidcService) {
        this.oidcService = oidcService;
    }

    public void setJwtService(JwtService jwtService) {
        this.jwtService = jwtService;
    }

    public void setProperties(final NiFiProperties properties) {
        this.properties = properties;
    }

    protected NiFiProperties getProperties() {
        return properties;
    }
}
