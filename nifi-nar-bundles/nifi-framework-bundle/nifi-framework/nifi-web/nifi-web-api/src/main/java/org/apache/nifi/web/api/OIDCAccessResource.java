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
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.StandardTlsConfiguration;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.security.util.TlsException;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.api.cookie.ApplicationCookieName;
import org.apache.nifi.web.security.jwt.provider.BearerTokenProvider;
import org.apache.nifi.web.security.oidc.OIDCEndpoints;
import org.apache.nifi.web.security.oidc.OidcService;
import org.apache.nifi.web.security.oidc.TruststoreStrategy;
import org.apache.nifi.web.security.token.LoginAuthenticationToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
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
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


@Path(OIDCEndpoints.OIDC_ACCESS_ROOT)
@Api(
        value = OIDCEndpoints.OIDC_ACCESS_ROOT,
        description = "Endpoints for obtaining an access token or checking access status."
)
public class OIDCAccessResource extends ApplicationResource {

    private static final Logger logger = LoggerFactory.getLogger(OIDCAccessResource.class);
    private static final String OIDC_AUTHENTICATION_NOT_CONFIGURED = "OIDC authentication not configured";
    private static final String OIDC_ID_TOKEN_AUTHN_ERROR = "Unable to exchange authorization for ID token: ";
    private static final String OIDC_REQUEST_IDENTIFIER_NOT_FOUND = "The request identifier was not found in the request.";
    private static final String OIDC_FAILED_TO_PARSE_REDIRECT_URI = "Unable to parse the redirect URI from the OpenId Connect Provider. Unable to continue login/logout process.";
    private static final String REVOKE_ACCESS_TOKEN_LOGOUT = "oidc_access_token_logout";
    private static final String ID_TOKEN_LOGOUT = "oidc_id_token_logout";
    private static final String STANDARD_LOGOUT = "oidc_standard_logout";
    private static final Pattern REVOKE_ACCESS_TOKEN_LOGOUT_FORMAT = Pattern.compile("(\\.google\\.com)");
    private static final Pattern ID_TOKEN_LOGOUT_FORMAT = Pattern.compile("(\\.okta)");
    private static final int msTimeout = 30_000;
    private static final boolean LOGGING_IN = true;

    private OidcService oidcService;
    private BearerTokenProvider bearerTokenProvider;

    public OIDCAccessResource() {

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

        try {
            validateOidcConfiguration();
        } catch (AuthenticationNotSupportedException e) {
            forwardToLoginMessagePage(httpServletRequest, httpServletResponse, e.getMessage());
            throw e;
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
        final Optional<String> requestIdentifier = getOidcRequestIdentifier(httpServletRequest);

        if (requestIdentifier.isPresent() && oidcResponse.indicatesSuccess()) {
            final AuthenticationSuccessResponse successfulOidcResponse = (AuthenticationSuccessResponse) oidcResponse;

            final String oidcRequestIdentifier = requestIdentifier.get();
            checkOidcState(httpServletResponse, oidcRequestIdentifier, successfulOidcResponse, LOGGING_IN);

            try {
                // exchange authorization code for id token
                final AuthorizationCode authorizationCode = successfulOidcResponse.getAuthorizationCode();
                final AuthorizationGrant authorizationGrant = new AuthorizationCodeGrant(authorizationCode, URI.create(getOidcCallback()));

                // get the oidc token
                LoginAuthenticationToken oidcToken = oidcService.exchangeAuthorizationCodeForLoginAuthenticationToken(authorizationGrant);

                // exchange the oidc token for the NiFi token
                final String bearerToken = bearerTokenProvider.getBearerToken(oidcToken);

                // store the NiFi token
                oidcService.storeJwt(oidcRequestIdentifier, bearerToken);
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
            forwardToLoginMessagePage(httpServletRequest, httpServletResponse, "Unsuccessful login attempt.");
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

        try {
            validateOidcConfiguration();
        } catch (final AuthenticationNotSupportedException e) {
            logger.debug("OIDC authentication not supported", e);
            return Response.status(Response.Status.CONFLICT).entity(e.getMessage()).build();
        }

        final Optional<String> requestIdentifier = getOidcRequestIdentifier(httpServletRequest);
        if (!requestIdentifier.isPresent()) {
            final String message = "The login request identifier was not found in the request. Unable to continue.";
            logger.warn(message);
            return Response.status(Response.Status.BAD_REQUEST).entity(message).build();
        }

        // remove the oidc request cookie
        removeOidcRequestCookie(httpServletResponse);

        // get the jwt
        final String jwt = oidcService.getJwt(requestIdentifier.get());
        if (jwt == null) {
            throw new IllegalArgumentException("A JWT for this login request identifier could not be found. Unable to continue.");
        }

        setBearerToken(httpServletResponse, jwt);
        return generateOkResponse(jwt).build();
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

        try {
            validateOidcConfiguration();
        } catch (final AuthenticationNotSupportedException e) {
            throw e;
        }

        final String mappedUserIdentity = NiFiUserUtils.getNiFiUserIdentity();
        applicationCookieService.removeCookie(getCookieResourceUri(), httpServletResponse, ApplicationCookieName.AUTHORIZATION_BEARER);
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
        final Optional<String> requestIdentifier = getOidcRequestIdentifier(httpServletRequest);

        if (requestIdentifier.isPresent() && oidcResponse != null && oidcResponse.indicatesSuccess()) {
            final AuthenticationSuccessResponse successfulOidcResponse = (AuthenticationSuccessResponse) oidcResponse;

            // confirm state
            final String oidcRequestIdentifier = requestIdentifier.get();
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
            forwardToLogoutMessagePage(httpServletRequest, httpServletResponse, "Unsuccessful logout attempt.");
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
        applicationCookieService.addCookie(getCookieResourceUri(), httpServletResponse, ApplicationCookieName.OIDC_REQUEST_IDENTIFIER, oidcRequestIdentifier);
        final State state = oidcService.createState(oidcRequestIdentifier);
        return UriBuilder.fromUri(oidcService.getAuthorizationEndpoint())
                .queryParam("client_id", oidcService.getClientId())
                .queryParam("response_type", "code")
                .queryParam("scope", oidcService.getScope().toString())
                .queryParam("state", state.getValue())
                .queryParam("redirect_uri", callback)
                .build();
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
        final CloseableHttpClient httpClient = getHttpClient();
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
        } finally {
            httpClient.close();
        }
    }

    private CloseableHttpClient getHttpClient() {
        RequestConfig config = RequestConfig.custom()
                .setConnectTimeout(msTimeout)
                .setConnectionRequestTimeout(msTimeout)
                .setSocketTimeout(msTimeout)
                .build();

        HttpClientBuilder builder = HttpClientBuilder
                .create()
                .setDefaultRequestConfig(config);

        if (TruststoreStrategy.NIFI.name().equals(properties.getOidcClientTruststoreStrategy())) {
            builder.setSSLContext(getSslContext());
        }

        return builder.build();
    }

    protected AuthenticationResponse parseOidcResponse(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse, boolean isLogin) throws Exception {
        final String pageTitle = getForwardPageTitle(isLogin);

        try {
            validateOidcConfiguration();
        } catch (final AuthenticationNotSupportedException e) {
            forwardToMessagePage(httpServletRequest, httpServletResponse, pageTitle, e.getMessage());
            throw e;
        }

        final Optional<String> requestIdentifier = getOidcRequestIdentifier(httpServletRequest);
        if (!requestIdentifier.isPresent()) {
            forwardToMessagePage(httpServletRequest, httpServletResponse, pageTitle, OIDC_REQUEST_IDENTIFIER_NOT_FOUND);
            throw new IllegalStateException(OIDC_REQUEST_IDENTIFIER_NOT_FOUND);
        }

        final com.nimbusds.openid.connect.sdk.AuthenticationResponse oidcResponse;

        try {
            oidcResponse = AuthenticationResponseParser.parse(getRequestUri());
            return oidcResponse;
        } catch (final ParseException e) {
            logger.error(OIDC_FAILED_TO_PARSE_REDIRECT_URI);

            // remove the oidc request cookie
            removeOidcRequestCookie(httpServletResponse);

            // forward to the error page
            forwardToMessagePage(httpServletRequest, httpServletResponse, pageTitle, OIDC_FAILED_TO_PARSE_REDIRECT_URI);
            throw e;
        }
    }

    protected void checkOidcState(HttpServletResponse httpServletResponse, final String oidcRequestIdentifier, AuthenticationSuccessResponse successfulOidcResponse, boolean isLogin) throws Exception {
        // confirm state
        final State state = successfulOidcResponse.getState();
        if (state == null || !oidcService.isStateValid(oidcRequestIdentifier, state)) {
            logger.error("OIDC Request [{}] State [{}] not valid", oidcRequestIdentifier, state);

            removeOidcRequestCookie(httpServletResponse);

            forwardToMessagePage(httpServletRequest, httpServletResponse, getForwardPageTitle(isLogin), "Purposed state does not match " +
                    "the stored state. Unable to continue login/logout process.");
        }
    }

    private SSLContext getSslContext() {
        TlsConfiguration tlsConfiguration = StandardTlsConfiguration.fromNiFiProperties(properties);
        try {
            return SslContextFactory.createSslContext(tlsConfiguration);
        } catch (TlsException e) {
            throw new RuntimeException("Unable to establish an SSL context for OIDC access resource from nifi.properties", e);
        }
    }

    private void validateOidcConfiguration() throws AuthenticationNotSupportedException {
        // only consider user specific access over https
        if (!httpServletRequest.isSecure()) {
            throw new AuthenticationNotSupportedException(AccessResource.AUTHENTICATION_NOT_ENABLED_MSG);
        }

        // ensure OIDC is actually configured/enabled
        if (!oidcService.isOidcEnabled()) {
            throw new AuthenticationNotSupportedException(OIDC_AUTHENTICATION_NOT_CONFIGURED);
        }
    }

    private String getForwardPageTitle(boolean isLogin) {
        return isLogin ? ApplicationResource.LOGIN_ERROR_TITLE : ApplicationResource.LOGOUT_ERROR_TITLE;
    }

    protected String getOidcCallback() {
        return generateResourceUri("access", "oidc", "callback");
    }

    private String getOidcLogoutCallback() {
        return generateResourceUri("access", "oidc", "logoutCallback");
    }

    private URI getRevokeEndpoint() {
        return oidcService.getRevocationEndpoint();
    }

    private void removeOidcRequestCookie(final HttpServletResponse httpServletResponse) {
        applicationCookieService.removeCookie(getCookieResourceUri(), httpServletResponse, ApplicationCookieName.OIDC_REQUEST_IDENTIFIER);
    }

    private Optional<String> getOidcRequestIdentifier(final HttpServletRequest request) {
        return applicationCookieService.getCookieValue(request, ApplicationCookieName.OIDC_REQUEST_IDENTIFIER);
    }

    public void setOidcService(OidcService oidcService) {
        this.oidcService = oidcService;
    }

    public void setBearerTokenProvider(final BearerTokenProvider bearerTokenProvider) {
        this.bearerTokenProvider = bearerTokenProvider;
    }

    public void setProperties(final NiFiProperties properties) {
        this.properties = properties;
    }

    protected NiFiProperties getProperties() {
        return properties;
    }
}
