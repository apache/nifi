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
package org.apache.nifi.web.security.oidc;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.proc.BadJOSEException;
import com.nimbusds.jose.util.DefaultResourceRetriever;
import com.nimbusds.jose.util.ResourceRetriever;
import com.nimbusds.jwt.JWT;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.oauth2.sdk.AuthorizationGrant;
import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.oauth2.sdk.Scope;
import com.nimbusds.oauth2.sdk.TokenErrorResponse;
import com.nimbusds.oauth2.sdk.TokenRequest;
import com.nimbusds.oauth2.sdk.TokenResponse;
import com.nimbusds.oauth2.sdk.auth.ClientAuthentication;
import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod;
import com.nimbusds.oauth2.sdk.auth.ClientSecretBasic;
import com.nimbusds.oauth2.sdk.auth.ClientSecretPost;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.http.HTTPRequest;
import com.nimbusds.oauth2.sdk.http.HTTPResponse;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.token.BearerAccessToken;
import com.nimbusds.openid.connect.sdk.OIDCScopeValue;
import com.nimbusds.openid.connect.sdk.OIDCTokenResponse;
import com.nimbusds.openid.connect.sdk.OIDCTokenResponseParser;
import com.nimbusds.openid.connect.sdk.UserInfoErrorResponse;
import com.nimbusds.openid.connect.sdk.UserInfoRequest;
import com.nimbusds.openid.connect.sdk.UserInfoResponse;
import com.nimbusds.openid.connect.sdk.UserInfoSuccessResponse;
import com.nimbusds.openid.connect.sdk.claims.IDTokenClaimsSet;
import com.nimbusds.openid.connect.sdk.op.OIDCProviderMetadata;
import com.nimbusds.openid.connect.sdk.token.OIDCTokens;
import com.nimbusds.openid.connect.sdk.validators.IDTokenValidator;
import net.minidev.json.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.security.jwt.JwtService;
import org.apache.nifi.web.security.token.LoginAuthenticationToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.nimbusds.openid.connect.sdk.claims.UserInfo.EMAIL_CLAIM_NAME;


/**
 * OidcProvider for managing the OpenId Connect Authorization flow.
 */
public class StandardOidcIdentityProvider implements OidcIdentityProvider {

    private static final Logger logger = LoggerFactory.getLogger(StandardOidcIdentityProvider.class);

    private NiFiProperties properties;
    private JwtService jwtService;
    private OIDCProviderMetadata oidcProviderMetadata;
    private int oidcConnectTimeout;
    private int oidcReadTimeout;
    private IDTokenValidator tokenValidator;
    private ClientID clientId;
    private Secret clientSecret;

    /**
     * Creates a new StandardOidcIdentityProvider.
     *
     * @param jwtService jwt service
     * @param properties properties
     */
    public StandardOidcIdentityProvider(final JwtService jwtService, final NiFiProperties properties) {
        this.properties = properties;
        this.jwtService = jwtService;

        // attempt to process the oidc configuration if configured
        if (properties.isOidcEnabled()) {
            if (properties.isLoginIdentityProviderEnabled() || properties.isKnoxSsoEnabled()) {
                throw new RuntimeException("OpenId Connect support cannot be enabled if the Login Identity Provider or Apache Knox SSO is configured.");
            }

            // oidc connect timeout
            final String rawConnectTimeout = properties.getOidcConnectTimeout();
            try {
                oidcConnectTimeout = (int) FormatUtils.getTimeDuration(rawConnectTimeout, TimeUnit.MILLISECONDS);
            } catch (final Exception e) {
                logger.warn("Failed to parse value of property '{}' as a valid time period. Value was '{}'. Ignoring this value and using the default value of '{}'",
                        NiFiProperties.SECURITY_USER_OIDC_CONNECT_TIMEOUT, rawConnectTimeout, NiFiProperties.DEFAULT_SECURITY_USER_OIDC_CONNECT_TIMEOUT);
                oidcConnectTimeout = (int) FormatUtils.getTimeDuration(NiFiProperties.DEFAULT_SECURITY_USER_OIDC_CONNECT_TIMEOUT, TimeUnit.MILLISECONDS);
            }

            // oidc read timeout
            final String rawReadTimeout = properties.getOidcReadTimeout();
            try {
                oidcReadTimeout = (int) FormatUtils.getTimeDuration(rawReadTimeout, TimeUnit.MILLISECONDS);
            } catch (final Exception e) {
                logger.warn("Failed to parse value of property '{}' as a valid time period. Value was '{}'. Ignoring this value and using the default value of '{}'",
                        NiFiProperties.SECURITY_USER_OIDC_READ_TIMEOUT, rawReadTimeout, NiFiProperties.DEFAULT_SECURITY_USER_OIDC_READ_TIMEOUT);
                oidcReadTimeout = (int) FormatUtils.getTimeDuration(NiFiProperties.DEFAULT_SECURITY_USER_OIDC_READ_TIMEOUT, TimeUnit.MILLISECONDS);
            }

            // client id
            final String rawClientId = properties.getOidcClientId();
            if (StringUtils.isBlank(rawClientId)) {
                throw new RuntimeException("Client ID is required when configuring an OIDC Provider.");
            }
            clientId = new ClientID(rawClientId);

            // client secret
            final String rawClientSecret = properties.getOidcClientSecret();
            if (StringUtils.isBlank(rawClientSecret)) {
                throw new RuntimeException("Client secret is required when configured an OIDC Provider.");
            }
            clientSecret = new Secret(rawClientSecret);

            try {
                // retrieve the oidc provider metadata
                oidcProviderMetadata = retrieveOidcProviderMetadata(properties.getOidcDiscoveryUrl());
            } catch (IOException | ParseException e) {
                throw new RuntimeException("Unable to retrieve OpenId Connect Provider metadata from: " + properties.getOidcDiscoveryUrl(), e);
            }

            // ensure the authorization endpoint is present
            if (oidcProviderMetadata.getAuthorizationEndpointURI() == null) {
                throw new RuntimeException("OpenId Connect Provider metadata does not contain an Authorization Endpoint.");
            }

            // ensure the token endpoint is present
            if (oidcProviderMetadata.getTokenEndpointURI() == null) {
                throw new RuntimeException("OpenId Connect Provider metadata does not contain a Token Endpoint.");
            }

            // ensure the required scopes are present
            if (oidcProviderMetadata.getScopes() == null) {
                if (!oidcProviderMetadata.getScopes().contains(OIDCScopeValue.OPENID)) {
                    throw new RuntimeException("OpenId Connect Provider does not support the required scope: " + OIDCScopeValue.OPENID.getValue());
                }

                if (!oidcProviderMetadata.getScopes().contains(OIDCScopeValue.EMAIL) && oidcProviderMetadata.getUserInfoEndpointURI() == null) {
                    throw new RuntimeException(String.format("OpenId Connect Provider does not support '%s' scope and does not provide a UserInfo Endpoint.", OIDCScopeValue.EMAIL.getValue()));
                }
            }

            // ensure the oidc provider supports basic or post client auth
            List<ClientAuthenticationMethod> clientAuthenticationMethods = oidcProviderMetadata.getTokenEndpointAuthMethods();
            logger.info("OpenId Connect: Available clientAuthenticationMethods {} ", clientAuthenticationMethods);
            if (clientAuthenticationMethods == null || clientAuthenticationMethods.isEmpty()) {
                clientAuthenticationMethods = new ArrayList<>();
                clientAuthenticationMethods.add(ClientAuthenticationMethod.CLIENT_SECRET_BASIC);
                oidcProviderMetadata.setTokenEndpointAuthMethods(clientAuthenticationMethods);
                logger.warn("OpenId Connect: ClientAuthenticationMethods is null, Setting clientAuthenticationMethods as CLIENT_SECRET_BASIC");
            } else if (!clientAuthenticationMethods.contains(ClientAuthenticationMethod.CLIENT_SECRET_BASIC)
                    && !clientAuthenticationMethods.contains(ClientAuthenticationMethod.CLIENT_SECRET_POST)) {
                throw new RuntimeException(String.format("OpenId Connect Provider does not support %s or %s",
                        ClientAuthenticationMethod.CLIENT_SECRET_BASIC.getValue(),
                        ClientAuthenticationMethod.CLIENT_SECRET_POST.getValue()));
            }

            // extract the supported json web signature algorithms
            final List<JWSAlgorithm> allowedAlgorithms = oidcProviderMetadata.getIDTokenJWSAlgs();
            if (allowedAlgorithms == null || allowedAlgorithms.isEmpty()) {
                throw new RuntimeException("The OpenId Connect Provider does not support any JWS algorithms.");
            }

            try {
                // get the preferred json web signature algorithm
                final String rawPreferredJwsAlgorithm = properties.getOidcPreferredJwsAlgorithm();

                final JWSAlgorithm preferredJwsAlgorithm;
                if (StringUtils.isBlank(rawPreferredJwsAlgorithm)) {
                    preferredJwsAlgorithm = JWSAlgorithm.RS256;
                } else {
                    if ("none".equalsIgnoreCase(rawPreferredJwsAlgorithm)) {
                        preferredJwsAlgorithm = null;
                    } else {
                        preferredJwsAlgorithm = JWSAlgorithm.parse(rawPreferredJwsAlgorithm);
                    }
                }

                if (preferredJwsAlgorithm == null) {
                    tokenValidator = new IDTokenValidator(oidcProviderMetadata.getIssuer(), clientId);
                } else if (JWSAlgorithm.HS256.equals(preferredJwsAlgorithm) || JWSAlgorithm.HS384.equals(preferredJwsAlgorithm) || JWSAlgorithm.HS512.equals(preferredJwsAlgorithm)) {
                    tokenValidator = new IDTokenValidator(oidcProviderMetadata.getIssuer(), clientId, preferredJwsAlgorithm, clientSecret);
                } else {
                    final ResourceRetriever retriever = new DefaultResourceRetriever(oidcConnectTimeout, oidcReadTimeout);
                    tokenValidator = new IDTokenValidator(oidcProviderMetadata.getIssuer(), clientId, preferredJwsAlgorithm, oidcProviderMetadata.getJWKSetURI().toURL(), retriever);
                }
            } catch (final Exception e) {
                throw new RuntimeException("Unable to create the ID token validator for the configured OpenId Connect Provider: " + e.getMessage(), e);
            }
        }
    }

    private OIDCProviderMetadata retrieveOidcProviderMetadata(final String discoveryUri) throws IOException, ParseException {
        final URL url = new URL(discoveryUri);
        final HTTPRequest httpRequest = new HTTPRequest(HTTPRequest.Method.GET, url);
        httpRequest.setConnectTimeout(oidcConnectTimeout);
        httpRequest.setReadTimeout(oidcReadTimeout);

        final HTTPResponse httpResponse = httpRequest.send();

        if (httpResponse.getStatusCode() != 200) {
            throw new IOException("Unable to download OpenId Connect Provider metadata from " + url + ": Status code " + httpResponse.getStatusCode());
        }

        final JSONObject jsonObject = httpResponse.getContentAsJSONObject();
        return OIDCProviderMetadata.parse(jsonObject);
    }

    @Override
    public boolean isOidcEnabled() {
        return properties.isOidcEnabled();
    }

    @Override
    public URI getAuthorizationEndpoint() {
        if (!isOidcEnabled()) {
            throw new IllegalStateException(OPEN_ID_CONNECT_SUPPORT_IS_NOT_CONFIGURED);
        }

        return oidcProviderMetadata.getAuthorizationEndpointURI();
    }

    @Override
    public URI getEndSessionEndpoint() {
        if (!isOidcEnabled()) {
            throw new IllegalStateException(OPEN_ID_CONNECT_SUPPORT_IS_NOT_CONFIGURED);
        }
        return oidcProviderMetadata.getEndSessionEndpointURI();
    }

    @Override
    public Scope getScope() {
        if (!isOidcEnabled()) {
            throw new IllegalStateException(OPEN_ID_CONNECT_SUPPORT_IS_NOT_CONFIGURED);
        }

        final Scope scope = new Scope("openid");

        // if this provider supports email scope, include it to prevent a subsequent request to the user endpoint
        if (oidcProviderMetadata.getScopes() != null && oidcProviderMetadata.getScopes().contains(OIDCScopeValue.EMAIL)) {
            scope.add("email");
        }
        return scope;
    }

    @Override
    public ClientID getClientId() {
        if (!isOidcEnabled()) {
            throw new IllegalStateException(OPEN_ID_CONNECT_SUPPORT_IS_NOT_CONFIGURED);
        }

        return clientId;
    }

    @Override
    public String exchangeAuthorizationCode(final AuthorizationGrant authorizationGrant) throws IOException {
        if (!isOidcEnabled()) {
            throw new IllegalStateException(OPEN_ID_CONNECT_SUPPORT_IS_NOT_CONFIGURED);
        }

        final ClientAuthentication clientAuthentication;
        if (oidcProviderMetadata.getTokenEndpointAuthMethods().contains(ClientAuthenticationMethod.CLIENT_SECRET_POST)) {
            clientAuthentication = new ClientSecretPost(clientId, clientSecret);
        } else {
            clientAuthentication = new ClientSecretBasic(clientId, clientSecret);
        }

        try {
            // build the token request
            final TokenRequest request = new TokenRequest(oidcProviderMetadata.getTokenEndpointURI(), clientAuthentication, authorizationGrant, getScope());
            final HTTPRequest tokenHttpRequest = request.toHTTPRequest();
            tokenHttpRequest.setConnectTimeout(oidcConnectTimeout);
            tokenHttpRequest.setReadTimeout(oidcReadTimeout);

            // get the token response
            final TokenResponse response = OIDCTokenResponseParser.parse(tokenHttpRequest.send());

            if (response.indicatesSuccess()) {
                final OIDCTokenResponse oidcTokenResponse = (OIDCTokenResponse) response;
                final OIDCTokens oidcTokens = oidcTokenResponse.getOIDCTokens();
                final JWT oidcJwt = oidcTokens.getIDToken();

                // validate the token - no nonce required for authorization code flow
                final IDTokenClaimsSet claimsSet = tokenValidator.validate(oidcJwt, null);

                // attempt to extract the email from the id token if possible
                String email = claimsSet.getStringClaim(EMAIL_CLAIM_NAME);
                if (StringUtils.isBlank(email)) {
                    // extract the bearer access token
                    final BearerAccessToken bearerAccessToken = oidcTokens.getBearerAccessToken();
                    if (bearerAccessToken == null) {
                        throw new IllegalStateException("No access token found in the ID tokens");
                    }

                    // invoke the UserInfo endpoint
                    email = lookupEmail(bearerAccessToken);
                }

                // extract expiration details from the claims set
                final Calendar now = Calendar.getInstance();
                final Date expiration = claimsSet.getExpirationTime();
                final long expiresIn = expiration.getTime() - now.getTimeInMillis();

                // convert into a nifi jwt for retrieval later
                final LoginAuthenticationToken loginToken = new LoginAuthenticationToken(email, email, expiresIn, claimsSet.getIssuer().getValue());
                return jwtService.generateSignedToken(loginToken);
            } else {
                final TokenErrorResponse errorResponse = (TokenErrorResponse) response;
                throw new RuntimeException("An error occurred while invoking the Token endpoint: " + errorResponse.getErrorObject().getDescription());
            }
        } catch (final ParseException | JOSEException | BadJOSEException e) {
            throw new RuntimeException("Unable to parse the response from the Token request: " + e.getMessage());
        }
    }

    private String lookupEmail(final BearerAccessToken bearerAccessToken) throws IOException {
        try {
            // build the user request
            final UserInfoRequest request = new UserInfoRequest(oidcProviderMetadata.getUserInfoEndpointURI(), bearerAccessToken);
            final HTTPRequest tokenHttpRequest = request.toHTTPRequest();
            tokenHttpRequest.setConnectTimeout(oidcConnectTimeout);
            tokenHttpRequest.setReadTimeout(oidcReadTimeout);

            // send the user request
            final UserInfoResponse response = UserInfoResponse.parse(request.toHTTPRequest().send());

            // interpret the details
            if (response.indicatesSuccess()) {
                final UserInfoSuccessResponse successResponse = (UserInfoSuccessResponse) response;

                final JWTClaimsSet claimsSet;
                if (successResponse.getUserInfo() != null) {
                    claimsSet = successResponse.getUserInfo().toJWTClaimsSet();
                } else {
                    claimsSet = successResponse.getUserInfoJWT().getJWTClaimsSet();
                }

                final String email = claimsSet.getStringClaim(EMAIL_CLAIM_NAME);

                // ensure we were able to get the user email
                if (StringUtils.isBlank(email)) {
                    throw new IllegalStateException("Unable to extract email from the UserInfo token.");
                } else {
                    return email;
                }
            } else {
                final UserInfoErrorResponse errorResponse = (UserInfoErrorResponse) response;
                throw new RuntimeException("An error occurred while invoking the UserInfo endpoint: " + errorResponse.getErrorObject().getDescription());
            }
        } catch (final ParseException | java.text.ParseException e) {
            throw new RuntimeException("Unable to parse the response from the UserInfo token request: " + e.getMessage());
        }
    }
}
