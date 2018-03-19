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

package org.apache.nifi.oauth;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.oauth.httpclient.OAuthHTTPConnectionClient;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.oltu.oauth2.client.HttpClient;
import org.apache.oltu.oauth2.client.request.OAuthClientRequest;
import org.apache.oltu.oauth2.common.message.types.GrantType;

@Tags({ "oauth2", "client", "secret", "post"})
@CapabilityDescription("POSTs the ClientId and ClientSecret to the OAuth2 authentication server to retrieve the" +
        " access token. The access token is stored locally in the controller service and used by processors " +
        "referencing this controller service.")
public class OAuth2ClientCredentialsGrantControllerService
        extends AbstractOAuthControllerService
        implements OAuth2ClientService {

    public static final PropertyDescriptor CLIENT_ID = new PropertyDescriptor
            .Builder().name("OAuth2 Client ID")
            .displayName("OAuth2 Client ID")
            .description("OAuth2 Client ID passed to the authorization server")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CLIENT_SECRET = new PropertyDescriptor
            .Builder().name("OAuth2 Client Secret")
            .displayName("OAuth2 Client Secret")
            .description("OAuth2 Client Secret that will be passed to the authorization server in exchange for an access token")
            .sensitive(true)
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> properties;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(AUTH_SERVER_URL);
        props.add(CLIENT_ID);
        props.add(CLIENT_SECRET);
        props.add(RESPONSE_ACCESS_TOKEN_FIELD_NAME);
        props.add(RESPONSE_EXPIRE_TIME_FIELD_NAME);
        props.add(RESPONSE_EXPIRE_IN_FIELD_NAME);
        props.add(RESPONSE_SCOPE_FIELD_NAME);
        props.add(RESPONSE_TOKEN_TYPE_FIELD_NAME);
        properties = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    private HttpClient con = null;
    private String clientId = null;
    private String clientSecret = null;
    private Base64.Encoder enc = Base64.getEncoder();

    /**
     * @param context the configuration context
     * @throws InitializationException error which occurred while trying to setup the client properties.
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {

        super.onEnabled(context);

        clientId = context.getProperty(CLIENT_ID).getValue();
        clientSecret = context.getProperty(CLIENT_SECRET).getValue();
    }

    public boolean authenticate() {

        try {

            String base64String = enc.encodeToString((clientId + ":" + clientSecret).getBytes());

            OAuthClientRequest.TokenRequestBuilder authTokenRequest =
                    new OAuthClientRequest.TokenRequestBuilder(authUrl)
                            .setClientId(clientId)
                            .setClientSecret(clientSecret)
                            .setGrantType(GrantType.CLIENT_CREDENTIALS);

            OAuthClientRequest headerRequest = authTokenRequest.buildHeaderMessage();
            headerRequest.setHeader("Cache-Control", "no-cache");
            headerRequest.setHeader("Content-Type", "application/x-www-form-urlencoded");
            headerRequest.setHeader("Authorization", "Basic " + base64String);

            // Add the dynamic headers specified by the user
            if (extraHeaders != null && extraHeaders.size() > 0) {
                Iterator<String> itr = extraHeaders.keySet().iterator();
                while (itr.hasNext()) {
                    String key = itr.next();
                    headerRequest.setHeader(key, extraHeaders.get(key));
                }
            }

            headerRequest.setBody("grant_type=client_credentials");

            con = new OAuthHTTPConnectionClient(accessTokenRespName, tokenTypeRespName, scopeRespName, expireInRespName, expireTimeRespName);
            OAuthHTTPConnectionClient.CustomOAuthAccessTokenResponse authResp = con.execute(headerRequest, null, "POST", OAuthHTTPConnectionClient.CustomOAuthAccessTokenResponse.class);

            if (authResp != null) {
                this.accessToken = authResp.getAccessToken();
                this.expiresIn = authResp.getExpiresIn();
                this.refreshToken = authResp.getRefreshToken();
                this.tokenType = authResp.getTokenType();
                this.lastResponseTimestamp = System.currentTimeMillis();

                if (getLogger().isDebugEnabled()) {
                    getLogger().debug("Local variables set and OAuth2 InvokeHTTP processor is now ready for operation with Access Token");
                }
                return true;
            }

        } catch (Exception e) {
            getLogger().error(e.getMessage());
        } finally {
            if (con != null)
                con.shutdown();
        }

        return false;
    }

    @OnDisabled
    public void shutdown() {
        // nothing to do here
    }

    public HttpClient getCon() {
        return con;
    }

    public void setCon(HttpClient con) {
        this.con = con;
    }
}