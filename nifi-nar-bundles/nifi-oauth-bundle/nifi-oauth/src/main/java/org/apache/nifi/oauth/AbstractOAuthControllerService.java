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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;


public abstract class AbstractOAuthControllerService
    extends AbstractControllerService implements OAuth2ClientService {

    protected String accessToken = null;
    protected String refreshToken = null;
    protected String tokenType = null;
    protected long expiresIn = -1;
    protected long expiresTime = -1;
    protected long lastResponseTimestamp = -1;
    protected Map<String, String> extraHeaders = new HashMap<String, String>();
    protected String authUrl = null;
    protected long expireTimeSafetyNetSeconds = -1;
    protected String accessTokenRespName = null;
    protected String expireTimeRespName = null;
    protected String expireInRespName = null;
    protected String tokenTypeRespName = null;
    protected String scopeRespName = null;

    public static final PropertyDescriptor AUTH_SERVER_URL = new PropertyDescriptor
            .Builder().name("oauth2_authorization_server_url")
            .displayName("OAuth2 Authorization Server")
            .description("OAuth2 Authorization Server that grants access to the protected resources on the behalf of the resource owner.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor RESPONSE_ACCESS_TOKEN_FIELD_NAME = new PropertyDescriptor
            .Builder().name("JSON_response_access_token_name")
            .displayName("JSON response 'access_token' name")
            .description("Name of the field in the JSON response that contains the access token. IETF OAuth2 spec default is 'access_token' if your API provider's" +
                    " response field is different this is where you can change that.")
            .defaultValue("access_token")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor RESPONSE_EXPIRE_TIME_FIELD_NAME = new PropertyDescriptor
            .Builder().name("JSON_response_access_token_name")
            .displayName("JSON response 'expire_time' name")
            .description("Name of the field in the JSON response that contains the expire time. IETF OAuth2 spec default is 'expire_time' if your API provider's" +
                    " response field is different this is where you can change that.")
            .defaultValue("expire_time")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor RESPONSE_EXPIRE_IN_FIELD_NAME = new PropertyDescriptor
            .Builder().name("JSON_response_access_token_name")
            .displayName("JSON response 'expire_in' name")
            .description("Name of the field in the JSON response that contains the expire in. IETF OAuth2 spec default is 'expire_in' if your API provider's" +
                    " response field is different this is where you can change that.")
            .defaultValue("expire_in")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor RESPONSE_TOKEN_TYPE_FIELD_NAME = new PropertyDescriptor
            .Builder().name("JSON_response_access_token_name")
            .displayName("JSON response 'token_type' name")
            .description("Name of the field in the JSON response that contains the token type. IETF OAuth2 spec default is 'token_type' if your API provider's" +
                    " response field is different this is where you can change that.")
            .defaultValue("token_type")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor RESPONSE_SCOPE_FIELD_NAME = new PropertyDescriptor
            .Builder().name("JSON_response_scope_name")
            .displayName("JSON response 'scope' name")
            .description("Name of the field in the JSON response that contains the scope. IETF OAuth2 spec default is 'scope' if your API provider's" +
                    " response field is different this is where you can change that.")
            .defaultValue("scope")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected void onEnabled(final ConfigurationContext context)
            throws InitializationException {
        Map<PropertyDescriptor, String> allProperties = context.getProperties();
        Iterator<PropertyDescriptor> itr = allProperties.keySet().iterator();
        while (itr.hasNext()) {
            PropertyDescriptor pd = itr.next();
            if (pd.isDynamic()) {
                extraHeaders.put(pd.getName(), allProperties.get(pd));
            }
        }

        this.authUrl = context.getProperty(AUTH_SERVER_URL).evaluateAttributeExpressions().getValue();

        // Name of the OAuth2 spec fields that should be extracted from the JSON authentication response. While in theory every provider should be
        // using the same names in the response JSON that rarely happens. These values are used to ensure that if the provider does NOT follow the
        // spec the user can still use this integration approach instead of having to write something custom.
        this.accessTokenRespName = context.getProperty(RESPONSE_ACCESS_TOKEN_FIELD_NAME).evaluateAttributeExpressions().getValue();
        this.expireTimeRespName = context.getProperty(RESPONSE_EXPIRE_TIME_FIELD_NAME).evaluateAttributeExpressions().getValue();
        this.expireInRespName = context.getProperty(RESPONSE_EXPIRE_IN_FIELD_NAME).evaluateAttributeExpressions().getValue();
        this.tokenTypeRespName = context.getProperty(RESPONSE_TOKEN_TYPE_FIELD_NAME).evaluateAttributeExpressions().getValue();
        this.scopeRespName = context.getProperty(RESPONSE_SCOPE_FIELD_NAME).evaluateAttributeExpressions().getValue();
    }

    public boolean isOAuthTokenExpired() {
        if (expiresTime == -1 && expiresIn == -1)
            return true;

        if (expiresTime > 0) {
            // Use the actual clock time that the token will expire
            if (System.currentTimeMillis() >= expiresTime) {
                return true;
            }

            if (expiresTime <= System.currentTimeMillis()) {
                return true;
            } else {
                return false;
            }
        } else if (expiresIn > 0) {
            long tombStoneTS = ((lastResponseTimestamp + (expiresIn * 1000)));
            if (System.currentTimeMillis() >= tombStoneTS) {
                return true;
            } else {
                return false;
            }
        } else {
            getLogger().warn("Neither 'expire_in' nor 'expire_time' is set. This makes it impossible to determine if the access token in still valid" +
                    " or not. Assuming the token is invalid.");
            return true;
        }
    }

    public String getAccessToken() {
        return this.accessToken;
    }
}
