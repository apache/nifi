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
package org.apache.nifi.kafka.service.security;

import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.ClientJwtValidator;
import org.apache.kafka.common.security.oauthbearer.JwtValidatorException;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.apache.kafka.common.security.oauthbearer.internals.secured.JaasOptionsUtils;
import org.apache.nifi.kafka.shared.login.OAuthBearerLoginConfigProvider;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.processor.exception.ProcessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.Callback;
import javax.security.auth.login.AppConfigurationEntry;

import java.util.List;
import java.util.Map;

/**
 * {@link org.apache.kafka.common.security.auth.AuthenticateCallbackHandler} implementation to support OAuth 2 in NiFi Kafka components.
 * It uses {@link org.apache.nifi.oauth2.OAuth2AccessTokenProvider} controller service to acquire Access Tokens. The service reference is injected via the Kafka configuration.
 * The service identifier will be validated against the serviceId provided in the JAAS configuration to ensure consistency.
 * For Access Token validation and parsing, the handler relies on the Kafka OAuth support classes. Only the token retrieval is NiFi specific.
 */
public class OAuthBearerLoginCallbackHandler implements AuthenticateCallbackHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(OAuthBearerLoginCallbackHandler.class);

    public static final String PROPERTY_KEY_NIFI_OAUTH_2_ACCESS_TOKEN_PROVIDER = "nifi.oauth2.access.token.provider";

    private OAuth2AccessTokenProvider accessTokenProvider;
    private ClientJwtValidator accessTokenValidator;

    @Override
    public void configure(final Map<String, ?> configs, final String saslMechanism, final List<AppConfigurationEntry> jaasConfigEntries) {
        final Map<String, Object> options = JaasOptionsUtils.getOptions(saslMechanism, jaasConfigEntries);

        final String serviceId = (String) options.get(OAuthBearerLoginConfigProvider.SERVICE_ID_KEY);
        if (serviceId == null) {
            throw new ProcessException(String.format("JAAS configuration must contain %s. [%s]", OAuthBearerLoginConfigProvider.SERVICE_ID_KEY, options));
        }

        final Object service = configs.get(PROPERTY_KEY_NIFI_OAUTH_2_ACCESS_TOKEN_PROVIDER);
        if (!(service instanceof OAuth2AccessTokenProvider)) {
            throw new ProcessException(String.format("OAuth2AccessTokenProvider must be provided via %s property in Kafka configuration", PROPERTY_KEY_NIFI_OAUTH_2_ACCESS_TOKEN_PROVIDER));
        }

        final OAuth2AccessTokenProvider accessTokenProvider = (OAuth2AccessTokenProvider) service;
        if (!accessTokenProvider.getIdentifier().equals(serviceId)) {
            throw new ProcessException(String.format("OAuth2AccessTokenProvider's identifier [%s] does not mach %s [%s] in JAAS configuration",
                    accessTokenProvider.getIdentifier(), OAuthBearerLoginConfigProvider.SERVICE_ID_KEY, serviceId));
        }

        this.accessTokenProvider = accessTokenProvider;
        this.accessTokenValidator = new ClientJwtValidator();
        this.accessTokenValidator.configure(configs, saslMechanism, List.of());
    }

    @Override
    public void handle(final Callback[] callbacks) {
        for (final Callback callback : callbacks) {
            if (callback instanceof OAuthBearerTokenCallback) {
                handleTokenCallback((OAuthBearerTokenCallback) callback);
            }
        }
    }

    private void handleTokenCallback(final OAuthBearerTokenCallback callback) {
        final String accessToken;
        try {
            accessToken = accessTokenProvider.getAccessDetails().getAccessToken();
        } catch (Exception e) {
            LOGGER.error("Could not retrieve access token", e);
            callback.error("service_error", e.getMessage(), null);
            return;
        }

        try {
            final OAuthBearerToken token = accessTokenValidator.validate(accessToken);
            callback.token(token);
        } catch (JwtValidatorException e) {
            LOGGER.error("Could not validate and parse access token", e);
            callback.error("invalid_token", e.getMessage(), null);
        }
    }

    @Override
    public void close() {
    }
}
