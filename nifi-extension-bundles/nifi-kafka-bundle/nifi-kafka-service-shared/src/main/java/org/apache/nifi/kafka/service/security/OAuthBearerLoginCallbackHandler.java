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
import org.apache.kafka.common.security.auth.SaslExtensions;
import org.apache.kafka.common.security.auth.SaslExtensionsCallback;
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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;

import static org.apache.nifi.kafka.shared.util.SaslExtensionUtil.isSaslExtensionProperty;
import static org.apache.nifi.kafka.shared.util.SaslExtensionUtil.removeSaslExtensionPropertyPrefix;

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

    private Map<String, String> saslExtensions;

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

        this.saslExtensions = options.entrySet().stream()
                .filter(entry -> isSaslExtensionProperty(entry.getKey()))
                .collect(Collectors.collectingAndThen(
                        Collectors.toMap(entry -> removeSaslExtensionPropertyPrefix(entry.getKey()), entry -> entry.getValue().toString()),
                        Collections::unmodifiableMap));
    }

    @Override
    public void handle(final Callback[] callbacks) throws UnsupportedCallbackException {
        for (final Callback callback : callbacks) {
            if (callback instanceof OAuthBearerTokenCallback) {
                handleTokenCallback((OAuthBearerTokenCallback) callback);
            } else if (callback instanceof SaslExtensionsCallback) {
                handleExtensionsCallback((SaslExtensionsCallback) callback);
            } else {
                throw new UnsupportedCallbackException(callback);
            }
        }
    }

    private void handleTokenCallback(final OAuthBearerTokenCallback callback) {
        final String accessToken;
        try {
            // Kafka's ExpiringCredentialRefreshingLogin calls this method when the current token is about to expire and expects a refreshed token, so forcefully update it
            accessTokenProvider.refreshAccessDetails();
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

    private void handleExtensionsCallback(final SaslExtensionsCallback callback) {
        // a unique SaslExtensions object must be returned otherwise it will be lost upon relogin
        callback.extensions(new SaslExtensions(saslExtensions));
    }

    @Override
    public void close() {
    }
}
