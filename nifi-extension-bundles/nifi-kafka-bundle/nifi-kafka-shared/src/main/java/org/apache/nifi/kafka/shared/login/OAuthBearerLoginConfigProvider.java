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
package org.apache.nifi.kafka.shared.login;

import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.kafka.shared.component.KafkaClientComponent;

import static javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag.REQUIRED;
import static org.apache.nifi.kafka.shared.util.SaslExtensionUtil.isSaslExtensionProperty;

/**
 * SASL OAuthBearer Login Module implementation of configuration provider
 */
public class OAuthBearerLoginConfigProvider implements LoginConfigProvider {
    private static final String MODULE_CLASS_NAME = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule";

    public static final String SERVICE_ID_KEY = "serviceId";

    /**
     * Get JAAS configuration for activating OAuthBearer Login Module.
     * The login module uses callback handlers to acquire Access Tokens. NiFi's callback handler relies on {@link org.apache.nifi.oauth2.OAuth2AccessTokenProvider} controller service to get the token.
     * The controller service will be passed to the callback handler via Kafka config map (as an object, instead of the string-based JAAS config).
     * The JAAS config contains the service id and the SASL extension properties in order to make the callback handler unique to the given configuration
     * (the Kafka framework creates separate callback handlers based on JAAS config).
     *
     * @param context Property Context
     * @return JAAS configuration with OAuthBearer Login Module
     */
    @Override
    public String getConfiguration(final PropertyContext context) {
        final LoginConfigBuilder builder = new LoginConfigBuilder(MODULE_CLASS_NAME, REQUIRED);

        final String serviceId = context.getProperty(KafkaClientComponent.OAUTH2_ACCESS_TOKEN_PROVIDER_SERVICE).getValue();
        builder.append(SERVICE_ID_KEY, serviceId);

        context.getAllProperties().entrySet().stream()
                .filter(entry -> isSaslExtensionProperty(entry.getKey()))
                .forEach(entry -> builder.append(entry.getKey(), entry.getValue()));

        return builder.build();
    }
}
