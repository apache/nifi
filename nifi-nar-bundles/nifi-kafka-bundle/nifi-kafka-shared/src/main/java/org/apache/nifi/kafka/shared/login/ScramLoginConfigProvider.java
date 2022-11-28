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

/**
 * SASL SCRAM Login Module implementation of configuration provider
 */
public class ScramLoginConfigProvider implements LoginConfigProvider {
    private static final String MODULE_CLASS_NAME = "org.apache.kafka.common.security.scram.ScramLoginModule";

    private static final String FORMAT = "%s required username=\"%s\" password=\"%s\"";

    private static final String TOKEN_AUTH_ENABLED = "tokenauth=true";

    private static final String SEMI_COLON = ";";

    /**
     * Get JAAS configuration using configured username and password with optional token authentication
     *
     * @param context Property Context
     * @return JAAS configuration with SCRAM Login Module
     */
    @Override
    public String getConfiguration(final PropertyContext context) {
        final StringBuilder builder = new StringBuilder();

        final String username = context.getProperty(KafkaClientComponent.SASL_USERNAME).evaluateAttributeExpressions().getValue();
        final String password = context.getProperty(KafkaClientComponent.SASL_PASSWORD).evaluateAttributeExpressions().getValue();

        final String moduleUsernamePassword = String.format(FORMAT, MODULE_CLASS_NAME, username, password);
        builder.append(moduleUsernamePassword);

        final Boolean tokenAuthenticationEnabled = context.getProperty(KafkaClientComponent.TOKEN_AUTHENTICATION).asBoolean();
        if (Boolean.TRUE == tokenAuthenticationEnabled) {
            builder.append(TOKEN_AUTH_ENABLED);
        }

        builder.append(SEMI_COLON);
        return builder.toString();
    }
}
