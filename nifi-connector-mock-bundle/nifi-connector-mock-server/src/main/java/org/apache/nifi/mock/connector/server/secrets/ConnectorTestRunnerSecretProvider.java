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

package org.apache.nifi.mock.connector.server.secrets;

import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.components.connector.Secret;
import org.apache.nifi.components.connector.secrets.SecretProvider;
import org.apache.nifi.components.connector.secrets.StandardSecret;
import org.apache.nifi.mock.connector.server.ConnectorTestRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConnectorTestRunnerSecretProvider implements SecretProvider {
    public static final String GROUP_NAME = "Default";
    private static final Authorizable AUTHORIZABLE = new ConnectorTestRunnerAuthorizable();

    private final Map<String, String> secrets = new HashMap<>();

    public void addSecret(final String key, final String value) {
        this.secrets.put(key, value);
    }

    @Override
    public String getProviderId() {
        return ConnectorTestRunner.SECRET_PROVIDER_ID;
    }

    @Override
    public String getProviderName() {
        return ConnectorTestRunner.SECRET_PROVIDER_ID;
    }

    @Override
    public List<Secret> getAllSecrets() {
        final List<Secret> secrets = new ArrayList<>();
        for (final Map.Entry<String, String> entry : this.secrets.entrySet()) {
            final Secret secret = new StandardSecret.Builder()
                .providerId(ConnectorTestRunner.SECRET_PROVIDER_ID)
                .providerName(ConnectorTestRunner.SECRET_PROVIDER_NAME)
                .groupName(GROUP_NAME)
                .name(entry.getKey())
                .value(entry.getValue())
                .authorizable(AUTHORIZABLE)
                .fullyQualifiedName(GROUP_NAME + "." + entry.getKey())
                .build();

            secrets.add(secret);
        }

        return secrets;
    }

    @Override
    public List<Secret> getSecrets(final List<String> fullyQualifiedSecretNames) {
        final List<Secret> matchingSecrets = new ArrayList<>();

        for (final String secretName : fullyQualifiedSecretNames) {
            final String value = secrets.get(secretName);

            if (value != null) {
                final Secret secret = new StandardSecret.Builder()
                    .providerId(ConnectorTestRunner.SECRET_PROVIDER_ID)
                    .providerName(ConnectorTestRunner.SECRET_PROVIDER_NAME)
                    .groupName(GROUP_NAME)
                    .name(secretName)
                    .fullyQualifiedName(GROUP_NAME + "." + secretName)
                    .value(value)
                    .authorizable(AUTHORIZABLE)
                    .build();

                matchingSecrets.add(secret);
            }
        }
        return matchingSecrets;
    }
}
