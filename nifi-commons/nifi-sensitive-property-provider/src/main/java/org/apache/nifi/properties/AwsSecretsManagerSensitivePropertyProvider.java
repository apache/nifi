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
package org.apache.nifi.properties;

import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.ResourceExistsException;
import software.amazon.awssdk.services.secretsmanager.model.SecretsManagerException;

import java.util.Objects;

public class AwsSecretsManagerSensitivePropertyProvider extends AbstractSensitivePropertyProvider {
    private final SecretsManagerClient client;

    AwsSecretsManagerSensitivePropertyProvider(final SecretsManagerClient client) {
        super(null);

        this.client = client;
    }

    @Override
    public boolean isSupported() {
        return client != null;
    }

    @Override
    public String protect(final String unprotectedValue, final ProtectedPropertyContext context)
            throws SensitivePropertyProtectionException {
        Objects.requireNonNull(context, "Property context must be provided");
        Objects.requireNonNull(unprotectedValue, "Property value must be provided");

        if (client == null) {
            throw new SensitivePropertyProtectionException("AWS Secrets Manager Provider Not Configured");
        }

        final String secretName = context.getContextKey();
        try {
            try {
                return client.createSecret(builder -> builder.name(secretName).secretString(unprotectedValue)).name();
            } catch (final ResourceExistsException e) {
                return client.putSecretValue(builder -> builder.secretId(secretName).secretString(unprotectedValue)).name();
            }
        } catch (final SecretsManagerException e) {
            throw new SensitivePropertyProtectionException("AWS Secrets Manager Secret Could Not Be Stored", e);
        }
    }

    @Override
    public String unprotect(final String protectedValue, final ProtectedPropertyContext context)
            throws SensitivePropertyProtectionException {
        Objects.requireNonNull(context, "Property context must be provided");

        if (client == null) {
            throw new SensitivePropertyProtectionException("AWS Secrets Manager Provider Not Configured");
        }
        try {
            final GetSecretValueResponse response = client.getSecretValue(builder -> builder.secretId(context.getContextKey()));

            if (response.secretString() == null) {
                throw new SensitivePropertyProtectionException("Found No Secret String in AWS Secrets Manager Secret");
            }
            return response.secretString();
        } catch (final SecretsManagerException e) {
            throw new SensitivePropertyProtectionException("AWS Secrets Manager Secret Could Not Be Retrieved", e);
        }
    }

    @Override
    protected PropertyProtectionScheme getProtectionScheme() {
        return PropertyProtectionScheme.AWS_SECRETSMANAGER;
    }

    @Override
    public String getIdentifierKey() {
        return getProtectionScheme().getIdentifier();
    }

    @Override
    public void cleanUp() {
        if (client != null) {
            client.close();
        }
    }
}
