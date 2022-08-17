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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.ResourceNotFoundException;
import software.amazon.awssdk.services.secretsmanager.model.SecretsManagerException;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Amazon Web Services Secrets Manager implementation of Sensitive Property Provider
 */
public class AwsSecretsManagerSensitivePropertyProvider implements SensitivePropertyProvider {
    private static final String IDENTIFIER_KEY = "aws/secretsmanager";

    private final SecretsManagerClient client;
    private final ObjectMapper objectMapper;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    AwsSecretsManagerSensitivePropertyProvider(final SecretsManagerClient client) {
        this.client = client;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public String getIdentifierKey() {
        return IDENTIFIER_KEY;
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

        try {
            writeLock.lock();
            final String secretName = context.getContextName();
            final Optional<ObjectNode> secretKeyValuesOptional = getSecretKeyValues(context);
            final ObjectNode secretObject = secretKeyValuesOptional.orElse(objectMapper.createObjectNode());

            secretObject.put(context.getPropertyName(), unprotectedValue);
            final String secretString = objectMapper.writeValueAsString(secretObject);

            if (secretKeyValuesOptional.isPresent()) {
                client.putSecretValue(builder -> builder.secretId(secretName).secretString(secretString));
            } else {
                client.createSecret(builder -> builder.name(secretName).secretString(secretString));
            }
            return context.getContextKey();
        } catch (final SecretsManagerException | JsonProcessingException e) {
            throw new SensitivePropertyProtectionException(String.format("AWS Secrets Manager Secret Could Not Be Stored for [%s]", context), e);
        } finally {
            writeLock.unlock();
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
            readLock.lock();

            String propertyValue = null;
            final Optional<ObjectNode> secretKeyValuesOptional = getSecretKeyValues(context);
            if (secretKeyValuesOptional.isPresent()) {
                final ObjectNode secretKeyValues = secretKeyValuesOptional.get();
                final String propertyName = context.getPropertyName();
                if (secretKeyValues.has(propertyName)) {
                    propertyValue = secretKeyValues.get(propertyName).textValue();
                }
            }
            if (propertyValue == null) {
                throw new SensitivePropertyProtectionException(
                        String.format("AWS Secret Name [%s] Property Name [%s] not found", context.getContextName(), context.getPropertyName()));
            }

            return propertyValue;
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Returns the optional parsed JSON from the matching secret, or empty if the secret does not exist.
     * @param context The property context
     * @return The optional parsed JSON, or empty if the secret does not exist
     */
    private Optional<ObjectNode> getSecretKeyValues(final ProtectedPropertyContext context) {
        try {
            final GetSecretValueResponse response = client.getSecretValue(builder -> builder.secretId(context.getContextName()));

            if (response.secretString() == null) {
                throw new SensitivePropertyProtectionException(String.format("AWS Secret Name [%s] string value not found",
                        context.getContextKey()));
            }
            final JsonNode responseNode = objectMapper.readTree(response.secretString());
            if (!(responseNode instanceof ObjectNode)) {
                throw new SensitivePropertyProtectionException(String.format("AWS Secrets Manager Secret [%s] JSON parsing failed",
                        context.getContextKey()));
            }
            return Optional.of((ObjectNode) responseNode) ;
        } catch (final ResourceNotFoundException e) {
            return Optional.empty();
        }  catch (final SecretsManagerException e) {
            throw new SensitivePropertyProtectionException(String.format("AWS Secrets Manager Secret [%s] retrieval failed",
                    context.getContextKey()), e);
        } catch (final JsonProcessingException e) {
            throw new SensitivePropertyProtectionException(String.format("AWS Secrets Manager Secret [%s] JSON parsing failed",
                    context.getContextKey()), e);
        }
    }

    @Override
    public void cleanUp() {
        if (client != null) {
            client.close();
        }
    }
}
