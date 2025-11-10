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
package org.apache.nifi.processors.gcp.credentials.factory;

import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.GoogleCredentials;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processors.gcp.credentials.factory.strategies.ComputeEngineCredentialsStrategy;
import org.apache.nifi.processors.gcp.credentials.factory.strategies.ExplicitApplicationDefaultCredentialsStrategy;
import org.apache.nifi.processors.gcp.credentials.factory.strategies.ImplicitApplicationDefaultCredentialsStrategy;
import org.apache.nifi.processors.gcp.credentials.factory.strategies.JsonFileServiceAccountCredentialsStrategy;
import org.apache.nifi.processors.gcp.credentials.factory.strategies.JsonStringServiceAccountCredentialsStrategy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Generates GCP credentials in the form of GoogleCredential implementations for processors
 * and controller services.  The factory supports a number of strategies for specifying and validating
 * GCP credentials, interpreted as an ordered list of most-preferred to least-preferred.
 *
 * Additional strategies should implement CredentialsStrategy, then be added to the strategies list in the
 * constructor.
 *
 * @see org.apache.nifi.processors.gcp.credentials.factory.strategies
 */
public class CredentialsFactory {

    private final List<CredentialsStrategy> strategies = new ArrayList<>();
    private final Map<AuthenticationStrategy, CredentialsStrategy> strategiesByAuthentication = new EnumMap<>(AuthenticationStrategy.class);

    public CredentialsFactory() {
        // Primary Credential Strategies
        final CredentialsStrategy explicitApplicationDefaultCredentialsStrategy = new ExplicitApplicationDefaultCredentialsStrategy();
        final CredentialsStrategy jsonFileServiceAccountCredentialsStrategy = new JsonFileServiceAccountCredentialsStrategy();
        final CredentialsStrategy jsonStringServiceAccountCredentialsStrategy = new JsonStringServiceAccountCredentialsStrategy();
        final CredentialsStrategy computeEngineCredentialsStrategy = new ComputeEngineCredentialsStrategy();

        strategies.add(explicitApplicationDefaultCredentialsStrategy);
        strategies.add(jsonFileServiceAccountCredentialsStrategy);
        strategies.add(jsonStringServiceAccountCredentialsStrategy);
        strategies.add(computeEngineCredentialsStrategy);

        // Implicit Default is the catch-all primary strategy
        strategies.add(new ImplicitApplicationDefaultCredentialsStrategy());

        strategiesByAuthentication.put(AuthenticationStrategy.APPLICATION_DEFAULT, explicitApplicationDefaultCredentialsStrategy);
        strategiesByAuthentication.put(AuthenticationStrategy.SERVICE_ACCOUNT_JSON_FILE, jsonFileServiceAccountCredentialsStrategy);
        strategiesByAuthentication.put(AuthenticationStrategy.SERVICE_ACCOUNT_JSON, jsonStringServiceAccountCredentialsStrategy);
        strategiesByAuthentication.put(AuthenticationStrategy.COMPUTE_ENGINE, computeEngineCredentialsStrategy);
    }

    public CredentialsStrategy selectPrimaryStrategy(final Map<PropertyDescriptor, String> properties) {
        final CredentialsStrategy selectedStrategy = selectStrategyFromAuthenticationProperty(properties);
        if (selectedStrategy != null) {
            return selectedStrategy;
        }

        for (CredentialsStrategy strategy : strategies) {
            if (strategy.canCreatePrimaryCredential(properties)) {
                return strategy;
            }
        }
        return null;
    }

    private CredentialsStrategy selectStrategyFromAuthenticationProperty(final Map<PropertyDescriptor, String> properties) {
        final String authenticationStrategyValue = properties.get(CredentialPropertyDescriptors.AUTHENTICATION_STRATEGY);
        final Optional<AuthenticationStrategy> authenticationStrategy = AuthenticationStrategy.fromValue(authenticationStrategyValue);
        return authenticationStrategy.map(strategiesByAuthentication::get).orElse(null);
    }

    public CredentialsStrategy selectPrimaryStrategy(final ValidationContext validationContext) {
        final Map<PropertyDescriptor, String> properties = validationContext.getProperties();
        return selectPrimaryStrategy(properties);
    }

    /**
     * Validates GCP credential properties against the configured strategies to report any validation errors.
     * @return Validation errors
     */
    public Collection<ValidationResult> validate(final ValidationContext validationContext) {
        final CredentialsStrategy selectedStrategy = selectPrimaryStrategy(validationContext);
        final List<ValidationResult> validationFailureResults = new ArrayList<>();

        for (CredentialsStrategy strategy : strategies) {
            final Collection<ValidationResult> strategyValidationFailures = strategy.validate(validationContext,
                    selectedStrategy);
            if (strategyValidationFailures != null) {
                validationFailureResults.addAll(strategyValidationFailures);
            }
        }

        return validationFailureResults;
    }

    /**
     * Produces the AuthCredentials according to the given property set and the strategies configured in
     * the factory.
     * @return AuthCredentials
     *
     * @throws IOException if there is an issue accessing the credential files
     */
    public GoogleCredentials getGoogleCredentials(final Map<PropertyDescriptor, String> properties, final HttpTransportFactory transportFactory) throws IOException {
        final CredentialsStrategy primaryStrategy = selectPrimaryStrategy(properties);
        return primaryStrategy.getGoogleCredentials(properties, transportFactory);
    }
}
