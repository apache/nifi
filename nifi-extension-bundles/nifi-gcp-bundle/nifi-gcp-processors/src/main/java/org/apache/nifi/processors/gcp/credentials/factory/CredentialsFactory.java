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
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processors.gcp.credentials.factory.strategies.ApplicationDefaultCredentialsStrategy;
import org.apache.nifi.processors.gcp.credentials.factory.strategies.ComputeEngineCredentialsStrategy;
import org.apache.nifi.processors.gcp.credentials.factory.strategies.JsonFileServiceAccountCredentialsStrategy;
import org.apache.nifi.processors.gcp.credentials.factory.strategies.JsonStringServiceAccountCredentialsStrategy;
import org.apache.nifi.processors.gcp.credentials.factory.strategies.WorkloadIdentityFederationCredentialsStrategy;

import java.io.IOException;
import java.util.EnumMap;
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

    private final Map<AuthenticationStrategy, CredentialsStrategy> strategiesByAuthentication = new EnumMap<>(AuthenticationStrategy.class);

    public CredentialsFactory() {
        strategiesByAuthentication.put(AuthenticationStrategy.APPLICATION_DEFAULT, new ApplicationDefaultCredentialsStrategy());
        strategiesByAuthentication.put(AuthenticationStrategy.SERVICE_ACCOUNT_JSON_FILE, new JsonFileServiceAccountCredentialsStrategy());
        strategiesByAuthentication.put(AuthenticationStrategy.SERVICE_ACCOUNT_JSON, new JsonStringServiceAccountCredentialsStrategy());
        strategiesByAuthentication.put(AuthenticationStrategy.WORKLOAD_IDENTITY_FEDERATION, new WorkloadIdentityFederationCredentialsStrategy());
        strategiesByAuthentication.put(AuthenticationStrategy.COMPUTE_ENGINE, new ComputeEngineCredentialsStrategy());
    }

    public CredentialsStrategy selectPrimaryStrategy(final Map<PropertyDescriptor, String> properties) {
        final String authenticationStrategyValue = properties.get(CredentialPropertyDescriptors.AUTHENTICATION_STRATEGY);
        final Optional<AuthenticationStrategy> authenticationStrategy = AuthenticationStrategy.fromValue(authenticationStrategyValue);
        return authenticationStrategy.map(strategiesByAuthentication::get)
                .orElse(strategiesByAuthentication.get(AuthenticationStrategy.APPLICATION_DEFAULT));
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
        if (primaryStrategy == null) {
            throw new IllegalStateException("No matching authentication strategy is configured");
        }
        return primaryStrategy.getGoogleCredentials(properties, transportFactory);
    }

    public GoogleCredentials getGoogleCredentials(final ConfigurationContext context, final HttpTransportFactory transportFactory) throws IOException {
        final CredentialsStrategy primaryStrategy = selectPrimaryStrategy(context.getProperties());
        if (primaryStrategy == null) {
            throw new IllegalStateException("No matching authentication strategy is configured");
        }
        return primaryStrategy.getGoogleCredentials(context, transportFactory);
    }
}
