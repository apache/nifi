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
import java.util.List;
import java.util.Map;

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

    private final List<CredentialsStrategy> strategies = new ArrayList<CredentialsStrategy>();

    public CredentialsFactory() {
        // Primary Credential Strategies
        strategies.add(new ExplicitApplicationDefaultCredentialsStrategy());
        strategies.add(new JsonFileServiceAccountCredentialsStrategy());
        strategies.add(new JsonStringServiceAccountCredentialsStrategy());
        strategies.add(new ComputeEngineCredentialsStrategy());

        // Implicit Default is the catch-all primary strategy
        strategies.add(new ImplicitApplicationDefaultCredentialsStrategy());
    }

    public CredentialsStrategy selectPrimaryStrategy(final Map<PropertyDescriptor, String> properties) {
        for (CredentialsStrategy strategy : strategies) {
            if (strategy.canCreatePrimaryCredential(properties)) {
                return strategy;
            }
        }
        return null;
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
        final ArrayList<ValidationResult> validationFailureResults = new ArrayList<ValidationResult>();

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
