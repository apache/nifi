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
package org.apache.nifi.processors.aws.credentials.provider.factory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.processors.aws.credentials.provider.factory.strategies.ExplicitDefaultCredentialsStrategy;
import org.apache.nifi.processors.aws.credentials.provider.factory.strategies.AccessKeyPairCredentialsStrategy;
import org.apache.nifi.processors.aws.credentials.provider.factory.strategies.FileCredentialsStrategy;
import org.apache.nifi.processors.aws.credentials.provider.factory.strategies.NamedProfileCredentialsStrategy;
import org.apache.nifi.processors.aws.credentials.provider.factory.strategies.AnonymousCredentialsStrategy;
import org.apache.nifi.processors.aws.credentials.provider.factory.strategies.ImplicitDefaultCredentialsStrategy;
import org.apache.nifi.processors.aws.credentials.provider.factory.strategies.AssumeRoleCredentialsStrategy;

import com.amazonaws.auth.AWSCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;


/**
 * Generates AWS credentials in the form of AWSCredentialsProvider implementations for processors
 * and controller services.  The factory supports a number of strategies for specifying and validating
 * AWS credentials, interpreted as an ordered list of most-preferred to least-preferred.  It also supports
 * derived credential strategies like Assume Role, which require a primary credential as an input.
 *
 * Additional strategies should implement CredentialsStrategy, then be added to the strategies list in the
 * constructor.
 *
 * @see org.apache.nifi.processors.aws.credentials.provider.factory.strategies
 */
public class CredentialsProviderFactory {

    private final List<CredentialsStrategy> strategies = new ArrayList<CredentialsStrategy>();

    public CredentialsProviderFactory() {
        // Primary Credential Strategies
        strategies.add(new ExplicitDefaultCredentialsStrategy());
        strategies.add(new AccessKeyPairCredentialsStrategy());
        strategies.add(new FileCredentialsStrategy());
        strategies.add(new NamedProfileCredentialsStrategy());
        strategies.add(new AnonymousCredentialsStrategy());

        // Implicit Default is the catch-all primary strategy
        strategies.add(new ImplicitDefaultCredentialsStrategy());

        // Derived Credential Strategies
        strategies.add(new AssumeRoleCredentialsStrategy());
    }

    public CredentialsStrategy selectPrimaryStrategy(final PropertyContext propertyContext) {
        for (CredentialsStrategy strategy : strategies) {
            if (strategy.canCreatePrimaryCredential(propertyContext)) {
                return strategy;
            }
        }
        return null;
    }

    /**
     * Validates AWS credential properties against the configured strategies to report any validation errors.
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
     * Produces the AWSCredentialsProvider according to the given property set and the strategies configured in
     * the factory.
     * @return AWSCredentialsProvider implementation
     */
    public AWSCredentialsProvider getCredentialsProvider(final PropertyContext propertyContext) {
        final CredentialsStrategy primaryStrategy = selectPrimaryStrategy(propertyContext);
        AWSCredentialsProvider primaryCredentialsProvider = primaryStrategy.getCredentialsProvider(propertyContext);
        AWSCredentialsProvider derivedCredentialsProvider = null;

        for (CredentialsStrategy strategy : strategies) {
            if (strategy.canCreateDerivedCredential(propertyContext)) {
                derivedCredentialsProvider = strategy.getDerivedCredentialsProvider(propertyContext, primaryCredentialsProvider);
                break;
            }
        }

        if (derivedCredentialsProvider != null) {
            return derivedCredentialsProvider;
        } else {
            return primaryCredentialsProvider;
        }
    }

    /**
     * Produces the AwsCredentialsProvider according to the given property set and the strategies configured in
     * the factory.
     * @return AwsCredentialsProvider implementation
     */
    public AwsCredentialsProvider getAwsCredentialsProvider(final PropertyContext propertyContext) {
        final CredentialsStrategy primaryStrategy = selectPrimaryStrategy(propertyContext);
        final AwsCredentialsProvider primaryCredentialsProvider = primaryStrategy.getAwsCredentialsProvider(propertyContext);
        AwsCredentialsProvider derivedCredentialsProvider = null;

        for (final CredentialsStrategy strategy : strategies) {
            if (strategy.canCreateDerivedCredential(propertyContext)) {
                derivedCredentialsProvider = strategy.getDerivedAwsCredentialsProvider(propertyContext, primaryCredentialsProvider);
                break;
            }
        }

        return derivedCredentialsProvider == null ? primaryCredentialsProvider : derivedCredentialsProvider;
    }
}
