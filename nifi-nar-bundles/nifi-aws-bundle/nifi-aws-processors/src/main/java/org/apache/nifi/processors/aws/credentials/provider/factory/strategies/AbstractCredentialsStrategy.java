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
package org.apache.nifi.processors.aws.credentials.provider.factory.strategies;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.processors.aws.credentials.provider.factory.CredentialsStrategy;

import com.amazonaws.auth.AWSCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;


/**
 * Partial implementation of CredentialsStrategy to support most simple property-based strategies.
 */
public abstract class AbstractCredentialsStrategy implements CredentialsStrategy {
    private final String name;
    private final PropertyDescriptor[] requiredProperties;

    public AbstractCredentialsStrategy(final String name, PropertyDescriptor[] requiredProperties) {
        this.name = name;
        this.requiredProperties = requiredProperties;
    }

    @Override
    public boolean canCreatePrimaryCredential(final PropertyContext propertyContext) {
        for (final PropertyDescriptor requiredProperty : requiredProperties) {
            final PropertyValue propertyValue = propertyContext.getProperty(requiredProperty);
            if (!propertyValue.isSet()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Collection<ValidationResult> validate(final ValidationContext validationContext,
                                                 final CredentialsStrategy primaryStrategy) {
        final boolean thisIsSelectedStrategy = this == primaryStrategy;
        final String requiredMessageFormat = "property %1$s must be set with %2$s";
        final String excludedMessageFormat = "property %1$s cannot be used with %2$s";
        final String failureFormat = thisIsSelectedStrategy ? requiredMessageFormat : excludedMessageFormat;
        Collection<ValidationResult> validationFailureResults = null;

        for (final PropertyDescriptor requiredProperty : requiredProperties) {
            final boolean requiredPropertyIsSet = validationContext.getProperty(requiredProperty).isSet();
            if (requiredPropertyIsSet != thisIsSelectedStrategy) {
                String message = String.format(failureFormat, requiredProperty.getDisplayName(),
                        primaryStrategy.getName());
                if (validationFailureResults == null) {
                    validationFailureResults = new ArrayList<>();
                }
                validationFailureResults.add(new ValidationResult.Builder()
                        .subject(requiredProperty.getDisplayName())
                        .valid(false)
                        .explanation(message).build());
            }
        }

        return validationFailureResults;
    }

    public abstract AWSCredentialsProvider getCredentialsProvider(final PropertyContext propertyContext);

    public String getName() {
        return name;
    }


    @Override
    public boolean canCreateDerivedCredential(final PropertyContext propertyContext) {
        return false;
    }

    @Override
    public AWSCredentialsProvider getDerivedCredentialsProvider(final PropertyContext propertyContext,
                                                                final AWSCredentialsProvider primaryCredentialsProvider) {
        return null;
    }

    @Override
    public AwsCredentialsProvider getDerivedAwsCredentialsProvider(final PropertyContext propertyContext,
                                                                   final AwsCredentialsProvider primaryCredentialsProvider) {
        return null;
    }
}
