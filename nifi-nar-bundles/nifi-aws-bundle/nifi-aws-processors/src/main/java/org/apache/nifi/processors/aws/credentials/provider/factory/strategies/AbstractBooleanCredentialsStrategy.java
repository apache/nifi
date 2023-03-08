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


/**
 * Partial implementation of CredentialsStrategy to provide support for credential strategies specified by
 * a single boolean property.
 */
public abstract class AbstractBooleanCredentialsStrategy extends AbstractCredentialsStrategy {

    private PropertyDescriptor strategyProperty;

    public AbstractBooleanCredentialsStrategy(final String name, final PropertyDescriptor strategyProperty) {
        super("Default Credentials", new PropertyDescriptor[]{
            strategyProperty
        });
        this.strategyProperty = strategyProperty;
    }

    @Override
    public boolean canCreatePrimaryCredential(final PropertyContext propertyContext) {
        PropertyValue strategyPropertyValue = propertyContext.getProperty(strategyProperty);
        if (strategyPropertyValue == null) {
            return false;
        }
        if (strategyProperty.isExpressionLanguageSupported()) {
            strategyPropertyValue = strategyPropertyValue.evaluateAttributeExpressions();
        }
        final String useStrategyString = strategyPropertyValue.getValue();
        final Boolean useStrategy = Boolean.parseBoolean(useStrategyString);
        return useStrategy;
    }

    @Override
    public Collection<ValidationResult> validate(final ValidationContext validationContext,
                                                 final CredentialsStrategy primaryStrategy) {
        final boolean thisIsSelectedStrategy = this == primaryStrategy;
        final Boolean useStrategy = validationContext.getProperty(strategyProperty).asBoolean();
        if (!thisIsSelectedStrategy && useStrategy) {
            final String failureFormat = "property %1$s cannot be used with %2$s";
            final Collection<ValidationResult> validationFailureResults = new ArrayList<ValidationResult>();
            final String message = String.format(failureFormat, strategyProperty.getDisplayName(),
                    primaryStrategy.getName());
            validationFailureResults.add(new ValidationResult.Builder()
                    .subject(strategyProperty.getDisplayName())
                    .valid(false)
                    .explanation(message).build());
            return  validationFailureResults;
        }
        return null;
    }

}
