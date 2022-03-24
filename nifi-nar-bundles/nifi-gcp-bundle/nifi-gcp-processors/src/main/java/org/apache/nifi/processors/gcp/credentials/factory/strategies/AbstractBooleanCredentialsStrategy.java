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
package org.apache.nifi.processors.gcp.credentials.factory.strategies;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processors.gcp.credentials.factory.CredentialsStrategy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;


/**
 * Partial implementation of CredentialsStrategy to provide support for credential strategies specified by
 * a single boolean property.
 */
public abstract class AbstractBooleanCredentialsStrategy extends AbstractCredentialsStrategy {

    private PropertyDescriptor strategyProperty;

    public AbstractBooleanCredentialsStrategy(String name, PropertyDescriptor strategyProperty) {
        super(name, new PropertyDescriptor[]{
                strategyProperty
        });
        this.strategyProperty = strategyProperty;
    }

    @Override
    public boolean canCreatePrimaryCredential(Map<PropertyDescriptor, String> properties) {
        return (properties.get(this.strategyProperty) != null
                && properties.get(this.strategyProperty).equalsIgnoreCase("true"));
    }

    @Override
    public Collection<ValidationResult> validate(final ValidationContext validationContext,
                                                 final CredentialsStrategy primaryStrategy) {
        boolean thisIsSelectedStrategy = this == primaryStrategy;
        Boolean useStrategy = validationContext.getProperty(strategyProperty).asBoolean();
        if (!thisIsSelectedStrategy && (useStrategy == null ? false : useStrategy)) {
            String failureFormat = "property %1$s cannot be used with %2$s";
            Collection<ValidationResult> validationFailureResults = new ArrayList<ValidationResult>();
            String message = String.format(failureFormat, strategyProperty.getDisplayName(),
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
