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
 * Partial implementation of CredentialsStrategy to support most simple property-based strategies.
 */
public abstract class AbstractCredentialsStrategy implements CredentialsStrategy {
    private final String name;
    private final PropertyDescriptor[] requiredProperties;

    public AbstractCredentialsStrategy(String name, PropertyDescriptor[] requiredProperties) {
        this.name = name;
        this.requiredProperties = requiredProperties;
    }

    @Override
    public boolean canCreatePrimaryCredential(Map<PropertyDescriptor, String> properties) {
        for (PropertyDescriptor requiredProperty : requiredProperties) {
            boolean containsRequiredProperty = properties.containsKey(requiredProperty);
            String propertyValue = properties.get(requiredProperty);
            boolean containsValue = propertyValue != null;
            if (!containsRequiredProperty || !containsValue) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Collection<ValidationResult> validate(final ValidationContext validationContext,
                                                 final CredentialsStrategy primaryStrategy) {
        boolean thisIsSelectedStrategy = this == primaryStrategy;
        String requiredMessageFormat = "property %1$s must be set with %2$s";
        String excludedMessageFormat = "property %1$s cannot be used with %2$s";
        String failureFormat = thisIsSelectedStrategy ? requiredMessageFormat : excludedMessageFormat;
        Collection<ValidationResult> validationFailureResults = null;

        for (PropertyDescriptor requiredProperty : requiredProperties) {
            boolean requiredPropertyIsSet = validationContext.getProperty(requiredProperty).isSet();
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

    public String getName() {
        return name;
    }
}
