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
package org.apache.nifi.stateless.parameter;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A base class for secret-based <code>ParameterValueProvider</code>s, which map a ParameterContext to a named "Secret" with key/value pairs.  This
 * class allows a default Secret name to be configured for parameters not found in specific ParameterContext Secrets, and uses dynamic user-added
 * properties to map ParameterContext names to different Secret names.  Subclasses must provide the implementation for retrieving the actual
 * secret values.
 */
public abstract class AbstractSecretBasedParameterValueProvider extends AbstractParameterValueProvider implements ParameterValueProvider {
    private static final Validator NON_EMPTY_VALIDATOR = (subject, value, context) ->
            new ValidationResult.Builder().subject(subject).input(value).valid(value != null && !value.isEmpty()).explanation(subject + " cannot be empty").build();

    public static final PropertyDescriptor DEFAULT_SECRET_NAME = new PropertyDescriptor.Builder()
            .displayName("Default Secret Name")
            .name("default-secret-name")
            .description("The default secret name to use.  This secret represents a default Parameter Context if there is not a matching key within the mapped Parameter Context secret")
            .addValidator(NON_EMPTY_VALIDATOR)
            .build();

    private List<PropertyDescriptor> descriptors;

    private String defaultSecretName = null;

    private Map<String, String> contextToSecretMapping;

    /**
     * Define any additional properties.
     * @return Any additional property descriptors
     */
    protected abstract List<PropertyDescriptor> getAdditionalSupportedPropertyDescriptors();

    /**
     * Perform any additional initialization based on the context.
     * @param context The initialization context
     */
    protected abstract void additionalInit(final ParameterValueProviderInitializationContext context);

    /**
     * Extract the value for the given key from the secret with the given name.
     * @param secretName The name of a secret
     * @param keyName The key within the secret
     * @return The secret value, or null if either the secret or the key is not found
     */
    protected abstract String getSecretValue(final String secretName, final String keyName);

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .displayName(propertyDescriptorName)
                .name(propertyDescriptorName)
                .dynamic(true)
                .addValidator(NON_EMPTY_VALIDATOR)
                .build();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    protected final void init(final ParameterValueProviderInitializationContext context) {
        super.init(context);

        final List<PropertyDescriptor> propertyDescriptors = new ArrayList<>(getAdditionalSupportedPropertyDescriptors());
        propertyDescriptors.add(DEFAULT_SECRET_NAME);
        this.descriptors = Collections.unmodifiableList(propertyDescriptors);

        defaultSecretName = context.getProperty(DEFAULT_SECRET_NAME).getValue();
        contextToSecretMapping = new HashMap<>();
        for (final Map.Entry<String, String> entry : context.getAllProperties().entrySet()) {
            if (getPropertyDescriptor(entry.getKey()).isDynamic()) {
                contextToSecretMapping.put(entry.getKey(), entry.getValue());
            }
        }
        this.additionalInit(context);
    }

    @Override
    public boolean isParameterDefined(final String contextName, final String parameterName) {
        return getParameterValue(contextName, parameterName) != null;
    }

    @Override
    public String getParameterValue(final String contextName, final String parameterName) {
        final String contextBasedValue = getSecretValue(getSecretName(contextName), parameterName);
        return contextBasedValue != null || defaultSecretName == null ? contextBasedValue : getSecretValue(defaultSecretName, parameterName);
    }

    private String getSecretName(final String contextName) {
        return contextToSecretMapping.getOrDefault(contextName, contextName);
    }
}
