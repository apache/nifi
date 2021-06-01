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
import org.apache.nifi.components.Validator;
import org.apache.nifi.stateless.config.ParameterOverride;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ParameterOverrideProvider extends AbstractParameterProvider implements ParameterProvider {
    // Effectively final
    private List<ParameterOverride> parameterOverrides;

    @Override
    public void init(final ParameterProviderInitializationContext context) {
        parameterOverrides = parseConfiguration(context);
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
            .name(propertyDescriptorName)
            .addValidator(Validator.VALID)
            .build();
    }

    private List<ParameterOverride> parseConfiguration(final ParameterProviderInitializationContext context) {
        final List<ParameterOverride> overrides = new ArrayList<>();

        final Map<String, String> properties = context.getAllProperties();
        for (final Map.Entry<String, String> entry : properties.entrySet()) {
            final String propertyName = entry.getKey();
            final String propertyValue = entry.getValue();

            final ParameterOverride override;
            if (propertyName.contains(":")) {
                final String[] splits = propertyName.split(":", 2);
                final String contextName = splits[0];
                final String parameterName = splits[1];
                override = new ParameterOverride(contextName, parameterName, propertyValue);
            } else {
                override = new ParameterOverride(propertyName, propertyValue);
            }

            overrides.add(override);
        }

        return overrides;
    }

    @Override
    public String getParameterValue(final String contextName, final String parameterName) {
        final ParameterOverride override = getParameterOverride(contextName, parameterName);
        return (override == null) ? null : override.getParameterValue();
    }

    @Override
    public boolean isParameterDefined(final String contextName, final String parameterName) {
        final ParameterOverride override = getParameterOverride(contextName, parameterName);
        return override != null;
    }

    private ParameterOverride getParameterOverride(final String contextName, final String parameterName) {
        for (final ParameterOverride override : parameterOverrides) {
            if ((override.getContextName() == null || Objects.equals(override.getContextName(), contextName)) && Objects.equals(override.getParameterName(), parameterName)) {
                return override;
            }
        }

        return null;
    }
}
