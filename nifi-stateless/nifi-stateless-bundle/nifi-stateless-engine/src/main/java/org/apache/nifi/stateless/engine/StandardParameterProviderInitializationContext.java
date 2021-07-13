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

package org.apache.nifi.stateless.engine;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.service.StandardPropertyContext;
import org.apache.nifi.stateless.parameter.ParameterProvider;
import org.apache.nifi.stateless.parameter.ParameterProviderInitializationContext;

import java.util.LinkedHashMap;
import java.util.Map;

public class StandardParameterProviderInitializationContext extends StandardPropertyContext implements ParameterProviderInitializationContext {
    private final String identifier;

    public StandardParameterProviderInitializationContext(final ParameterProvider parameterProvider, final Map<String, String> propertyValues, final String identifier) {
        super(createPropertyMap(parameterProvider, propertyValues), parameterProvider);
        this.identifier = identifier;
    }

    private static Map<PropertyDescriptor, String> createPropertyMap(final ParameterProvider provider, final Map<String, String> propertyValues) {
        final Map<PropertyDescriptor, String> propertyMap = new LinkedHashMap<>();
        for (final Map.Entry<String, String> entry : propertyValues.entrySet()) {
            final PropertyDescriptor descriptor = provider.getPropertyDescriptor(entry.getKey());
            propertyMap.put(descriptor, entry.getValue());
        }
        return propertyMap;
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }
}
