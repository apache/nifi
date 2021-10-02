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
package org.apache.nifi.parameter.tests.system;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.parameter.AbstractParameterProvider;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.apache.nifi.parameter.ParameterProvider;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Parameters are provided by the dynamic properties.
 */
public class DynamicPropertyParameterProvider extends AbstractParameterProvider implements ParameterProvider {

    private Map<PropertyDescriptor, String> dynamicProperties = new HashMap<>();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.emptyList();
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .displayName(propertyDescriptorName)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .dynamic(true)
                .build();
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (newValue == null) {
            dynamicProperties.remove(descriptor);
        } else {
            dynamicProperties.put(descriptor, newValue);
        }
    }

    @Override
    public List<Parameter> fetchParameters(final ConfigurationContext context) {
        return dynamicProperties.entrySet().stream()
                .map(entry -> {
                    final ParameterDescriptor parameterDescriptor = new ParameterDescriptor.Builder()
                            .name(entry.getKey().getName())
                            .build();
                    return new Parameter(parameterDescriptor, entry.getValue(), null, true);
                })
                .collect(Collectors.toList());
    }
}
