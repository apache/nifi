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
import org.apache.nifi.parameter.ParameterGroup;
import org.apache.nifi.parameter.ParameterProvider;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Parameters are provided by properties-style configuration.
 */
public class PropertiesParameterProvider extends AbstractParameterProvider implements ParameterProvider {

    private PropertyDescriptor PARAMETERS = new PropertyDescriptor.Builder()
            .name("parameters")
            .displayName("Parameters")
            .description("Specifies parameters in a properties file format")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.singletonList(PARAMETERS);
    }

    @Override
    public List<ParameterGroup> fetchParameters(final ConfigurationContext context) {

        final List<Parameter> parameters = context.getProperty(PARAMETERS).isSet()
                ? fetchParametersFromProperties(context.getProperty(PARAMETERS).getValue())
                : Collections.emptyList();
        return Arrays.asList(new ParameterGroup("Parameters", parameters));
    }

    private List<Parameter> fetchParametersFromProperties(final String parametersPropertiesValue) {
        final Properties parameters = new Properties();
        try {
            parameters.load(new ByteArrayInputStream(parametersPropertiesValue.getBytes(StandardCharsets.UTF_8)));
        } catch (final IOException e) {
            throw new RuntimeException("Could not parse parameters as properties: " + parametersPropertiesValue);
        }
       return parameters.entrySet().stream()
                .map(entry -> new Parameter.Builder()
                    .name(entry.getKey().toString())
                    .value(entry.getValue().toString())
                    .provided(true)
                    .build())
                .collect(Collectors.toList());
    }
}
