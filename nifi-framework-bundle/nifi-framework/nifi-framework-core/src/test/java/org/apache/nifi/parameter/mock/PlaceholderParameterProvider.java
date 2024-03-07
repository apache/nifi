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
package org.apache.nifi.parameter.mock;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.parameter.AbstractParameterProvider;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterGroup;
import org.apache.nifi.parameter.ParameterProvider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class PlaceholderParameterProvider extends AbstractParameterProvider implements ParameterProvider {

    private static final String[] STATIC_PARAMETERS = new String[] {"Parameter One", "Parameter Two"};

    public static final PropertyDescriptor SERVICE = new PropertyDescriptor.Builder()
            .name("Controller Service")
            .identifiesControllerService(ControllerService.class)
            .required(true)
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(SERVICE);
        return descriptors;
    }

    @Override
    public List<ParameterGroup> fetchParameters(final ConfigurationContext context) {
        final List<Parameter> parameters = Arrays.stream(STATIC_PARAMETERS)
                .map(parameterName ->
                    new Parameter.Builder()
                        .name(parameterName)
                        .value(parameterName + "-value")
                        .provided(true)
                        .build()
                )
                .collect(Collectors.toList());
        return Collections.singletonList(new ParameterGroup("Group", parameters));
    }
}
