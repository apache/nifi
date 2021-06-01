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

package org.apache.nifi.stateless.parameters;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.stateless.parameter.AbstractParameterProvider;

import java.util.Arrays;
import java.util.List;

import static org.apache.nifi.expression.ExpressionLanguageScope.NONE;
import static org.apache.nifi.processor.util.StandardValidators.NON_EMPTY_VALIDATOR;

public class ParameterProviderWithProperties extends AbstractParameterProvider {

    static final PropertyDescriptor REQUIRED_PARAMETER = new PropertyDescriptor.Builder()
        .name("Required")
        .displayName("Required")
        .description("A required parameter")
        .required(true)
        .addValidator(NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(NONE)
        .build();

    static final PropertyDescriptor OPTIONAL_PARAMETER = new PropertyDescriptor.Builder()
        .name("Optional")
        .displayName("Optional")
        .description("An optional parameter")
        .required(false)
        .addValidator(NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(NONE)
        .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Arrays.asList(REQUIRED_PARAMETER, OPTIONAL_PARAMETER);
    }

    @Override
    public String getParameterValue(final String contextName, final String parameterName) {
        return getPropertyContext().getAllProperties().get(parameterName);
    }

    @Override
    public boolean isParameterDefined(final String contextName, final String parameterName) {
        return getPropertyContext().getAllProperties().containsKey(parameterName);
    }
}
