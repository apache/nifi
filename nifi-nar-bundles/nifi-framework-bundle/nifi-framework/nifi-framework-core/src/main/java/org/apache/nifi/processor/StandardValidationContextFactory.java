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
package org.apache.nifi.processor;

import java.util.Map;
import java.util.Set;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.controller.ValidationContextFactory;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.registry.VariableRegistry;

public class StandardValidationContextFactory implements ValidationContextFactory {

    private final ControllerServiceProvider serviceProvider;
    private final VariableRegistry variableRegistry;

    public StandardValidationContextFactory(final ControllerServiceProvider serviceProvider, final VariableRegistry variableRegistry) {
        this.serviceProvider = serviceProvider;
        this.variableRegistry = variableRegistry;
    }

    @Override
    public ValidationContext newValidationContext(final Map<PropertyDescriptor, String> properties, final String annotationData, final String groupId, final String componentId) {
        return new StandardValidationContext(serviceProvider, properties, annotationData, groupId, componentId,variableRegistry);
    }

    @Override
    public ValidationContext newValidationContext(final Set<String> serviceIdentifiersToNotValidate,
            final Map<PropertyDescriptor, String> properties, final String annotationData, final String groupId, String componentId) {
        return new StandardValidationContext(serviceProvider, serviceIdentifiersToNotValidate, properties, annotationData, groupId, componentId,variableRegistry);
    }
}
