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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.controller.PropertyConfiguration;
import org.apache.nifi.controller.ValidationContextFactory;
import org.apache.nifi.controller.flowanalysis.FlowAnalyzer;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.validation.RuleViolationsManager;

import java.util.Map;
import java.util.Optional;

public class StandardValidationContextFactory implements ValidationContextFactory {

    private final ControllerServiceProvider serviceProvider;
    private final RuleViolationsManager ruleViolationsManager;
    private final FlowAnalyzer flowAnalyzer;

    public StandardValidationContextFactory(final ControllerServiceProvider serviceProvider) {
        this(serviceProvider, null, null);
    }

    public StandardValidationContextFactory(
        final ControllerServiceProvider serviceProvider,
        final RuleViolationsManager ruleViolationsManager,
        final FlowAnalyzer flowAnalyzer
    ) {
        this.serviceProvider = serviceProvider;
        this.ruleViolationsManager = ruleViolationsManager;
        this.flowAnalyzer = flowAnalyzer;
    }

    @Override
    public ValidationContext newValidationContext(final Map<PropertyDescriptor, PropertyConfiguration> properties, final String annotationData, final String groupId, final String componentId,
                                                  final ParameterContext parameterContext, final boolean validateConnections) {
        return new StandardValidationContext(serviceProvider, properties, annotationData, groupId, componentId, parameterContext, validateConnections);
    }

    @Override
    public Optional<RuleViolationsManager> getRuleViolationsManager() {
        return Optional.ofNullable(ruleViolationsManager);
    }

    @Override
    public Optional<FlowAnalyzer> getFlowAnalyzer() {
        return Optional.ofNullable(flowAnalyzer);
    }
}