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

package org.apache.nifi.connectors.tests.system;

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.connector.AbstractConnector;
import org.apache.nifi.components.connector.ConfigurationStep;
import org.apache.nifi.components.connector.ConnectorPropertyDescriptor;
import org.apache.nifi.components.connector.ConnectorPropertyGroup;
import org.apache.nifi.components.connector.PropertyType;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;
import java.util.Map;

/**
 * Declares a required property with a default so a system test can confirm the framework fills that value when an
 * external configuration provider omits it.
 */
public class RequiredDefaultConnector extends AbstractConnector {
    static final String STEP_NAME = "Defaults Step";
    static final String SUPPLIED_PROPERTY_NAME = "Source";
    static final String OMITTED_PROPERTY_NAME = "Greeting";
    static final String OMITTED_PROPERTY_DEFAULT = "Hello";

    private static final ConnectorPropertyDescriptor SUPPLIED_PROPERTY = new ConnectorPropertyDescriptor.Builder()
        .name(SUPPLIED_PROPERTY_NAME)
        .description("Required property supplied by the external configuration provider.")
        .required(true)
        .type(PropertyType.STRING)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    private static final ConnectorPropertyDescriptor OMITTED_PROPERTY = new ConnectorPropertyDescriptor.Builder()
        .name(OMITTED_PROPERTY_NAME)
        .description("Required property with a default that the external configuration provider omits.")
        .required(true)
        .defaultValue(OMITTED_PROPERTY_DEFAULT)
        .type(PropertyType.STRING)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    private static final ConnectorPropertyGroup PROPERTY_GROUP = new ConnectorPropertyGroup.Builder()
        .name("Defaults")
        .description("Properties used to verify that an omitted required default is populated.")
        .properties(List.of(SUPPLIED_PROPERTY, OMITTED_PROPERTY))
        .build();

    private static final ConfigurationStep CONFIGURATION_STEP = new ConfigurationStep.Builder()
        .name(STEP_NAME)
        .propertyGroups(List.of(PROPERTY_GROUP))
        .build();

    @Override
    protected void onStepConfigured(final String stepName, final FlowContext workingContext) {
    }

    @Override
    public VersionedExternalFlow getInitialFlow() {
        final VersionedProcessGroup group = new VersionedProcessGroup();
        group.setName("Required Default Flow");

        final VersionedExternalFlow flow = new VersionedExternalFlow();
        flow.setFlowContents(group);
        return flow;
    }

    @Override
    public VersionedExternalFlow getActiveFlow(final FlowContext activeFlowContext) {
        return getInitialFlow();
    }

    @Override
    public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final Map<String, String> propertyValueOverrides, final FlowContext flowContext) {
        return List.of(new ConfigVerificationResult.Builder()
            .outcome(Outcome.SUCCESSFUL)
            .subject(stepName)
            .verificationStepName("Required Default Verification")
            .explanation("Successful verification")
            .build());
    }

    @Override
    public List<ConfigurationStep> getConfigurationSteps() {
        return List.of(CONFIGURATION_STEP);
    }

    @Override
    public void applyUpdate(final FlowContext workingFlowContext, final FlowContext activeFlowContext) {
    }
}
