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
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.connector.AbstractConnector;
import org.apache.nifi.components.connector.ConfigurationStep;
import org.apache.nifi.components.connector.ConnectorPropertyDescriptor;
import org.apache.nifi.components.connector.ConnectorPropertyGroup;
import org.apache.nifi.components.connector.PropertyType;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class NopConnector extends AbstractConnector {

    private static final ConnectorPropertyDescriptor IGNORED_PROPERTY = new ConnectorPropertyDescriptor.Builder()
        .name("Ignored Property")
        .description("This property is ignored by the NopConnector.")
        .required(false)
        .type(PropertyType.STRING)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    private static final ConnectorPropertyGroup IGNORED_PROPERTY_GROUP = new ConnectorPropertyGroup.Builder()
        .name("Ignored Property Group")
        .description("This property group is ignored by the NopConnector.")
        .properties(List.of(IGNORED_PROPERTY))
        .build();

    private static final ConnectorPropertyDescriptor SECRET_PROPERTY = new ConnectorPropertyDescriptor.Builder()
        .name("Secret Property")
        .description("This is a secret property that is ignored by the NopConnector.")
        .required(false)
        .type(PropertyType.SECRET)
        .addValidator(new SuperSecretValidator())
        .build();
    private static final ConnectorPropertyGroup SECRET_PROPERTY_GROUP = new ConnectorPropertyGroup.Builder()
        .name("Secret Property Group")
        .description("This property group is ignored by the NopConnector.")
        .properties(List.of(SECRET_PROPERTY))
        .build();

    private static final ConfigurationStep IGNORED_STEP = new ConfigurationStep.Builder()
        .name("Ignored Step")
        .propertyGroups(List.of(IGNORED_PROPERTY_GROUP, SECRET_PROPERTY_GROUP))
        .build();

    private final List<ConfigurationStep> configurationSteps = List.of(IGNORED_STEP);

    @Override
    protected void onStepConfigured(final String stepName, final FlowContext workingContext) {
    }

    @Override
    public VersionedExternalFlow getInitialFlow() {
        final VersionedProcessGroup group = new VersionedProcessGroup();
        group.setName("Nop Flow");

        final VersionedParameter someText = new VersionedParameter();
        someText.setName("SOME_TEXT");
        someText.setValue("Lorem ipsum");
        someText.setSensitive(false);
        someText.setProvided(false);
        someText.setReferencedAssets(List.of());

        final VersionedParameterContext parameterContext = new VersionedParameterContext();
        parameterContext.setName("Nop Parameter Context");
        parameterContext.setParameters(Set.of(someText));

        final VersionedExternalFlow flow = new VersionedExternalFlow();
        flow.setParameterContexts(Map.of(parameterContext.getName(), parameterContext));
        flow.setFlowContents(group);
        return flow;
    }

    @Override
    public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final Map<String, String> propertyValueOverrides, final FlowContext flowContext) {
        return List.of(new ConfigVerificationResult.Builder()
            .outcome(Outcome.SUCCESSFUL)
            .subject(stepName)
            .verificationStepName("Nop Verification")
            .explanation("Successful verification with properties: " + propertyValueOverrides)
            .build());
    }

    @Override
    public List<ConfigurationStep> getConfigurationSteps() {
        return configurationSteps;
    }

    @Override
    public void applyUpdate(final FlowContext workingFlowContext, final FlowContext activeFlowContext) {
    }

    private static class SuperSecretValidator implements Validator {
        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            return new ValidationResult.Builder()
                .subject(subject)
                .input(input)
                .explanation("Value must be 'supersecret'")
                .valid(input.equals("supersecret"))
                .build();
        }
    }
}
