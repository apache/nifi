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

package org.apache.nifi.components.connector;

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.components.connector.components.ProcessorFacade;
import org.apache.nifi.components.connector.util.VersionedFlowUtils;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DynamicAllowableValuesConnector extends AbstractConnector {
    static final ConnectorPropertyDescriptor FILE_PATH = new ConnectorPropertyDescriptor.Builder()
        .name("File Path")
        .description("The path to the file")
        .addValidator(new SimpleFileExistsValidator())
        .required(true)
        .build();

    static final ConnectorPropertyGroup FILE_PROPERTY_GROUP = new ConnectorPropertyGroup.Builder()
        .name("")
        .addProperty(FILE_PATH)
        .build();

    static final ConfigurationStep FILE_STEP = new ConfigurationStep.Builder()
        .name("File")
        .propertyGroups(List.of(FILE_PROPERTY_GROUP))
        .build();


    @Override
    public VersionedExternalFlow getInitialFlow() {
        return null;
    }

    @Override
    public List<ConfigurationStep> getConfigurationSteps(final FlowContext flowContext) {
        final List<ConfigurationStep> steps = new ArrayList<>();
        steps.add(FILE_STEP);

        final ConnectorConfigurationContext configContext = flowContext.getConfigurationContext();

        if (configContext.getProperty(FILE_STEP, FILE_PROPERTY_GROUP, FILE_PATH) != null) {
            final Set<ProcessorFacade> processorsFacades = flowContext.getRootGroup().getProcessors();
            if (processorsFacades.isEmpty()) {
                return steps;
            }

            final ProcessorFacade processorFacade = processorsFacades.iterator().next();
            try {
                final List<String> fileValues = (List<String>) processorFacade.invokeConnectorMethod("getFileValues", Map.of());
                steps.add(createColorConfigurationStep(fileValues));
            } catch (final InvocationFailedException ignored) {
                // If we can't get file values, don't add the step.
            }
        }

        return steps;
    }

    private ConfigurationStep createColorConfigurationStep(final List<String> values) {
        final ConnectorPropertyDescriptor FIRST_PRIMARY_COLOR = new ConnectorPropertyDescriptor.Builder()
            .name("First Primary Color")
            .description("The first primary color")
            .defaultValue(values.getFirst())
            .allowableValues(values.toArray(new String[0]))
            .required(true)
            .build();

        final ConnectorPropertyGroup PRIMARY_COLORS_PROPERTY_GROUP = new ConnectorPropertyGroup.Builder()
            .name("Primary Colors")
            .addProperty(FIRST_PRIMARY_COLOR)
            .build();

        return new ConfigurationStep.Builder()
            .name("Colors")
            .propertyGroups(List.of(PRIMARY_COLORS_PROPERTY_GROUP))
            .build();
    }

    @Override
    protected void onStepConfigured(final String stepName, final FlowContext flowContext) {
    }

    @Override
    public void applyUpdate(final FlowContext workingContext, final FlowContext activeContext) throws FlowUpdateException {
        final VersionedExternalFlow externalFlow = VersionedFlowUtils.loadFlowFromResource("flows/choose-color.json");
        final VersionedProcessGroup rootGroup = externalFlow.getFlowContents();
        final VersionedProcessor processor = rootGroup.getProcessors().iterator().next();
        processor.setProperties(Map.of("File", workingContext.getConfigurationContext().getProperty(FILE_STEP, FILE_PROPERTY_GROUP, FILE_PATH).getValue()));

        getInitializationContext().updateFlow(activeContext, externalFlow);
    }

    @Override
    public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final List<PropertyGroupConfiguration> overrides, final FlowContext flowContext) {
        return List.of();
    }


    public static class SimpleFileExistsValidator implements Validator {
        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext validationContext) {
            final File file = new File(input);
            if (file.exists()) {
                return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
            }

            return new ValidationResult.Builder().subject(subject).input(input).valid(false).explanation("File does not exist.").build();
        }
    }
}
