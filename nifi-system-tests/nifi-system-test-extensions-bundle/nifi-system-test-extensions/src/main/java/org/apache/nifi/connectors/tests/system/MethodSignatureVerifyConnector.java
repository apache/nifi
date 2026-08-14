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
import org.apache.nifi.components.connector.BundleCompatibility;
import org.apache.nifi.components.connector.ConfigurationStep;
import org.apache.nifi.components.connector.ConnectorConfigurationContext;
import org.apache.nifi.components.connector.ConnectorPropertyDescriptor;
import org.apache.nifi.components.connector.ConnectorPropertyGroup;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.InvocationFailedException;
import org.apache.nifi.components.connector.PropertyType;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.components.connector.components.ProcessorFacade;
import org.apache.nifi.components.connector.util.VersionedFlowUtils;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.processors.tests.system.VerifyMethodSignatureResource;

import java.util.List;
import java.util.Map;

/**
 * Test connector that reproduces the additional-classpath method-discovery scenario. Its flow contains a single
 * {@link VerifyMethodSignatureResource} whose declared methods include a private method returning a type available only
 * through the configured asset. Invoking the unrelated, constant-returning {@code returnConstant} ConnectorMethod forces
 * the framework to resolve every declared method signature, so the asset must be present on the classpath even though
 * the invoked method itself does not reference the dynamically loaded type.
 */
public class MethodSignatureVerifyConnector extends AbstractConnector {

    public static final String STEP_NAME = "Method Signature Configuration";
    public static final String METHOD_SIGNATURE_STEP = "Discover Connector Method With Dynamic Signature";

    private static final String FLOW_JSON_PATH = "flows/method-signature-verify-connector.json";
    private static final String CLASSPATH_PARAMETER_NAME = "Classpath Resource";

    public static final ConnectorPropertyDescriptor CLASSPATH_RESOURCE = new ConnectorPropertyDescriptor.Builder()
            .name("Classpath Resource")
            .description("An asset JAR to place on the processor classpath")
            .required(true)
            .type(PropertyType.ASSET)
            .build();

    private static final ConnectorPropertyGroup PROPERTY_GROUP = new ConnectorPropertyGroup.Builder()
            .name("Classpath Settings")
            .description("Classpath resource providing the dynamically loaded method-signature type")
            .properties(List.of(CLASSPATH_RESOURCE))
            .build();

    private static final ConfigurationStep CONFIGURATION_STEP = new ConfigurationStep.Builder()
            .name(STEP_NAME)
            .description("Configure the classpath resource used for method-signature discovery")
            .propertyGroups(List.of(PROPERTY_GROUP))
            .build();

    @Override
    public List<ConfigurationStep> getConfigurationSteps() {
        return List.of(CONFIGURATION_STEP);
    }

    @Override
    public VersionedExternalFlow getInitialFlow() {
        return buildFlow(null);
    }

    @Override
    public VersionedExternalFlow getActiveFlow(final FlowContext activeFlowContext) {
        final ConnectorConfigurationContext configurationContext = activeFlowContext.getConfigurationContext();
        final String classpathResource = configurationContext.getProperty(STEP_NAME, CLASSPATH_RESOURCE.getName()).getValue();
        return buildFlow(classpathResource);
    }

    @Override
    protected void onStepConfigured(final String stepName, final FlowContext workingContext) throws FlowUpdateException {
        final ConnectorConfigurationContext configurationContext = workingContext.getConfigurationContext();
        final String classpathResource = configurationContext.getProperty(STEP_NAME, CLASSPATH_RESOURCE.getName()).getValue();
        getInitializationContext().updateFlow(workingContext, buildFlow(classpathResource), BundleCompatibility.RESOLVE_BUNDLE);
    }

    @Override
    public void applyUpdate(final FlowContext workingContext, final FlowContext activeContext) throws FlowUpdateException {
        final ConnectorConfigurationContext configurationContext = workingContext.getConfigurationContext();
        final String classpathResource = configurationContext.getProperty(STEP_NAME, CLASSPATH_RESOURCE.getName()).getValue();
        getInitializationContext().updateFlow(activeContext, buildFlow(classpathResource), BundleCompatibility.RESOLVE_BUNDLE);
    }

    @Override
    public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final Map<String, String> propertyValueOverrides, final FlowContext flowContext) {
        final ProcessorFacade processorFacade = flowContext.getRootGroup().getProcessors().stream()
                .filter(processor -> processor.getDefinition().getType().endsWith("VerifyMethodSignatureResource"))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("VerifyMethodSignatureResource processor not found in flow"));

        try {
            final String result = processorFacade.invokeConnectorMethod("returnConstant", Map.of(), String.class);
            return List.of(new ConfigVerificationResult.Builder()
                    .verificationStepName(METHOD_SIGNATURE_STEP)
                    .outcome(Outcome.SUCCESSFUL)
                    .explanation("Successfully invoked ConnectorMethod returning " + result)
                    .build());
        } catch (final InvocationFailedException | NoClassDefFoundError e) {
            return List.of(new ConfigVerificationResult.Builder()
                    .verificationStepName(METHOD_SIGNATURE_STEP)
                    .outcome(Outcome.FAILED)
                    .explanation("Failed to discover ConnectorMethod because a declared method signature type was unavailable: " + e)
                    .build());
        }
    }

    private VersionedExternalFlow buildFlow(final String classpathResource) {
        final VersionedExternalFlow flow = VersionedFlowUtils.loadFlowFromResource(FLOW_JSON_PATH);
        if (classpathResource != null) {
            VersionedFlowUtils.setParameterValues(flow, Map.of(CLASSPATH_PARAMETER_NAME, classpathResource));
        }
        return flow;
    }
}
