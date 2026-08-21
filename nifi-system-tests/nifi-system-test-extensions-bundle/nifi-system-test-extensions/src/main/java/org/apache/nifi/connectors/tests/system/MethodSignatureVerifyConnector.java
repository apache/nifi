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
import org.apache.nifi.components.connector.components.ControllerServiceFacade;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.components.connector.components.ProcessorFacade;
import org.apache.nifi.components.connector.util.VersionedFlowUtils;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.Position;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.processors.tests.system.VerifyMethodSignatureResource;

import java.util.List;
import java.util.Map;

/**
 * Test connector that exercises additional-classpath method discovery. The Working flow contains either a
 * processor or a Controller Service whose declared methods include a private method returning a type available only
 * through the configured asset. Invoking the unrelated, constant-returning {@code returnConstant} Connector Method
 * forces the framework to resolve every declared method signature, so the asset must be present on the component
 * classpath even though the invoked method itself does not reference the dynamically loaded type.
 */
public class MethodSignatureVerifyConnector extends AbstractConnector {

    public static final String STEP_NAME = "Method Signature Configuration";
    public static final String APPLICATION_PARAMETER = "Parameter";
    public static final String APPLICATION_PROCESSOR_PROPERTY = "Processor Property";
    public static final String APPLICATION_CONTROLLER_SERVICE_PROPERTY = "Controller Service Property";
    public static final String METHOD_SIGNATURE_STEP = "Discover Connector Method With Dynamic Signature";

    public static final ConnectorPropertyDescriptor CLASSPATH_RESOURCE = new ConnectorPropertyDescriptor.Builder()
            .name("Classpath Resource")
            .description("An asset JAR to place on the component classpath")
            .required(true)
            .type(PropertyType.ASSET)
            .build();

    public static final ConnectorPropertyDescriptor CLASSPATH_APPLICATION = new ConnectorPropertyDescriptor.Builder()
            .name("Classpath Application")
            .description("Whether the classpath asset is applied through a Parameter, a Processor property, or a Controller Service property")
            .required(true)
            .type(PropertyType.STRING)
            .defaultValue(APPLICATION_PARAMETER)
            .allowableValues(APPLICATION_PARAMETER, APPLICATION_PROCESSOR_PROPERTY, APPLICATION_CONTROLLER_SERVICE_PROPERTY)
            .build();

    private static final String FLOW_JSON_PATH = "flows/method-signature-verify-connector.json";
    private static final String CLASSPATH_PARAMETER_NAME = "Classpath Resource";
    private static final String PROCESSOR_TYPE = "org.apache.nifi.processors.tests.system.VerifyMethodSignatureResource";
    private static final String PROCESSOR_NAME = "Verify Method Signature Resource";
    private static final String CONTROLLER_SERVICE_TYPE = "org.apache.nifi.cs.tests.system.VerifyMethodSignatureService";
    private static final String CONTROLLER_SERVICE_NAME = "Verify Method Signature Service";
    private static final String DYNAMIC_CLASSPATH_CLASS = "org.apache.nifi.tests.system.dynamicclasspath.DynamicallyLoadedType";
    private static final Bundle SYSTEM_TEST_EXTENSIONS_BUNDLE = new Bundle("org.apache.nifi", "nifi-system-test-extensions-nar", "2.8.0-SNAPSHOT");
    private static final Bundle SYSTEM_TEST_EXTENSIONS_SERVICES_BUNDLE = new Bundle("org.apache.nifi", "nifi-system-test-extensions-services-nar", "2.8.0-SNAPSHOT");

    private static final ConnectorPropertyGroup PROPERTY_GROUP = new ConnectorPropertyGroup.Builder()
            .name("Classpath Settings")
            .description("Classpath resource providing the dynamically loaded method-signature type")
            .properties(List.of(CLASSPATH_RESOURCE, CLASSPATH_APPLICATION))
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
        return buildFlow(null, APPLICATION_PARAMETER);
    }

    @Override
    public VersionedExternalFlow getActiveFlow(final FlowContext activeFlowContext) {
        return buildFlowFromContext(activeFlowContext);
    }

    @Override
    protected void onStepConfigured(final String stepName, final FlowContext workingContext) throws FlowUpdateException {
        getInitializationContext().updateFlow(workingContext, buildFlowFromContext(workingContext), BundleCompatibility.RESOLVE_BUNDLE);
    }

    @Override
    public void applyUpdate(final FlowContext workingContext, final FlowContext activeContext) throws FlowUpdateException {
        getInitializationContext().updateFlow(activeContext, buildFlowFromContext(workingContext), BundleCompatibility.RESOLVE_BUNDLE);
    }

    @Override
    public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final Map<String, String> propertyValueOverrides, final FlowContext flowContext) {
        final ConnectorConfigurationContext configurationContext = flowContext.getConfigurationContext().createWithOverrides(stepName, propertyValueOverrides);
        final String classpathApplication = configurationContext.getProperty(STEP_NAME, CLASSPATH_APPLICATION.getName()).getValue();

        try {
            final String result;
            if (APPLICATION_CONTROLLER_SERVICE_PROPERTY.equals(classpathApplication)) {
                result = findControllerService(flowContext).invokeConnectorMethod("loadClass", Map.of("className", DYNAMIC_CLASSPATH_CLASS), String.class);
            } else {
                result = findProcessor(flowContext).invokeConnectorMethod("returnConstant", Map.of(), String.class);
            }

            return List.of(new ConfigVerificationResult.Builder()
                    .verificationStepName(METHOD_SIGNATURE_STEP)
                    .outcome(Outcome.SUCCESSFUL)
                    .explanation("Successfully invoked ConnectorMethod returning " + result)
                    .build());
        } catch (final InvocationFailedException e) {
            return List.of(new ConfigVerificationResult.Builder()
                    .verificationStepName(METHOD_SIGNATURE_STEP)
                    .outcome(Outcome.FAILED)
                    .explanation("Failed to discover ConnectorMethod because a declared method signature type was unavailable: " + e)
                    .build());
        }
    }

    private VersionedExternalFlow buildFlowFromContext(final FlowContext flowContext) {
        final ConnectorConfigurationContext configurationContext = flowContext.getConfigurationContext();
        final String classpathResource = configurationContext.getProperty(STEP_NAME, CLASSPATH_RESOURCE.getName()).getValue();
        final String classpathApplication = configurationContext.getProperty(STEP_NAME, CLASSPATH_APPLICATION.getName()).getValue();
        return buildFlow(classpathResource, classpathApplication);
    }

    private VersionedExternalFlow buildFlow(final String classpathResource, final String classpathApplication) {
        if (APPLICATION_PROCESSOR_PROPERTY.equals(classpathApplication)) {
            return buildProcessorPropertyFlow(classpathResource);
        }

        if (APPLICATION_CONTROLLER_SERVICE_PROPERTY.equals(classpathApplication)) {
            return buildControllerServicePropertyFlow(classpathResource);
        }

        return buildParameterFlow(classpathResource);
    }

    private VersionedExternalFlow buildParameterFlow(final String classpathResource) {
        final VersionedExternalFlow flow = VersionedFlowUtils.loadFlowFromResource(FLOW_JSON_PATH);
        if (classpathResource != null) {
            VersionedFlowUtils.setParameterValues(flow, Map.of(CLASSPATH_PARAMETER_NAME, classpathResource));
        }

        return flow;
    }

    private VersionedExternalFlow buildProcessorPropertyFlow(final String classpathResource) {
        final VersionedProcessGroup group = VersionedFlowUtils.createProcessGroup("method-signature-verify-flow-id", "Method Signature Verify Flow");
        final VersionedProcessor processor = VersionedFlowUtils.addProcessor(group, PROCESSOR_TYPE, SYSTEM_TEST_EXTENSIONS_BUNDLE, PROCESSOR_NAME, new Position(0, 0));
        if (classpathResource != null) {
            processor.getProperties().put(VerifyMethodSignatureResource.CLASSPATH_RESOURCE.getName(), classpathResource);
        }

        final VersionedExternalFlow flow = new VersionedExternalFlow();
        flow.setFlowContents(group);
        flow.setParameterContexts(Map.of());
        return flow;
    }

    private VersionedExternalFlow buildControllerServicePropertyFlow(final String classpathResource) {
        final VersionedProcessGroup group = VersionedFlowUtils.createProcessGroup("method-signature-verify-flow-id", "Method Signature Verify Flow");
        final VersionedControllerService controllerService = VersionedFlowUtils.addControllerService(group, CONTROLLER_SERVICE_TYPE, SYSTEM_TEST_EXTENSIONS_SERVICES_BUNDLE, CONTROLLER_SERVICE_NAME);
        if (classpathResource != null) {
            controllerService.getProperties().put(CLASSPATH_PARAMETER_NAME, classpathResource);
        }

        final VersionedExternalFlow flow = new VersionedExternalFlow();
        flow.setFlowContents(group);
        flow.setParameterContexts(Map.of());
        return flow;
    }

    private ProcessorFacade findProcessor(final FlowContext flowContext) {
        return flowContext.getRootGroup().getProcessors().stream()
                .filter(processor -> processor.getDefinition().getType().endsWith("VerifyMethodSignatureResource"))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("VerifyMethodSignatureResource processor not found in flow"));
    }

    private ControllerServiceFacade findControllerService(final FlowContext flowContext) {
        return flowContext.getRootGroup().getControllerServices().stream()
                .filter(controllerService -> controllerService.getDefinition().getType().endsWith("VerifyMethodSignatureService"))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("VerifyMethodSignatureService controller service not found in flow"));
    }
}
