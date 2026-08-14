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
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.Position;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.tests.system.VerifyClasspathResource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test connector that exercises additional classpath loading during configuration verification.
 * Supports two strategies: delegating to {@code ProcessorFacade.verify} with property overrides,
 * or invoking a {@code @ConnectorMethod} without calling processor verify.
 */
public class ClasspathVerifyConnector extends AbstractConnector {

    public static final String STEP_NAME = "Classpath Configuration";
    public static final String STRATEGY_PROCESSOR_VERIFY = "Processor Verify";
    public static final String STRATEGY_CONNECTOR_METHOD = "Connector Method";
    public static final String CONNECTOR_METHOD_STEP = "Invoke loadClass Connector Method";

    private static final String PROCESSOR_TYPE = "org.apache.nifi.processors.tests.system.VerifyClasspathResource";
    private static final String PROCESSOR_NAME = "Verify Classpath Resource";
    private static final Bundle SYSTEM_TEST_EXTENSIONS_BUNDLE = new Bundle("org.apache.nifi", "nifi-system-test-extensions-nar", "2.8.0-SNAPSHOT");

    public static final ConnectorPropertyDescriptor CLASSPATH_RESOURCE = new ConnectorPropertyDescriptor.Builder()
            .name("Classpath Resource")
            .description("An asset JAR to place on the processor classpath")
            .required(true)
            .type(PropertyType.ASSET)
            .build();

    public static final ConnectorPropertyDescriptor CLASS_TO_LOAD = new ConnectorPropertyDescriptor.Builder()
            .name("Class to Load")
            .description("Fully-qualified class name that must be loadable from the classpath resource")
            .required(true)
            .type(PropertyType.STRING)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final ConnectorPropertyDescriptor VERIFICATION_STRATEGY = new ConnectorPropertyDescriptor.Builder()
            .name("Verification Strategy")
            .description("Whether verification should call ProcessorFacade.verify with property overrides, or invoke the loadClass ConnectorMethod only")
            .required(true)
            .type(PropertyType.STRING)
            .defaultValue(STRATEGY_PROCESSOR_VERIFY)
            .allowableValues(STRATEGY_PROCESSOR_VERIFY, STRATEGY_CONNECTOR_METHOD)
            .build();

    private static final ConnectorPropertyGroup PROPERTY_GROUP = new ConnectorPropertyGroup.Builder()
            .name("Classpath Settings")
            .description("Classpath resource and verification strategy")
            .properties(List.of(CLASSPATH_RESOURCE, CLASS_TO_LOAD, VERIFICATION_STRATEGY))
            .build();

    private static final ConfigurationStep CONFIGURATION_STEP = new ConfigurationStep.Builder()
            .name(STEP_NAME)
            .description("Configure classpath verification")
            .propertyGroups(List.of(PROPERTY_GROUP))
            .build();

    @Override
    public List<ConfigurationStep> getConfigurationSteps() {
        return List.of(CONFIGURATION_STEP);
    }

    @Override
    public VersionedExternalFlow getInitialFlow() {
        return buildFlow(null, null);
    }

    @Override
    public VersionedExternalFlow getActiveFlow(final FlowContext activeFlowContext) {
        final ConnectorConfigurationContext configurationContext = activeFlowContext.getConfigurationContext();
        final String classpathResource = configurationContext.getProperty(STEP_NAME, CLASSPATH_RESOURCE.getName()).getValue();
        final String classToLoad = configurationContext.getProperty(STEP_NAME, CLASS_TO_LOAD.getName()).getValue();
        return buildFlow(classpathResource, classToLoad);
    }

    @Override
    protected void onStepConfigured(final String stepName, final FlowContext workingContext) throws FlowUpdateException {
        final ConnectorConfigurationContext configurationContext = workingContext.getConfigurationContext();
        final String classpathResource = configurationContext.getProperty(STEP_NAME, CLASSPATH_RESOURCE.getName()).getValue();
        final String classToLoad = configurationContext.getProperty(STEP_NAME, CLASS_TO_LOAD.getName()).getValue();
        getInitializationContext().updateFlow(workingContext, buildFlow(classpathResource, classToLoad), BundleCompatibility.RESOLVE_BUNDLE);
    }

    @Override
    public void applyUpdate(final FlowContext workingContext, final FlowContext activeContext) throws FlowUpdateException {
        final ConnectorConfigurationContext configurationContext = workingContext.getConfigurationContext();
        final String classpathResource = configurationContext.getProperty(STEP_NAME, CLASSPATH_RESOURCE.getName()).getValue();
        final String classToLoad = configurationContext.getProperty(STEP_NAME, CLASS_TO_LOAD.getName()).getValue();
        getInitializationContext().updateFlow(activeContext, buildFlow(classpathResource, classToLoad), BundleCompatibility.RESOLVE_BUNDLE);
    }

    @Override
    public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final Map<String, String> propertyValueOverrides, final FlowContext flowContext) {
        final ConnectorConfigurationContext configurationContext = flowContext.getConfigurationContext().createWithOverrides(stepName, propertyValueOverrides);
        final String classpathResource = configurationContext.getProperty(STEP_NAME, CLASSPATH_RESOURCE.getName()).getValue();
        final String classToLoad = configurationContext.getProperty(STEP_NAME, CLASS_TO_LOAD.getName()).getValue();
        final String strategy = configurationContext.getProperty(STEP_NAME, VERIFICATION_STRATEGY.getName()).getValue();

        final ProcessorFacade processorFacade = flowContext.getRootGroup().getProcessors().stream()
                .filter(processor -> processor.getDefinition().getType().endsWith("VerifyClasspathResource"))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("VerifyClasspathResource processor not found in flow"));

        if (STRATEGY_CONNECTOR_METHOD.equals(strategy)) {
            return invokeLoadClassMethod(processorFacade, classToLoad);
        }

        return verifyWithPropertyOverrides(processorFacade, classpathResource, classToLoad);
    }

    private List<ConfigVerificationResult> verifyWithPropertyOverrides(final ProcessorFacade processorFacade, final String classpathResource, final String classToLoad) {
        final Map<String, String> propertyOverrides = new HashMap<>();
        if (classpathResource != null) {
            propertyOverrides.put(VerifyClasspathResource.CLASSPATH_RESOURCE.getName(), classpathResource);
        }
        if (classToLoad != null) {
            propertyOverrides.put(VerifyClasspathResource.CLASS_TO_LOAD.getName(), classToLoad);
        }

        return processorFacade.verify(propertyOverrides, Map.of());
    }

    private List<ConfigVerificationResult> invokeLoadClassMethod(final ProcessorFacade processorFacade, final String classToLoad) {
        try {
            final String loadedClass = processorFacade.invokeConnectorMethod("loadClass", Map.of("className", classToLoad), String.class);
            return List.of(new ConfigVerificationResult.Builder()
                    .verificationStepName(CONNECTOR_METHOD_STEP)
                    .outcome(Outcome.SUCCESSFUL)
                    .explanation("Successfully loaded class " + loadedClass + " via ConnectorMethod")
                    .build());
        } catch (final InvocationFailedException e) {
            return List.of(new ConfigVerificationResult.Builder()
                    .verificationStepName(CONNECTOR_METHOD_STEP)
                    .outcome(Outcome.FAILED)
                    .explanation("Failed to load class " + classToLoad + " via ConnectorMethod: " + e.getMessage())
                    .build());
        }
    }

    private VersionedExternalFlow buildFlow(final String classpathResource, final String classToLoad) {
        final VersionedProcessGroup group = VersionedFlowUtils.createProcessGroup("classpath-verify-flow-id", "Classpath Verify Flow");
        final VersionedProcessor processor = VersionedFlowUtils.addProcessor(group, PROCESSOR_TYPE, SYSTEM_TEST_EXTENSIONS_BUNDLE, PROCESSOR_NAME, new Position(0, 0));

        if (classpathResource != null) {
            processor.getProperties().put(VerifyClasspathResource.CLASSPATH_RESOURCE.getName(), classpathResource);
        }
        if (classToLoad != null) {
            processor.getProperties().put(VerifyClasspathResource.CLASS_TO_LOAD.getName(), classToLoad);
        }

        final VersionedExternalFlow flow = new VersionedExternalFlow();
        flow.setFlowContents(group);
        flow.setParameterContexts(Map.of());
        return flow;
    }
}
