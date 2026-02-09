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
import org.apache.nifi.components.connector.AbstractConnector;
import org.apache.nifi.components.connector.BundleCompatibility;
import org.apache.nifi.components.connector.ConfigurationStep;
import org.apache.nifi.components.connector.ConnectorPropertyDescriptor;
import org.apache.nifi.components.connector.ConnectorPropertyGroup;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.PropertyType;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.components.connector.util.VersionedFlowUtils;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.Position;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedPort;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A test connector that creates a flow with nested Process Groups and Parameter Context inheritance.
 * Used to verify that sensitive parameters and asset-referencing parameters work correctly
 * when inherited from child Parameter Contexts.
 *
 * The flow structure:
 * - Root Group (bound to Parent Parameter Context)
 *   - GenerateFlowFile (generates 1 FlowFile)
 *   - Process Group A (bound to Parent Parameter Context)
 *     - Input Port
 *     - UpdateContent (Sensitive Content = #{sensitive_param})
 *     - WriteToFile (target/sensitive.txt)
 *   - Process Group B (bound to Parent Parameter Context)
 *     - Input Port
 *     - ReplaceWithFile (Filename = #{asset_param})
 *     - WriteToFile (target/asset.txt)
 *
 * Parameter Context hierarchy:
 * - Parent Parameter Context (inherits from Child Context A and Child Context B)
 *   - Child Context A: sensitive_param (sensitive)
 *   - Child Context B: asset_param (asset reference)
 */
public class ParameterContextConnector extends AbstractConnector {

    private static final String CONFIGURATION_STEP_NAME = "Parameter Context Configuration";

    private static final String ROOT_GROUP_ID = "root-group";
    private static final String GROUP_A_ID = "process-group-a";
    private static final String GROUP_B_ID = "process-group-b";

    private static final String PARENT_CONTEXT_NAME = "Parent Parameter Context";
    private static final String CHILD_CONTEXT_A_NAME = "Child Context A";
    private static final String CHILD_CONTEXT_B_NAME = "Child Context B";

    private static final String SENSITIVE_PARAM_NAME = "sensitive_param";
    private static final String ASSET_PARAM_NAME = "asset_param";

    static final ConnectorPropertyDescriptor SENSITIVE_VALUE = new ConnectorPropertyDescriptor.Builder()
            .name("Sensitive Value")
            .description("The sensitive value to be stored in the sensitive parameter")
            .required(true)
            .type(PropertyType.SECRET)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final ConnectorPropertyDescriptor ASSET_FILE = new ConnectorPropertyDescriptor.Builder()
            .name("Asset File")
            .description("The asset file whose contents will be used via the asset parameter")
            .required(true)
            .type(PropertyType.ASSET)
            .build();

    static final ConnectorPropertyDescriptor SENSITIVE_OUTPUT_FILE = new ConnectorPropertyDescriptor.Builder()
            .name("Sensitive Output File")
            .description("The file path where the sensitive value output will be written")
            .required(true)
            .type(PropertyType.STRING)
            .defaultValue("target/sensitive.txt")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final ConnectorPropertyDescriptor ASSET_OUTPUT_FILE = new ConnectorPropertyDescriptor.Builder()
            .name("Asset Output File")
            .description("The file path where the asset contents output will be written")
            .required(true)
            .type(PropertyType.STRING)
            .defaultValue("target/asset.txt")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final ConnectorPropertyGroup PROPERTY_GROUP = new ConnectorPropertyGroup.Builder()
            .name("Parameter Context Configuration")
            .description("Configuration properties for parameter context testing")
            .properties(List.of(SENSITIVE_VALUE, ASSET_FILE, SENSITIVE_OUTPUT_FILE, ASSET_OUTPUT_FILE))
            .build();

    private static final ConfigurationStep CONFIG_STEP = new ConfigurationStep.Builder()
            .name(CONFIGURATION_STEP_NAME)
            .description("Configure the sensitive value and asset file for parameter context testing")
            .propertyGroups(List.of(PROPERTY_GROUP))
            .build();

    @Override
    protected void onStepConfigured(final String stepName, final FlowContext workingContext) throws FlowUpdateException {
    }

    @Override
    public VersionedExternalFlow getInitialFlow() {
        return createEmptyFlow();
    }

    private VersionedExternalFlow createEmptyFlow() {
        final VersionedProcessGroup rootGroup = VersionedFlowUtils.createProcessGroup(ROOT_GROUP_ID, "Parameter Context Test Flow");

        final VersionedExternalFlow flow = new VersionedExternalFlow();
        flow.setFlowContents(rootGroup);
        flow.setParameterContexts(Collections.emptyMap());
        return flow;
    }

    @Override
    public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final Map<String, String> propertyValueOverrides, final FlowContext flowContext) {
        return List.of();
    }

    @Override
    public List<ConfigurationStep> getConfigurationSteps() {
        return List.of(CONFIG_STEP);
    }

    @Override
    public void applyUpdate(final FlowContext workingContext, final FlowContext activeContext) throws FlowUpdateException {
        final String sensitiveValue = workingContext.getConfigurationContext().getProperty(CONFIG_STEP, SENSITIVE_VALUE).getValue();
        final String assetFilePath = workingContext.getConfigurationContext().getProperty(CONFIG_STEP, ASSET_FILE).getValue();
        final String sensitiveOutputFile = workingContext.getConfigurationContext().getProperty(CONFIG_STEP, SENSITIVE_OUTPUT_FILE).getValue();
        final String assetOutputFile = workingContext.getConfigurationContext().getProperty(CONFIG_STEP, ASSET_OUTPUT_FILE).getValue();

        if (sensitiveValue == null || assetFilePath == null) {
            return;
        }

        final VersionedExternalFlow flow = createFlow(sensitiveValue, assetFilePath, sensitiveOutputFile, assetOutputFile);
        getInitializationContext().updateFlow(activeContext, flow, BundleCompatibility.RESOLVE_BUNDLE);
    }

    private VersionedExternalFlow createFlow(final String sensitiveValue, final String assetFilePath,
                                              final String sensitiveOutputFile, final String assetOutputFile) {
        final Bundle bundle = new Bundle("org.apache.nifi", "nifi-system-test-extensions-nar", "2.8.0-SNAPSHOT");
        final Map<String, VersionedParameterContext> parameterContexts = createParameterContexts(sensitiveValue, assetFilePath);

        final VersionedProcessGroup rootGroup = VersionedFlowUtils.createProcessGroup(ROOT_GROUP_ID, "Parameter Context Test Flow");
        rootGroup.setParameterContextName(PARENT_CONTEXT_NAME);

        final VersionedProcessor generateProcessor = VersionedFlowUtils.addProcessor(rootGroup,
                "org.apache.nifi.processors.tests.system.GenerateFlowFile", bundle, "GenerateFlowFile", new Position(0, 0));
        generateProcessor.getProperties().putAll(Map.of("Max FlowFiles", "1", "File Size", "0 B"));
        generateProcessor.setSchedulingPeriod("60 sec");

        final VersionedProcessGroup groupA = createProcessGroupA(bundle, sensitiveOutputFile);
        rootGroup.getProcessGroups().add(groupA);

        final VersionedProcessGroup groupB = createProcessGroupB(bundle, assetOutputFile);
        rootGroup.getProcessGroups().add(groupB);

        final VersionedPort inputPortA = groupA.getInputPorts().iterator().next();
        final VersionedPort inputPortB = groupB.getInputPorts().iterator().next();

        VersionedFlowUtils.addConnection(rootGroup, VersionedFlowUtils.createConnectableComponent(generateProcessor),
                VersionedFlowUtils.createConnectableComponent(inputPortA), Set.of("success"));
        VersionedFlowUtils.addConnection(rootGroup, VersionedFlowUtils.createConnectableComponent(generateProcessor),
                VersionedFlowUtils.createConnectableComponent(inputPortB), Set.of("success"));

        final VersionedExternalFlow flow = new VersionedExternalFlow();
        flow.setFlowContents(rootGroup);
        flow.setParameterContexts(parameterContexts);
        return flow;
    }

    private Map<String, VersionedParameterContext> createParameterContexts(final String sensitiveValue, final String assetFilePath) {
        final VersionedParameter sensitiveParam = new VersionedParameter();
        sensitiveParam.setName(SENSITIVE_PARAM_NAME);
        sensitiveParam.setSensitive(true);
        sensitiveParam.setValue(sensitiveValue);
        sensitiveParam.setProvided(false);
        sensitiveParam.setReferencedAssets(List.of());

        final VersionedParameterContext childContextA = new VersionedParameterContext();
        childContextA.setName(CHILD_CONTEXT_A_NAME);
        childContextA.setParameters(Set.of(sensitiveParam));

        final VersionedParameter assetParam = new VersionedParameter();
        assetParam.setName(ASSET_PARAM_NAME);
        assetParam.setSensitive(false);
        assetParam.setValue(assetFilePath);
        assetParam.setProvided(false);
        assetParam.setReferencedAssets(List.of());

        final VersionedParameterContext childContextB = new VersionedParameterContext();
        childContextB.setName(CHILD_CONTEXT_B_NAME);
        childContextB.setParameters(Set.of(assetParam));

        final VersionedParameterContext parentContext = new VersionedParameterContext();
        parentContext.setName(PARENT_CONTEXT_NAME);
        parentContext.setParameters(Set.of());
        parentContext.setInheritedParameterContexts(List.of(CHILD_CONTEXT_A_NAME, CHILD_CONTEXT_B_NAME));

        final Map<String, VersionedParameterContext> contexts = new HashMap<>();
        contexts.put(CHILD_CONTEXT_A_NAME, childContextA);
        contexts.put(CHILD_CONTEXT_B_NAME, childContextB);
        contexts.put(PARENT_CONTEXT_NAME, parentContext);
        return contexts;
    }

    private VersionedProcessGroup createProcessGroupA(final Bundle bundle, final String outputFile) {
        final VersionedProcessGroup groupA = VersionedFlowUtils.createProcessGroup(GROUP_A_ID, "Process Group A - Sensitive Value");
        groupA.setGroupIdentifier(ROOT_GROUP_ID);
        groupA.setParameterContextName(PARENT_CONTEXT_NAME);

        final VersionedPort inputPortA = VersionedFlowUtils.addInputPort(groupA, "Input Port A", new Position(0, 0));

        final VersionedProcessor updateContent = VersionedFlowUtils.addProcessor(groupA,
                "org.apache.nifi.processors.tests.system.UpdateContent", bundle, "UpdateContent", new Position(0, 0));
        updateContent.getProperties().putAll(Map.of("Sensitive Content", "#{" + SENSITIVE_PARAM_NAME + "}", "Update Strategy", "Replace"));

        final VersionedProcessor writeToFile = VersionedFlowUtils.addProcessor(groupA,
                "org.apache.nifi.processors.tests.system.WriteToFile", bundle, "WriteToFile", new Position(0, 0));
        writeToFile.getProperties().put("Filename", outputFile);
        writeToFile.setAutoTerminatedRelationships(Set.of("success", "failure"));

        VersionedFlowUtils.addConnection(groupA, VersionedFlowUtils.createConnectableComponent(inputPortA),
                VersionedFlowUtils.createConnectableComponent(updateContent), Set.of());
        VersionedFlowUtils.addConnection(groupA, VersionedFlowUtils.createConnectableComponent(updateContent),
                VersionedFlowUtils.createConnectableComponent(writeToFile), Set.of("success"));

        return groupA;
    }

    private VersionedProcessGroup createProcessGroupB(final Bundle bundle, final String outputFile) {
        final VersionedProcessGroup groupB = VersionedFlowUtils.createProcessGroup(GROUP_B_ID, "Process Group B - Asset Value");
        groupB.setGroupIdentifier(ROOT_GROUP_ID);
        groupB.setParameterContextName(PARENT_CONTEXT_NAME);

        final VersionedPort inputPortB = VersionedFlowUtils.addInputPort(groupB, "Input Port B", new Position(0, 0));

        final VersionedProcessor replaceWithFile = VersionedFlowUtils.addProcessor(groupB,
                "org.apache.nifi.processors.tests.system.ReplaceWithFile", bundle, "ReplaceWithFile", new Position(0, 0));
        replaceWithFile.getProperties().put("Filename", "#{" + ASSET_PARAM_NAME + "}");

        final VersionedProcessor writeToFile = VersionedFlowUtils.addProcessor(groupB,
                "org.apache.nifi.processors.tests.system.WriteToFile", bundle, "WriteToFile", new Position(0, 0));
        writeToFile.getProperties().put("Filename", outputFile);
        writeToFile.setAutoTerminatedRelationships(Set.of("success", "failure"));

        VersionedFlowUtils.addConnection(groupB, VersionedFlowUtils.createConnectableComponent(inputPortB),
                VersionedFlowUtils.createConnectableComponent(replaceWithFile), Set.of());
        VersionedFlowUtils.addConnection(groupB, VersionedFlowUtils.createConnectableComponent(replaceWithFile),
                VersionedFlowUtils.createConnectableComponent(writeToFile), Set.of("success"));

        return groupB;
    }
}
