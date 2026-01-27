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
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.ConnectableComponent;
import org.apache.nifi.flow.ConnectableComponentType;
import org.apache.nifi.flow.PortType;
import org.apache.nifi.flow.Position;
import org.apache.nifi.flow.ScheduledState;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedPort;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
    private static final String GENERATE_PROCESSOR_ID = "generate-flowfile";
    private static final String GROUP_A_ID = "process-group-a";
    private static final String GROUP_B_ID = "process-group-b";
    private static final String INPUT_PORT_A_ID = "input-port-a";
    private static final String INPUT_PORT_B_ID = "input-port-b";
    private static final String UPDATE_CONTENT_ID = "update-content";
    private static final String REPLACE_WITH_FILE_ID = "replace-with-file";
    private static final String WRITE_SENSITIVE_ID = "write-sensitive";
    private static final String WRITE_ASSET_ID = "write-asset";

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
        final VersionedProcessGroup rootGroup = new VersionedProcessGroup();
        rootGroup.setIdentifier(ROOT_GROUP_ID);
        rootGroup.setName("Parameter Context Test Flow");
        rootGroup.setProcessors(new HashSet<>());
        rootGroup.setProcessGroups(new HashSet<>());
        rootGroup.setConnections(new HashSet<>());
        rootGroup.setInputPorts(new HashSet<>());
        rootGroup.setOutputPorts(new HashSet<>());
        rootGroup.setControllerServices(new HashSet<>());

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
        final Bundle bundle = createBundle();

        // Create Parameter Contexts with inheritance
        final Map<String, VersionedParameterContext> parameterContexts = createParameterContexts(sensitiveValue, assetFilePath);

        // Create root group
        final VersionedProcessGroup rootGroup = new VersionedProcessGroup();
        rootGroup.setIdentifier(ROOT_GROUP_ID);
        rootGroup.setName("Parameter Context Test Flow");
        rootGroup.setParameterContextName(PARENT_CONTEXT_NAME);

        // Create GenerateFlowFile at root level
        final VersionedProcessor generateProcessor = createProcessor(GENERATE_PROCESSOR_ID, ROOT_GROUP_ID, "GenerateFlowFile",
                "org.apache.nifi.processors.tests.system.GenerateFlowFile", bundle,
                Map.of("Max FlowFiles", "1", "File Size", "0 B"), ScheduledState.ENABLED);
        generateProcessor.setSchedulingPeriod("60 sec");

        // Create Process Group A (sensitive value path)
        final VersionedProcessGroup groupA = createProcessGroupA(bundle, sensitiveOutputFile);

        // Create Process Group B (asset path)
        final VersionedProcessGroup groupB = createProcessGroupB(bundle, assetOutputFile);

        // Create connections from GenerateFlowFile to both process group input ports
        final VersionedConnection connectionToA = createConnection("conn-to-group-a", ROOT_GROUP_ID,
                GENERATE_PROCESSOR_ID, ConnectableComponentType.PROCESSOR,
                INPUT_PORT_A_ID, ConnectableComponentType.INPUT_PORT, GROUP_A_ID,
                Set.of("success"));

        final VersionedConnection connectionToB = createConnection("conn-to-group-b", ROOT_GROUP_ID,
                GENERATE_PROCESSOR_ID, ConnectableComponentType.PROCESSOR,
                INPUT_PORT_B_ID, ConnectableComponentType.INPUT_PORT, GROUP_B_ID,
                Set.of("success"));

        rootGroup.setProcessors(Set.of(generateProcessor));
        rootGroup.setProcessGroups(Set.of(groupA, groupB));
        rootGroup.setConnections(Set.of(connectionToA, connectionToB));
        rootGroup.setInputPorts(new HashSet<>());
        rootGroup.setOutputPorts(new HashSet<>());
        rootGroup.setControllerServices(new HashSet<>());

        final VersionedExternalFlow flow = new VersionedExternalFlow();
        flow.setFlowContents(rootGroup);
        flow.setParameterContexts(parameterContexts);
        return flow;
    }

    private Map<String, VersionedParameterContext> createParameterContexts(final String sensitiveValue, final String assetFilePath) {
        // Child Context A - sensitive parameter
        final VersionedParameter sensitiveParam = new VersionedParameter();
        sensitiveParam.setName(SENSITIVE_PARAM_NAME);
        sensitiveParam.setSensitive(true);
        sensitiveParam.setValue(sensitiveValue);
        sensitiveParam.setProvided(false);
        sensitiveParam.setReferencedAssets(List.of());

        final VersionedParameterContext childContextA = new VersionedParameterContext();
        childContextA.setName(CHILD_CONTEXT_A_NAME);
        childContextA.setParameters(Set.of(sensitiveParam));

        // Child Context B - asset parameter
        final VersionedParameter assetParam = new VersionedParameter();
        assetParam.setName(ASSET_PARAM_NAME);
        assetParam.setSensitive(false);
        assetParam.setValue(assetFilePath);
        assetParam.setProvided(false);
        assetParam.setReferencedAssets(List.of());

        final VersionedParameterContext childContextB = new VersionedParameterContext();
        childContextB.setName(CHILD_CONTEXT_B_NAME);
        childContextB.setParameters(Set.of(assetParam));

        // Parent Context - inherits from both child contexts
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
        final VersionedProcessGroup groupA = new VersionedProcessGroup();
        groupA.setIdentifier(GROUP_A_ID);
        groupA.setGroupIdentifier(ROOT_GROUP_ID);
        groupA.setName("Process Group A - Sensitive Value");
        groupA.setParameterContextName(PARENT_CONTEXT_NAME);

        // Input Port
        final VersionedPort inputPortA = createInputPort(INPUT_PORT_A_ID, GROUP_A_ID, "Input Port A");

        // UpdateContent processor using sensitive parameter
        final VersionedProcessor updateContent = createProcessor(UPDATE_CONTENT_ID, GROUP_A_ID, "UpdateContent",
                "org.apache.nifi.processors.tests.system.UpdateContent", bundle,
                Map.of("Sensitive Content", "#{" + SENSITIVE_PARAM_NAME + "}", "Update Strategy", "Replace"),
                ScheduledState.ENABLED);

        // WriteToFile processor
        final VersionedProcessor writeToFile = createProcessor(WRITE_SENSITIVE_ID, GROUP_A_ID, "WriteToFile",
                "org.apache.nifi.processors.tests.system.WriteToFile", bundle,
                Map.of("Filename", outputFile), ScheduledState.ENABLED);
        writeToFile.setAutoTerminatedRelationships(Set.of("success", "failure"));

        // Connections within Group A
        final VersionedConnection inputToUpdate = createConnection("input-to-update", GROUP_A_ID,
                INPUT_PORT_A_ID, ConnectableComponentType.INPUT_PORT,
                UPDATE_CONTENT_ID, ConnectableComponentType.PROCESSOR, null,
                Set.of());

        final VersionedConnection updateToWrite = createConnection("update-to-write", GROUP_A_ID,
                UPDATE_CONTENT_ID, ConnectableComponentType.PROCESSOR,
                WRITE_SENSITIVE_ID, ConnectableComponentType.PROCESSOR, null,
                Set.of("success"));

        groupA.setInputPorts(Set.of(inputPortA));
        groupA.setOutputPorts(new HashSet<>());
        groupA.setProcessors(Set.of(updateContent, writeToFile));
        groupA.setConnections(Set.of(inputToUpdate, updateToWrite));
        groupA.setProcessGroups(new HashSet<>());
        groupA.setControllerServices(new HashSet<>());

        return groupA;
    }

    private VersionedProcessGroup createProcessGroupB(final Bundle bundle, final String outputFile) {
        final VersionedProcessGroup groupB = new VersionedProcessGroup();
        groupB.setIdentifier(GROUP_B_ID);
        groupB.setGroupIdentifier(ROOT_GROUP_ID);
        groupB.setName("Process Group B - Asset Value");
        groupB.setParameterContextName(PARENT_CONTEXT_NAME);

        // Input Port
        final VersionedPort inputPortB = createInputPort(INPUT_PORT_B_ID, GROUP_B_ID, "Input Port B");

        // ReplaceWithFile processor using asset parameter
        final VersionedProcessor replaceWithFile = createProcessor(REPLACE_WITH_FILE_ID, GROUP_B_ID, "ReplaceWithFile",
                "org.apache.nifi.processors.tests.system.ReplaceWithFile", bundle,
                Map.of("Filename", "#{" + ASSET_PARAM_NAME + "}"), ScheduledState.ENABLED);

        // WriteToFile processor
        final VersionedProcessor writeToFile = createProcessor(WRITE_ASSET_ID, GROUP_B_ID, "WriteToFile",
                "org.apache.nifi.processors.tests.system.WriteToFile", bundle,
                Map.of("Filename", outputFile), ScheduledState.ENABLED);
        writeToFile.setAutoTerminatedRelationships(Set.of("success", "failure"));

        // Connections within Group B
        final VersionedConnection inputToReplace = createConnection("input-to-replace", GROUP_B_ID,
                INPUT_PORT_B_ID, ConnectableComponentType.INPUT_PORT,
                REPLACE_WITH_FILE_ID, ConnectableComponentType.PROCESSOR, null,
                Set.of());

        final VersionedConnection replaceToWrite = createConnection("replace-to-write", GROUP_B_ID,
                REPLACE_WITH_FILE_ID, ConnectableComponentType.PROCESSOR,
                WRITE_ASSET_ID, ConnectableComponentType.PROCESSOR, null,
                Set.of("success"));

        groupB.setInputPorts(Set.of(inputPortB));
        groupB.setOutputPorts(new HashSet<>());
        groupB.setProcessors(Set.of(replaceWithFile, writeToFile));
        groupB.setConnections(Set.of(inputToReplace, replaceToWrite));
        groupB.setProcessGroups(new HashSet<>());
        groupB.setControllerServices(new HashSet<>());

        return groupB;
    }

    private Bundle createBundle() {
        final Bundle bundle = new Bundle();
        bundle.setGroup("org.apache.nifi");
        bundle.setArtifact("nifi-system-test-extensions-nar");
        bundle.setVersion("2.8.0-SNAPSHOT");
        return bundle;
    }

    private VersionedPort createInputPort(final String identifier, final String groupIdentifier, final String name) {
        final VersionedPort port = new VersionedPort();
        port.setIdentifier(identifier);
        port.setGroupIdentifier(groupIdentifier);
        port.setName(name);
        port.setType(PortType.INPUT_PORT);
        port.setScheduledState(ScheduledState.ENABLED);
        port.setConcurrentlySchedulableTaskCount(1);
        port.setPosition(new Position(0, 0));
        port.setAllowRemoteAccess(false);
        return port;
    }

    private VersionedProcessor createProcessor(final String identifier, final String groupIdentifier, final String name,
                                                final String type, final Bundle bundle, final Map<String, String> properties,
                                                final ScheduledState scheduledState) {
        final VersionedProcessor processor = new VersionedProcessor();
        processor.setIdentifier(identifier);
        processor.setGroupIdentifier(groupIdentifier);
        processor.setName(name);
        processor.setType(type);
        processor.setBundle(bundle);
        processor.setProperties(properties);
        processor.setPropertyDescriptors(Collections.emptyMap());
        processor.setScheduledState(scheduledState);

        processor.setBulletinLevel("WARN");
        processor.setSchedulingStrategy("TIMER_DRIVEN");
        processor.setSchedulingPeriod("0 sec");
        processor.setExecutionNode("ALL");
        processor.setConcurrentlySchedulableTaskCount(1);
        processor.setPenaltyDuration("30 sec");
        processor.setYieldDuration("1 sec");
        processor.setRunDurationMillis(0L);
        processor.setPosition(new Position(0, 0));

        processor.setAutoTerminatedRelationships(Collections.emptySet());
        processor.setRetryCount(10);
        processor.setRetriedRelationships(Collections.emptySet());
        processor.setBackoffMechanism("PENALIZE_FLOWFILE");
        processor.setMaxBackoffPeriod("10 mins");

        return processor;
    }

    private VersionedConnection createConnection(final String identifier, final String groupIdentifier,
                                                  final String sourceId, final ConnectableComponentType sourceType,
                                                  final String destinationId, final ConnectableComponentType destinationType,
                                                  final String destinationGroupId,
                                                  final Set<String> selectedRelationships) {
        final ConnectableComponent source = new ConnectableComponent();
        source.setId(sourceId);
        source.setType(sourceType);
        source.setGroupId(groupIdentifier);

        final ConnectableComponent destination = new ConnectableComponent();
        destination.setId(destinationId);
        destination.setType(destinationType);
        destination.setGroupId(destinationGroupId != null ? destinationGroupId : groupIdentifier);

        final VersionedConnection connection = new VersionedConnection();
        connection.setIdentifier(identifier);
        connection.setGroupIdentifier(groupIdentifier);
        connection.setSource(source);
        connection.setDestination(destination);
        connection.setSelectedRelationships(selectedRelationships);
        connection.setBackPressureDataSizeThreshold("1 GB");
        connection.setBackPressureObjectThreshold(10_000L);
        connection.setBends(Collections.emptyList());
        connection.setLabelIndex(1);
        connection.setFlowFileExpiration("0 sec");
        connection.setPrioritizers(Collections.emptyList());
        connection.setzIndex(0L);

        return connection;
    }
}
