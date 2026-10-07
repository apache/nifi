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
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Pre-upgrade connector. Its flow queues FlowFiles on a connection that the version 2 connector removes.
 */
public class UpgradeDrainConnector extends AbstractConnector {

    public static final String GATE_FILE_PATH_NAME = "Gate File Path";
    public static final String EVOLUTION_MODE_NAME = "Evolution Mode";
    public static final String REMOVE_CONNECTION_NAME = "Remove Connection";
    public static final String COMMENT_NAME = "Comment";
    public static final String UPGRADE_SECRET_NAME = "Upgrade Secret";
    public static final String REMOVED_CONNECTION_NAME = "Removed Connection";

    static final ConnectorPropertyDescriptor GATE_FILE_PATH = new ConnectorPropertyDescriptor.Builder()
        .name(GATE_FILE_PATH_NAME)
        .description("Relative path of the file that lets the terminating processor run.")
        .required(true)
        .type(PropertyType.STRING)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    static final ConnectorPropertyDescriptor EVOLUTION_MODE = new ConnectorPropertyDescriptor.Builder()
        .name(EVOLUTION_MODE_NAME)
        .description("How the upgraded connector handles removed connections.")
        .required(true)
        .defaultValue("DRAIN")
        .allowableValues("DRAIN", "INHERITED_DRAIN", "NO_OP")
        .type(PropertyType.STRING)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    static final ConnectorPropertyDescriptor REMOVE_CONNECTION = new ConnectorPropertyDescriptor.Builder()
        .name(REMOVE_CONNECTION_NAME)
        .description("When true, the upgraded flow drops the connection that queues generated FlowFiles.")
        .required(true)
        .defaultValue("true")
        .allowableValues("true", "false")
        .type(PropertyType.STRING)
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .build();

    static final ConnectorPropertyDescriptor COMMENT = new ConnectorPropertyDescriptor.Builder()
        .name(COMMENT_NAME)
        .description("Comment copied onto the generating processor.")
        .required(false)
        .type(PropertyType.STRING)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    static final ConnectorPropertyDescriptor UPGRADE_SECRET = new ConnectorPropertyDescriptor.Builder()
        .name(UPGRADE_SECRET_NAME)
        .description("Secret copied into the managed Parameter Context for upgrade restoration tests.")
        .required(false)
        .type(PropertyType.SECRET)
        .build();

    public static final String LEGACY_NOTE_NAME = "Legacy Note";

    static final ConnectorPropertyDescriptor LEGACY_NOTE = new ConnectorPropertyDescriptor.Builder()
        .name(LEGACY_NOTE_NAME)
        .description("Optional note retained only by this version.")
        .required(false)
        .type(PropertyType.STRING)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final String REMOVED_PROCESSOR_CHAIN_NAME = "Removed Processor Chain";

    static final ConnectorPropertyDescriptor REMOVED_PROCESSOR_CHAIN = new ConnectorPropertyDescriptor.Builder()
        .name(REMOVED_PROCESSOR_CHAIN_NAME)
        .description("When true, generated FlowFiles queue on the connection from a processor the upgrade removes to the terminator that stays.")
        .required(false)
        .defaultValue("false")
        .allowableValues("true", "false")
        .type(PropertyType.STRING)
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .build();

    private static final ConnectorPropertyGroup PROPERTY_GROUP = new ConnectorPropertyGroup.Builder()
        .name("Upgrade")
        .addProperty(GATE_FILE_PATH)
        .addProperty(EVOLUTION_MODE)
        .addProperty(REMOVE_CONNECTION)
        .addProperty(COMMENT)
        .addProperty(UPGRADE_SECRET)
        .addProperty(LEGACY_NOTE)
        .addProperty(REMOVED_PROCESSOR_CHAIN)
        .build();

    private static final ConfigurationStep CONFIG_STEP = new ConfigurationStep.Builder()
        .name("Upgrade")
        .propertyGroups(List.of(PROPERTY_GROUP))
        .build();

    private static final Bundle GATE_BUNDLE = new Bundle("org.apache.nifi", "nifi-system-test-extensions2-nar", "2.13.0-SNAPSHOT");
    private static final String ROOT_GROUP_ID = "upgrade-drain-root";
    private static final String GENERATE_TYPE = "org.apache.nifi.processors.tests.system.UpgradeDrainGenerate";
    private static final String RELAY_TYPE = "org.apache.nifi.processors.tests.system.UpgradeDrainRelay";
    private static final String TERMINATE_TYPE = "org.apache.nifi.processors.tests.system.UpgradeDrainTerminate";

    @Override
    protected void onStepConfigured(final String stepName, final FlowContext workingContext) {
    }

    @Override
    public VersionedExternalFlow getInitialFlow() {
        return buildFlow(null);
    }

    @Override
    public VersionedExternalFlow getActiveFlow(final FlowContext flowContext) {
        return buildFlow(flowContext);
    }

    @Override
    public List<ConfigurationStep> getConfigurationSteps() {
        return List.of(CONFIG_STEP);
    }

    @Override
    public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final Map<String, String> overrides, final FlowContext flowContext) {
        return List.of();
    }

    @Override
    public void applyUpdate(final FlowContext workingContext, final FlowContext activeContext) throws FlowUpdateException {
        getInitializationContext().updateFlow(activeContext, buildFlow(workingContext), BundleCompatibility.RESOLVE_BUNDLE);
    }

    private VersionedExternalFlow buildFlow(final FlowContext flowContext) {
        final VersionedProcessGroup rootGroup = VersionedFlowUtils.createProcessGroup(ROOT_GROUP_ID, "Upgrade Drain");
        final VersionedProcessor generate = VersionedFlowUtils.addProcessor(rootGroup, GENERATE_TYPE, GATE_BUNDLE, "GenerateFlowFile", new Position(0, 0));
        generate.getProperties().put("File Size", "1 B");
        generate.setSchedulingPeriod("100 millis");
        final String gateFile = property(flowContext, GATE_FILE_PATH);
        if (gateFile != null) {
            generate.getProperties().put("Gate File", gateFile);
        }

        final String comment = property(flowContext, COMMENT);
        if (comment != null && !comment.isBlank()) {
            generate.setComments(comment);
        }

        // Identifiers are assigned by how many components of that type already exist. Generate and Terminate are added
        // first, in both versions, so they keep their identifiers. The relay and the removed processor come after them.
        // The queued connection is added last so the upgraded flow, which creates only two connections, does not reuse
        // its identifier. That connection is deleted, and a non-empty queue there requires a drain.
        final boolean removedProcessorChain = Boolean.parseBoolean(property(flowContext, REMOVED_PROCESSOR_CHAIN));
        final Position terminatePosition = removedProcessorChain ? new Position(1200, 0) : new Position(400, 0);
        final VersionedProcessor terminate = VersionedFlowUtils.addProcessor(rootGroup, TERMINATE_TYPE, GATE_BUNDLE, "TerminateFlowFile", terminatePosition);
        if (gateFile != null) {
            terminate.getProperties().put("Gate File", gateFile);
        }

        if (removedProcessorChain) {
            final VersionedProcessor relay = VersionedFlowUtils.addProcessor(rootGroup, RELAY_TYPE, GATE_BUNDLE, "RelayFlowFile", new Position(400, 0));
            final VersionedProcessor removed = VersionedFlowUtils.addProcessor(rootGroup, RELAY_TYPE, GATE_BUNDLE, "RemovedFlowFile", new Position(800, 0));
            final VersionedConnection relayInput = VersionedFlowUtils.addConnection(rootGroup, VersionedFlowUtils.createConnectableComponent(generate),
                VersionedFlowUtils.createConnectableComponent(relay), Set.of("success"));
            relayInput.setName("Relay Input Connection");
            final VersionedConnection removedInput = VersionedFlowUtils.addConnection(rootGroup, VersionedFlowUtils.createConnectableComponent(relay),
                VersionedFlowUtils.createConnectableComponent(removed), Set.of("success"));
            removedInput.setName("Removed Input Connection");
            final VersionedConnection queued = VersionedFlowUtils.addConnection(rootGroup, VersionedFlowUtils.createConnectableComponent(removed),
                VersionedFlowUtils.createConnectableComponent(terminate), Set.of("success"));
            queued.setName(REMOVED_CONNECTION_NAME);
        } else {
            final VersionedConnection connection = VersionedFlowUtils.addConnection(rootGroup, VersionedFlowUtils.createConnectableComponent(generate),
                VersionedFlowUtils.createConnectableComponent(terminate), Set.of("success"));
            connection.setName(REMOVED_CONNECTION_NAME);
        }

        final VersionedExternalFlow flow = new VersionedExternalFlow();
        flow.setFlowContents(rootGroup);
        final String upgradeSecret = property(flowContext, UPGRADE_SECRET);
        if (upgradeSecret != null) {
            generate.getProperties().put("Secret Value", "#{" + UPGRADE_SECRET_NAME + "}");
            final VersionedParameter parameter = new VersionedParameter();
            parameter.setName(UPGRADE_SECRET_NAME);
            parameter.setSensitive(true);
            parameter.setValue(upgradeSecret);
            final VersionedParameterContext parameterContext = new VersionedParameterContext();
            parameterContext.setName("Upgrade Drain Parameters");
            parameterContext.setParameters(Set.of(parameter));
            rootGroup.setParameterContextName(parameterContext.getName());
            flow.setParameterContexts(Map.of(parameterContext.getName(), parameterContext));
        }
        return flow;
    }

    private static String property(final FlowContext flowContext, final ConnectorPropertyDescriptor descriptor) {
        if (flowContext == null) {
            return descriptor.getDefaultValue();
        }
        final String value = flowContext.getConfigurationContext().getProperty(CONFIG_STEP, descriptor).getValue();
        return value == null ? descriptor.getDefaultValue() : value;
    }
}
