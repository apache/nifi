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
import org.apache.nifi.components.connector.FlowEvolutionContext;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.PropertyType;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.components.connector.components.ProcessGroupFacade;
import org.apache.nifi.components.connector.components.ProcessorFacade;
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Post-upgrade connector. It drops the pre-upgrade queue connection unless configured to keep it.
 */
public class UpgradeDrainConnector extends AbstractConnector {

    static final ConnectorPropertyDescriptor GATE_FILE_PATH = new ConnectorPropertyDescriptor.Builder()
        .name("Gate File Path")
        .description("Relative path of the file that lets the terminating processor run.")
        .required(true)
        .type(PropertyType.STRING)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    static final ConnectorPropertyDescriptor EVOLUTION_MODE = new ConnectorPropertyDescriptor.Builder()
        .name("Evolution Mode")
        .description("How this connector handles connections removed by the upgrade.")
        .required(true)
        .defaultValue("DRAIN")
        .allowableValues("DRAIN", "INHERITED_DRAIN", "NO_OP")
        .type(PropertyType.STRING)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    static final ConnectorPropertyDescriptor REMOVE_CONNECTION = new ConnectorPropertyDescriptor.Builder()
        .name("Remove Connection")
        .description("When true, the upgraded flow drops the connection that queues generated FlowFiles.")
        .required(true)
        .defaultValue("true")
        .allowableValues("true", "false")
        .type(PropertyType.STRING)
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .build();

    static final ConnectorPropertyDescriptor COMMENT = new ConnectorPropertyDescriptor.Builder()
        .name("Comment")
        .description("Comment copied onto the generating processor.")
        .required(false)
        .type(PropertyType.STRING)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    static final ConnectorPropertyDescriptor UPGRADE_SECRET = new ConnectorPropertyDescriptor.Builder()
        .name("Upgrade Secret")
        .description("Secret copied into the managed Parameter Context for upgrade restoration tests.")
        .required(false)
        .type(PropertyType.SECRET)
        .build();

    public static final String DRAIN_LABEL_NAME = "Drain Label";

    static final ConnectorPropertyDescriptor DRAIN_LABEL = new ConnectorPropertyDescriptor.Builder()
        .name(DRAIN_LABEL_NAME)
        .description("Label added by the upgraded connector.")
        .required(true)
        .defaultValue("upgraded")
        .type(PropertyType.STRING)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    static final ConnectorPropertyDescriptor REMOVED_PROCESSOR_CHAIN = new ConnectorPropertyDescriptor.Builder()
        .name("Removed Processor Chain")
        .description("When true, the upgraded flow replaces the first middle processor and removes the next one.")
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
        .addProperty(DRAIN_LABEL)
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
    private static final String SWAPPED_PROCESSOR_ID = "upgrade-drain-swapped-processor";

    @Override
    protected void onStepConfigured(final String stepName, final FlowContext workingContext) {
    }

    @Override
    public VersionedExternalFlow getInitialFlow() {
        return buildFlow(null);
    }

    @Override
    public VersionedExternalFlow getActiveFlow(final FlowContext flowContext) {
        final String activeFlowSwitch = readSwitch("upgrade-drain-active-flow.txt");
        if ("NULL".equals(activeFlowSwitch)) {
            return null;
        }
        if ("THROW".equals(activeFlowSwitch)) {
            throw new IllegalStateException("active flow failed");
        }
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
        if (Files.exists(Path.of("upgrade-drain-apply-fail.txt"))) {
            throw new FlowUpdateException("apply failed");
        }

        getInitializationContext().updateFlow(activeContext, buildFlow(workingContext), BundleCompatibility.RESOLVE_BUNDLE);
    }

    @Override
    public CompletableFuture<Void> prepareForFlowEvolution(final FlowEvolutionContext context) throws FlowUpdateException {
        final String hookResult = readSwitch("upgrade-drain-hook-result.txt");
        if ("NULL_FUTURE".equals(hookResult)) {
            return null;
        }
        if ("FAILED_FUTURE".equals(hookResult)) {
            return CompletableFuture.failedFuture(new FlowUpdateException("hook failed"));
        }

        final String mode = property(context.getPreUpgradeActiveFlowContext(), EVOLUTION_MODE);
        if ("INHERITED_DRAIN".equals(mode)) {
            return drainFlowFiles(context.getPreUpgradeActiveFlowContext());
        } else if ("DRAIN".equals(mode)) {
            // Start everything except the generator. Starting the generator as well refills the queue faster than it can empty.
            final ProcessGroupFacade rootGroup = context.getPreUpgradeActiveFlowContext().getRootGroup();
            try {
                for (final ProcessorFacade processor : rootGroup.getProcessors()) {
                    if (!"GenerateFlowFile".equals(processor.getDefinition().getName())) {
                        processor.getLifecycle().start().get();
                    }
                }
            } catch (final Exception e) {
                throw new FlowUpdateException("Failed to start the pre-upgrade flow so its queues can drain", e);
            }

            while (rootGroup.getConnections().stream().anyMatch(connection -> connection.getQueueSize().getObjectCount() > 0)) {
                try {
                    Thread.sleep(200L);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new FlowUpdateException("Flow evolution was interrupted", e);
                }
            }
        }

        return CompletableFuture.completedFuture(null);
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

        final boolean removedProcessorChain = Boolean.parseBoolean(property(flowContext, REMOVED_PROCESSOR_CHAIN));
        if (removedProcessorChain) {
            // Terminate is added before the swapped processor so it keeps the pre-upgrade identifier. The swapped
            // processor would otherwise receive the relay's count-based identifier and the relay would be renamed
            // instead of replaced. Only two connections are created, so the queued pre-upgrade connection is deleted.
            final VersionedProcessor terminate = VersionedFlowUtils.addProcessor(rootGroup, TERMINATE_TYPE, GATE_BUNDLE, "TerminateFlowFile", new Position(800, 0));
            if (gateFile != null) {
                terminate.getProperties().put("Gate File", gateFile);
            }

            final VersionedProcessor swapped = VersionedFlowUtils.addProcessor(rootGroup, RELAY_TYPE, GATE_BUNDLE, "SwappedFlowFile", new Position(400, 0));
            swapped.setIdentifier(SWAPPED_PROCESSOR_ID);
            final VersionedConnection swappedInput = VersionedFlowUtils.addConnection(rootGroup, VersionedFlowUtils.createConnectableComponent(generate),
                VersionedFlowUtils.createConnectableComponent(swapped), Set.of("success"));
            swappedInput.setName("Swapped Input Connection");
            final VersionedConnection swappedOutput = VersionedFlowUtils.addConnection(rootGroup, VersionedFlowUtils.createConnectableComponent(swapped),
                VersionedFlowUtils.createConnectableComponent(terminate), Set.of("success"));
            swappedOutput.setName("Swapped Output Connection");
        } else {
            final boolean removeConnection = flowContext == null || Boolean.parseBoolean(property(flowContext, REMOVE_CONNECTION));
            if (removeConnection) {
                generate.setAutoTerminatedRelationships(Set.of("success"));
            } else {
                final VersionedProcessor terminate = VersionedFlowUtils.addProcessor(rootGroup, TERMINATE_TYPE, GATE_BUNDLE, "TerminateFlowFile", new Position(400, 0));
                if (gateFile != null) {
                    terminate.getProperties().put("Gate File", gateFile);
                }

                final VersionedConnection connection = VersionedFlowUtils.addConnection(rootGroup, VersionedFlowUtils.createConnectableComponent(generate),
                    VersionedFlowUtils.createConnectableComponent(terminate), Set.of("success"));
                connection.setName("Removed Connection");
            }
        }

        final VersionedExternalFlow flow = new VersionedExternalFlow();
        flow.setFlowContents(rootGroup);
        final String upgradeSecret = property(flowContext, UPGRADE_SECRET);
        if (upgradeSecret != null) {
            generate.getProperties().put("Secret Value", "#{" + UPGRADE_SECRET.getName() + "}");
            final VersionedParameter parameter = new VersionedParameter();
            parameter.setName(UPGRADE_SECRET.getName());
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

    private static String readSwitch(final String fileName) {
        try {
            return Files.exists(Path.of(fileName)) ? Files.readString(Path.of(fileName)).trim() : null;
        } catch (final IOException e) {
            throw new IllegalStateException("Failed to read " + fileName, e);
        }
    }
}
