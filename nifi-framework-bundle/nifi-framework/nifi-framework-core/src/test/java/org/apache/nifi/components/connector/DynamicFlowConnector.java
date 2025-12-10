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
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.components.connector.processors.CreateDummyFlowFile;
import org.apache.nifi.components.connector.processors.DuplicateFlowFile;
import org.apache.nifi.components.connector.processors.LogFlowFileContents;
import org.apache.nifi.components.connector.processors.OverwriteFlowFile;
import org.apache.nifi.components.connector.services.impl.StandardCounterService;
import org.apache.nifi.components.connector.util.VersionedFlowUtils;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.ConnectableComponent;
import org.apache.nifi.flow.Position;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedPort;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DynamicFlowConnector extends AbstractConnector {
    private volatile boolean initialized = false;

    static final ConnectorPropertyDescriptor SOURCE_TEXT = new ConnectorPropertyDescriptor.Builder()
        .name("Source Text")
        .type(PropertyType.STRING)
        .addValidator(Validator.VALID)
        .required(true)
        .defaultValue("Hello World")
        .build();

    static final ConnectorPropertyDescriptor COUNT_FLOWFILES = new ConnectorPropertyDescriptor.Builder()
        .name("Count FlowFiles")
        .description("If true, the Counter Service will be used to count the number of FlowFiles created")
        .type(PropertyType.BOOLEAN)
        .allowableValues("true", "false")
        .required(true)
        .defaultValue("false")
        .build();

    static final ConnectorPropertyDescriptor NUM_COPIES = new ConnectorPropertyDescriptor.Builder()
        .name("Number of Copies")
        .type(PropertyType.INTEGER)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .required(true)
        .defaultValue("1")
        .build();

    static final ConnectorPropertyDescriptor LOG_FLOWFILE_CONTENTS = new ConnectorPropertyDescriptor.Builder()
        .name("Log FlowFile Contents")
        .type(PropertyType.BOOLEAN)
        .allowableValues("true", "false")
        .required(true)
        .defaultValue("false")
        .build();

    private static final ConnectorPropertyGroup SOURCE_GROUP = new ConnectorPropertyGroup.Builder()
        .name("Source Settings")
        .description("Settings for the source of FlowFiles")
        .addProperty(SOURCE_TEXT)
        .addProperty(COUNT_FLOWFILES)
        .build();

    private static final ConfigurationStep SOURCE_STEP = new ConfigurationStep.Builder()
        .name("Source")
        .propertyGroups(List.of(SOURCE_GROUP))
        .build();

    private static final ConnectorPropertyGroup DUPLICATION_GROUP = new ConnectorPropertyGroup.Builder()
        .addProperty(NUM_COPIES)
        .build();

    private static final ConfigurationStep DUPLICATION_STEP = new ConfigurationStep.Builder()
        .name("Duplication")
        .propertyGroups(List.of(DUPLICATION_GROUP))
        .build();

    private static final ConnectorPropertyGroup DESTINATION_GROUP = new ConnectorPropertyGroup.Builder()
        .addProperty(LOG_FLOWFILE_CONTENTS)
        .build();

    private static final ConfigurationStep DESTINATION_STEP = new ConfigurationStep.Builder()
        .name("Destination")
        .propertyGroups(List.of(DESTINATION_GROUP))
        .build();

    private static final List<ConfigurationStep> configurationSteps = List.of(
        SOURCE_STEP,
        DUPLICATION_STEP,
        DESTINATION_STEP
    );

    @Override
    public List<ConfigurationStep> getConfigurationSteps(final FlowContext flowContext) {
        return configurationSteps;
    }

    @Override
    protected void init() {
        initialized = true;
    }

    @Override
    public VersionedExternalFlow getInitialFlow() {
        return VersionedFlowUtils.loadFlowFromResource("flows/generate-duplicate-log-flow.json");
    }

    public boolean isInitialized() {
        return initialized;
    }

    @Override
    public void applyUpdate(final FlowContext workingContext, final FlowContext activeContext) throws FlowUpdateException {
        // Apply the updates to the active flow that were made to the working flow
        final VersionedExternalFlow versionedFlow = getFlow(workingContext);
        getInitializationContext().updateFlow(activeContext, versionedFlow);
    }

    @Override
    public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final Map<String, String> overrides, final FlowContext flowContext) {
        return List.of();
    }

    @Override
    public void onStepConfigured(final String stepName, final FlowContext workingContext) throws FlowUpdateException {
        // Now that configuration is available, update the flow based on configured properties
        final VersionedExternalFlow versionedFlow = getFlow(workingContext);
        getInitializationContext().updateFlow(workingContext, versionedFlow);
    }

    private VersionedExternalFlow getFlow(final FlowContext flowContext) {
        final VersionedExternalFlow externalFlow = VersionedFlowUtils.loadFlowFromResource("flows/generate-duplicate-log-flow.json");
        final VersionedProcessGroup versionedProcessGroup = externalFlow.getFlowContents();

        // Update the flow based on configured properties
        updateSourceStep(flowContext, versionedProcessGroup);
        updateDuplicationStep(flowContext, versionedProcessGroup);
        updateDestinationStep(flowContext, versionedProcessGroup);

        return externalFlow;
    }

    private void updateSourceStep(final FlowContext flowContext, final VersionedProcessGroup rootGroup) {
        final ConnectorConfigurationContext configContext = flowContext.getConfigurationContext();
        final String sourceText = configContext.getProperty(SOURCE_STEP, SOURCE_TEXT).getValue();

        final VersionedProcessor sourceTextProcessor = VersionedFlowUtils.findProcessor(rootGroup,
            p -> p.getType().equals(OverwriteFlowFile.class.getName())).orElseThrow();
        sourceTextProcessor.getProperties().put(OverwriteFlowFile.CONTENT.getName(), sourceText);

        final boolean count = configContext.getProperty(SOURCE_STEP, COUNT_FLOWFILES).asBoolean();
        if (count) {
            final Bundle systemBundle = new Bundle();
            systemBundle.setArtifact("system");
            systemBundle.setGroup("default");
            systemBundle.setVersion("unversioned");

            final VersionedControllerService controllerService = VersionedFlowUtils.addControllerService(rootGroup, StandardCounterService.class.getName(), systemBundle, "Count");
            rootGroup.getControllerServices().add(controllerService);

            final VersionedProcessor generateProcessor = VersionedFlowUtils.findProcessor(rootGroup,
                p -> p.getType().equals(CreateDummyFlowFile.class.getName())).orElseThrow();
            generateProcessor.getProperties().put("Counter Service", controllerService.getIdentifier());
        }
    }

    private void updateDuplicationStep(final FlowContext flowContext, final VersionedProcessGroup rootGroup) {
        final ConnectorConfigurationContext configContext = flowContext.getConfigurationContext();

        final int numCopies = configContext.getProperty(DUPLICATION_STEP, NUM_COPIES).asInteger();
        final VersionedProcessor duplicateProcessor = VersionedFlowUtils.findProcessor(rootGroup,
            p -> p.getType().equals(DuplicateFlowFile.class.getName())).orElseThrow();
        duplicateProcessor.getProperties().put(DuplicateFlowFile.NUM_DUPLICATES.getName(), String.valueOf(numCopies));

        // Need to determine how many Connections exist going out of the DuplicateFlowFile processor
        // and then add/remove connections as necessary to match the number of copies.
        final VersionedProcessGroup duplicatesGroup = VersionedFlowUtils.findGroupForProcessor(rootGroup, duplicateProcessor);
        if (duplicatesGroup == null) {
            return;
        }

        final List<VersionedConnection> outboundConnections = VersionedFlowUtils.findOutboundConnections(rootGroup, duplicateProcessor);
        final int currentConnections = outboundConnections.size();

        if (numCopies > currentConnections) {
            // Add new connections for the additional copies
            addConnectionsForDuplicates(duplicatesGroup, duplicateProcessor, outboundConnections, numCopies);
        } else if (numCopies < currentConnections) {
            // Remove excess connections
            removeExcessConnections(duplicatesGroup, outboundConnections, numCopies);
        }
    }

    private void updateDestinationStep(final FlowContext flowContext, final VersionedProcessGroup rootGroup) {
        final ConnectorConfigurationContext configContext = flowContext.getConfigurationContext();

        final boolean logContents = configContext.getProperty(DESTINATION_STEP, LOG_FLOWFILE_CONTENTS).asBoolean();
        if (!logContents) {
            return;
        }

        final VersionedProcessGroup destinationGroup = rootGroup.getProcessGroups().stream()
            .filter(group -> group.getName().equals("Destination"))
            .findFirst()
            .orElseThrow();

        // Add a LogFlowFileContents processor to the Destination group. Move the TerminateFlowFile processor down about
        // 250 pixels and update connections so that it's Port -> LogFlowFileContents -> TerminateFlowFile

        // Find the existing TerminateFlowFile processor
        final VersionedProcessor terminateProcessor = VersionedFlowUtils.findProcessor(destinationGroup,
            p -> p.getType().equals("org.apache.nifi.components.connector.processors.TerminateFlowFile")).orElseThrow();

        // Get the first (and only) input port
        final VersionedPort inputPort = destinationGroup.getInputPorts().iterator().next();

        // Create LogFlowFileContents processor
        final VersionedProcessor logProcessor = createLogFlowFileContentsProcessor(destinationGroup, terminateProcessor);

        // Move TerminateFlowFile processor down by 250 pixels
        final Position terminatePosition = terminateProcessor.getPosition();
        terminateProcessor.setPosition(new Position(terminatePosition.getX(), terminatePosition.getY() + 250));

        // Update connections: Port -> LogFlowFileContents -> TerminateFlowFile
        updateDestinationConnections(destinationGroup, inputPort, logProcessor, terminateProcessor);
    }

    private void addConnectionsForDuplicates(final VersionedProcessGroup duplicatesGroup, final VersionedProcessor duplicateProcessor,
                final List<VersionedConnection> existingConnections, final int targetNumCopies) {

        if (existingConnections.isEmpty()) {
            return;
        }

        // Use the first existing connection as a template for creating new connections
        final VersionedConnection templateConnection = existingConnections.getFirst();
        final ConnectableComponent sourceComponent = VersionedFlowUtils.createConnectableComponent(duplicateProcessor);
        final ConnectableComponent destinationComponent = templateConnection.getDestination();

        final int currentConnections = existingConnections.size();
        for (int i = currentConnections + 1; i <= targetNumCopies; i++) {
            final Set<String> relationships = Set.of(String.valueOf(i));
            VersionedFlowUtils.addConnection(duplicatesGroup, sourceComponent, destinationComponent, relationships);
        }
    }

    private void removeExcessConnections(final VersionedProcessGroup duplicatesGroup, final List<VersionedConnection> outboundConnections, final int targetNumCopies) {
        final Set<VersionedConnection> connectionsToRemove = new HashSet<>();

        // Sort connections by relationship name (which should be numeric) and remove the highest numbered ones
        final List<VersionedConnection> sortedConnections = new ArrayList<>(outboundConnections);
        sortedConnections.sort((c1, c2) -> {
            final String rel1 = c1.getSelectedRelationships().iterator().next();
            final String rel2 = c2.getSelectedRelationships().iterator().next();
            try {
                final int num1 = Integer.parseInt(rel1);
                final int num2 = Integer.parseInt(rel2);
                return Integer.compare(num2, num1); // Sort descending to remove highest numbers first
            } catch (final NumberFormatException e) {
                return rel2.compareTo(rel1);
            }
        });

        final int connectionsToRemoveCount = outboundConnections.size() - targetNumCopies;
        for (int i = 0; i < connectionsToRemoveCount && i < sortedConnections.size(); i++) {
            connectionsToRemove.add(sortedConnections.get(i));
        }

        // Remove the connections from the process group
        final Set<VersionedConnection> groupConnections = duplicatesGroup.getConnections();
        if (groupConnections != null) {
            groupConnections.removeAll(connectionsToRemove);
        }
    }

    private void updateDestinationConnections(final VersionedProcessGroup destinationGroup, final VersionedPort inputPort,
        final VersionedProcessor logProcessor, final VersionedProcessor terminateProcessor) {
        // Find and remove the existing connection from input port to terminate processor
        final Set<VersionedConnection> connections = destinationGroup.getConnections();
        if (connections != null) {
            connections.stream()
                .filter(conn -> conn.getSource().getId().equals(inputPort.getIdentifier()))
                .filter(conn -> conn.getDestination().getId().equals(terminateProcessor.getIdentifier()))
                .findFirst()
                .ifPresent(connections::remove);
        }

        // Create connection from input port to LogFlowFileContents processor
        final ConnectableComponent inputPortComponent = VersionedFlowUtils.createConnectableComponent(inputPort);
        final ConnectableComponent logProcessorComponent = VersionedFlowUtils.createConnectableComponent(logProcessor);
        VersionedFlowUtils.addConnection(destinationGroup, inputPortComponent, logProcessorComponent, Set.of(""));

        // Create connection from LogFlowFileContents processor to TerminateFlowFile processor
        final ConnectableComponent terminateProcessorComponent = VersionedFlowUtils.createConnectableComponent(terminateProcessor);
        VersionedFlowUtils.addConnection(destinationGroup, logProcessorComponent, terminateProcessorComponent, Set.of("success"));
    }

    private VersionedProcessor createLogFlowFileContentsProcessor(final VersionedProcessGroup destinationGroup, final VersionedProcessor terminateProcessor) {
        // Position new processor where terminate processor is; terminate processor will be moved down later
        final Position terminatePosition = terminateProcessor.getPosition();
        final Position logProcessorPosition = new Position(terminatePosition.getX(), terminatePosition.getY());

        return VersionedFlowUtils.addProcessor(
            destinationGroup,
            LogFlowFileContents.class.getName(),
            terminateProcessor.getBundle(),
            "Log FlowFile Contents",
            logProcessorPosition
        );
    }
}
