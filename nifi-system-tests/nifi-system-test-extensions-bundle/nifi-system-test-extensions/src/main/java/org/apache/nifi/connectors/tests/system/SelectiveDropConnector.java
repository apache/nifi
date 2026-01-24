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
import org.apache.nifi.components.connector.ConfigurationStep;
import org.apache.nifi.components.connector.DropFlowFileSummary;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.components.ConnectionFacade;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.components.connector.components.ProcessGroupFacade;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.ConnectableComponent;
import org.apache.nifi.flow.ConnectableComponentType;
import org.apache.nifi.flow.Position;
import org.apache.nifi.flow.ScheduledState;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flowfile.FlowFile;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Connector for testing selective dropping of FlowFiles.
 * When started, it generates FlowFiles with a 'flowFileIndex' attribute.
 * When stopped, it drops all FlowFiles where the flowFileIndex attribute has an even value.
 */
public class SelectiveDropConnector extends AbstractConnector {

    private static final String CONNECTION_ID = "generate-to-terminate-connection";

    @Override
    protected void onStepConfigured(final String stepName, final FlowContext workingContext) {
    }

    @Override
    public VersionedExternalFlow getInitialFlow() {
        final Bundle bundle = new Bundle();
        bundle.setGroup("org.apache.nifi");
        bundle.setArtifact("nifi-system-test-extensions-nar");
        bundle.setVersion("2.7.0-SNAPSHOT");

        // GenerateFlowFile processor configured to generate 1-byte FlowFiles with flowFileIndex attribute
        final Map<String, String> generateProperties = Map.of(
            "File Size", "1 B",
            "Batch Size", "20000",
            "Max FlowFiles", "20000",
            "flowFileIndex", "${nextInt()}"
        );
        final VersionedProcessor generate = createVersionedProcessor("gen-1", "1234", "GenerateFlowFile",
            "org.apache.nifi.processors.tests.system.GenerateFlowFile", bundle, generateProperties, ScheduledState.ENABLED);
        generate.setSchedulingPeriod("10 sec");

        final VersionedProcessor terminate = createVersionedProcessor("term-1", "1234", "TerminateFlowFile",
            "org.apache.nifi.processors.tests.system.TerminateFlowFile", bundle, Collections.emptyMap(), ScheduledState.DISABLED);

        final ConnectableComponent source = new ConnectableComponent();
        source.setId(generate.getIdentifier());
        source.setType(ConnectableComponentType.PROCESSOR);
        source.setGroupId("1234");

        final ConnectableComponent destination = new ConnectableComponent();
        destination.setId(terminate.getIdentifier());
        destination.setType(ConnectableComponentType.PROCESSOR);
        destination.setGroupId("1234");

        final VersionedConnection connection = new VersionedConnection();
        connection.setIdentifier(CONNECTION_ID);
        connection.setSource(source);
        connection.setDestination(destination);
        connection.setGroupIdentifier("1234");
        connection.setSelectedRelationships(Set.of("success"));
        connection.setBackPressureDataSizeThreshold("100 GB");
        connection.setBackPressureObjectThreshold(100_000L);
        connection.setBends(Collections.emptyList());
        connection.setLabelIndex(1);
        connection.setFlowFileExpiration("0 sec");
        connection.setPrioritizers(Collections.emptyList());
        connection.setzIndex(1L);

        final VersionedProcessGroup rootGroup = new VersionedProcessGroup();
        rootGroup.setName("Selective Drop Connector");
        rootGroup.setIdentifier("1234");
        rootGroup.setProcessors(Set.of(generate, terminate));
        rootGroup.setConnections(Set.of(connection));

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
        return List.of();
    }

    @Override
    public void applyUpdate(final FlowContext workingFlowContext, final FlowContext activeFlowContext) throws FlowUpdateException {
        final VersionedExternalFlow flow = getInitialFlow();
        getInitializationContext().updateFlow(activeFlowContext, flow);
    }

    @Override
    public void stop(final FlowContext context) throws FlowUpdateException {
        // First, stop the processors via the parent class
        super.stop(context);

        // Then, drop all FlowFiles where flowFileIndex has an even value
        final ProcessGroupFacade rootGroup = context.getRootGroup();
        final ConnectionFacade connection = findConnectionById(rootGroup, CONNECTION_ID);

        if (connection == null) {
            getLogger().warn("Could not find connection with ID {} to perform selective drop", CONNECTION_ID);
            return;
        }

        try {
            final AtomicInteger dropCount = new AtomicInteger();
            final AtomicInteger seenCount = new AtomicInteger();
            final DropFlowFileSummary summary = connection.dropFlowFiles(ff -> {
                final boolean shouldDrop = hasEvenFlowFileIndex(ff);
                seenCount.incrementAndGet();
                if (shouldDrop) {
                    dropCount.incrementAndGet();
                }
                return shouldDrop;
            });
            getLogger().info("Selectively dropped {} out of {} FlowFiles ({} bytes) with even flowFileIndex values",
                dropCount.get(), seenCount.get(), summary.getDroppedBytes());
        } catch (final IOException e) {
            throw new FlowUpdateException("Failed to selectively drop FlowFiles", e);
        }
    }

    private boolean hasEvenFlowFileIndex(final FlowFile flowFile) {
        return Integer.parseInt(flowFile.getAttribute("flowFileIndex")) % 2 == 0;
    }

    private ConnectionFacade findConnectionById(final ProcessGroupFacade group, final String connectionId) {
        for (final ConnectionFacade connection : group.getConnections()) {
            if (connectionId.equals(connection.getDefinition().getIdentifier())) {
                return connection;
            }
        }

        for (final ProcessGroupFacade childGroup : group.getProcessGroups()) {
            final ConnectionFacade found = findConnectionById(childGroup, connectionId);
            if (found != null) {
                return found;
            }
        }

        return null;
    }

    private VersionedProcessor createVersionedProcessor(final String identifier, final String groupIdentifier, final String name,
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
}
