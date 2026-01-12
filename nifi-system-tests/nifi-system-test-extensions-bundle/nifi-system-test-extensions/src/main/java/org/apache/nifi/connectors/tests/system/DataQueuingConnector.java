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
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.ConnectableComponent;
import org.apache.nifi.flow.ConnectableComponentType;
import org.apache.nifi.flow.Position;
import org.apache.nifi.flow.ScheduledState;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DataQueuingConnector extends AbstractConnector {
    @Override
    protected void onStepConfigured(final String stepName, final FlowContext workingContext) {

    }

    @Override
    public VersionedExternalFlow getInitialFlow() {
        final Bundle bundle = new Bundle();
        bundle.setGroup("org.apache.nifi");
        bundle.setArtifact("nifi-system-test-extensions-nar");
        bundle.setVersion("2.7.0-SNAPSHOT");

        final VersionedProcessor generate = createVersionedProcessor("gen-1", "1234", "GenerateFlowFile",
            "org.apache.nifi.processors.tests.system.GenerateFlowFile", bundle, Map.of("File Size", "1 KB"), ScheduledState.RUNNING);

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
        connection.setIdentifier("generate-to-terminate-1");
        connection.setSource(source);
        connection.setDestination(destination);
        connection.setGroupIdentifier("1234");
        connection.setSelectedRelationships(Set.of("success"));
        connection.setBackPressureDataSizeThreshold("1 GB");
        connection.setBackPressureObjectThreshold(10_000L);
        connection.setBends(Collections.emptyList());
        connection.setLabelIndex(1);
        connection.setFlowFileExpiration("0 sec");
        connection.setPrioritizers(Collections.emptyList());
        connection.setzIndex(1L);

        final VersionedProcessGroup rootGroup = new VersionedProcessGroup();
        rootGroup.setName("Data Queuing Connector");
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
    public void applyUpdate(final FlowContext workingFlowContext, final FlowContext activeFlowContext) {
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
