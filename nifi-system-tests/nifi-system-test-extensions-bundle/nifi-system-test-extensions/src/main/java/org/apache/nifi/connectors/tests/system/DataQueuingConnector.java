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
import org.apache.nifi.flow.ConnectableComponent;
import org.apache.nifi.flow.ConnectableComponentType;
import org.apache.nifi.flow.ScheduledState;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class DataQueuingConnector extends AbstractConnector {
    @Override
    protected void onStepConfigured(final String stepName, final FlowContext workingContext) {

    }

    @Override
    public VersionedExternalFlow getInitialFlow() {
        final VersionedProcessor generate = new VersionedProcessor();
        generate.setName("GenerateFlowFile");
        generate.setType("org.apache.nifi.processors.tests.system.GenerateFlowFile");
        generate.setIdentifier("gen-1");
        generate.setGroupIdentifier("1234");
        generate.setProperties(Map.of("File Size", "1 KB"));

        final VersionedProcessor terminate = new VersionedProcessor();
        terminate.setName("TerminateFlowFile");
        terminate.setType("org.apache.nifi.processors.tests.system.TerminateFlowFile");
        terminate.setIdentifier("term-1");
        terminate.setGroupIdentifier("1234");
        terminate.setScheduledState(ScheduledState.DISABLED);

        final ConnectableComponent source = new ConnectableComponent();
        source.setId(generate.getIdentifier());
        source.setType(ConnectableComponentType.PROCESSOR);
        source.setGroupId("1234");

        final ConnectableComponent destination = new ConnectableComponent();
        destination.setId(terminate.getIdentifier());
        destination.setType(ConnectableComponentType.PROCESSOR);
        destination.setGroupId("1234");

        final VersionedConnection connection = new VersionedConnection();
        connection.setSource(source);
        connection.setDestination(destination);
        connection.setGroupIdentifier("1234");
        connection.setIdentifier("generate-to-terminate-1");

        final VersionedProcessGroup rootGroup = new VersionedProcessGroup();
        rootGroup.setName("Data Queuing Connector");
        rootGroup.setIdentifier("1234");
        rootGroup.setProcessors(Set.of(generate, terminate));
        rootGroup.setConnections(Set.of(connection));

        final VersionedExternalFlow flow = new VersionedExternalFlow();
        flow.setFlowContents(rootGroup);
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
}
