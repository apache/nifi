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
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedProcessGroup;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Test Connector that creates a flow with a nested (child) process group.
 * Used to verify that child process groups within a connector's managed flow
 * can be properly accessed via the connector API.
 */
public class NestedProcessGroupConnector extends AbstractConnector {

    public static final String CHILD_PROCESS_GROUP_ID = "child-process-group-id";

    private final List<ConfigurationStep> configurationSteps = List.of();

    @Override
    protected void onStepConfigured(final String stepName, final FlowContext workingContext) {
    }

    @Override
    public VersionedExternalFlow getInitialFlow() {
        final VersionedProcessGroup childGroup = new VersionedProcessGroup();
        childGroup.setIdentifier(CHILD_PROCESS_GROUP_ID);
        childGroup.setName("Child Process Group");

        final VersionedProcessGroup rootGroup = new VersionedProcessGroup();
        rootGroup.setName("Nested Flow");
        rootGroup.setProcessGroups(Set.of(childGroup));

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
        return configurationSteps;
    }

    @Override
    public void applyUpdate(final FlowContext workingFlowContext, final FlowContext activeFlowContext) {
    }
}

