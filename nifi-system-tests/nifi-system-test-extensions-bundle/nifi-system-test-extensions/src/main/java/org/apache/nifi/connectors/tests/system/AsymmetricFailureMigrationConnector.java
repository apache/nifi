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
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.components.connector.migration.ConnectorMigrationContext;
import org.apache.nifi.components.connector.migration.MigratableConnector;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedProcessGroup;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Test connector whose {@code migrateConfiguration} succeeds on every cluster node except the one whose
 * {@code -DnodeNumber=} JVM argument matches {@value #FAILING_NODE_NUMBER}. Used to validate that the cluster
 * migration-request endpoint merger surfaces a per-node failure even when other nodes report success.
 */
public class AsymmetricFailureMigrationConnector extends AbstractConnector implements MigratableConnector {
    private static final String FAILING_NODE_NUMBER = "2";

    @Override
    public VersionedExternalFlow getInitialFlow() {
        final VersionedProcessGroup processGroup = new VersionedProcessGroup();
        processGroup.setName("Asymmetric Migration Flow");
        processGroup.setProcessors(new HashSet<>());
        processGroup.setConnections(new HashSet<>());
        processGroup.setProcessGroups(new HashSet<>());
        processGroup.setControllerServices(new HashSet<>());

        final VersionedExternalFlow flow = new VersionedExternalFlow();
        flow.setFlowContents(processGroup);
        flow.setParameterContexts(Collections.emptyMap());
        return flow;
    }

    @Override
    public VersionedExternalFlow getActiveFlow(final FlowContext activeFlowContext) {
        return getInitialFlow();
    }

    @Override
    public boolean isMigrationSupported(final ConnectorMigrationContext context) {
        return true;
    }

    @Override
    public void migrateConfiguration(final ConnectorMigrationContext context) throws FlowUpdateException {
        // Fail on the targeted node before recording any configuration so the merger sees a clean per-node failure
        // while the other nodes succeed.
        final String currentNodeNumber = System.getProperty("nodeNumber");
        if (FAILING_NODE_NUMBER.equals(currentNodeNumber)) {
            throw new FlowUpdateException("Simulated migration failure on node " + currentNodeNumber);
        }
    }

    @Override
    public void migrateState(final ConnectorMigrationContext context) {
        // No state to migrate; success on every non-failing node.
    }

    @Override
    protected void onStepConfigured(final String stepName, final FlowContext workingContext) {
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
