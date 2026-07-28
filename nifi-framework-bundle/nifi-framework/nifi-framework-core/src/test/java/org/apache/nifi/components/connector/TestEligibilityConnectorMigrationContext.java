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

import org.apache.nifi.asset.AssetManager;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.controller.ClusterTopologyProvider;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class TestEligibilityConnectorMigrationContext {

    private static final String CONNECTOR_ID = "connector-1";

    @Test
    public void testEligibilityContextRejectsCopyAssetFromSource() {
        final VersionedExternalFlow sourceFlow = mock(VersionedExternalFlow.class);
        final FrameworkFlowContext activeFlowContext = mock(FrameworkFlowContext.class);
        final AssetManager sourceAssetManager = mock(AssetManager.class);
        final ConnectorRepository connectorRepository = mock(ConnectorRepository.class);
        final StateManagerProvider stateManagerProvider = mock(StateManagerProvider.class);
        final ClusterTopologyProvider clusterTopologyProvider = mock(ClusterTopologyProvider.class);

        final MutableConnectorConfigurationContext workingConfiguration = mock(MutableConnectorConfigurationContext.class);
        final StandardFrameworkConnectorMigrationContext underlying = new StandardFrameworkConnectorMigrationContext(
                CONNECTOR_ID, sourceFlow, true, activeFlowContext, workingConfiguration, sourceAssetManager, connectorRepository,
                stateManagerProvider, clusterTopologyProvider);
        final EligibilityConnectorMigrationContext eligibilityContext = new EligibilityConnectorMigrationContext(underlying);

        final IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> eligibilityContext.copyAssetFromSource("any-asset-id"));
        assertTrue(thrown.getMessage().contains("copyAssetFromSource"));
        assertTrue(thrown.getMessage().contains("isMigrationSupported"));

        // Every other accessor must return whatever the underlying context returns.
        assertSame(sourceFlow, eligibilityContext.getSourceFlow());
        assertTrue(eligibilityContext.isLocalMigration());
        // The underlying StandardFrameworkConnectorMigrationContext wraps the active flow context in a
        // MigrationFlowContext so any updateFlow() call from the eligibility path throws as well; the eligibility
        // wrapper just propagates that read-only handle.
        assertTrue(eligibilityContext.getActiveFlowContext() instanceof MigrationFlowContext,
                "Eligibility context must propagate the read-only MigrationFlowContext returned by the underlying migration context");
        assertSame(sourceAssetManager, eligibilityContext.getSourceAssetManager());
        assertSame(connectorRepository, eligibilityContext.getConnectorRepository());
        assertSame(stateManagerProvider, eligibilityContext.getStateManagerProvider());
        assertSame(clusterTopologyProvider, eligibilityContext.getClusterTopologyProvider());
        assertEquals(underlying.getCopiedAssetIds(), eligibilityContext.getCopiedAssetIds());
    }
}
