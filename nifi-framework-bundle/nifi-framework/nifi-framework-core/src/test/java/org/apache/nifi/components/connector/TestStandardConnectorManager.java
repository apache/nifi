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

import org.apache.nifi.asset.Asset;
import org.apache.nifi.asset.AssetManager;
import org.apache.nifi.nar.ExtensionManager;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestStandardConnectorManager {

    private StandardConnectorManager createInitializedManager() {
        return createInitializedManager(mock(AssetManager.class));
    }

    private StandardConnectorManager createInitializedManager(final AssetManager assetManager) {
        final StandardConnectorManager manager = new StandardConnectorManager();
        final ConnectorManagerInitializationContext initContext = mock(ConnectorManagerInitializationContext.class);
        when(initContext.getExtensionManager()).thenReturn(mock(ExtensionManager.class));
        when(initContext.getAssetManager()).thenReturn(assetManager);
        when(initContext.getConnectorRepository()).thenReturn(mock(ConnectorRepository.class));
        when(initContext.getConnectorLifecycleManager()).thenReturn(mock(ConnectorLifecycleManager.class));
        manager.initialize(initContext);
        return manager;
    }

    @Test
    public void testAddAndGetConnectors() {
        final StandardConnectorManager manager = createInitializedManager();

        final ConnectorNode connector1 = mock(ConnectorNode.class);
        final ConnectorNode connector2 = mock(ConnectorNode.class);
        when(connector1.getIdentifier()).thenReturn("connector-1");
        when(connector2.getIdentifier()).thenReturn("connector-2");

        manager.addConnector(connector1);
        manager.addConnector(connector2);

        final List<ConnectorNode> connectors = manager.getConnectors();
        assertEquals(2, connectors.size());
        assertTrue(connectors.contains(connector1));
        assertTrue(connectors.contains(connector2));
    }

    @Test
    public void testRemoveConnector() {
        final StandardConnectorManager manager = createInitializedManager();

        final Connector mockConnector = mock(Connector.class);
        final ConnectorNode connector = mock(ConnectorNode.class);
        when(connector.getIdentifier()).thenReturn("connector-1");
        when(connector.getConnector()).thenReturn(mockConnector);

        manager.addConnector(connector);
        manager.removeConnector("connector-1");

        assertEquals(0, manager.getConnectors().size());
        assertNull(manager.getConnector("connector-1"));
    }

    @Test
    public void testRestoreConnector() {
        final StandardConnectorManager manager = createInitializedManager();

        final ConnectorNode connector = mock(ConnectorNode.class);
        when(connector.getIdentifier()).thenReturn("connector-1");

        manager.restoreConnector(connector);
        assertEquals(1, manager.getConnectors().size());
        assertEquals(connector, manager.getConnector("connector-1"));
    }

    @Test
    public void testGetConnectorsReturnsNewListInstances() {
        final StandardConnectorManager manager = createInitializedManager();

        final ConnectorNode connector1 = mock(ConnectorNode.class);
        final ConnectorNode connector2 = mock(ConnectorNode.class);
        when(connector1.getIdentifier()).thenReturn("connector-1");
        when(connector2.getIdentifier()).thenReturn("connector-2");

        manager.addConnector(connector1);
        manager.addConnector(connector2);

        final List<ConnectorNode> connectors1 = manager.getConnectors();
        final List<ConnectorNode> connectors2 = manager.getConnectors();

        assertEquals(2, connectors1.size());
        assertEquals(2, connectors2.size());
        assertEquals(connectors1, connectors2);
        assertNotSame(connectors1, connectors2);
    }

    @Test
    public void testAddConnectorWithDuplicateIdReplaces() {
        final StandardConnectorManager manager = createInitializedManager();

        final ConnectorNode connector1 = mock(ConnectorNode.class);
        final ConnectorNode connector2 = mock(ConnectorNode.class);
        when(connector1.getIdentifier()).thenReturn("same-id");
        when(connector2.getIdentifier()).thenReturn("same-id");

        manager.addConnector(connector1);
        manager.addConnector(connector2);

        final List<ConnectorNode> connectors = manager.getConnectors();
        assertEquals(1, connectors.size());
        assertEquals(connector2, manager.getConnector("same-id"));
    }

    @Test
    public void testDiscardWorkingConfigurationPreservesWorkingAssets() {
        final String connectorId = "test-connector";
        final String activeAssetId = "active-asset-id";
        final String workingAssetId = "working-asset-id";
        final String unreferencedAssetId = "unreferenced-asset-id";

        final AssetManager assetManager = mock(AssetManager.class);
        final StandardConnectorManager manager = createInitializedManager(assetManager);

        final Asset activeAsset = mock(Asset.class);
        when(activeAsset.getIdentifier()).thenReturn(activeAssetId);
        when(activeAsset.getName()).thenReturn("active-asset.jar");

        final Asset workingAsset = mock(Asset.class);
        when(workingAsset.getIdentifier()).thenReturn(workingAssetId);
        when(workingAsset.getName()).thenReturn("working-asset.jar");

        final Asset unreferencedAsset = mock(Asset.class);
        when(unreferencedAsset.getIdentifier()).thenReturn(unreferencedAssetId);
        when(unreferencedAsset.getName()).thenReturn("unreferenced-asset.jar");

        final MutableConnectorConfigurationContext activeConfigContext = mock(MutableConnectorConfigurationContext.class);
        final StepConfiguration activeStepConfig = new StepConfiguration(
            Map.of("prop1", new AssetReference(Set.of(activeAssetId)))
        );
        final ConnectorConfiguration activeConfig = new ConnectorConfiguration(
            Set.of(new NamedStepConfiguration("step1", activeStepConfig))
        );
        when(activeConfigContext.toConnectorConfiguration()).thenReturn(activeConfig);

        final MutableConnectorConfigurationContext workingConfigContext = mock(MutableConnectorConfigurationContext.class);
        final StepConfiguration workingStepConfig = new StepConfiguration(
            Map.of("prop1", new AssetReference(Set.of(workingAssetId)))
        );
        final ConnectorConfiguration workingConfig = new ConnectorConfiguration(
            Set.of(new NamedStepConfiguration("step1", workingStepConfig))
        );
        when(workingConfigContext.toConnectorConfiguration()).thenReturn(workingConfig);

        final FrameworkFlowContext activeFlowContext = mock(FrameworkFlowContext.class);
        when(activeFlowContext.getConfigurationContext()).thenReturn(activeConfigContext);

        final FrameworkFlowContext workingFlowContext = mock(FrameworkFlowContext.class);
        when(workingFlowContext.getConfigurationContext()).thenReturn(workingConfigContext);

        final ConnectorNode connector = mock(ConnectorNode.class);
        when(connector.getIdentifier()).thenReturn(connectorId);
        when(connector.getActiveFlowContext()).thenReturn(activeFlowContext);
        when(connector.getWorkingFlowContext()).thenReturn(workingFlowContext);

        when(assetManager.getAssets(connectorId)).thenReturn(List.of(activeAsset, workingAsset, unreferencedAsset));

        manager.addConnector(connector);
        manager.discardWorkingConfiguration(connector);

        verify(assetManager, never()).deleteAsset(activeAssetId);
        verify(assetManager, never()).deleteAsset(workingAssetId);
        verify(assetManager).deleteAsset(unreferencedAssetId);
    }
}
