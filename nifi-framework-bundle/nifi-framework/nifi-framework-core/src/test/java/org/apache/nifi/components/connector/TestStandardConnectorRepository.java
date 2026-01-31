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

public class TestStandardConnectorRepository {

    @Test
    public void testAddAndGetConnectors() {
        final StandardConnectorRepository repository = new StandardConnectorRepository();

        final ConnectorNode connector1 = mock(ConnectorNode.class);
        final ConnectorNode connector2 = mock(ConnectorNode.class);
        when(connector1.getIdentifier()).thenReturn("connector-1");
        when(connector2.getIdentifier()).thenReturn("connector-2");

        repository.addConnector(connector1);
        repository.addConnector(connector2);

        final List<ConnectorNode> connectors = repository.getConnectors();
        assertEquals(2, connectors.size());
        assertTrue(connectors.contains(connector1));
        assertTrue(connectors.contains(connector2));
    }

    @Test
    public void testRemoveConnector() {
        final StandardConnectorRepository repository = new StandardConnectorRepository();

        final ConnectorRepositoryInitializationContext initContext = mock(ConnectorRepositoryInitializationContext.class);
        final ExtensionManager extensionManager = mock(ExtensionManager.class);
        when(initContext.getExtensionManager()).thenReturn(extensionManager);
        repository.initialize(initContext);

        final Connector mockConnector = mock(Connector.class);
        final ConnectorNode connector = mock(ConnectorNode.class);
        when(connector.getIdentifier()).thenReturn("connector-1");
        when(connector.getConnector()).thenReturn(mockConnector);

        repository.addConnector(connector);
        repository.removeConnector("connector-1");

        assertEquals(0, repository.getConnectors().size());
        assertNull(repository.getConnector("connector-1"));
    }

    @Test
    public void testRestoreConnector() {
        final StandardConnectorRepository repository = new StandardConnectorRepository();

        final ConnectorNode connector = mock(ConnectorNode.class);
        when(connector.getIdentifier()).thenReturn("connector-1");

        repository.restoreConnector(connector);
        assertEquals(1, repository.getConnectors().size());
        assertEquals(connector, repository.getConnector("connector-1"));
    }

    @Test
    public void testGetConnectorsReturnsNewListInstances() {
        final StandardConnectorRepository repository = new StandardConnectorRepository();

        final ConnectorNode connector1 = mock(ConnectorNode.class);
        final ConnectorNode connector2 = mock(ConnectorNode.class);
        when(connector1.getIdentifier()).thenReturn("connector-1");
        when(connector2.getIdentifier()).thenReturn("connector-2");

        repository.addConnector(connector1);
        repository.addConnector(connector2);

        final List<ConnectorNode> connectors1 = repository.getConnectors();
        final List<ConnectorNode> connectors2 = repository.getConnectors();

        assertEquals(2, connectors1.size());
        assertEquals(2, connectors2.size());
        assertEquals(connectors1, connectors2);
        assertNotSame(connectors1, connectors2);
    }

    @Test
    public void testAddConnectorWithDuplicateIdReplaces() {
        final StandardConnectorRepository repository = new StandardConnectorRepository();

        final ConnectorNode connector1 = mock(ConnectorNode.class);
        final ConnectorNode connector2 = mock(ConnectorNode.class);
        when(connector1.getIdentifier()).thenReturn("same-id");
        when(connector2.getIdentifier()).thenReturn("same-id");

        repository.addConnector(connector1);
        repository.addConnector(connector2);

        final List<ConnectorNode> connectors = repository.getConnectors();
        assertEquals(1, connectors.size());
        assertEquals(connector2, repository.getConnector("same-id"));
    }

    @Test
    public void testDiscardWorkingConfigurationPreservesWorkingAssets() {
        final String connectorId = "test-connector";
        final String activeAssetId = "active-asset-id";
        final String workingAssetId = "working-asset-id";
        final String unreferencedAssetId = "unreferenced-asset-id";

        final StandardConnectorRepository repository = new StandardConnectorRepository();

        final AssetManager assetManager = mock(AssetManager.class);
        final ConnectorRepositoryInitializationContext initContext = mock(ConnectorRepositoryInitializationContext.class);
        when(initContext.getExtensionManager()).thenReturn(mock(ExtensionManager.class));
        when(initContext.getAssetManager()).thenReturn(assetManager);
        repository.initialize(initContext);

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

        repository.addConnector(connector);
        repository.discardWorkingConfiguration(connector);

        verify(assetManager, never()).deleteAsset(activeAssetId);
        verify(assetManager, never()).deleteAsset(workingAssetId);
        verify(assetManager).deleteAsset(unreferencedAssetId);
    }
}
