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
import org.apache.nifi.flow.VersionedConfigurationStep;
import org.apache.nifi.flow.VersionedConnectorValueReference;
import org.apache.nifi.nar.ExtensionManager;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
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

    @Test
    public void testGetConnectorWithProviderOverridesWorkingConfig() {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        final StandardConnectorRepository repository = createRepositoryWithProvider(provider);

        final MutableConnectorConfigurationContext workingConfigContext = mock(MutableConnectorConfigurationContext.class);
        final ConnectorNode connector = createConnectorNodeWithWorkingConfig("connector-1", "Original Name", workingConfigContext);
        repository.addConnector(connector);

        final ConnectorWorkingConfiguration externalConfig = new ConnectorWorkingConfiguration();
        externalConfig.setName("External Name");
        final VersionedConfigurationStep externalStep = createVersionedStep("step1", Map.of("prop1", createStringLiteralRef("external-value")));
        externalConfig.setWorkingFlowConfiguration(List.of(externalStep));
        when(provider.load("connector-1")).thenReturn(Optional.of(externalConfig));

        final ConnectorNode result = repository.getConnector("connector-1");

        assertNotNull(result);
        verify(connector).setName("External Name");
        verify(workingConfigContext).replaceProperties(eq("step1"), any(StepConfiguration.class));
    }

    @Test
    public void testGetConnectorWithProviderReturnsEmpty() {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        final StandardConnectorRepository repository = createRepositoryWithProvider(provider);

        final ConnectorNode connector = createSimpleConnectorNode("connector-1", "Original Name");
        repository.addConnector(connector);

        when(provider.load("connector-1")).thenReturn(Optional.empty());

        final ConnectorNode result = repository.getConnector("connector-1");

        assertNotNull(result);
        verify(connector, never()).setName(anyString());
    }

    @Test
    public void testGetConnectorWithProviderThrowsException() {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        final StandardConnectorRepository repository = createRepositoryWithProvider(provider);

        final ConnectorNode connector = createSimpleConnectorNode("connector-1", "Original Name");
        repository.addConnector(connector);

        when(provider.load("connector-1")).thenThrow(new ConnectorConfigurationProviderException("Provider failure"));

        assertThrows(ConnectorConfigurationProviderException.class, () -> repository.getConnector("connector-1"));
        verify(connector, never()).setName(anyString());
    }

    @Test
    public void testGetConnectorWithNullProvider() {
        final StandardConnectorRepository repository = createRepositoryWithProvider(null);

        final ConnectorNode connector = createSimpleConnectorNode("connector-1", "Original Name");
        repository.addConnector(connector);

        final ConnectorNode result = repository.getConnector("connector-1");

        assertNotNull(result);
        verify(connector, never()).setName(anyString());
    }

    @Test
    public void testGetConnectorsWithProviderOverrides() {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        final StandardConnectorRepository repository = createRepositoryWithProvider(provider);

        final MutableConnectorConfigurationContext workingConfig1 = mock(MutableConnectorConfigurationContext.class);
        final MutableConnectorConfigurationContext workingConfig2 = mock(MutableConnectorConfigurationContext.class);
        final ConnectorNode connector1 = createConnectorNodeWithWorkingConfig("connector-1", "Name 1", workingConfig1);
        final ConnectorNode connector2 = createConnectorNodeWithWorkingConfig("connector-2", "Name 2", workingConfig2);
        repository.addConnector(connector1);
        repository.addConnector(connector2);

        final ConnectorWorkingConfiguration externalConfig1 = new ConnectorWorkingConfiguration();
        externalConfig1.setName("External Name 1");
        externalConfig1.setWorkingFlowConfiguration(List.of());
        when(provider.load("connector-1")).thenReturn(Optional.of(externalConfig1));
        when(provider.load("connector-2")).thenReturn(Optional.empty());

        final List<ConnectorNode> results = repository.getConnectors();

        assertEquals(2, results.size());
        verify(connector1).setName("External Name 1");
        verify(connector2, never()).setName(anyString());
    }

    @Test
    public void testConfigureConnectorSavesToProviderBeforeModifyingNode() throws FlowUpdateException {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        final StandardConnectorRepository repository = createRepositoryWithProvider(provider);

        final ConnectorNode connector = createConnectorNodeWithEmptyWorkingConfig("connector-1", "Test Connector");
        repository.addConnector(connector);

        when(provider.load("connector-1")).thenReturn(Optional.empty());

        final StepConfiguration incomingConfig = new StepConfiguration(Map.of("prop1", new StringLiteralValue("new-value")));
        repository.configureConnector(connector, "step1", incomingConfig);

        final ArgumentCaptor<ConnectorWorkingConfiguration> configCaptor = ArgumentCaptor.forClass(ConnectorWorkingConfiguration.class);
        verify(provider).save(eq("connector-1"), configCaptor.capture());

        final ConnectorWorkingConfiguration savedConfig = configCaptor.getValue();
        assertNotNull(savedConfig);
        assertEquals("Test Connector", savedConfig.getName());

        final List<VersionedConfigurationStep> savedSteps = savedConfig.getWorkingFlowConfiguration();
        assertNotNull(savedSteps);
        final VersionedConfigurationStep savedStep = savedSteps.stream()
            .filter(s -> "step1".equals(s.getName())).findFirst().orElse(null);
        assertNotNull(savedStep);
        assertEquals("STRING_LITERAL", savedStep.getProperties().get("prop1").getValueType());
        assertEquals("new-value", savedStep.getProperties().get("prop1").getValue());

        verify(connector).setConfiguration("step1", incomingConfig);
    }

    @Test
    public void testConfigureConnectorProviderSaveFailsDoesNotModifyNode() throws FlowUpdateException {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        final StandardConnectorRepository repository = createRepositoryWithProvider(provider);

        final ConnectorNode connector = createConnectorNodeWithEmptyWorkingConfig("connector-1", "Test Connector");
        repository.addConnector(connector);

        when(provider.load("connector-1")).thenReturn(Optional.empty());
        doThrow(new RuntimeException("Save failed")).when(provider).save(anyString(), any(ConnectorWorkingConfiguration.class));

        final StepConfiguration incomingConfig = new StepConfiguration(Map.of("prop1", new StringLiteralValue("new-value")));

        assertThrows(RuntimeException.class, () -> repository.configureConnector(connector, "step1", incomingConfig));

        verify(connector, never()).setConfiguration(anyString(), any(StepConfiguration.class));
    }

    @Test
    public void testConfigureConnectorMergesPartialStepConfig() throws FlowUpdateException {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        final StandardConnectorRepository repository = createRepositoryWithProvider(provider);

        final ConnectorNode connector = createConnectorNodeWithEmptyWorkingConfig("connector-1", "Test Connector");
        repository.addConnector(connector);

        final VersionedConfigurationStep existingStep = createVersionedStep("step1",
            Map.of("propA", createStringLiteralRef("old-A"), "propB", createStringLiteralRef("old-B")));
        final ConnectorWorkingConfiguration existingConfig = new ConnectorWorkingConfiguration();
        existingConfig.setName("Test Connector");
        existingConfig.setWorkingFlowConfiguration(new ArrayList<>(List.of(existingStep)));
        when(provider.load("connector-1")).thenReturn(Optional.of(existingConfig));

        final Map<String, ConnectorValueReference> incomingProps = new HashMap<>();
        incomingProps.put("propA", new StringLiteralValue("new-A"));
        incomingProps.put("propC", new StringLiteralValue("new-C"));
        final StepConfiguration incomingConfig = new StepConfiguration(incomingProps);

        repository.configureConnector(connector, "step1", incomingConfig);

        final ArgumentCaptor<ConnectorWorkingConfiguration> configCaptor = ArgumentCaptor.forClass(ConnectorWorkingConfiguration.class);
        verify(provider).save(eq("connector-1"), configCaptor.capture());

        final ConnectorWorkingConfiguration savedConfig = configCaptor.getValue();
        final VersionedConfigurationStep savedStep = savedConfig.getWorkingFlowConfiguration().stream()
            .filter(s -> "step1".equals(s.getName())).findFirst().orElse(null);
        assertNotNull(savedStep);

        final Map<String, VersionedConnectorValueReference> savedProps = savedStep.getProperties();
        assertEquals("new-A", savedProps.get("propA").getValue());
        assertEquals("old-B", savedProps.get("propB").getValue());
        assertEquals("new-C", savedProps.get("propC").getValue());

        verify(connector).setConfiguration("step1", incomingConfig);
    }

    @Test
    public void testDiscardWorkingConfigurationCallsProviderDiscard() {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        final StandardConnectorRepository repository = createRepositoryWithProvider(provider);

        final ConnectorNode connector = createConnectorNodeWithEmptyWorkingConfig("connector-1", "Test Connector");
        repository.addConnector(connector);

        repository.discardWorkingConfiguration(connector);

        verify(provider).discard("connector-1");
        verify(provider, never()).save(anyString(), any(ConnectorWorkingConfiguration.class));
        verify(connector).discardWorkingConfiguration();
    }

    @Test
    public void testDiscardWorkingConfigurationProviderThrowsException() {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        final StandardConnectorRepository repository = createRepositoryWithProvider(provider);

        final ConnectorNode connector = createConnectorNodeWithEmptyWorkingConfig("connector-1", "Test Connector");
        repository.addConnector(connector);

        doThrow(new ConnectorConfigurationProviderException("Discard failed")).when(provider).discard("connector-1");

        assertThrows(ConnectorConfigurationProviderException.class, () -> repository.discardWorkingConfiguration(connector));
    }

    @Test
    public void testRemoveConnectorCallsProviderDelete() {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        final StandardConnectorRepository repository = createRepositoryWithProvider(provider);

        final Connector mockConnector = mock(Connector.class);
        final ConnectorNode connector = mock(ConnectorNode.class);
        when(connector.getIdentifier()).thenReturn("connector-1");
        when(connector.getConnector()).thenReturn(mockConnector);
        repository.addConnector(connector);

        repository.removeConnector("connector-1");

        verify(provider).delete("connector-1");
    }

    @Test
    public void testRemoveConnectorProviderThrowsException() {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        final StandardConnectorRepository repository = createRepositoryWithProvider(provider);

        final Connector mockConnector = mock(Connector.class);
        final ConnectorNode connector = mock(ConnectorNode.class);
        when(connector.getIdentifier()).thenReturn("connector-1");
        when(connector.getConnector()).thenReturn(mockConnector);
        repository.addConnector(connector);

        doThrow(new ConnectorConfigurationProviderException("Delete failed")).when(provider).delete("connector-1");

        assertThrows(ConnectorConfigurationProviderException.class, () -> repository.removeConnector("connector-1"));
    }

    @Test
    public void testUpdateConnectorSavesNameToProvider() {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        final StandardConnectorRepository repository = createRepositoryWithProvider(provider);

        final ConnectorNode connector = createConnectorNodeWithEmptyWorkingConfig("connector-1", "Old Name");
        repository.addConnector(connector);

        repository.updateConnector(connector, "New Name");

        final ArgumentCaptor<ConnectorWorkingConfiguration> configCaptor = ArgumentCaptor.forClass(ConnectorWorkingConfiguration.class);
        verify(provider).save(eq("connector-1"), configCaptor.capture());
        assertEquals("New Name", configCaptor.getValue().getName());

        verify(connector).setName("New Name");
    }

    @Test
    public void testInheritConfigurationDoesNotCallProvider() throws FlowUpdateException {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        final StandardConnectorRepository repository = createRepositoryWithProvider(provider);

        when(provider.load("connector-1")).thenReturn(Optional.empty());

        final ConnectorNode connector = mock(ConnectorNode.class);
        when(connector.getIdentifier()).thenReturn("connector-1");
        repository.addConnector(connector);

        // Reset interactions so we can verify that inheritConfiguration itself does not call the provider
        reset(provider);

        repository.inheritConfiguration(connector, List.of(), List.of(), null);

        verifyNoInteractions(provider);
    }

    @Test
    public void testVerifyCreateWithExistingConnectorThrows() {
        final StandardConnectorRepository repository = createRepositoryWithProvider(null);

        final ConnectorNode connector = createSimpleConnectorNode("connector-1", "Test Connector");
        repository.addConnector(connector);

        assertThrows(IllegalStateException.class, () -> repository.verifyCreate("connector-1"));
    }

    @Test
    public void testVerifyCreateWithNewConnectorSucceeds() {
        final StandardConnectorRepository repository = createRepositoryWithProvider(null);

        repository.verifyCreate("connector-1");
    }

    @Test
    public void testVerifyCreateDelegatesToProvider() {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        final StandardConnectorRepository repository = createRepositoryWithProvider(provider);

        repository.verifyCreate("connector-1");

        verify(provider).verifyCreate("connector-1");
    }

    @Test
    public void testVerifyCreateProviderRejectsThrows() {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        final StandardConnectorRepository repository = createRepositoryWithProvider(provider);

        doThrow(new ConnectorConfigurationProviderException("Create not supported")).when(provider).verifyCreate("connector-1");

        assertThrows(ConnectorConfigurationProviderException.class, () -> repository.verifyCreate("connector-1"));
    }

    @Test
    public void testVerifyCreateExistingConnectorDoesNotCallProvider() {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        final StandardConnectorRepository repository = createRepositoryWithProvider(provider);

        when(provider.load("connector-1")).thenReturn(Optional.empty());

        final ConnectorNode connector = createSimpleConnectorNode("connector-1", "Test Connector");
        repository.addConnector(connector);

        assertThrows(IllegalStateException.class, () -> repository.verifyCreate("connector-1"));
        verify(provider, never()).verifyCreate(anyString());
    }

    private StandardConnectorRepository createRepositoryWithProvider(final ConnectorConfigurationProvider provider) {
        final StandardConnectorRepository repository = new StandardConnectorRepository();
        final ConnectorRepositoryInitializationContext initContext = mock(ConnectorRepositoryInitializationContext.class);
        when(initContext.getExtensionManager()).thenReturn(mock(ExtensionManager.class));
        when(initContext.getAssetManager()).thenReturn(mock(AssetManager.class));
        when(initContext.getConnectorConfigurationProvider()).thenReturn(provider);
        repository.initialize(initContext);
        return repository;
    }

    private ConnectorNode createSimpleConnectorNode(final String id, final String name) {
        final ConnectorNode connector = mock(ConnectorNode.class);
        when(connector.getIdentifier()).thenReturn(id);
        when(connector.getName()).thenReturn(name);
        return connector;
    }

    private ConnectorNode createConnectorNodeWithWorkingConfig(final String id, final String name, final MutableConnectorConfigurationContext workingConfigContext) {
        final ConnectorNode connector = mock(ConnectorNode.class);
        when(connector.getIdentifier()).thenReturn(id);
        when(connector.getName()).thenReturn(name);

        final FrameworkFlowContext workingFlowContext = mock(FrameworkFlowContext.class);
        when(workingFlowContext.getConfigurationContext()).thenReturn(workingConfigContext);
        when(connector.getWorkingFlowContext()).thenReturn(workingFlowContext);

        return connector;
    }

    private ConnectorNode createConnectorNodeWithEmptyWorkingConfig(final String id, final String name) {
        final ConnectorNode connector = mock(ConnectorNode.class);
        when(connector.getIdentifier()).thenReturn(id);
        when(connector.getName()).thenReturn(name);

        final MutableConnectorConfigurationContext workingConfigContext = mock(MutableConnectorConfigurationContext.class);
        final ConnectorConfiguration emptyConfig = new ConnectorConfiguration(Set.of());
        when(workingConfigContext.toConnectorConfiguration()).thenReturn(emptyConfig);

        final FrameworkFlowContext workingFlowContext = mock(FrameworkFlowContext.class);
        when(workingFlowContext.getConfigurationContext()).thenReturn(workingConfigContext);
        when(connector.getWorkingFlowContext()).thenReturn(workingFlowContext);

        final FrameworkFlowContext activeFlowContext = mock(FrameworkFlowContext.class);
        final MutableConnectorConfigurationContext activeConfigContext = mock(MutableConnectorConfigurationContext.class);
        when(activeConfigContext.toConnectorConfiguration()).thenReturn(emptyConfig);
        when(activeFlowContext.getConfigurationContext()).thenReturn(activeConfigContext);
        when(connector.getActiveFlowContext()).thenReturn(activeFlowContext);

        return connector;
    }

    private VersionedConfigurationStep createVersionedStep(final String name, final Map<String, VersionedConnectorValueReference> properties) {
        final VersionedConfigurationStep step = new VersionedConfigurationStep();
        step.setName(name);
        step.setProperties(new HashMap<>(properties));
        return step;
    }

    private VersionedConnectorValueReference createStringLiteralRef(final String value) {
        final VersionedConnectorValueReference ref = new VersionedConnectorValueReference();
        ref.setValueType("STRING_LITERAL");
        ref.setValue(value);
        return ref;
    }
}
