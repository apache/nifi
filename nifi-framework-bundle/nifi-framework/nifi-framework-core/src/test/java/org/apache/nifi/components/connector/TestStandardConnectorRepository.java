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
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.components.connector.components.FlowContextType;
import org.apache.nifi.components.connector.secrets.SecretsManager;
import org.apache.nifi.controller.ParameterProviderNode;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.VersionedConfigurationStep;
import org.apache.nifi.flow.VersionedConnector;
import org.apache.nifi.flow.VersionedConnectorState;
import org.apache.nifi.flow.VersionedConnectorValueReference;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.util.MockComponentLog;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
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

        final List<ConnectorNode> connectors = repository.getConnectors(ConnectorSyncMode.SYNC_WITH_PROVIDER);
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

        assertEquals(0, repository.getConnectors(ConnectorSyncMode.SYNC_WITH_PROVIDER).size());
        assertNull(repository.getConnector("connector-1", ConnectorSyncMode.SYNC_WITH_PROVIDER));
    }

    @Test
    public void testRestoreConnector() {
        final StandardConnectorRepository repository = new StandardConnectorRepository();

        final ConnectorNode connector = mock(ConnectorNode.class);
        when(connector.getIdentifier()).thenReturn("connector-1");

        repository.restoreConnector(connector);
        assertEquals(1, repository.getConnectors(ConnectorSyncMode.SYNC_WITH_PROVIDER).size());
        assertEquals(connector, repository.getConnector("connector-1", ConnectorSyncMode.SYNC_WITH_PROVIDER));
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

        final List<ConnectorNode> connectors1 = repository.getConnectors(ConnectorSyncMode.SYNC_WITH_PROVIDER);
        final List<ConnectorNode> connectors2 = repository.getConnectors(ConnectorSyncMode.SYNC_WITH_PROVIDER);

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

        final List<ConnectorNode> connectors = repository.getConnectors(ConnectorSyncMode.SYNC_WITH_PROVIDER);
        assertEquals(1, connectors.size());
        assertEquals(connector2, repository.getConnector("same-id", ConnectorSyncMode.SYNC_WITH_PROVIDER));
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
    public void testGetConnectorWithProviderOverridesWorkingConfig() throws FlowUpdateException {
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

        final ConnectorNode result = repository.getConnector("connector-1", ConnectorSyncMode.SYNC_WITH_PROVIDER);

        assertNotNull(result);
        verify(connector).setName("External Name");
        verify(connector).replaceWorkingConfiguration(eq("step1"), any(StepConfiguration.class));
    }

    @Test
    public void testGetConnectorWithProviderReturnsEmpty() {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        final StandardConnectorRepository repository = createRepositoryWithProvider(provider);

        final ConnectorNode connector = createSimpleConnectorNode("connector-1", "Original Name");
        repository.addConnector(connector);

        when(provider.load("connector-1")).thenReturn(Optional.empty());

        final ConnectorNode result = repository.getConnector("connector-1", ConnectorSyncMode.SYNC_WITH_PROVIDER);

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

        assertThrows(ConnectorConfigurationProviderException.class, () -> repository.getConnector("connector-1", ConnectorSyncMode.SYNC_WITH_PROVIDER));
        verify(connector, never()).setName(anyString());
    }

    @Test
    public void testGetConnectorWithNullProvider() {
        final StandardConnectorRepository repository = createRepositoryWithProvider(null);

        final ConnectorNode connector = createSimpleConnectorNode("connector-1", "Original Name");
        repository.addConnector(connector);

        final ConnectorNode result = repository.getConnector("connector-1", ConnectorSyncMode.SYNC_WITH_PROVIDER);

        assertNotNull(result);
        verify(connector, never()).setName(anyString());
    }

    @Test
    public void testGetConnectorLocalOnlyDoesNotCallProvider() {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        final StandardConnectorRepository repository = createRepositoryWithProvider(provider);

        final ConnectorNode connector = createSimpleConnectorNode("connector-1", "Local Name");
        repository.restoreConnector(connector);

        // Restoring the connector sources provider-supplied logging attributes; reset so that the assertions
        // below verify only that the LOCAL_ONLY get path does not consult the provider.
        reset(provider);

        final ConnectorNode result = repository.getConnector("connector-1", ConnectorSyncMode.LOCAL_ONLY);

        assertNotNull(result);
        verifyNoInteractions(provider);
        verify(connector, never()).setName(anyString());
    }

    @Test
    public void testGetConnectorsLocalOnlyDoesNotCallProvider() {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        final StandardConnectorRepository repository = createRepositoryWithProvider(provider);

        final ConnectorNode connector1 = createSimpleConnectorNode("connector-1", "Local Name 1");
        final ConnectorNode connector2 = createSimpleConnectorNode("connector-2", "Local Name 2");
        repository.restoreConnector(connector1);
        repository.restoreConnector(connector2);

        // Restoring the connectors sources provider-supplied logging attributes; reset so that the assertions
        // below verify only that the LOCAL_ONLY get path does not consult the provider.
        reset(provider);

        final List<ConnectorNode> results = repository.getConnectors(ConnectorSyncMode.LOCAL_ONLY);

        assertEquals(2, results.size());
        verifyNoInteractions(provider);
        verify(connector1, never()).setName(anyString());
        verify(connector2, never()).setName(anyString());
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

        final List<ConnectorNode> results = repository.getConnectors(ConnectorSyncMode.SYNC_WITH_PROVIDER);

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
    public void testVerifyEnterTroubleshootingDelegatesToConnectorAndProvider() {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        final StandardConnectorRepository repository = createRepositoryWithProvider(provider);

        final ConnectorNode connector = mock(ConnectorNode.class);
        when(connector.getIdentifier()).thenReturn("connector-1");
        repository.restoreConnector(connector);

        repository.verifyEnterTroubleshooting(connector);

        verify(connector).verifyCanEnterTroubleshooting();
        verify(provider).verifyEnterTroubleshooting("connector-1");
    }

    @Test
    public void testVerifyEnterTroubleshootingWithoutProvider() {
        final StandardConnectorRepository repository = createRepositoryWithProvider(null);

        final ConnectorNode connector = mock(ConnectorNode.class);
        when(connector.getIdentifier()).thenReturn("connector-1");
        repository.restoreConnector(connector);

        repository.verifyEnterTroubleshooting(connector);

        verify(connector).verifyCanEnterTroubleshooting();
    }

    @Test
    public void testVerifyEnterTroubleshootingProviderVetoThrows() {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        final StandardConnectorRepository repository = createRepositoryWithProvider(provider);

        final ConnectorNode connector = mock(ConnectorNode.class);
        when(connector.getIdentifier()).thenReturn("connector-1");
        repository.restoreConnector(connector);

        doThrow(new IllegalStateException("External system does not permit troubleshooting at this time"))
                .when(provider).verifyEnterTroubleshooting("connector-1");

        final IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> repository.verifyEnterTroubleshooting(connector));
        assertEquals("External system does not permit troubleshooting at this time", thrown.getMessage());

        verify(connector).verifyCanEnterTroubleshooting();
    }

    @Test
    public void testVerifyEnterTroubleshootingConnectorVetoDoesNotCallProvider() {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        final StandardConnectorRepository repository = createRepositoryWithProvider(provider);

        final ConnectorNode connector = mock(ConnectorNode.class);
        when(connector.getIdentifier()).thenReturn("connector-1");
        doThrow(new IllegalStateException("Connector is currently UPDATING"))
                .when(connector).verifyCanEnterTroubleshooting();
        repository.restoreConnector(connector);

        assertThrows(IllegalStateException.class, () -> repository.verifyEnterTroubleshooting(connector));
        verify(provider, never()).verifyEnterTroubleshooting(anyString());
    }

    @Test
    public void testEnterTroubleshootingInvokesVerifyThenTransition() {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        final StandardConnectorRepository repository = createRepositoryWithProvider(provider);

        final ConnectorNode connector = mock(ConnectorNode.class);
        when(connector.getIdentifier()).thenReturn("connector-1");
        repository.restoreConnector(connector);

        repository.enterTroubleshooting(connector);

        verify(connector).verifyCanEnterTroubleshooting();
        verify(provider).verifyEnterTroubleshooting("connector-1");
        verify(connector).enterTroubleshooting();
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

    // --- Asset Lifecycle Tests ---

    @Test
    public void testStoreAssetDelegatesToProviderAndReturnsStoredAsset() throws IOException {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        final AssetManager assetManager = mock(AssetManager.class);
        final StandardConnectorRepository repository = createRepositoryWithProviderAndAssetManager(provider, assetManager);

        final Asset storedAsset = mock(Asset.class);
        when(storedAsset.getIdentifier()).thenReturn("nifi-uuid-1");
        // After the provider stores the asset locally, the framework retrieves it via getAsset()
        when(assetManager.getAsset("nifi-uuid-1")).thenReturn(Optional.of(storedAsset));

        final InputStream content = new ByteArrayInputStream("test-content".getBytes());
        final Asset result = repository.storeAsset("connector-1", "nifi-uuid-1", "driver.jar", content);

        assertNotNull(result);
        assertEquals("nifi-uuid-1", result.getIdentifier());
        // Provider handles both local storage and external upload -- no direct assetManager.saveAsset() call
        verify(provider).storeAsset(eq("connector-1"), eq("nifi-uuid-1"), eq("driver.jar"), any(InputStream.class));
        verify(assetManager, never()).saveAsset(any(), any(), any(), any());
    }

    @Test
    public void testStoreAssetWrapsProviderFailureAsIOException() throws IOException {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        final AssetManager assetManager = mock(AssetManager.class);
        final StandardConnectorRepository repository = createRepositoryWithProviderAndAssetManager(provider, assetManager);

        doThrow(new RuntimeException("Provider upload failed"))
                .when(provider).storeAsset(eq("connector-1"), eq("nifi-uuid-1"), eq("driver.jar"), any(InputStream.class));

        final InputStream content = new ByteArrayInputStream("test-content".getBytes());
        assertThrows(IOException.class, () -> repository.storeAsset("connector-1", "nifi-uuid-1", "driver.jar", content));

        // Rollback of local storage is the provider's responsibility; the framework does not call deleteAsset
        verify(assetManager, never()).deleteAsset(anyString());
    }

    @Test
    public void testStoreAssetWithoutProviderDelegatesToAssetManagerOnly() throws IOException {
        final AssetManager assetManager = mock(AssetManager.class);
        final StandardConnectorRepository repository = createRepositoryWithProviderAndAssetManager(null, assetManager);

        final Asset localAsset = mock(Asset.class);
        when(localAsset.getIdentifier()).thenReturn("nifi-uuid-1");
        when(assetManager.saveAsset(eq("connector-1"), eq("nifi-uuid-1"), eq("driver.jar"), any(InputStream.class)))
                .thenReturn(localAsset);

        final Asset result = repository.storeAsset("connector-1", "nifi-uuid-1", "driver.jar",
                new ByteArrayInputStream("content".getBytes()));
        assertNotNull(result);
        verify(assetManager).saveAsset(eq("connector-1"), eq("nifi-uuid-1"), eq("driver.jar"), any(InputStream.class));
    }

    @Test
    public void testGetAssetDelegatesToAssetManager() {
        final AssetManager assetManager = mock(AssetManager.class);
        final StandardConnectorRepository repository = createRepositoryWithProviderAndAssetManager(null, assetManager);

        final Asset mockAsset = mock(Asset.class);
        when(mockAsset.getIdentifier()).thenReturn("asset-1");
        when(assetManager.getAsset("asset-1")).thenReturn(Optional.of(mockAsset));

        final Optional<Asset> result = repository.getAsset("asset-1");
        assertTrue(result.isPresent());
        assertEquals("asset-1", result.get().getIdentifier());
    }

    @Test
    public void testGetAssetsDelegatesToAssetManager() {
        final AssetManager assetManager = mock(AssetManager.class);
        final StandardConnectorRepository repository = createRepositoryWithProviderAndAssetManager(null, assetManager);

        final Asset asset1 = mock(Asset.class);
        final Asset asset2 = mock(Asset.class);
        when(assetManager.getAssets("connector-1")).thenReturn(List.of(asset1, asset2));

        final List<Asset> result = repository.getAssets("connector-1");
        assertEquals(2, result.size());
    }

    @Test
    public void testDeleteAssetsDelegatesToProviderByNifiUuid() {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        final AssetManager assetManager = mock(AssetManager.class);
        final StandardConnectorRepository repository = createRepositoryWithProviderAndAssetManager(provider, assetManager);

        final Asset asset = mock(Asset.class);
        when(asset.getIdentifier()).thenReturn("nifi-uuid-1");
        when(assetManager.getAssets("connector-1")).thenReturn(List.of(asset));

        repository.deleteAssets("connector-1");

        // Provider receives NiFi UUID; provider handles external cleanup and local deletion
        verify(provider).deleteAsset("connector-1", "nifi-uuid-1");
        verify(assetManager, never()).deleteAsset(anyString());
    }

    @Test
    public void testDeleteAssetsWithoutProviderDelegatesToAssetManager() {
        final AssetManager assetManager = mock(AssetManager.class);
        final StandardConnectorRepository repository = createRepositoryWithProviderAndAssetManager(null, assetManager);

        final Asset asset = mock(Asset.class);
        when(asset.getIdentifier()).thenReturn("nifi-uuid-1");
        when(assetManager.getAssets("connector-1")).thenReturn(List.of(asset));

        repository.deleteAssets("connector-1");

        verify(assetManager).deleteAsset("nifi-uuid-1");
    }

    @Test
    public void testSyncFromProviderAppliesNifiUuidsDirectly() throws FlowUpdateException {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        final StandardConnectorRepository repository = createRepositoryWithProvider(provider);

        final MutableConnectorConfigurationContext workingConfigContext = mock(MutableConnectorConfigurationContext.class);
        final ConnectorNode connector = createConnectorNodeWithWorkingConfig("connector-1", "Test Connector", workingConfigContext);
        repository.addConnector(connector);

        // Provider returns config with NiFi UUIDs (no translation needed in framework)
        final VersionedConnectorValueReference assetRef = new VersionedConnectorValueReference();
        assetRef.setValueType("ASSET_REFERENCE");
        assetRef.setAssetIds(Set.of("nifi-uuid-from-provider"));
        final VersionedConfigurationStep step = createVersionedStep("step1", Map.of("driver", assetRef));

        final ConnectorWorkingConfiguration config = new ConnectorWorkingConfiguration();
        config.setName("Test Connector");
        config.setWorkingFlowConfiguration(List.of(step));
        when(provider.load("connector-1")).thenReturn(Optional.of(config));

        repository.getConnector("connector-1", ConnectorSyncMode.SYNC_WITH_PROVIDER);

        // Working config is replaced with NiFi UUIDs as-is -- no translation in the repository
        verify(connector).replaceWorkingConfiguration(eq("step1"), any(StepConfiguration.class));
    }

    @Test
    public void testGetConnectorTriggersOnConfigurationStepConfiguredWhenValuesChange() throws FlowUpdateException {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        final StandardConnectorRepository repository = createRepositoryWithProvider(provider);

        final TrackingConnector trackingConnector = new TrackingConnector();
        final StandardConnectorNode connectorNode = createRealConnectorNode("connector-1", trackingConnector);
        seedWorkingConfiguration(connectorNode, "step1", Map.of("prop1", "initial-value"));
        repository.restoreConnector(connectorNode);

        trackingConnector.reset();

        final VersionedConfigurationStep externalStep = createVersionedStep("step1",
                Map.of("prop1", createStringLiteralRef("updated-value")));
        final ConnectorWorkingConfiguration externalConfig = new ConnectorWorkingConfiguration();
        externalConfig.setName("connector-1");
        externalConfig.setWorkingFlowConfiguration(List.of(externalStep));
        when(provider.load("connector-1")).thenReturn(Optional.of(externalConfig));

        final ConnectorNode result = repository.getConnector("connector-1", ConnectorSyncMode.SYNC_WITH_PROVIDER);

        assertNotNull(result);
        assertTrue(trackingConnector.wasOnStepConfiguredCalled("step1"));
    }

    @Test
    public void testGetConnectorDoesNotTriggerOnConfigurationStepConfiguredWhenValuesUnchanged() throws FlowUpdateException {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        final StandardConnectorRepository repository = createRepositoryWithProvider(provider);

        final TrackingConnector trackingConnector = new TrackingConnector();
        final StandardConnectorNode connectorNode = createRealConnectorNode("connector-1", trackingConnector);
        seedWorkingConfiguration(connectorNode, "step1", Map.of("prop1", "same-value"));
        repository.restoreConnector(connectorNode);

        trackingConnector.reset();

        final VersionedConfigurationStep externalStep = createVersionedStep("step1",
                Map.of("prop1", createStringLiteralRef("same-value")));
        final ConnectorWorkingConfiguration externalConfig = new ConnectorWorkingConfiguration();
        externalConfig.setName("connector-1");
        externalConfig.setWorkingFlowConfiguration(List.of(externalStep));
        when(provider.load("connector-1")).thenReturn(Optional.of(externalConfig));

        final ConnectorNode result = repository.getConnector("connector-1", ConnectorSyncMode.SYNC_WITH_PROVIDER);

        assertNotNull(result);
        assertFalse(trackingConnector.wasOnStepConfiguredCalled("step1"));
    }

    @Test
    public void testGetConnectorContinuesWhenOneStepFailsToReplace() throws FlowUpdateException {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        final StandardConnectorRepository repository = createRepositoryWithProvider(provider);

        final ConnectorNode connector = mock(ConnectorNode.class);
        when(connector.getIdentifier()).thenReturn("connector-1");
        doThrow(new FlowUpdateException("Simulated failure for step1"))
                .when(connector).replaceWorkingConfiguration(eq("step1"), any(StepConfiguration.class));
        repository.addConnector(connector);

        final VersionedConfigurationStep stepOne = createVersionedStep("step1",
                Map.of("prop1", createStringLiteralRef("value1")));
        final VersionedConfigurationStep stepTwo = createVersionedStep("step2",
                Map.of("prop2", createStringLiteralRef("value2")));
        final ConnectorWorkingConfiguration externalConfig = new ConnectorWorkingConfiguration();
        externalConfig.setName("connector-1");
        externalConfig.setWorkingFlowConfiguration(List.of(stepOne, stepTwo));
        when(provider.load("connector-1")).thenReturn(Optional.of(externalConfig));

        final ConnectorNode result = repository.getConnector("connector-1", ConnectorSyncMode.SYNC_WITH_PROVIDER);

        assertNotNull(result);
        verify(connector).replaceWorkingConfiguration(eq("step1"), any(StepConfiguration.class));
        verify(connector).replaceWorkingConfiguration(eq("step2"), any(StepConfiguration.class));
    }

    @Test
    public void testSyncAssetsFromProviderCallsSyncAssetsThenReloads() {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        final StandardConnectorRepository repository = createRepositoryWithProvider(provider);

        final MutableConnectorConfigurationContext workingConfigContext = mock(MutableConnectorConfigurationContext.class);
        final ConnectorNode connector = createConnectorNodeWithWorkingConfig("connector-1", "Test Connector", workingConfigContext);
        repository.addConnector(connector);

        final ConnectorWorkingConfiguration config = new ConnectorWorkingConfiguration();
        config.setName("Test Connector");
        config.setWorkingFlowConfiguration(List.of());
        when(provider.load("connector-1")).thenReturn(Optional.of(config));

        repository.syncAssetsFromProvider(connector);

        // Step 1: provider.syncAssets() called
        verify(provider).syncAssets("connector-1");
        // Step 2: provider.load() called to reload updated config (may also have been called during addConnector)
        verify(provider, org.mockito.Mockito.atLeastOnce()).load("connector-1");
    }

    @Test
    public void testSyncAssetsFromProviderNoOpWithoutProvider() {
        final AssetManager assetManager = mock(AssetManager.class);
        final StandardConnectorRepository repository = createRepositoryWithProviderAndAssetManager(null, assetManager);

        final ConnectorNode connector = createSimpleConnectorNode("connector-1", "Test");
        repository.addConnector(connector);

        repository.syncAssetsFromProvider(connector);

        verifyNoInteractions(assetManager);
    }

    @Test
    public void testCleanUpAssetsCallsProviderDeleteForUnreferencedAssets() {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        final AssetManager assetManager = mock(AssetManager.class);
        final StandardConnectorRepository repository = createRepositoryWithProviderAndAssetManager(provider, assetManager);

        final String connectorId = "connector-1";
        final String referencedAssetId = "referenced-uuid";
        final String unreferencedAssetId = "unreferenced-uuid";

        final Asset referencedAsset = mock(Asset.class);
        when(referencedAsset.getIdentifier()).thenReturn(referencedAssetId);
        when(referencedAsset.getName()).thenReturn("referenced.jar");

        final Asset unreferencedAsset = mock(Asset.class);
        when(unreferencedAsset.getIdentifier()).thenReturn(unreferencedAssetId);
        when(unreferencedAsset.getName()).thenReturn("unreferenced.jar");

        final MutableConnectorConfigurationContext activeConfigContext = mock(MutableConnectorConfigurationContext.class);
        final ConnectorConfiguration activeConfig = new ConnectorConfiguration(Set.of(
            new NamedStepConfiguration("step1", new StepConfiguration(
                Map.of("prop", new AssetReference(Set.of(referencedAssetId)))))
        ));
        when(activeConfigContext.toConnectorConfiguration()).thenReturn(activeConfig);
        final FrameworkFlowContext activeFlowContext = mock(FrameworkFlowContext.class);
        when(activeFlowContext.getConfigurationContext()).thenReturn(activeConfigContext);

        final MutableConnectorConfigurationContext workingConfigContext = mock(MutableConnectorConfigurationContext.class);
        final ConnectorConfiguration workingConfig = new ConnectorConfiguration(Set.of());
        when(workingConfigContext.toConnectorConfiguration()).thenReturn(workingConfig);
        final FrameworkFlowContext workingFlowContext = mock(FrameworkFlowContext.class);
        when(workingFlowContext.getConfigurationContext()).thenReturn(workingConfigContext);

        final ConnectorNode connector = mock(ConnectorNode.class);
        when(connector.getIdentifier()).thenReturn(connectorId);
        when(connector.getActiveFlowContext()).thenReturn(activeFlowContext);
        when(connector.getWorkingFlowContext()).thenReturn(workingFlowContext);

        when(assetManager.getAssets(connectorId)).thenReturn(List.of(referencedAsset, unreferencedAsset));
        when(provider.load(connectorId)).thenReturn(Optional.empty());
        repository.addConnector(connector);

        repository.discardWorkingConfiguration(connector);

        // Provider.deleteAsset called with NiFi UUID for unreferenced asset
        verify(provider).deleteAsset(connectorId, unreferencedAssetId);
        // Referenced asset is not deleted
        verify(provider, never()).deleteAsset(connectorId, referencedAssetId);
        // AssetManager.deleteAsset NOT called directly since provider handles local deletion
        verify(assetManager, never()).deleteAsset(anyString());
    }

    @Test
    public void testInheritConfigurationFailureCallsAbortUpdateAndMarkInvalid() throws FlowUpdateException {
        final StandardConnectorRepository repository = createRepositoryWithProvider(null);

        final ConnectorNode connector = mock(ConnectorNode.class);
        when(connector.getIdentifier()).thenReturn("connector-1");
        doThrow(new FlowUpdateException("Simulated failure"))
            .when(connector).inheritConfiguration(any(), any(), any());

        repository.addConnector(connector);

        assertThrows(FlowUpdateException.class, () -> repository.inheritConfiguration(connector, List.of(), List.of(), null));

        verify(connector).abortUpdate(any(FlowUpdateException.class));
        verify(connector).markInvalid(eq("Flow Update Failure"), eq("The flow could not be updated: Simulated failure"));
    }

    @Test
    public void testApplyUpdateFailureCallsAbortUpdateButNotMarkInvalid() throws FlowUpdateException {
        final AssetManager assetManager = mock(AssetManager.class);
        when(assetManager.getAssets("connector-1")).thenReturn(List.of());
        final StandardConnectorRepository repository = createRepositoryWithProviderAndAssetManager(null, assetManager);

        final ConnectorNode connector = mock(ConnectorNode.class);
        when(connector.getIdentifier()).thenReturn("connector-1");
        when(connector.getDesiredState()).thenReturn(ConnectorState.STOPPED);
        doThrow(new FlowUpdateException("Simulated failure"))
            .when(connector).prepareForUpdate();

        repository.addConnector(connector);

        repository.applyUpdate(connector, mock(ConnectorUpdateContext.class));

        verify(connector, timeout(5000)).abortUpdate(any(FlowUpdateException.class));
        verify(connector, never()).markInvalid(anyString(), anyString());
    }

    @Test
    public void testApplyUpdateInTroubleshootingThrowsBeforeProviderConsultation() {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        final StandardConnectorRepository repository = createRepositoryWithProvider(provider);

        final ConnectorNode connector = mock(ConnectorNode.class);
        when(connector.getIdentifier()).thenReturn("connector-1");
        when(connector.getCurrentState()).thenReturn(ConnectorState.TROUBLESHOOTING);
        repository.restoreConnector(connector);

        final IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> repository.applyUpdate(connector, mock(ConnectorUpdateContext.class)));
        assertTrue(thrown.getMessage().contains("Troubleshooting"),
                "Exception message should reference Troubleshooting: " + thrown.getMessage());

        // Provider must not be consulted; allowing shouldApplyUpdate to silently veto would let the framework return
        // success while the Connector was still in Troubleshooting.
        verify(provider, never()).shouldApplyUpdate(anyString());
        verify(connector, never()).transitionStateForUpdating();
    }

    // --- syncConnector tests ---

    @Test
    public void testSyncConnectorStoppedWithConfigChange() throws Exception {
        final FlowManager flowManager = mock(FlowManager.class);
        final StandardConnectorRepository repository = createRepositoryWithFlowManager(null, flowManager);

        final ConnectorNode connector = createConnectorNodeWithEmptyWorkingConfig("connector-1", "Test Connector");
        when(flowManager.createConnector(anyString(), eq("connector-1"), any(), anyBoolean(), anyBoolean())).thenReturn(connector);

        final VersionedConnector versioned = createVersionedConnector("connector-1", "Test Connector", VersionedConnectorState.RUNNING,
                List.of(createVersionedStep("step1", Map.of("prop1", createStringLiteralRef("value1")))));

        final ConnectorSyncResult result = repository.syncConnector(versioned);

        assertEquals(ConnectorSyncResult.Outcome.SYNCED, result.getOutcome());
        verify(connector).inheritConfiguration(any(), any(), any());
    }

    @Test
    public void testSyncConnectorStoppedNoConfigChange() throws Exception {
        final StandardConnectorRepository repository = createRepositoryWithProvider(null);

        final ConnectorNode connector = createConnectorNodeWithEmptyWorkingConfig("connector-1", "Test Connector");
        when(connector.getCurrentState()).thenReturn(ConnectorState.STOPPED);
        repository.restoreConnector(connector);

        final VersionedConnector versioned = createVersionedConnector("connector-1", "Test Connector", VersionedConnectorState.ENABLED, List.of());

        final ConnectorSyncResult result = repository.syncConnector(versioned);

        assertEquals(ConnectorSyncResult.Outcome.SYNCED_CONFIG_UNCHANGED, result.getOutcome());
        verify(connector, never()).inheritConfiguration(any(), any(), any());
    }

    @Test
    public void testSyncConnectorRunningWithConfigChange() throws Exception {
        final StandardConnectorRepository repository = createRepositoryWithProvider(null);

        final ConnectorNode connector = createConnectorNodeWithEmptyWorkingConfig("connector-1", "Test Connector");
        when(connector.getCurrentState()).thenReturn(ConnectorState.RUNNING);
        repository.restoreConnector(connector);

        final VersionedConnector versioned = createVersionedConnector("connector-1", "Test Connector", VersionedConnectorState.RUNNING,
                List.of(createVersionedStep("step1", Map.of("prop1", createStringLiteralRef("new-value")))));

        final ConnectorSyncResult result = repository.syncConnector(versioned);

        assertEquals(ConnectorSyncResult.Outcome.SYNCED, result.getOutcome());
        verify(connector).inheritConfiguration(any(), any(), any());
    }

    @Test
    public void testSyncConnectorDrainingIsRejected() throws Exception {
        final StandardConnectorRepository repository = createRepositoryWithProvider(null);

        final ConnectorNode connector = createConnectorNodeWithEmptyWorkingConfig("connector-1", "Test Connector");
        when(connector.getCurrentState()).thenReturn(ConnectorState.DRAINING);
        repository.restoreConnector(connector);

        final VersionedConnector versioned = createVersionedConnector("connector-1", "Test Connector", VersionedConnectorState.ENABLED, List.of());

        final ConnectorSyncResult result = repository.syncConnector(versioned);

        assertEquals(ConnectorSyncResult.Outcome.REJECTED, result.getOutcome());
        verify(connector).markInvalid(eq("Flow Synchronization Failure"), anyString());
        verify(connector, never()).inheritConfiguration(any(), any(), any());
    }

    @Test
    public void testSyncConnectorPreparingForUpdateIsRejected() throws Exception {
        final StandardConnectorRepository repository = createRepositoryWithProvider(null);

        final ConnectorNode connector = createConnectorNodeWithEmptyWorkingConfig("connector-1", "Test Connector");
        when(connector.getCurrentState()).thenReturn(ConnectorState.PREPARING_FOR_UPDATE);
        repository.restoreConnector(connector);

        final VersionedConnector versioned = createVersionedConnector("connector-1", "Test Connector", VersionedConnectorState.ENABLED, List.of());

        final ConnectorSyncResult result = repository.syncConnector(versioned);

        assertEquals(ConnectorSyncResult.Outcome.REJECTED, result.getOutcome());
        verify(connector).markInvalid(eq("Flow Synchronization Failure"), anyString());
    }

    @Test
    public void testSyncConnectorUpdatingIsRejected() throws Exception {
        final StandardConnectorRepository repository = createRepositoryWithProvider(null);

        final ConnectorNode connector = createConnectorNodeWithEmptyWorkingConfig("connector-1", "Test Connector");
        when(connector.getCurrentState()).thenReturn(ConnectorState.UPDATING);
        repository.restoreConnector(connector);

        final VersionedConnector versioned = createVersionedConnector("connector-1", "Test Connector", VersionedConnectorState.ENABLED, List.of());

        final ConnectorSyncResult result = repository.syncConnector(versioned);

        assertEquals(ConnectorSyncResult.Outcome.REJECTED, result.getOutcome());
    }

    @Test
    public void testSyncConnectorUpdateFailedRecovery() throws Exception {
        final StandardConnectorRepository repository = createRepositoryWithProvider(null);

        final ConnectorNode connector = createConnectorNodeWithEmptyWorkingConfig("connector-1", "Test Connector");
        when(connector.getCurrentState()).thenReturn(ConnectorState.UPDATE_FAILED);
        repository.restoreConnector(connector);

        final VersionedConnector versioned = createVersionedConnector("connector-1", "Test Connector", VersionedConnectorState.ENABLED,
                List.of(createVersionedStep("step1", Map.of("prop1", createStringLiteralRef("recovery-value")))));

        final ConnectorSyncResult result = repository.syncConnector(versioned);

        assertEquals(ConnectorSyncResult.Outcome.SYNCED, result.getOutcome());
        verify(connector).inheritConfiguration(any(), any(), any());
    }

    @Test
    public void testSyncConnectorUpdatedState() throws Exception {
        final FlowManager flowManager = mock(FlowManager.class);
        final StandardConnectorRepository repository = createRepositoryWithFlowManager(null, flowManager);

        final ConnectorNode connector = createConnectorNodeWithEmptyWorkingConfig("connector-1", "Test Connector");
        when(flowManager.createConnector(anyString(), eq("connector-1"), any(), anyBoolean(), anyBoolean())).thenReturn(connector);

        final VersionedConnector versioned = createVersionedConnector("connector-1", "Test Connector", VersionedConnectorState.RUNNING,
                List.of(createVersionedStep("step1", Map.of("prop1", createStringLiteralRef("value1")))));

        final ConnectorSyncResult result = repository.syncConnector(versioned);

        assertEquals(ConnectorSyncResult.Outcome.SYNCED, result.getOutcome());
    }

    @Test
    public void testSyncConnectorInheritConfigurationFailureWhenRunning() throws Exception {
        final StandardConnectorRepository repository = createRepositoryWithProvider(null);

        final ConnectorNode connector = createConnectorNodeWithEmptyWorkingConfig("connector-1", "Test Connector");
        when(connector.getCurrentState()).thenReturn(ConnectorState.RUNNING);
        doThrow(new FlowUpdateException("Simulated failure"))
                .when(connector).inheritConfiguration(any(), any(), any());
        repository.restoreConnector(connector);

        final VersionedConnector versioned = createVersionedConnector("connector-1", "Test Connector", VersionedConnectorState.RUNNING,
                List.of(createVersionedStep("step1", Map.of("prop1", createStringLiteralRef("value1")))));

        final ConnectorSyncResult result = repository.syncConnector(versioned);

        assertEquals(ConnectorSyncResult.Outcome.FAILED, result.getOutcome());
        verify(connector).stop(any());
    }

    @Test
    public void testSyncConnectorInheritConfigurationFailureWhenStopped() throws Exception {
        final StandardConnectorRepository repository = createRepositoryWithProvider(null);

        final ConnectorNode connector = createConnectorNodeWithEmptyWorkingConfig("connector-1", "Test Connector");
        when(connector.getCurrentState()).thenReturn(ConnectorState.STOPPED);
        doThrow(new FlowUpdateException("Simulated failure"))
                .when(connector).inheritConfiguration(any(), any(), any());
        repository.restoreConnector(connector);

        final VersionedConnector versioned = createVersionedConnector("connector-1", "Test Connector", VersionedConnectorState.RUNNING,
                List.of(createVersionedStep("step1", Map.of("prop1", createStringLiteralRef("value1")))));

        final ConnectorSyncResult result = repository.syncConnector(versioned);

        assertEquals(ConnectorSyncResult.Outcome.FAILED, result.getOutcome());
        verify(connector, never()).stop(any());
    }

    @Test
    public void testSyncConnectorProviderRejectsSync() throws Exception {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        when(provider.getSyncDirective(eq("connector-1"), any())).thenReturn(ConnectorSyncDirective.reject());
        final StandardConnectorRepository repository = createRepositoryWithProvider(provider);

        final ConnectorNode connector = createConnectorNodeWithEmptyWorkingConfig("connector-1", "Test Connector");
        when(connector.getCurrentState()).thenReturn(ConnectorState.STOPPED);
        repository.restoreConnector(connector);

        final VersionedConnector versioned = createVersionedConnector("connector-1", "Test Connector", VersionedConnectorState.ENABLED, List.of());

        final ConnectorSyncResult result = repository.syncConnector(versioned);

        assertEquals(ConnectorSyncResult.Outcome.REJECTED, result.getOutcome());
        verify(connector).markInvalid(eq("Flow Synchronization Failure"), anyString());
        verify(connector, never()).inheritConfiguration(any(), any(), any());
    }

    @Test
    public void testSyncConnectorProviderThrowsException() throws Exception {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        when(provider.getSyncDirective(eq("connector-1"), any()))
                .thenThrow(new ConnectorConfigurationProviderException("Provider error"));
        final StandardConnectorRepository repository = createRepositoryWithProvider(provider);

        final ConnectorNode connector = createConnectorNodeWithEmptyWorkingConfig("connector-1", "Test Connector");
        when(connector.getCurrentState()).thenReturn(ConnectorState.STOPPED);
        repository.restoreConnector(connector);

        final VersionedConnector versioned = createVersionedConnector("connector-1", "Test Connector", VersionedConnectorState.ENABLED, List.of());

        final ConnectorSyncResult result = repository.syncConnector(versioned);

        assertEquals(ConnectorSyncResult.Outcome.FAILED, result.getOutcome());
        verify(connector).markInvalid(eq("Flow Synchronization Failure"), anyString());
    }

    @Test
    public void testSyncConnectorStartingWaitsForRunning() throws Exception {
        final StandardConnectorRepository repository = createRepositoryWithShortTimeout(null);

        final ConnectorNode connector = createConnectorNodeWithEmptyWorkingConfig("connector-1", "Test Connector");
        when(connector.getCurrentState())
                .thenReturn(ConnectorState.STARTING)
                .thenReturn(ConnectorState.STARTING)
                .thenReturn(ConnectorState.RUNNING);
        repository.restoreConnector(connector);

        final VersionedConnector versioned = createVersionedConnector("connector-1", "Test Connector", VersionedConnectorState.RUNNING, List.of());

        final ConnectorSyncResult result = repository.syncConnector(versioned);

        assertEquals(ConnectorSyncResult.Outcome.SYNCED_CONFIG_UNCHANGED, result.getOutcome());
    }

    @Test
    public void testSyncConnectorStartingTimesOut() throws Exception {
        final StandardConnectorRepository repository = createRepositoryWithShortTimeout(null);

        final ConnectorNode connector = createConnectorNodeWithEmptyWorkingConfig("connector-1", "Test Connector");
        when(connector.getCurrentState()).thenReturn(ConnectorState.STARTING);
        repository.restoreConnector(connector);

        final VersionedConnector versioned = createVersionedConnector("connector-1", "Test Connector", VersionedConnectorState.RUNNING, List.of());

        final ConnectorSyncResult result = repository.syncConnector(versioned);

        assertEquals(ConnectorSyncResult.Outcome.REJECTED, result.getOutcome());
        verify(connector).markInvalid(eq("Flow Synchronization Failure"), anyString());
    }

    @Test
    public void testSyncConnectorProviderReturnsRemoveStopsAndRemoves() throws Exception {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        when(provider.getSyncDirective(eq("connector-1"), any())).thenReturn(ConnectorSyncDirective.remove());
        final StandardConnectorRepository repository = createRepositoryWithProvider(provider);

        final Connector mockExtension = mock(Connector.class);
        final ConnectorNode connector = createConnectorNodeWithEmptyWorkingConfig("connector-1", "Test Connector");
        when(connector.getCurrentState())
                .thenReturn(ConnectorState.RUNNING)
                .thenReturn(ConnectorState.RUNNING)
                .thenReturn(ConnectorState.STOPPED);
        when(connector.getConnector()).thenReturn(mockExtension);
        when(connector.stop(any())).thenReturn(CompletableFuture.completedFuture(null));
        repository.restoreConnector(connector);

        final VersionedConnector versioned = createVersionedConnector("connector-1", "Test Connector", VersionedConnectorState.ENABLED, List.of());

        final ConnectorSyncResult result = repository.syncConnector(versioned);

        assertEquals(ConnectorSyncResult.Outcome.REMOVED, result.getOutcome());
        verify(connector).stop(any());
        verify(connector).verifyCanDelete();
    }

    @Test
    public void testSyncConnectorProviderReturnsRemoveStoppedConnector() throws Exception {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        when(provider.getSyncDirective(eq("connector-1"), any())).thenReturn(ConnectorSyncDirective.remove());
        final StandardConnectorRepository repository = createRepositoryWithProvider(provider);

        final Connector mockExtension = mock(Connector.class);
        final ConnectorNode connector = createConnectorNodeWithEmptyWorkingConfig("connector-1", "Test Connector");
        when(connector.getCurrentState()).thenReturn(ConnectorState.STOPPED);
        when(connector.getConnector()).thenReturn(mockExtension);
        repository.restoreConnector(connector);

        final VersionedConnector versioned = createVersionedConnector("connector-1", "Test Connector", VersionedConnectorState.ENABLED, List.of());

        final ConnectorSyncResult result = repository.syncConnector(versioned);

        assertEquals(ConnectorSyncResult.Outcome.REMOVED, result.getOutcome());
        verify(connector, never()).stop(any());
        verify(connector).verifyCanDelete();
    }

    @Test
    public void testSyncConnectorProviderReturnsRemoveForNonExistent() throws Exception {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        when(provider.getSyncDirective(eq("connector-1"), any())).thenReturn(ConnectorSyncDirective.remove());
        final StandardConnectorRepository repository = createRepositoryWithProvider(provider);

        final VersionedConnector versioned = createVersionedConnector("connector-1", "Test Connector", VersionedConnectorState.ENABLED, List.of());

        final ConnectorSyncResult result = repository.syncConnector(versioned);

        assertEquals(ConnectorSyncResult.Outcome.REMOVED, result.getOutcome());
    }

    @Test
    public void testSyncConnectorProviderReturnsRejectCreatesNodeForNewConnector() throws Exception {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        when(provider.getSyncDirective(eq("connector-1"), any())).thenReturn(ConnectorSyncDirective.reject());

        final FlowManager flowManager = mock(FlowManager.class);
        final StandardConnectorRepository repository = createRepositoryWithFlowManagerAndProvider(provider, flowManager);

        final ConnectorNode connector = createConnectorNodeWithEmptyWorkingConfig("connector-1", "Test Connector");
        when(flowManager.createConnector(anyString(), eq("connector-1"), any(), anyBoolean(), anyBoolean())).thenReturn(connector);

        final VersionedConnector versioned = createVersionedConnector("connector-1", "Test Connector", VersionedConnectorState.ENABLED, List.of());

        final ConnectorSyncResult result = repository.syncConnector(versioned);

        assertEquals(ConnectorSyncResult.Outcome.REJECTED, result.getOutcome());
        verify(flowManager).createConnector(anyString(), eq("connector-1"), any(), anyBoolean(), anyBoolean());
        verify(connector).markInvalid(eq("Flow Synchronization Failure"), anyString());
    }

    @Test
    public void testSyncConnectorProviderAllowWithScheduledStateOverride() throws Exception {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        when(provider.getSyncDirective(eq("connector-1"), any()))
                .thenReturn(ConnectorSyncDirective.allow(null, VersionedConnectorState.ENABLED));
        final StandardConnectorRepository repository = createRepositoryWithProvider(provider);

        final ConnectorNode connector = createConnectorNodeWithEmptyWorkingConfig("connector-1", "Test Connector");
        when(connector.getCurrentState()).thenReturn(ConnectorState.STOPPED);
        repository.restoreConnector(connector);

        final VersionedConnector versioned = createVersionedConnector("connector-1", "Test Connector", VersionedConnectorState.RUNNING, List.of());

        final ConnectorSyncResult result = repository.syncConnector(versioned);

        assertEquals(ConnectorSyncResult.Outcome.SYNCED_CONFIG_UNCHANGED, result.getOutcome());
        assertEquals(VersionedConnectorState.ENABLED, result.getEffectiveScheduledState());
    }

    @Test
    public void testSyncConnectorTroubleshootingInheritsConfigurationThenOverlaysSnapshotThenEntersTroubleshooting() throws Exception {
        final StandardConnectorRepository repository = createRepositoryWithProvider(null);

        final ConnectorNode connector = createConnectorNodeWithEmptyWorkingConfig("connector-1", "Test Connector");
        when(connector.getCurrentState()).thenReturn(ConnectorState.STOPPED);
        repository.restoreConnector(connector);

        final VersionedProcessGroup persistedManagedGroup = new VersionedProcessGroup();
        // The Connector's managed Parameter Context is not registered with the global ParameterContextManager and is
        // therefore not persisted in flow.json. When restoring a Connector whose effective scheduled state is
        // TROUBLESHOOTING, the framework must run inheritConfiguration so the Connector re-populates its in-memory
        // Parameter Context from the persisted active configuration. The Connector-supplied flow shape that produces
        // is then overlaid by restoreTroubleshootingFlow with the user's persisted snapshot before the FlowFile Repository
        // attaches FlowFiles to queues, so the transient Connector flow shape is invisible to data movement.
        final List<VersionedConfigurationStep> activeConfig = List.of(
                createVersionedStep("step1", Map.of("prop1", createStringLiteralRef("new-value"))));
        final VersionedConnector versioned = createVersionedConnector("connector-1", "Test Connector",
                VersionedConnectorState.TROUBLESHOOTING, activeConfig);
        versioned.setManagedProcessGroup(persistedManagedGroup);

        final ConnectorSyncResult result = repository.syncConnector(versioned);

        assertEquals(ConnectorSyncResult.Outcome.SYNCED_CONFIG_UNCHANGED, result.getOutcome());
        assertEquals(VersionedConnectorState.TROUBLESHOOTING, result.getEffectiveScheduledState());

        final FrameworkFlowContext activeFlowContext = connector.getActiveFlowContext();
        final InOrder inOrder = inOrder(connector, activeFlowContext);
        inOrder.verify(connector).inheritConfiguration(eq(activeConfig), eq(activeConfig), any());
        inOrder.verify(activeFlowContext).restoreTroubleshootingFlow(persistedManagedGroup);
        inOrder.verify(connector).restoreTroubleshootingState();
    }

    @Test
    public void testSyncConnectorTroubleshootingPreservesExistingTroubleshootingState() throws Exception {
        final StandardConnectorRepository repository = createRepositoryWithProvider(null);

        final ConnectorNode connector = createConnectorNodeWithEmptyWorkingConfig("connector-1", "Test Connector");
        when(connector.getCurrentState()).thenReturn(ConnectorState.TROUBLESHOOTING);
        repository.restoreConnector(connector);

        final VersionedProcessGroup persistedManagedGroup = new VersionedProcessGroup();
        final VersionedConnector versioned = createVersionedConnector("connector-1", "Test Connector",
                VersionedConnectorState.TROUBLESHOOTING, List.of());
        versioned.setManagedProcessGroup(persistedManagedGroup);

        final ConnectorSyncResult result = repository.syncConnector(versioned);

        assertEquals(ConnectorSyncResult.Outcome.SYNCED_CONFIG_UNCHANGED, result.getOutcome());
        verify(connector).inheritConfiguration(eq(List.of()), eq(List.of()), any());
        verify(connector.getActiveFlowContext()).restoreTroubleshootingFlow(persistedManagedGroup);
        // Connector is already in TROUBLESHOOTING; restoreTroubleshootingState should not be invoked again.
        verify(connector, never()).restoreTroubleshootingState();
    }

    @Test
    public void testSyncConnectorTroubleshootingWithoutSnapshotLeavesManagedGroupUnchanged() throws Exception {
        final StandardConnectorRepository repository = createRepositoryWithProvider(null);

        final ConnectorNode connector = createConnectorNodeWithEmptyWorkingConfig("connector-1", "Test Connector");
        when(connector.getCurrentState()).thenReturn(ConnectorState.STOPPED);
        repository.restoreConnector(connector);

        final VersionedConnector versioned = createVersionedConnector("connector-1", "Test Connector",
                VersionedConnectorState.TROUBLESHOOTING, List.of());

        final ConnectorSyncResult result = repository.syncConnector(versioned);

        assertEquals(ConnectorSyncResult.Outcome.SYNCED_CONFIG_UNCHANGED, result.getOutcome());
        verify(connector).inheritConfiguration(eq(List.of()), eq(List.of()), any());
        verify(connector.getActiveFlowContext(), never()).restoreTroubleshootingFlow(any());
        verify(connector).restoreTroubleshootingState();
    }

    @Test
    public void testSyncConnectorTroubleshootingSnapshotRestoreFailureMarksInvalid() throws Exception {
        final StandardConnectorRepository repository = createRepositoryWithProvider(null);

        final ConnectorNode connector = createConnectorNodeWithEmptyWorkingConfig("connector-1", "Test Connector");
        when(connector.getCurrentState()).thenReturn(ConnectorState.STOPPED);
        final FrameworkFlowContext activeFlowContext = connector.getActiveFlowContext();
        doThrow(new RuntimeException("Snapshot restore failure")).when(activeFlowContext).restoreTroubleshootingFlow(any());
        repository.restoreConnector(connector);

        final VersionedProcessGroup persistedManagedGroup = new VersionedProcessGroup();
        final VersionedConnector versioned = createVersionedConnector("connector-1", "Test Connector",
                VersionedConnectorState.TROUBLESHOOTING, List.of());
        versioned.setManagedProcessGroup(persistedManagedGroup);

        final ConnectorSyncResult result = repository.syncConnector(versioned);

        assertEquals(ConnectorSyncResult.Outcome.FAILED, result.getOutcome());
        verify(connector).markInvalid(eq("Flow Synchronization Failure"), anyString());
        verify(connector, never()).restoreTroubleshootingState();
    }

    @Test
    public void testSyncConnectorTroubleshootingInheritConfigurationFailureSkipsSnapshotRestore() throws Exception {
        final StandardConnectorRepository repository = createRepositoryWithProvider(null);

        final ConnectorNode connector = createConnectorNodeWithEmptyWorkingConfig("connector-1", "Test Connector");
        when(connector.getCurrentState()).thenReturn(ConnectorState.STOPPED);
        doThrow(new FlowUpdateException("Inherit failure"))
                .when(connector).inheritConfiguration(any(), any(), any());
        repository.restoreConnector(connector);

        final VersionedProcessGroup persistedManagedGroup = new VersionedProcessGroup();
        final VersionedConnector versioned = createVersionedConnector("connector-1", "Test Connector",
                VersionedConnectorState.TROUBLESHOOTING, List.of());
        versioned.setManagedProcessGroup(persistedManagedGroup);

        final ConnectorSyncResult result = repository.syncConnector(versioned);

        assertEquals(ConnectorSyncResult.Outcome.FAILED, result.getOutcome());
        // inheritConfiguration ran first and failed; the managed flow snapshot must not be overlaid and the
        // Connector must not transition into TROUBLESHOOTING.
        verify(connector.getActiveFlowContext(), never()).restoreTroubleshootingFlow(any());
        verify(connector, never()).restoreTroubleshootingState();
    }

    @Test
    public void testSyncConnectorProviderAllowWithWorkingConfig() throws Exception {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);

        final ConnectorWorkingConfiguration providerConfig = new ConnectorWorkingConfiguration();
        providerConfig.setName("Provider Name");
        providerConfig.setWorkingFlowConfiguration(List.of(
                createVersionedStep("step1", Map.of("prop1", createStringLiteralRef("provider-value")))));
        when(provider.getSyncDirective(eq("connector-1"), any()))
                .thenReturn(ConnectorSyncDirective.allow(providerConfig));
        final StandardConnectorRepository repository = createRepositoryWithProvider(provider);

        final ConnectorNode connector = createConnectorNodeWithEmptyWorkingConfig("connector-1", "Test Connector");
        when(connector.getCurrentState()).thenReturn(ConnectorState.STOPPED);
        repository.restoreConnector(connector);

        final VersionedConnector versioned = createVersionedConnector("connector-1", "Test Connector", VersionedConnectorState.ENABLED, List.of());

        final ConnectorSyncResult result = repository.syncConnector(versioned);

        assertEquals(ConnectorSyncResult.Outcome.SYNCED, result.getOutcome());
        verify(connector).setName("Provider Name");
        verify(connector).inheritConfiguration(any(), any(), any());
    }

    // --- findProviderIdByName ---

    @Test
    public void testFindProviderIdByNameResolvesUniqueMatch() {
        final ParameterProviderNode snowflake = mockParameterProvider("Snowflake Provider", "id-1");
        final ParameterProviderNode vault = mockParameterProvider("Vault Provider", "id-2");
        final FlowManager flowManager = mock(FlowManager.class);
        when(flowManager.getAllParameterProviders()).thenReturn(Set.of(snowflake, vault));

        final StandardConnectorRepository repository = createRepositoryWithFlowManager(mock(ConnectorConfigurationProvider.class), flowManager);

        assertEquals("id-1", repository.findProviderIdByName("Snowflake Provider"));
        assertEquals("id-2", repository.findProviderIdByName("Vault Provider"));
    }

    @Test
    public void testFindProviderIdByNameReturnsNullWhenNoMatch() {
        final ParameterProviderNode snowflake = mockParameterProvider("Snowflake Provider", "id-1");
        final FlowManager flowManager = mock(FlowManager.class);
        when(flowManager.getAllParameterProviders()).thenReturn(Set.of(snowflake));

        final StandardConnectorRepository repository = createRepositoryWithFlowManager(mock(ConnectorConfigurationProvider.class), flowManager);

        assertNull(repository.findProviderIdByName("Nonexistent Provider"));
    }

    @Test
    public void testFindProviderIdByNameReturnsNullWhenAmbiguousMatch() {
        // Two parameter providers share the same name; the resolution must be refused so that the
        // resulting SECRET_REFERENCE is surfaced as invalid in the UI rather than silently bound
        // to whichever provider happens to come first.
        final ParameterProviderNode dup1 = mockParameterProvider("Duplicate", "id-a");
        final ParameterProviderNode dup2 = mockParameterProvider("Duplicate", "id-b");
        final FlowManager flowManager = mock(FlowManager.class);
        when(flowManager.getAllParameterProviders()).thenReturn(Set.of(dup1, dup2));

        final StandardConnectorRepository repository = createRepositoryWithFlowManager(mock(ConnectorConfigurationProvider.class), flowManager);

        assertNull(repository.findProviderIdByName("Duplicate"));
    }

    @Test
    public void testFindProviderIdByNameReturnsNullForNullInput() {
        final ParameterProviderNode snowflake = mockParameterProvider("Snowflake Provider", "id-1");
        final FlowManager flowManager = mock(FlowManager.class);
        when(flowManager.getAllParameterProviders()).thenReturn(Set.of(snowflake));

        final StandardConnectorRepository repository = createRepositoryWithFlowManager(mock(ConnectorConfigurationProvider.class), flowManager);

        assertNull(repository.findProviderIdByName(null));
    }

    @Test
    public void testFindProviderIdByNameReturnsNullWhenNoProvidersRegistered() {
        final FlowManager flowManager = mock(FlowManager.class);
        when(flowManager.getAllParameterProviders()).thenReturn(Set.of());

        final StandardConnectorRepository repository = createRepositoryWithFlowManager(mock(ConnectorConfigurationProvider.class), flowManager);

        assertNull(repository.findProviderIdByName("Anything"));
    }

    private static ParameterProviderNode mockParameterProvider(final String name, final String id) {
        final ParameterProviderNode node = mock(ParameterProviderNode.class);
        when(node.getName()).thenReturn(name);
        when(node.getIdentifier()).thenReturn(id);
        return node;
    }

    // --- provider-sourced logging attribute application (add/restore) tests ---

    @Test
    public void testRestoreConnectorAppliesEmptyAttributesWhenNoProvider() {
        final StandardConnectorRepository repository = new StandardConnectorRepository();
        final ConnectorRepositoryInitializationContext initContext = mock(ConnectorRepositoryInitializationContext.class);
        when(initContext.getExtensionManager()).thenReturn(mock(ExtensionManager.class));
        when(initContext.getConnectorConfigurationProvider()).thenReturn(null);
        repository.initialize(initContext);

        final ConnectorNode connector = createSimpleConnectorNode("connector-1", "Connector 1");
        repository.restoreConnector(connector);

        verify(connector).setCustomLoggingAttributes(Map.of());
    }

    @Test
    public void testRestoreConnectorAppliesProviderLoggingAttributes() {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        final Map<String, String> expected = Map.of("connectorDefinitionId", "snowflake.runtime.postgres-cdc");
        when(provider.getLoggingAttributes("connector-1")).thenReturn(expected);

        final StandardConnectorRepository repository = createRepositoryWithProvider(provider);

        final ConnectorNode connector = createSimpleConnectorNode("connector-1", "Connector 1");
        repository.restoreConnector(connector);

        verify(provider).getLoggingAttributes("connector-1");
        verify(connector).setCustomLoggingAttributes(expected);
    }

    @Test
    public void testAddConnectorAppliesProviderLoggingAttributes() {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        final Map<String, String> expected = Map.of("connectorDefinitionId", "snowflake.runtime.postgres-cdc");
        when(provider.getLoggingAttributes("connector-1")).thenReturn(expected);

        final StandardConnectorRepository repository = createRepositoryWithProvider(provider);

        final ConnectorNode connector = createSimpleConnectorNode("connector-1", "Connector 1");
        repository.addConnector(connector);

        verify(provider).getLoggingAttributes("connector-1");
        verify(connector).setCustomLoggingAttributes(expected);
    }

    @Test
    public void testRestoreConnectorAppliesEmptyAttributesWhenProviderReturnsNull() {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        when(provider.getLoggingAttributes(anyString())).thenReturn(null);

        final StandardConnectorRepository repository = createRepositoryWithProvider(provider);

        final ConnectorNode connector = createSimpleConnectorNode("connector-1", "Connector 1");
        repository.restoreConnector(connector);

        verify(connector).setCustomLoggingAttributes(Map.of());
    }

    @Test
    public void testRestoreConnectorAppliesEmptyAttributesWhenProviderThrows() {
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        when(provider.getLoggingAttributes(anyString())).thenThrow(new RuntimeException("provider boom"));

        final StandardConnectorRepository repository = createRepositoryWithProvider(provider);

        final ConnectorNode connector = createSimpleConnectorNode("connector-1", "Connector 1");
        repository.restoreConnector(connector);

        verify(connector).setCustomLoggingAttributes(Map.of());
    }

    @Test
    public void testRestoreConnectorAppliesAllAttributesWhenAboveCardinalityThreshold() {
        // More than the soft cardinality threshold (5): the framework logs a WARN but must not drop attributes.
        final Map<String, String> expected = Map.of(
                "attr1", "v1",
                "attr2", "v2",
                "attr3", "v3",
                "attr4", "v4",
                "attr5", "v5",
                "attr6", "v6"
        );
        final ConnectorConfigurationProvider provider = mock(ConnectorConfigurationProvider.class);
        when(provider.getLoggingAttributes("connector-1")).thenReturn(expected);

        final StandardConnectorRepository repository = createRepositoryWithProvider(provider);

        final ConnectorNode connector = createSimpleConnectorNode("connector-1", "Connector 1");
        repository.restoreConnector(connector);

        verify(connector).setCustomLoggingAttributes(expected);
    }

    // --- Helper Methods ---

    private StandardConnectorRepository createRepositoryWithProviderAndAssetManager(
            final ConnectorConfigurationProvider provider, final AssetManager assetManager) {
        final StandardConnectorRepository repository = new StandardConnectorRepository();
        final ConnectorRepositoryInitializationContext initContext = mock(ConnectorRepositoryInitializationContext.class);
        when(initContext.getFlowManager()).thenReturn(mock(FlowManager.class));
        when(initContext.getExtensionManager()).thenReturn(mock(ExtensionManager.class));
        when(initContext.getAssetManager()).thenReturn(assetManager);
        when(initContext.getConnectorConfigurationProvider()).thenReturn(provider);
        repository.initialize(initContext);
        return repository;
    }

    private StandardConnectorRepository createRepositoryWithProvider(final ConnectorConfigurationProvider provider) {
        return createRepositoryWithFlowManager(provider, mock(FlowManager.class));
    }

    private StandardConnectorRepository createRepositoryWithFlowManager(final ConnectorConfigurationProvider provider, final FlowManager flowManager) {
        final StandardConnectorRepository repository = new StandardConnectorRepository();
        final ConnectorRepositoryInitializationContext initContext = mock(ConnectorRepositoryInitializationContext.class);
        when(initContext.getFlowManager()).thenReturn(flowManager);
        when(initContext.getExtensionManager()).thenReturn(mock(ExtensionManager.class));
        when(initContext.getAssetManager()).thenReturn(mock(AssetManager.class));
        when(initContext.getConnectorConfigurationProvider()).thenReturn(provider);
        repository.initialize(initContext);
        return repository;
    }

    private StandardConnectorRepository createRepositoryWithFlowManagerAndProvider(
            final ConnectorConfigurationProvider provider, final FlowManager flowManager) {
        return createRepositoryWithFlowManager(provider, flowManager);
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

    private VersionedConnector createVersionedConnector(final String id, final String name, final VersionedConnectorState scheduledState,
                                                        final List<VersionedConfigurationStep> activeConfig) {
        final VersionedConnector vc = new VersionedConnector();
        vc.setInstanceIdentifier(id);
        vc.setName(name);
        vc.setScheduledState(scheduledState);
        vc.setActiveFlowConfiguration(activeConfig);
        vc.setWorkingFlowConfiguration(activeConfig);
        final Bundle bundle = new Bundle();
        bundle.setGroup("org.apache.nifi");
        bundle.setArtifact("nifi-test-connector-nar");
        bundle.setVersion("2.0.0");
        vc.setBundle(bundle);
        vc.setType("org.apache.nifi.test.TestConnector");
        return vc;
    }

    private StandardConnectorNode createRealConnectorNode(final String identifier, final Connector connector) throws FlowUpdateException {
        final ExtensionManager extensionManager = mock(ExtensionManager.class);
        final AssetManager assetManager = mock(AssetManager.class);
        final SecretsManager secretsManager = mock(SecretsManager.class);
        when(secretsManager.getAllSecrets()).thenReturn(List.of());
        when(secretsManager.getSecrets(anySet())).thenReturn(Collections.emptyMap());

        final ProcessGroup managedProcessGroup = mock(ProcessGroup.class);
        when(managedProcessGroup.purge()).thenReturn(CompletableFuture.completedFuture(null));
        when(managedProcessGroup.getQueueSize()).thenReturn(new QueueSize(0, 0L));

        final FlowContextFactory flowContextFactory = new FlowContextFactory() {
            @Override
            public FrameworkFlowContext createActiveFlowContext(final String connectorId, final ComponentLog connectorLogger, final Bundle bundle) {
                final MutableConnectorConfigurationContext activeConfigurationContext =
                        new StandardConnectorConfigurationContext(assetManager, secretsManager);
                return new StandardFlowContext(managedProcessGroup, activeConfigurationContext,
                        mock(ProcessGroupFacadeFactory.class), mock(ParameterContextFacadeFactory.class),
                        connectorLogger, FlowContextType.ACTIVE, bundle);
            }

            @Override
            public FrameworkFlowContext createWorkingFlowContext(final String connectorId, final ComponentLog connectorLogger,
                    final MutableConnectorConfigurationContext currentConfiguration, final Bundle bundle) {
                return new StandardFlowContext(managedProcessGroup, currentConfiguration,
                        mock(ProcessGroupFacadeFactory.class), mock(ParameterContextFacadeFactory.class),
                        connectorLogger, FlowContextType.WORKING, bundle);
            }
        };

        final ConnectorStateTransition stateTransition = new StandardConnectorStateTransition("TestConnector-" + identifier);
        final ConnectorValidationTrigger validationTrigger = new ConnectorValidationTrigger() {
            @Override
            public void triggerAsync(final ConnectorNode node) {
                node.performValidation();
            }

            @Override
            public void trigger(final ConnectorNode node) {
                node.performValidation();
            }
        };

        final ComponentLog componentLog = new MockComponentLog("TestConnector", connector);
        final BundleCoordinate bundleCoordinate = new BundleCoordinate("org.apache.nifi", "test-standard-connector-node", "1.0.0");
        final ConnectorDetails connectorDetails = new ConnectorDetails(connector, bundleCoordinate, componentLog);

        final StandardConnectorNode node = new StandardConnectorNode(
                identifier, mock(FlowManager.class), extensionManager, null, connectorDetails,
                "TestConnector", connector.getClass().getCanonicalName(),
                new StandardConnectorConfigurationContext(assetManager, secretsManager),
                stateTransition, flowContextFactory, validationTrigger, false);

        final FrameworkConnectorInitializationContext initializationContext = mock(FrameworkConnectorInitializationContext.class);
        when(initializationContext.getSecretsManager()).thenReturn(secretsManager);
        when(initializationContext.getAssetManager()).thenReturn(assetManager);

        node.initializeConnector(initializationContext);
        node.loadInitialFlow();
        return node;
    }

    private void seedWorkingConfiguration(final StandardConnectorNode connectorNode, final String stepName,
            final Map<String, String> properties) throws FlowUpdateException {
        final Map<String, ConnectorValueReference> references = new HashMap<>();
        for (final Map.Entry<String, String> entry : properties.entrySet()) {
            references.put(entry.getKey(), new StringLiteralValue(entry.getValue()));
        }
        connectorNode.transitionStateForUpdating();
        connectorNode.prepareForUpdate();
        connectorNode.setConfiguration(stepName, new StepConfiguration(references));
        connectorNode.applyUpdate();
    }

    private StandardConnectorRepository createRepositoryWithShortTimeout(final ConnectorConfigurationProvider provider) {
        final StandardConnectorRepository repository = new StandardConnectorRepository();
        final ConnectorRepositoryInitializationContext initContext = mock(ConnectorRepositoryInitializationContext.class);
        when(initContext.getFlowManager()).thenReturn(mock(FlowManager.class));
        when(initContext.getExtensionManager()).thenReturn(mock(ExtensionManager.class));
        when(initContext.getAssetManager()).thenReturn(mock(AssetManager.class));
        when(initContext.getConnectorConfigurationProvider()).thenReturn(provider);
        when(initContext.getConnectorSyncTimeout()).thenReturn(Duration.ofSeconds(5));
        repository.initialize(initContext);
        return repository;
    }

    /**
     * Test connector that tracks invocations of onStepConfigured so tests can assert which
     * configuration steps were notified of changes.
     */
    private static class TrackingConnector extends AbstractConnector {
        private final Set<String> onStepConfiguredCalls = new HashSet<>();

        @Override
        public VersionedExternalFlow getInitialFlow() {
            return null;
        }

        @Override
        public VersionedExternalFlow getActiveFlow(final FlowContext activeFlowContext) {
            return null;
        }

        @Override
        public void prepareForUpdate(final FlowContext workingContext, final FlowContext activeContext) {
        }

        @Override
        public List<ConfigurationStep> getConfigurationSteps() {
            return List.of();
        }

        @Override
        public void applyUpdate(final FlowContext workingContext, final FlowContext activeContext) {
        }

        @Override
        protected void onStepConfigured(final String stepName, final FlowContext workingContext) {
            onStepConfiguredCalls.add(stepName);
        }

        @Override
        public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final Map<String, String> overrides, final FlowContext flowContext) {
            return List.of();
        }

        public boolean wasOnStepConfiguredCalled(final String stepName) {
            return onStepConfiguredCalls.contains(stepName);
        }

        public void reset() {
            onStepConfiguredCalls.clear();
        }
    }
}
