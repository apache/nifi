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
import org.apache.nifi.components.connector.secrets.SecretsManager;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.controller.ClusterTopologyProvider;
import org.apache.nifi.flow.VersionedComponentState;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestStandardFrameworkConnectorMigrationContext {

    private static final String CONNECTOR_ID = "connector-1";
    private static final String SOURCE_ASSET_ID = "source-asset-1";
    private static final String SOURCE_ASSET_NAME = "driver.jar";

    private AssetManager sourceAssetManager;
    private ConnectorRepository connectorRepository;
    private StateManagerProvider stateManagerProvider;
    private ClusterTopologyProvider clusterTopologyProvider;
    private VersionedExternalFlow sourceFlow;

    @BeforeEach
    public void setup() {
        sourceAssetManager = mock(AssetManager.class);
        connectorRepository = mock(ConnectorRepository.class);
        stateManagerProvider = mock(StateManagerProvider.class);
        clusterTopologyProvider = mock(ClusterTopologyProvider.class);
        sourceFlow = mock(VersionedExternalFlow.class);
    }

    @Test
    public void testCopyAssetFromSourceReturnsReferenceToCopiedAsset(@TempDir final Path tempDir) throws Exception {
        final File sourceFile = createAssetFile(tempDir, "source-contents");
        final Asset sourceAsset = mockAsset(SOURCE_ASSET_ID, SOURCE_ASSET_NAME, sourceFile);
        when(sourceAssetManager.getAsset(SOURCE_ASSET_ID)).thenReturn(Optional.of(sourceAsset));
        when(connectorRepository.getAsset(anyString())).thenReturn(Optional.empty());

        final File copiedFile = createAssetFile(tempDir, "copied-contents");
        when(connectorRepository.storeAsset(eq(CONNECTOR_ID), anyString(), eq(SOURCE_ASSET_NAME), any(InputStream.class)))
                .thenAnswer(invocation -> mockAsset(invocation.getArgument(1), SOURCE_ASSET_NAME, copiedFile));

        final StandardFrameworkConnectorMigrationContext context = createContext(true);

        final AssetReference reference = context.copyAssetFromSource(SOURCE_ASSET_ID);

        assertNotNull(reference);
        assertEquals(1, reference.getAssetIdentifiers().size());
        final String copiedAssetId = reference.getAssetIdentifiers().iterator().next();
        assertEquals(Set.of(copiedAssetId), context.getCopiedAssetIds());
        verify(connectorRepository).storeAsset(eq(CONNECTOR_ID), eq(copiedAssetId), eq(SOURCE_ASSET_NAME), any(InputStream.class));
    }

    @Test
    public void testCopyAssetFromSourceReturnsEmptyReferenceWhenSourceAssetIsMissing() throws Exception {
        when(sourceAssetManager.getAsset(SOURCE_ASSET_ID)).thenReturn(Optional.empty());
        when(connectorRepository.getAsset(anyString())).thenReturn(Optional.empty());

        final StandardFrameworkConnectorMigrationContext context = createContext(true);

        final AssetReference reference = context.copyAssetFromSource(SOURCE_ASSET_ID);

        assertNotNull(reference);
        assertTrue(reference.getAssetIdentifiers().isEmpty(),
                "Asset reference must be empty when the source asset cannot be located");
        assertTrue(context.getCopiedAssetIds().isEmpty(),
                "Copied asset bookkeeping must remain empty when no asset is copied");
        verify(connectorRepository, never()).storeAsset(anyString(), anyString(), anyString(), any(InputStream.class));
    }

    @Test
    public void testCopyAssetFromSourceReusesPreviouslyCopiedAsset(@TempDir final Path tempDir) throws Exception {
        final File copiedFile = createAssetFile(tempDir, "previously-copied");
        final Asset existing = mockAsset("already-copied", SOURCE_ASSET_NAME, copiedFile);
        when(connectorRepository.getAsset(anyString())).thenReturn(Optional.of(existing));

        final StandardFrameworkConnectorMigrationContext context = createContext(true);

        final AssetReference reference = context.copyAssetFromSource(SOURCE_ASSET_ID);

        assertNotNull(reference);
        assertEquals(1, reference.getAssetIdentifiers().size());
        verify(sourceAssetManager, never()).getAsset(anyString());
        verify(connectorRepository, never()).storeAsset(anyString(), anyString(), anyString(), any(InputStream.class));
    }

    @Test
    public void testCopyAssetFromSourceRejectsUploadedPayloadMigration() {
        final StandardFrameworkConnectorMigrationContext context = createContext(false);

        final IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> context.copyAssetFromSource(SOURCE_ASSET_ID));
        assertTrue(thrown.getMessage().contains("local Versioned Process Group"));
    }

    @Test
    public void testCopyAssetFromSourceRejectsBlankSourceAssetIdentifier() {
        final StandardFrameworkConnectorMigrationContext context = createContext(true);

        assertThrows(IllegalArgumentException.class, () -> context.copyAssetFromSource(null));
        assertThrows(IllegalArgumentException.class, () -> context.copyAssetFromSource(""));
        assertThrows(IllegalArgumentException.class, () -> context.copyAssetFromSource("   "));
    }

    @Test
    public void testSetPropertiesMergesOntoWorkingConfiguration() {
        final StandardFrameworkConnectorMigrationContext context = createContext(true);

        context.setProperties("Step One", Map.of("p1", "v1"));
        context.setProperties("Step One", Map.of("p2", "v2"));

        final MutableConnectorConfigurationContext merged = context.getMergedConfiguration();
        assertEquals("v1", merged.getProperty("Step One", "p1").getValue());
        assertEquals("v2", merged.getProperty("Step One", "p2").getValue());
        assertEquals(Set.of("p1", "p2"), merged.getPropertyNames("Step One"));
    }

    @Test
    public void testReplacePropertiesReplacesEntireStepOnWorkingConfiguration() {
        final StandardFrameworkConnectorMigrationContext context = createContext(true);

        context.setProperties("Step One", Map.of("p1", "v1"));
        context.replaceProperties("Step One", Map.of("p2", "v2"));

        final MutableConnectorConfigurationContext merged = context.getMergedConfiguration();
        assertNull(merged.getProperty("Step One", "p1").getValue(),
                "replaceProperties must drop properties not present in the replacement map");
        assertEquals("v2", merged.getProperty("Step One", "p2").getValue());
        assertEquals(Set.of("p2"), merged.getPropertyNames("Step One"));
    }

    @Test
    public void testSetValueReferenceMergesOntoWorkingConfiguration(@TempDir final Path tempDir) throws IOException {
        final File assetFile = createAssetFile(tempDir, "asset-value");
        final Asset asset = mockAsset("asset-1", "driver.jar", assetFile);
        when(sourceAssetManager.getAsset("asset-1")).thenReturn(Optional.of(asset));

        final StandardFrameworkConnectorMigrationContext context = createContext(true);
        context.setProperties("Step One", Map.of("p1", "v1"));
        context.setValueReference("Step One", "asset-prop", new AssetReference(Set.of("asset-1")));

        final MutableConnectorConfigurationContext merged = context.getMergedConfiguration();
        assertEquals("v1", merged.getProperty("Step One", "p1").getValue());
        assertEquals(assetFile.getAbsolutePath(), merged.getProperty("Step One", "asset-prop").getValue());
        assertEquals(Set.of("p1", "asset-prop"), merged.getPropertyNames("Step One"));

        // A null value reference removes the property from the step while leaving the other properties intact.
        context.setValueReference("Step One", "asset-prop", null);
        assertNull(merged.getProperty("Step One", "asset-prop").getValue());
        assertEquals("v1", merged.getProperty("Step One", "p1").getValue());
        assertEquals(Set.of("p1"), merged.getPropertyNames("Step One"));
    }

    @Test
    public void testNullValueRemovesPropertyFromStep() {
        final StandardFrameworkConnectorMigrationContext context = createContext(true);

        context.setProperties("Step", Map.of("keep", "keep-value", "drop", "drop-value"));

        final MutableConnectorConfigurationContext merged = context.getMergedConfiguration();
        assertEquals(Set.of("keep", "drop"), merged.getPropertyNames("Step"));

        // A null value in the String-based setProperties removes the property rather than storing a null value.
        final Map<String, String> stringUpdate = new HashMap<>();
        stringUpdate.put("drop", null);
        context.setProperties("Step", stringUpdate);
        assertEquals(Set.of("keep"), merged.getPropertyNames("Step"));
        assertNull(merged.getProperty("Step", "drop").getValue());

        context.setValueReferences("Step", Map.of("keep2", new StringLiteralValue("v2")));
        assertEquals(Set.of("keep", "keep2"), merged.getPropertyNames("Step"));

        // A null value reference in setValueReferences likewise removes the property.
        final Map<String, ConnectorValueReference> referenceUpdate = new HashMap<>();
        referenceUpdate.put("keep2", null);
        context.setValueReferences("Step", referenceUpdate);
        assertEquals(Set.of("keep"), merged.getPropertyNames("Step"));
    }

    @Test
    public void testSetValueReferencesMergesMultipleReferences(@TempDir final Path tempDir) throws IOException {
        final File assetFile = createAssetFile(tempDir, "multi-asset");
        final Asset asset = mockAsset("asset-2", "lib.jar", assetFile);
        when(sourceAssetManager.getAsset("asset-2")).thenReturn(Optional.of(asset));

        final StandardFrameworkConnectorMigrationContext context = createContext(true);
        final Map<String, ConnectorValueReference> references = new HashMap<>();
        references.put("literal-prop", new StringLiteralValue("literal-value"));
        references.put("asset-prop", new AssetReference(Set.of("asset-2")));
        context.setValueReferences("Step Two", references);

        final MutableConnectorConfigurationContext merged = context.getMergedConfiguration();
        assertEquals("literal-value", merged.getProperty("Step Two", "literal-prop").getValue());
        assertEquals(assetFile.getAbsolutePath(), merged.getProperty("Step Two", "asset-prop").getValue());
        assertEquals(Set.of("literal-prop", "asset-prop"), merged.getPropertyNames("Step Two"));
    }

    @Test
    public void testSetComponentStateBlockedInConfigurationPhase() {
        final StandardFrameworkConnectorMigrationContext context = createContext(true);
        final VersionedComponentState desiredState = new VersionedComponentState();
        desiredState.setClusterState(Map.of("k", "v"));
        desiredState.setLocalNodeStates(List.of());

        final IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> context.setComponentState("component-1", desiredState));
        assertTrue(thrown.getMessage().contains("migrateState"), thrown.getMessage());
    }

    @Test
    public void testConfigurationWritesBlockedInStatePhase() {
        final StandardFrameworkConnectorMigrationContext context = createContext(true);
        context.setPhase(StandardFrameworkConnectorMigrationContext.Phase.STATE);

        final IllegalStateException setProps = assertThrows(IllegalStateException.class,
                () -> context.setProperties("Step", Map.of("p", "v")));
        assertTrue(setProps.getMessage().contains("migrateConfiguration"), setProps.getMessage());

        final IllegalStateException replaceProps = assertThrows(IllegalStateException.class,
                () -> context.replaceProperties("Step", Map.of("p", "v")));
        assertTrue(replaceProps.getMessage().contains("migrateConfiguration"), replaceProps.getMessage());

        final IllegalStateException setValueRef = assertThrows(IllegalStateException.class,
                () -> context.setValueReference("Step", "p", new StringLiteralValue("v")));
        assertTrue(setValueRef.getMessage().contains("migrateConfiguration"), setValueRef.getMessage());

        final IllegalStateException setValueRefs = assertThrows(IllegalStateException.class,
                () -> context.setValueReferences("Step", Map.of("p", new StringLiteralValue("v"))));
        assertTrue(setValueRefs.getMessage().contains("migrateConfiguration"), setValueRefs.getMessage());
    }

    @Test
    public void testSetComponentStateStagesAndDrains() {
        final StandardFrameworkConnectorMigrationContext context = createContext(true);
        context.setPhase(StandardFrameworkConnectorMigrationContext.Phase.STATE);

        final VersionedComponentState clusterState = new VersionedComponentState();
        clusterState.setClusterState(Map.of("k", "v"));
        clusterState.setLocalNodeStates(List.of());
        final VersionedComponentState replacement = new VersionedComponentState();
        replacement.setClusterState(Map.of("k2", "v2"));
        replacement.setLocalNodeStates(List.of());
        context.setComponentState("component-1", clusterState);
        context.setComponentState("component-1", replacement);

        final Map<String, VersionedComponentState> drained = context.drainStagedComponentStates();
        assertEquals(1, drained.size());
        assertEquals(replacement, drained.get("component-1"));
        assertTrue(context.drainStagedComponentStates().isEmpty());
    }

    @Test
    public void testSetPhaseRejectsBackwardAndSelfTransitions() {
        final StandardFrameworkConnectorMigrationContext context = createContext(true);

        // Self-transition on the initial CONFIGURATION phase is rejected: phases must move strictly forward.
        assertThrows(IllegalStateException.class,
                () -> context.setPhase(StandardFrameworkConnectorMigrationContext.Phase.CONFIGURATION));

        context.setPhase(StandardFrameworkConnectorMigrationContext.Phase.STATE);

        // STATE -> CONFIGURATION is rejected (backward).
        assertThrows(IllegalStateException.class,
                () -> context.setPhase(StandardFrameworkConnectorMigrationContext.Phase.CONFIGURATION));
        // STATE -> STATE is rejected (self).
        assertThrows(IllegalStateException.class,
                () -> context.setPhase(StandardFrameworkConnectorMigrationContext.Phase.STATE));

        context.setPhase(StandardFrameworkConnectorMigrationContext.Phase.COMPLETED);

        // COMPLETED is terminal; any further transition is rejected.
        assertThrows(IllegalStateException.class,
                () -> context.setPhase(StandardFrameworkConnectorMigrationContext.Phase.CONFIGURATION));
        assertThrows(IllegalStateException.class,
                () -> context.setPhase(StandardFrameworkConnectorMigrationContext.Phase.STATE));
        assertThrows(IllegalStateException.class,
                () -> context.setPhase(StandardFrameworkConnectorMigrationContext.Phase.COMPLETED));
    }

    @Test
    public void testSetPhaseAllowsConfigurationToCompletedShortcutForFailureRollback() {
        final StandardFrameworkConnectorMigrationContext context = createContext(true);
        // Failure during the configuration phase moves the phase directly to COMPLETED without going through STATE.
        context.setPhase(StandardFrameworkConnectorMigrationContext.Phase.COMPLETED);
        assertEquals(StandardFrameworkConnectorMigrationContext.Phase.COMPLETED, context.getPhase());
    }

    @Test
    public void testGetActiveFlowContextReturnsMigrationFlowContextWrapperWhenDelegatePresent() {
        final FrameworkFlowContext delegate = mock(FrameworkFlowContext.class);
        final StandardFrameworkConnectorMigrationContext context = new StandardFrameworkConnectorMigrationContext(
                CONNECTOR_ID,
                sourceFlow,
                true,
                delegate,
                newWorkingConfiguration(),
                sourceAssetManager,
                connectorRepository,
                stateManagerProvider,
                clusterTopologyProvider);

        final FrameworkFlowContext active = context.getActiveFlowContext();
        assertNotNull(active);
        assertTrue(active instanceof MigrationFlowContext,
                "getActiveFlowContext() must return the read-only MigrationFlowContext wrapper");
    }

    private StandardFrameworkConnectorMigrationContext createContext(final boolean localMigration) {
        return new StandardFrameworkConnectorMigrationContext(
                CONNECTOR_ID,
                sourceFlow,
                localMigration,
                null,
                newWorkingConfiguration(),
                sourceAssetManager,
                connectorRepository,
                stateManagerProvider,
                clusterTopologyProvider);
    }

    private MutableConnectorConfigurationContext newWorkingConfiguration() {
        return new StandardConnectorConfigurationContext(sourceAssetManager, mock(SecretsManager.class));
    }

    private File createAssetFile(final Path tempDir, final String contents) throws IOException {
        final File file = tempDir.resolve("asset-" + contents + ".bin").toFile();
        Files.writeString(file.toPath(), contents);
        return file;
    }

    private Asset mockAsset(final String identifier, final String name, final File file) {
        final Asset asset = mock(Asset.class);
        when(asset.getIdentifier()).thenReturn(identifier);
        when(asset.getName()).thenReturn(name);
        when(asset.getFile()).thenReturn(file);
        return asset;
    }
}
