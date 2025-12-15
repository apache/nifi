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
package org.apache.nifi.web.util;

import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.migration.StandardPropertyConfiguration;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.registry.flow.FlowSnapshotContainer;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.apache.nifi.web.NiFiServiceFacade;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class FlowSnapshotPropertyMigratorTest {

    private static final String LEGACY_SSL_PROP = "ssl.context.service";
    private static final String NEW_SSL_PROP = "SSL Context Service";
    private static final String EXT_CS_ID = "00000000-0000-0000-0000-000000000001";

    @Mock
    private NiFiServiceFacade serviceFacade;

    private FlowSnapshotPropertyMigrator migrator;

    @BeforeEach
    public void setup() {
        migrator = new FlowSnapshotPropertyMigrator(serviceFacade);
    }

    @Test
    public void testMigratesPropertyRenames() {
        final RegisteredFlowSnapshot snapshot = mock(RegisteredFlowSnapshot.class);
        final VersionedProcessGroup root = mock(VersionedProcessGroup.class);
        final VersionedProcessor processor = mock(VersionedProcessor.class);

        final Map<String, String> props = new LinkedHashMap<>();
        props.put(LEGACY_SSL_PROP, EXT_CS_ID);

        when(snapshot.getFlowContents()).thenReturn(root);
        when(root.getProcessors()).thenReturn(new LinkedHashSet<>(Collections.singleton(processor)));
        when(root.getProcessGroups()).thenReturn(Collections.emptySet());
        when(processor.getProperties()).thenReturn(props);
        when(processor.getType()).thenReturn("org.apache.nifi.processors.TestMigratingProcessor");
        final org.apache.nifi.flow.Bundle bundle = mock(org.apache.nifi.flow.Bundle.class);
        when(bundle.getGroup()).thenReturn("group");
        when(bundle.getArtifact()).thenReturn("artifact");
        when(bundle.getVersion()).thenReturn("1.0.0");
        when(processor.getBundle()).thenReturn(bundle);

        final FlowSnapshotContainer container = new FlowSnapshotContainer(snapshot);

        when(serviceFacade.getCompatibleBundle(any(), any())).thenReturn(mock(BundleCoordinate.class));
        final Processor tempProcessor = mock(Processor.class);
        doAnswer(invocation -> {
            final StandardPropertyConfiguration cfg = invocation.getArgument(0);
            cfg.renameProperty(LEGACY_SSL_PROP, NEW_SSL_PROP);
            return null;
        }).when(tempProcessor).migrateProperties(any(StandardPropertyConfiguration.class));
        when(serviceFacade.getTempComponent(any(), any())).thenReturn(tempProcessor);

        migrator.migrate(container);

        // After migration, the rename should be applied to the snapshot properties
        assertTrue(props.containsKey(NEW_SSL_PROP), "Renamed key should be present in snapshot");
        assertFalse(props.containsKey(LEGACY_SSL_PROP), "Legacy key should be removed from snapshot");
        verify(serviceFacade).getTempComponent(any(), any());
    }

    @Test
    public void testNoopWhenNoMigration() {
        final RegisteredFlowSnapshot snapshot = mock(RegisteredFlowSnapshot.class);
        final VersionedProcessGroup root = mock(VersionedProcessGroup.class);
        final VersionedProcessor processor = mock(VersionedProcessor.class);

        final Map<String, String> props = new LinkedHashMap<>();
        props.put(LEGACY_SSL_PROP, EXT_CS_ID);

        when(snapshot.getFlowContents()).thenReturn(root);
        when(root.getProcessors()).thenReturn(new LinkedHashSet<>(Collections.singleton(processor)));
        when(root.getProcessGroups()).thenReturn(Collections.emptySet());
        when(processor.getProperties()).thenReturn(props);
        when(processor.getType()).thenReturn("org.apache.nifi.processors.TestMigratingProcessor");
        final org.apache.nifi.flow.Bundle bundle = mock(org.apache.nifi.flow.Bundle.class);
        when(bundle.getGroup()).thenReturn("group");
        when(bundle.getArtifact()).thenReturn("artifact");
        when(bundle.getVersion()).thenReturn("1.0.0");
        when(processor.getBundle()).thenReturn(bundle);

        final FlowSnapshotContainer container = new FlowSnapshotContainer(snapshot);

        when(serviceFacade.getCompatibleBundle(any(), any())).thenReturn(mock(BundleCoordinate.class));
        final Processor tempProcessor = mock(Processor.class);
        doAnswer(invocation -> null).when(tempProcessor).migrateProperties(any(StandardPropertyConfiguration.class));
        when(serviceFacade.getTempComponent(any(), any())).thenReturn(tempProcessor);

        migrator.migrate(container);

        assertTrue(props.containsKey(LEGACY_SSL_PROP), "Legacy property should remain when migration makes no changes");
    }

    @Test
    public void testPersistsOnlyRenamesNotValueChanges() {
        final RegisteredFlowSnapshot snapshot = mock(RegisteredFlowSnapshot.class);
        final VersionedProcessGroup root = mock(VersionedProcessGroup.class);
        final VersionedProcessor processor = mock(VersionedProcessor.class);

        final Map<String, String> props = new LinkedHashMap<>();
        props.put(LEGACY_SSL_PROP, EXT_CS_ID);

        when(snapshot.getFlowContents()).thenReturn(root);
        when(root.getProcessors()).thenReturn(new LinkedHashSet<>(Collections.singleton(processor)));
        when(root.getProcessGroups()).thenReturn(Collections.emptySet());
        when(processor.getProperties()).thenReturn(props);
        when(processor.getType()).thenReturn("org.apache.nifi.processors.TestMigratingProcessor");
        final org.apache.nifi.flow.Bundle bundle = mock(org.apache.nifi.flow.Bundle.class);
        when(bundle.getGroup()).thenReturn("group");
        when(bundle.getArtifact()).thenReturn("artifact");
        when(bundle.getVersion()).thenReturn("1.0.0");
        when(processor.getBundle()).thenReturn(bundle);

        final FlowSnapshotContainer container = new FlowSnapshotContainer(snapshot);

        when(serviceFacade.getCompatibleBundle(any(), any())).thenReturn(mock(BundleCoordinate.class));
        final Processor tempProcessor = mock(Processor.class);
        doAnswer(invocation -> {
            final StandardPropertyConfiguration cfg = invocation.getArgument(0);
            cfg.createControllerService("org.example.MyService", Collections.emptyMap());
            cfg.renameProperty(LEGACY_SSL_PROP, NEW_SSL_PROP);
            cfg.setProperty("Auto-Created Writer", "writer-id");
            return null;
        }).when(tempProcessor).migrateProperties(any(StandardPropertyConfiguration.class));
        when(serviceFacade.getTempComponent(any(), any())).thenReturn(tempProcessor);

        migrator.migrate(container);

        assertTrue(props.containsKey(NEW_SSL_PROP), "Renamed key should be present in snapshot");
        assertFalse(props.containsKey(LEGACY_SSL_PROP), "Legacy key should be removed from snapshot");
        assertFalse(props.containsKey("Auto-Created Writer"), "Non-rename value change should not be persisted in snapshot");
        assertTrue(props.containsValue(EXT_CS_ID), "Original value for renamed key should be preserved");
    }
}
