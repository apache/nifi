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

package org.apache.nifi.web.api;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.migration.StandardPropertyConfiguration;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.registry.flow.FlowSnapshotContainer;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.AffectedComponentEntity;
import org.apache.nifi.web.api.entity.Entity;
import org.apache.nifi.web.api.entity.FlowUpdateRequestEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupImportEntity;
import org.apache.nifi.web.api.entity.VersionedFlowUpdateRequestEntity;
import org.apache.nifi.web.util.ComponentLifecycle;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.URI;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestFlowUpdateResource {

    private static final String LEGACY_SSL_PROP = "ssl.context.service";
    private static final String NEW_SSL_PROP = "SSL Context Service";
    private static final String EXT_CS_ID = "00000000-0000-0000-0000-000000000001";

    private TestableFlowUpdateResource resource;

    @Mock
    private NiFiServiceFacade serviceFacade;

    @BeforeEach
    public void setup() {
        resource = new TestableFlowUpdateResource();
        resource.serviceFacade = serviceFacade;
        resource.clusterComponentLifecycle = mock(ComponentLifecycle.class);
        resource.localComponentLifecycle = mock(ComponentLifecycle.class);

        // Prevent NPEs in ApplicationResource.isReplicateRequest()
        final HttpServletRequest httpServletRequest = mock(HttpServletRequest.class);
        resource.httpServletRequest = httpServletRequest;

        final NiFiProperties properties = mock(NiFiProperties.class);
        when(properties.isNode()).thenReturn(Boolean.FALSE);
        resource.properties = properties;

        final UriInfo uriInfo = mock(UriInfo.class);
        when(uriInfo.getAbsolutePath()).thenReturn(URI.create("http://localhost/nifi-api"));
        resource.uriInfo = uriInfo;
    }

    @Test
    public void testPreflightMigratesBeforeResolver() {
        // Build a minimal snapshot -> group -> processor with legacy property key
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
        // Provide a Bundle with coordinates for createBundleDto
        final org.apache.nifi.flow.Bundle bundle = mock(org.apache.nifi.flow.Bundle.class);
        when(bundle.getGroup()).thenReturn("group");
        when(bundle.getArtifact()).thenReturn("artifact");
        when(bundle.getVersion()).thenReturn("1.0.0");
        when(processor.getBundle()).thenReturn(bundle);

        final FlowSnapshotContainer container = new FlowSnapshotContainer(snapshot);

        // Mock service facade to return a temp component that will rename the property during migrateProperties
        when(serviceFacade.getCompatibleBundle(any(), any())).thenReturn(mock(BundleCoordinate.class));

        final Processor tempProcessor = mock(Processor.class);
        doAnswer(invocation -> {
            final StandardPropertyConfiguration cfg = invocation.getArgument(0);
            cfg.renameProperty(LEGACY_SSL_PROP, NEW_SSL_PROP);
            return null;
        }).when(tempProcessor).migrateProperties(any(StandardPropertyConfiguration.class));
        when(serviceFacade.getTempComponent(any(), any())).thenReturn(tempProcessor);

        // Capture the container passed to resolver and assert that the property key has been migrated by then
        final ArgumentCaptor<FlowSnapshotContainer> containerCaptor = ArgumentCaptor.forClass(FlowSnapshotContainer.class);
        doAnswer(invocation -> {
            final FlowSnapshotContainer passed = invocation.getArgument(0);
            assertNotNull(passed);
            final RegisteredFlowSnapshot passedSnapshot = passed.getFlowSnapshot();
            final VersionedProcessGroup passedRoot = passedSnapshot.getFlowContents();
            final VersionedProcessor passedProcessor = passedRoot.getProcessors().iterator().next();
            final Map<String, String> seenProps = passedProcessor.getProperties();
            assertTrue(seenProps.containsKey(NEW_SSL_PROP), "Expected migrated property name before resolver runs");
            assertFalse(seenProps.containsKey(LEGACY_SSL_PROP), "Legacy property name should be migrated before resolver");
            return Collections.emptySet();
        }).when(serviceFacade).resolveInheritedControllerServices(containerCaptor.capture(), any(), any());

        when(serviceFacade.resolveParameterProviders(any(), any())).thenReturn(Collections.emptySet());
        when(serviceFacade.getComponentsAffectedByFlowUpdate(any(), any())).thenReturn(Collections.<AffectedComponentEntity>emptySet());

        final ProcessGroupImportEntity request = new ProcessGroupImportEntity();
        final RevisionDTO rev = new RevisionDTO();
        rev.setClientId("client");
        rev.setVersion(0L);
        request.setProcessGroupRevision(rev);

        final Supplier<FlowSnapshotContainer> supplier = () -> container;

        try (Response ignored = resource.initiateFlowUpdate("pg", request, true, "replace-requests", "/replace-requests", supplier)) {
            // no-op; assertions are inside resolver Answer
        }

        // Ensure migration was attempted via temp component
        verify(serviceFacade).getTempComponent(any(), any());
        // Ensure the temp component received a migrateProperties call
        verify((Processor) tempProcessor).migrateProperties(any(StandardPropertyConfiguration.class));
        // And ensure resolver was invoked with the same container
        verify(serviceFacade).resolveInheritedControllerServices(any(FlowSnapshotContainer.class), eq("pg"), any());
    }

    @Test
    public void testPreflightNoopWhenNoMigration() {
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
        // Do not modify the config in this case
        doAnswer(invocation -> null).when(tempProcessor).migrateProperties(any(StandardPropertyConfiguration.class));
        when(serviceFacade.getTempComponent(any(), any())).thenReturn(tempProcessor);

        // In this case, assert that the legacy key is still present when resolver runs
        doAnswer(invocation -> {
            final FlowSnapshotContainer passed = invocation.getArgument(0);
            final RegisteredFlowSnapshot passedSnapshot = passed.getFlowSnapshot();
            final VersionedProcessGroup passedRoot = passedSnapshot.getFlowContents();
            final VersionedProcessor passedProcessor = passedRoot.getProcessors().iterator().next();
            final Map<String, String> seenProps = passedProcessor.getProperties();
            assertTrue(seenProps.containsKey(LEGACY_SSL_PROP), "Legacy property should remain when migration makes no changes");
            return Collections.emptySet();
        }).when(serviceFacade).resolveInheritedControllerServices(any(FlowSnapshotContainer.class), any(), any());

        when(serviceFacade.resolveParameterProviders(any(), any())).thenReturn(Collections.emptySet());
        when(serviceFacade.getComponentsAffectedByFlowUpdate(any(), any())).thenReturn(Collections.<AffectedComponentEntity>emptySet());

        final ProcessGroupImportEntity request = new ProcessGroupImportEntity();
        final RevisionDTO rev = new RevisionDTO();
        rev.setClientId("client");
        rev.setVersion(0L);
        request.setProcessGroupRevision(rev);

        final Supplier<FlowSnapshotContainer> supplier = () -> container;

        try (Response ignored = resource.initiateFlowUpdate("pg", request, true, "replace-requests", "/replace-requests", supplier)) {
            // assertions performed in resolver Answer
        }
    }

    @Test
    public void testPreflightPersistsOnlyRenamesNotValueChanges() {
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
            // Attempt to create a service: should be a no-op in preflight (returns null)
            cfg.createControllerService("org.example.MyService", Collections.emptyMap());
            // Rename the SSL property (should persist)
            cfg.renameProperty(LEGACY_SSL_PROP, NEW_SSL_PROP);
            // Also set a non-rename property value (should NOT be persisted back to snapshot)
            cfg.setProperty("Auto-Created Writer", "writer-id");
            return null;
        }).when(tempProcessor).migrateProperties(any(StandardPropertyConfiguration.class));
        when(serviceFacade.getTempComponent(any(), any())).thenReturn(tempProcessor);

        when(serviceFacade.resolveInheritedControllerServices(any(FlowSnapshotContainer.class), any(), any())).thenReturn(Collections.emptySet());
        when(serviceFacade.resolveParameterProviders(any(), any())).thenReturn(Collections.emptySet());
        when(serviceFacade.getComponentsAffectedByFlowUpdate(any(), any())).thenReturn(Collections.<AffectedComponentEntity>emptySet());

        final ProcessGroupImportEntity request = new ProcessGroupImportEntity();
        final RevisionDTO rev = new RevisionDTO();
        rev.setClientId("client");
        rev.setVersion(0L);
        request.setProcessGroupRevision(rev);

        final Supplier<FlowSnapshotContainer> supplier = () -> container;

        try (Response ignored = resource.initiateFlowUpdate("pg", request, true, "replace-requests", "/replace-requests", supplier)) {
            // After preflight, only pure renames should be applied
            assertTrue(props.containsKey(NEW_SSL_PROP), "Renamed key should be present in snapshot");
            assertFalse(props.containsKey(LEGACY_SSL_PROP), "Legacy key should be removed from snapshot");
            assertFalse(props.containsKey("Auto-Created Writer"), "Non-rename value change should not be persisted in snapshot");
            assertTrue(props.containsValue(EXT_CS_ID), "Original value for renamed key should be preserved");
        }
    }

    /**
     * Minimal concrete subclass to expose initiateFlowUpdate and bypass complex locking. The
     * important behavior for these tests occurs before withWriteLock is invoked.
     */
    private static class TestableFlowUpdateResource extends FlowUpdateResource<ProcessGroupImportEntity, FlowUpdateRequestEntity> {
        @Override
        protected ProcessGroupEntity performUpdateFlow(final String groupId, final Revision revision, final ProcessGroupImportEntity requestEntity,
                                                       final RegisteredFlowSnapshot flowSnapshot, final String idGenerationSeed,
                                                       final boolean verifyNotModified, final boolean updateDescendantVersionedFlows) {
            return null;
        }

        @Override
        protected Entity createReplicateUpdateFlowEntity(final Revision revision, final ProcessGroupImportEntity requestEntity, final RegisteredFlowSnapshot flowSnapshot) {
            return null;
        }

        @Override
        protected FlowUpdateRequestEntity createUpdateRequestEntity() {
            // Return a concrete subclass since FlowUpdateRequestEntity is abstract
            return new VersionedFlowUpdateRequestEntity();
        }

        @Override
        protected void finalizeCompletedUpdateRequest(final FlowUpdateRequestEntity updateRequestEntity) {
        }

        // Override the withWriteLock used by initiateFlowUpdate to avoid threading/locking in unit tests
        @Override
        protected <T extends Entity> Response withWriteLock(final NiFiServiceFacade serviceFacade, final T entity,
                                                            final Revision revision, final org.apache.nifi.authorization.AuthorizeAccess authorizer,
                                                            final Runnable verify, final BiFunction<Revision, T, Response> action) {
            if (verify != null) {
                verify.run();
            }
            // Do not invoke the action in tests to avoid heavy dependencies; return OK to finish the request
            return Response.ok().build();
        }
    }
}
