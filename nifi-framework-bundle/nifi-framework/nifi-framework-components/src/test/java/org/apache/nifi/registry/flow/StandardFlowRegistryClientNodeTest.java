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
package org.apache.nifi.registry.flow;

import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.components.validation.ValidationTrigger;
import org.apache.nifi.controller.LoggableComponent;
import org.apache.nifi.controller.ReloadComponent;
import org.apache.nifi.controller.TerminationAwareLogger;
import org.apache.nifi.controller.ValidationContextFactory;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.flow.VersionedFlowCoordinates;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.nar.ExtensionManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

class StandardFlowRegistryClientNodeTest {
    private static final String BUCKET_ID = "bucket";
    private static final String BRANCH = "main";
    private static final String VERSION = "1";
    private static final String STORAGE_LOCATION = "storage";

    // top contains a, a contains b, b contains c: every flow below top is nested in another versioned flow
    // shared contains a twice, so each occurrence of a, b and c is fetched for its own copy of the contents
    private static final Map<String, List<String>> NESTED_FLOW_IDS = Map.of(
            "top", List.of("a"),
            "shared", List.of("a", "a"),
            "a", List.of("b"),
            "b", List.of("c"));

    private final Map<String, Integer> fetchCounts = new HashMap<>();

    private StandardFlowRegistryClientNode clientNode;

    @BeforeEach
    void setUp() throws Exception {
        final FlowRegistryClient flowRegistryClient = mock(FlowRegistryClient.class);
        when(flowRegistryClient.isStorageLocationApplicable(any(), anyString())).thenReturn(true);
        when(flowRegistryClient.getFlowContents(any(), any())).thenAnswer(invocation -> {
            final FlowVersionLocation location = invocation.getArgument(1);
            fetchCounts.merge(location.getFlowId(), 1, Integer::sum);
            return createSnapshot(location.getFlowId());
        });

        final FlowManager flowManager = mock(FlowManager.class);
        final LoggableComponent<FlowRegistryClient> component = new LoggableComponent<>(flowRegistryClient,
                mock(BundleCoordinate.class), mock(TerminationAwareLogger.class));
        final StandardFlowRegistryClientNode node = new StandardFlowRegistryClientNode(mock(Authorizable.class), flowManager, component, "client-id",
                mock(ValidationContextFactory.class), mock(ControllerServiceProvider.class), FlowRegistryClient.class.getName(), FlowRegistryClient.class.getName(),
                mock(ReloadComponent.class), mock(ExtensionManager.class), mock(ValidationTrigger.class), false);
        // Registry calls require a valid client node, and validating it for real needs a configured extension
        clientNode = spy(node);
        doReturn(ValidationStatus.VALID).when(clientNode).getValidationStatus();
        when(flowManager.getAllFlowRegistryClients()).thenReturn(Set.of(clientNode));
    }

    @Test
    void testGetFlowContentsFetchesEachNestedFlowOnce() throws Exception {
        final FlowSnapshotContainer container = clientNode.getFlowContents(FlowRegistryClientContextFactory.getAnonymousContext(),
                new FlowVersionLocation(BRANCH, BUCKET_ID, "top", VERSION), true);

        assertEquals(Map.of("top", 1, "a", 1, "b", 1, "c", 1), fetchCounts);

        final VersionedProcessGroup groupA = container.getFlowSnapshot().getFlowContents().getProcessGroups().iterator().next();
        final VersionedProcessGroup groupB = groupA.getProcessGroups().iterator().next();
        final VersionedProcessGroup groupC = groupB.getProcessGroups().iterator().next();
        assertEquals("processor-c", groupC.getProcessors().iterator().next().getIdentifier());

        assertNotNull(container.getChildSnapshot(groupA.getIdentifier()));
        assertNotNull(container.getChildSnapshot(groupC.getIdentifier()));

        final Map<String, VersionedParameterContext> parameterContexts = container.getFlowSnapshot().getParameterContexts();
        assertEquals(Set.of("context-top", "context-a", "context-b", "context-c"), parameterContexts.keySet());
    }

    @Test
    void testGetFlowContentsFetchesNestedFlowOncePerOccurrence() throws Exception {
        final FlowSnapshotContainer container = clientNode.getFlowContents(FlowRegistryClientContextFactory.getAnonymousContext(),
                new FlowVersionLocation(BRANCH, BUCKET_ID, "shared", VERSION), true);

        assertEquals(Map.of("shared", 1, "a", 2, "b", 2, "c", 2), fetchCounts);

        final Set<VersionedProcessGroup> groupsA = container.getFlowSnapshot().getFlowContents().getProcessGroups();
        assertEquals(2, groupsA.size());
        for (final VersionedProcessGroup groupA : groupsA) {
            final VersionedProcessGroup groupB = groupA.getProcessGroups().iterator().next();
            final VersionedProcessGroup groupC = groupB.getProcessGroups().iterator().next();
            assertEquals("processor-c", groupC.getProcessors().iterator().next().getIdentifier());
            assertNotNull(container.getChildSnapshot(groupA.getIdentifier()));
        }
    }

    private static RegisteredFlowSnapshot createSnapshot(final String flowId) {
        final VersionedProcessor processor = new VersionedProcessor();
        processor.setIdentifier("processor-" + flowId);

        final VersionedProcessGroup contents = new VersionedProcessGroup();
        contents.setIdentifier("contents-" + flowId);
        contents.setProcessors(Set.of(processor));

        final List<String> nestedFlowIds = NESTED_FLOW_IDS.getOrDefault(flowId, List.of());
        final Set<VersionedProcessGroup> nestedGroups = new HashSet<>();
        for (int i = 0; i < nestedFlowIds.size(); i++) {
            nestedGroups.add(createNestedGroup("group-%s-%d".formatted(nestedFlowIds.get(i), i), nestedFlowIds.get(i)));
        }
        contents.setProcessGroups(nestedGroups);

        final VersionedParameterContext parameterContext = new VersionedParameterContext();
        parameterContext.setName("context-" + flowId);
        final Map<String, VersionedParameterContext> parameterContexts = new HashMap<>();
        parameterContexts.put(parameterContext.getName(), parameterContext);

        final RegisteredFlowSnapshot snapshot = new RegisteredFlowSnapshot();
        snapshot.setFlowContents(contents);
        snapshot.setParameterContexts(parameterContexts);
        return snapshot;
    }

    private static VersionedProcessGroup createNestedGroup(final String groupId, final String flowId) {
        final VersionedFlowCoordinates coordinates = new VersionedFlowCoordinates();
        coordinates.setStorageLocation(STORAGE_LOCATION);
        coordinates.setBranch(BRANCH);
        coordinates.setBucketId(BUCKET_ID);
        coordinates.setFlowId(flowId);
        coordinates.setVersion(VERSION);

        final VersionedProcessGroup nestedGroup = new VersionedProcessGroup();
        nestedGroup.setIdentifier(groupId);
        nestedGroup.setVersionedFlowCoordinates(coordinates);
        return nestedGroup;
    }
}
