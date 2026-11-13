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
package org.apache.nifi.groups;

import org.apache.nifi.annotation.lifecycle.OnDrainCompleted;
import org.apache.nifi.annotation.lifecycle.OnDrainInitiated;
import org.apache.nifi.asset.AssetManager;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.connector.components.ComponentHierarchyScope;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.ClusterTopologyProvider;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.NodeTypeProvider;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReloadComponent;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.queue.DropFlowFileRequest;
import org.apache.nifi.controller.queue.DropFlowFileState;
import org.apache.nifi.controller.queue.DropFlowFileStatus;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.flow.ExecutionEngine;
import org.apache.nifi.lifecycle.ProcessorStopLifecycleMethods;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.registry.flow.VersionControlInformation;
import org.apache.nifi.registry.flow.VersionedFlowStatus;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.security.encryption.PropertyEncryptionProvider;
import org.apache.nifi.util.NiFiProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.quality.Strictness;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

@ExtendWith(MockitoExtension.class)
class StandardProcessGroupTest {

    private static final String ID = "12345678-4321";
    private static final String ID_PATH = "/12345678-4321";
    private static final String NAME = "TestGroup";
    private static final String NAME_PATH = "/TestGroup";

    private static final String PARENT_ID = "87654321-1234";
    private static final String PARENT_ID_PATH = "/87654321-1234/12345678-4321";
    private static final String PARENT_NAME = "ParentGroup";
    private static final String PARENT_NAME_PATH = "/ParentGroup/TestGroup";
    private static final String PARENT_REGISTERED_FLOW_IDENTIFIER_PATH = "/87654321-4321:1.0.0/UNREGISTERED";

    private static final String REGISTERED_FLOW_IDENTIFIER = "87654321-4321";
    private static final String REGISTERED_FLOW_VERSION = "1.0.0";
    private static final String REGISTERED_FLOW_IDENTIFIER_PATH = "/87654321-4321:1.0.0";

    private static final String DRAIN_ID = "drain-1";
    private static final String REMOTE_GROUP_ID = "remote-group";

    private static final String SERVICE_ID = "service-1";
    private static final String SERVICE_ID_2 = "service-2";
    private static final String SERVICE_ID_3 = "service-3";

    private static final AllowableValue VALUE_A = new AllowableValue("A", "Value A");
    private static final AllowableValue VALUE_B = new AllowableValue("B", "Value B");

    private static final PropertyDescriptor MODE_PROPERTY = new PropertyDescriptor.Builder()
        .name("mode")
        .displayName("Mode")
        .allowableValues(VALUE_A, VALUE_B)
        .defaultValue(VALUE_A.getValue())
        .required(true)
        .build();

    private static final PropertyDescriptor SERVICE_PROPERTY_NO_DEPENDENCY = new PropertyDescriptor.Builder()
        .name("service-no-dep")
        .displayName("Service (No Dependency)")
        .identifiesControllerService(ControllerService.class)
        .build();

    private static final PropertyDescriptor SERVICE_PROPERTY_WITH_DEPENDENCY = new PropertyDescriptor.Builder()
        .name("service-with-dep")
        .displayName("Service (With Dependency)")
        .identifiesControllerService(ControllerService.class)
        .dependsOn(MODE_PROPERTY, VALUE_B)
        .build();

    @Mock
    private ControllerServiceProvider controllerServiceProvider;

    @Mock
    private ProcessScheduler processScheduler;

    @Mock
    private PropertyEncryptionProvider propertyEncryptionProvider;

    @Mock
    private ExtensionManager extensionManager;

    @Mock
    private StateManagerProvider stateManagerProvider;

    @Mock
    private FlowManager flowManager;

    @Mock
    private ReloadComponent reloadComponent;

    @Mock
    private NodeTypeProvider nodeTypeProvider;

    @Mock
    private ClusterTopologyProvider clusterTopologyProvider;

    @Mock
    private NiFiProperties properties;

    @Mock
    private StatelessGroupNodeFactory statelessGroupNodeFactory;

    @Mock
    private AssetManager assetManager;

    @Mock
    private StateManager stateManager;

    @Mock
    private StateMap stateMap;

    @Mock
    private ProcessGroup parentProcessGroup;

    @Mock
    private VersionControlInformation versionControlInformation;

    @Mock
    private VersionedFlowStatus versionedFlowStatus;

    private StandardProcessGroup processGroup;

    @BeforeEach
    void setProcessGroup() throws IOException {
        when(stateManagerProvider.getStateManager(anyString())).thenReturn(stateManager);
        when(stateManager.getState(eq(Scope.LOCAL))).thenReturn(stateMap);

        processGroup = new StandardProcessGroup(
                ID,
                controllerServiceProvider,
                processScheduler,
                propertyEncryptionProvider,
                extensionManager,
                stateManagerProvider,
                flowManager,
                reloadComponent,
                nodeTypeProvider,
                clusterTopologyProvider,
                properties,
                statelessGroupNodeFactory,
                assetManager,
                null
        );
    }

    @Test
    void testStartAndStopProcessingDoNothingWhenParentIsStateless() throws Exception {
        // Callers that resolve a component's owning stateless group rely on this no-op: only the top-most stateless
        // group may be started or stopped directly. It is also what terminates the recursive stopComponents() walk,
        // so making it throw or delegate upward would break stateless shutdown.
        when(parentProcessGroup.getExecutionEngine()).thenReturn(ExecutionEngine.STATELESS);
        when(parentProcessGroup.resolveExecutionEngine()).thenReturn(ExecutionEngine.STATELESS);
        processGroup.setParent(parentProcessGroup);
        processGroup.setExecutionEngine(ExecutionEngine.STATELESS);

        processGroup.startProcessing();

        final CompletableFuture<Void> stopFuture = processGroup.stopProcessing();

        assertTrue(stopFuture.isDone());
        assertNull(stopFuture.get());
        verify(processScheduler, never()).startStatelessGroup(any());
        verify(processScheduler, never()).stopStatelessGroup(any());
    }

    @Test
    void testGetLoggingAttributesWithoutParentProcessGroup() {
        processGroup.setName(NAME);

        final Map<String, String> loggingAttributes = processGroup.getLoggingAttributes();

        assertNotNull(loggingAttributes);
        assertFalse(loggingAttributes.isEmpty());

        final Map<String, String> expected = Map.of(
                StandardProcessGroup.LoggingAttribute.PROCESS_GROUP_ID.getAttribute(), ID,
                StandardProcessGroup.LoggingAttribute.PROCESS_GROUP_ID_PATH.getAttribute(), ID_PATH,
                StandardProcessGroup.LoggingAttribute.PROCESS_GROUP_NAME.getAttribute(), NAME,
                StandardProcessGroup.LoggingAttribute.PROCESS_GROUP_NAME_PATH.getAttribute(), NAME_PATH
        );

        assertEquals(expected, loggingAttributes);
    }

    @Test
    void testGetLoggingAttributesWithParentProcessGroup() {
        processGroup.setName(NAME);

        when(parentProcessGroup.getIdentifier()).thenReturn(PARENT_ID);
        when(parentProcessGroup.getName()).thenReturn(PARENT_NAME);
        processGroup.setParent(parentProcessGroup);

        final Map<String, String> loggingAttributes = processGroup.getLoggingAttributes();

        assertNotNull(loggingAttributes);
        assertFalse(loggingAttributes.isEmpty());

        final Map<String, String> expected = Map.of(
                StandardProcessGroup.LoggingAttribute.PROCESS_GROUP_ID.getAttribute(), ID,
                StandardProcessGroup.LoggingAttribute.PROCESS_GROUP_ID_PATH.getAttribute(), PARENT_ID_PATH,
                StandardProcessGroup.LoggingAttribute.PROCESS_GROUP_NAME.getAttribute(), NAME,
                StandardProcessGroup.LoggingAttribute.PROCESS_GROUP_NAME_PATH.getAttribute(), PARENT_NAME_PATH
        );

        assertEquals(expected, loggingAttributes);
    }

    @Test
    void testGetLoggingAttributesWithParentVersionControlInformation() {
        processGroup.setName(NAME);

        when(versionControlInformation.getFlowIdentifier()).thenReturn(REGISTERED_FLOW_IDENTIFIER);
        when(versionControlInformation.getVersion()).thenReturn(REGISTERED_FLOW_VERSION);

        when(parentProcessGroup.getIdentifier()).thenReturn(PARENT_ID);
        when(parentProcessGroup.getName()).thenReturn(PARENT_NAME);
        when(parentProcessGroup.getVersionControlInformation()).thenReturn(versionControlInformation);
        processGroup.setParent(parentProcessGroup);

        final Map<String, String> loggingAttributes = processGroup.getLoggingAttributes();

        assertNotNull(loggingAttributes);
        assertFalse(loggingAttributes.isEmpty());

        final Map<String, String> expected = Map.of(
                StandardProcessGroup.LoggingAttribute.PROCESS_GROUP_ID.getAttribute(), ID,
                StandardProcessGroup.LoggingAttribute.PROCESS_GROUP_ID_PATH.getAttribute(), PARENT_ID_PATH,
                StandardProcessGroup.LoggingAttribute.PROCESS_GROUP_NAME.getAttribute(), NAME,
                StandardProcessGroup.LoggingAttribute.PROCESS_GROUP_NAME_PATH.getAttribute(), PARENT_NAME_PATH,
                StandardProcessGroup.LoggingAttribute.REGISTERED_FLOW_IDENTIFIER_PATH.getAttribute(), PARENT_REGISTERED_FLOW_IDENTIFIER_PATH
        );

        assertEquals(expected, loggingAttributes);
    }

    @Test
    void testGetLoggingAttributesWithVersionControlInformation() {
        processGroup.setName(NAME);

        when(versionControlInformation.getFlowIdentifier()).thenReturn(REGISTERED_FLOW_IDENTIFIER);
        when(versionControlInformation.getVersion()).thenReturn(REGISTERED_FLOW_VERSION);
        when(versionControlInformation.getStatus()).thenReturn(versionedFlowStatus);
        processGroup.setVersionControlInformation(versionControlInformation, Map.of());

        final Map<String, String> loggingAttributes = processGroup.getLoggingAttributes();

        assertNotNull(loggingAttributes);
        assertFalse(loggingAttributes.isEmpty());

        final Map<String, String> expected = Map.of(
                StandardProcessGroup.LoggingAttribute.PROCESS_GROUP_ID.getAttribute(), ID,
                StandardProcessGroup.LoggingAttribute.PROCESS_GROUP_ID_PATH.getAttribute(), ID_PATH,
                StandardProcessGroup.LoggingAttribute.PROCESS_GROUP_NAME.getAttribute(), NAME,
                StandardProcessGroup.LoggingAttribute.PROCESS_GROUP_NAME_PATH.getAttribute(), NAME_PATH,
                StandardProcessGroup.LoggingAttribute.REGISTERED_FLOW_IDENTIFIER.getAttribute(), REGISTERED_FLOW_IDENTIFIER,
                StandardProcessGroup.LoggingAttribute.REGISTERED_FLOW_VERSION.getAttribute(), REGISTERED_FLOW_VERSION,
                StandardProcessGroup.LoggingAttribute.REGISTERED_FLOW_IDENTIFIER_PATH.getAttribute(), REGISTERED_FLOW_IDENTIFIER_PATH
        );

        assertEquals(expected, loggingAttributes);
    }

    @Test
    void testSetConnectorLoggingAttributesMergesIntoLoggingAttributes() {
        processGroup.setName(NAME);

        final Map<String, String> connectorAttributes = Map.of(
                "connectorId", "connector-1",
                "connectorName", "My Connector",
                "connectorComponent", "com.example.MyConnector"
        );

        processGroup.setConnectorLoggingAttributes(connectorAttributes);

        final Map<String, String> loggingAttributes = processGroup.getLoggingAttributes();
        assertEquals("connector-1", loggingAttributes.get("connectorId"));
        assertEquals("My Connector", loggingAttributes.get("connectorName"));
        assertEquals("com.example.MyConnector", loggingAttributes.get("connectorComponent"));
        assertEquals(NAME, loggingAttributes.get(StandardProcessGroup.LoggingAttribute.PROCESS_GROUP_NAME.getAttribute()));
        assertEquals(ID, loggingAttributes.get(StandardProcessGroup.LoggingAttribute.PROCESS_GROUP_ID.getAttribute()));
    }

    @Test
    void testSetConnectorLoggingAttributesCascadesToChildProcessGroups() {
        processGroup.setName(NAME);

        final StandardProcessGroup child = createStandardProcessGroup("child-id");
        child.setName("Child");
        processGroup.addProcessGroup(child);

        final Map<String, String> connectorAttributes = Map.of(
                "connectorId", "connector-1",
                "connectorName", "Postgres CDC"
        );

        processGroup.setConnectorLoggingAttributes(connectorAttributes);

        assertEquals("connector-1", child.getLoggingAttributes().get("connectorId"));
        assertEquals("Postgres CDC", child.getLoggingAttributes().get("connectorName"));
    }

    @Test
    void testAddProcessGroupInheritsConnectorLoggingAttributesFromParent() {
        processGroup.setName(NAME);
        processGroup.setConnectorLoggingAttributes(Map.of(
                "connectorId", "connector-1",
                "connectorName", "Postgres CDC"
        ));

        final StandardProcessGroup lateChild = createStandardProcessGroup("late-child-id");
        lateChild.setName("Late Child");
        processGroup.addProcessGroup(lateChild);

        assertEquals("connector-1", lateChild.getLoggingAttributes().get("connectorId"));
        assertEquals("Postgres CDC", lateChild.getLoggingAttributes().get("connectorName"));
        assertEquals(Map.copyOf(processGroup.getConnectorLoggingAttributes()), lateChild.getConnectorLoggingAttributes());
    }

    @Test
    void testEmptyConnectorLoggingAttributesAddsNothing() {
        processGroup.setName(NAME);
        processGroup.setConnectorLoggingAttributes(Map.of());

        final Map<String, String> loggingAttributes = processGroup.getLoggingAttributes();
        assertFalse(loggingAttributes.containsKey("connectorId"));
        assertTrue(processGroup.getConnectorLoggingAttributes().isEmpty());
        // Verify the PG-level keys are still present.
        assertEquals(NAME, loggingAttributes.get(StandardProcessGroup.LoggingAttribute.PROCESS_GROUP_NAME.getAttribute()));
    }

    @Test
    void testSetConnectorLoggingAttributesReplacesPreviousValues() {
        processGroup.setName(NAME);
        processGroup.setConnectorLoggingAttributes(Map.of(
                "connectorId", "connector-1",
                "connectorName", "Old Name",
                "customKey", "customValue"
        ));

        processGroup.setConnectorLoggingAttributes(Map.of(
                "connectorId", "connector-1",
                "connectorName", "New Name"
        ));

        final Map<String, String> loggingAttributes = processGroup.getLoggingAttributes();
        assertEquals("New Name", loggingAttributes.get("connectorName"));
        assertFalse(loggingAttributes.containsKey("customKey"));
    }

    @Test
    void shouldPropagateLoggingAttributesChangesToChildren() {
        final String rootId = "root";
        final String rootName = "RootGroup";

        final String middleId = "middle";
        final String middleName = "MiddleGroup";

        final String leafId = "leaf";
        final String leafName = "LeafGroup";

        final StandardProcessGroup leaf = createStandardProcessGroup(leafId);
        leaf.setName(leafName);

        final StandardProcessGroup middle = createStandardProcessGroup(middleId);
        middle.setName(middleName);

        final StandardProcessGroup versionedRoot = createStandardProcessGroup(rootId);
        versionedRoot.setName(rootName);

        when(flowManager.getFlowAnalyzer()).thenReturn(Optional.empty());
        versionedRoot.addProcessGroup(middle);
        middle.addProcessGroup(leaf);

        when(versionControlInformation.getFlowIdentifier()).thenReturn(REGISTERED_FLOW_IDENTIFIER);
        when(versionControlInformation.getVersion()).thenReturn(REGISTERED_FLOW_VERSION);
        when(versionControlInformation.getStatus()).thenReturn(versionedFlowStatus);
        versionedRoot.setVersionControlInformation(versionControlInformation, Map.of());

        final Map<String, String> expected = Map.of(
                StandardProcessGroup.LoggingAttribute.PROCESS_GROUP_ID.getAttribute(), leafId,
                StandardProcessGroup.LoggingAttribute.PROCESS_GROUP_NAME.getAttribute(), leafName,
                StandardProcessGroup.LoggingAttribute.PROCESS_GROUP_ID_PATH.getAttribute(), "/" + rootId + "/" + middleId + "/" + leafId,
                StandardProcessGroup.LoggingAttribute.PROCESS_GROUP_NAME_PATH.getAttribute(), "/" + rootName + "/" + middleName + "/" + leafName,
                StandardProcessGroup.LoggingAttribute.REGISTERED_FLOW_IDENTIFIER_PATH.getAttribute(), "/" + REGISTERED_FLOW_IDENTIFIER + ":" + REGISTERED_FLOW_VERSION + "/UNREGISTERED/UNREGISTERED"
        );

        assertEquals(expected, leaf.getLoggingAttributes());
    }

    @Test
    void testFindOwningConnectorIdentifierWalksParentHierarchy() {
        final StandardProcessGroup connectorGroup = createStandardProcessGroup("connector-group", "connector-1");
        final StandardProcessGroup child = createStandardProcessGroup("child");
        final StandardProcessGroup grandchild = createStandardProcessGroup("grandchild");

        child.setParent(connectorGroup);
        grandchild.setParent(child);

        assertEquals(Optional.of("connector-1"), connectorGroup.findOwningConnectorIdentifier());
        assertEquals(Optional.of("connector-1"), child.findOwningConnectorIdentifier());
        assertEquals(Optional.of("connector-1"), grandchild.findOwningConnectorIdentifier());
        assertEquals(Optional.empty(), processGroup.findOwningConnectorIdentifier());
    }

    @Test
    void testFindOwningConnectorIdentifierResolvesHierarchyOnlyOnce() {
        when(parentProcessGroup.getConnectorIdentifier()).thenReturn(Optional.of("connector-1"));
        processGroup.setParent(parentProcessGroup);

        assertEquals(Optional.of("connector-1"), processGroup.findOwningConnectorIdentifier());
        assertEquals(Optional.of("connector-1"), processGroup.findOwningConnectorIdentifier());
        assertEquals(Optional.of("connector-1"), processGroup.findOwningConnectorIdentifier());
        verify(parentProcessGroup, times(1)).getConnectorIdentifier();

        final ProcessGroup unmanagedParent = mock(ProcessGroup.class);
        final StandardProcessGroup unmanagedGroup = createStandardProcessGroup("unmanaged-group");
        unmanagedGroup.setParent(unmanagedParent);

        assertEquals(Optional.empty(), unmanagedGroup.findOwningConnectorIdentifier());
        assertEquals(Optional.empty(), unmanagedGroup.findOwningConnectorIdentifier());

        verify(unmanagedParent, times(1)).getConnectorIdentifier();
    }

    @Test
    void testFindOwningConnectorIdentifierResolvedAgainAfterParentAssigned() {
        assertEquals(Optional.empty(), processGroup.findOwningConnectorIdentifier());

        when(parentProcessGroup.getConnectorIdentifier()).thenReturn(Optional.of("connector-1"));
        processGroup.setParent(parentProcessGroup);

        assertEquals(Optional.of("connector-1"), processGroup.findOwningConnectorIdentifier());
    }

    @Test
    void testDropAllFlowFilesCompletionWaitsForEveryConnection() {
        when(flowManager.getFlowAnalyzer()).thenReturn(Optional.empty());

        final DropFlowFileRequest emptyDrop = new DropFlowFileRequest("drop-all");
        emptyDrop.setOriginalSize(new QueueSize(0, 0L));
        emptyDrop.setCurrentSize(new QueueSize(0, 0L));
        emptyDrop.setState(DropFlowFileState.COMPLETE);

        final DropFlowFileRequest pendingDrop = new DropFlowFileRequest("drop-all");
        pendingDrop.setOriginalSize(new QueueSize(300, 300L));
        pendingDrop.setCurrentSize(new QueueSize(300, 300L));
        pendingDrop.setState(DropFlowFileState.DROPPING_FLOWFILES);

        addConnectionWithDropStatus("empty-connection", emptyDrop);
        addConnectionWithDropStatus("queued-connection", pendingDrop);

        final DropFlowFileStatus dropStatus = processGroup.dropAllFlowFiles("drop-all", "test-user");
        final CompletableFuture<Void> completionFuture = dropStatus.getCompletionFuture();

        assertFalse(completionFuture.isDone());

        pendingDrop.setState(DropFlowFileState.COMPLETE);

        assertTrue(completionFuture.isDone());
    }

    @Test
    void testStatelessContentMaxHeapDefaultsToZeroBytesWithUnsetLimits() {
        assertNull(processGroup.getStatelessContentMaxHeap());
        assertNull(processGroup.getStatelessContentMaxHeapPercentage());
        assertEquals(0L, processGroup.resolveStatelessContentMaxHeap());
    }

    @Test
    void testSetStatelessContentMaxHeapParsesDataSize() {
        processGroup.setStatelessContentMaxHeapPercentage(null);
        processGroup.setStatelessContentMaxHeap("100 MB");
        assertEquals("100 MB", processGroup.getStatelessContentMaxHeap());
        assertEquals(100L * 1024 * 1024, processGroup.resolveStatelessContentMaxHeap());

        processGroup.setStatelessContentMaxHeap("1 KB");
        assertEquals(1024L, processGroup.resolveStatelessContentMaxHeap());

        processGroup.setStatelessContentMaxHeap("1.5 kb");
        assertEquals(1536L, processGroup.resolveStatelessContentMaxHeap());

        processGroup.setStatelessContentMaxHeap("0 B");
        assertEquals("0 B", processGroup.getStatelessContentMaxHeap());
        assertEquals(0L, processGroup.resolveStatelessContentMaxHeap());

        processGroup.setStatelessContentMaxHeap("   ");
        assertNull(processGroup.getStatelessContentMaxHeap());
        assertEquals(0L, processGroup.resolveStatelessContentMaxHeap());
    }

    @Test
    void testSetStatelessContentMaxHeapPercentage() {
        processGroup.setStatelessContentMaxHeapPercentage(0);
        assertEquals(0, processGroup.getStatelessContentMaxHeapPercentage());
        assertEquals(0L, processGroup.resolveStatelessContentMaxHeap());

        processGroup.setStatelessContentMaxHeapPercentage(50);
        assertEquals(50, processGroup.getStatelessContentMaxHeapPercentage());
        assertEquals(Runtime.getRuntime().maxMemory() / 2, processGroup.resolveStatelessContentMaxHeap());

        processGroup.setStatelessContentMaxHeapPercentage(90);
        final long expectedNinetyPercent = new BigDecimal("90")
            .multiply(BigDecimal.valueOf(Runtime.getRuntime().maxMemory()))
            .divide(BigDecimal.valueOf(100), 0, RoundingMode.DOWN)
            .longValue();
        assertEquals(expectedNinetyPercent, processGroup.resolveStatelessContentMaxHeap());
    }

    @Test
    void testResolveStatelessContentMaxHeapUsesTheSmallerLimitWhenBothAreSet() {
        processGroup.setStatelessContentMaxHeap("1 B");
        processGroup.setStatelessContentMaxHeapPercentage(90);
        assertEquals(1L, processGroup.resolveStatelessContentMaxHeap());

        processGroup.setStatelessContentMaxHeap(Runtime.getRuntime().maxMemory() + " B");
        processGroup.setStatelessContentMaxHeapPercentage(50);
        assertEquals(Runtime.getRuntime().maxMemory() / 2, processGroup.resolveStatelessContentMaxHeap());
    }

    @Test
    void testSetStatelessContentMaxHeapRejectsInvalidLimits() {
        processGroup.setStatelessContentMaxHeapPercentage(null);
        processGroup.setStatelessContentMaxHeap("1 MB");
        assertThrows(IllegalArgumentException.class, () -> processGroup.setStatelessContentMaxHeap("not a size"));
        assertThrows(IllegalArgumentException.class, () -> processGroup.setStatelessContentMaxHeap("-1 MB"));
        assertThrows(IllegalArgumentException.class, () -> processGroup.setStatelessContentMaxHeap("limit 1 MB"));
        assertThrows(IllegalArgumentException.class, () -> processGroup.setStatelessContentMaxHeap("0.9 B"));
        assertThrows(IllegalArgumentException.class, () -> processGroup.setStatelessContentMaxHeap("999999999999999999999999999999999999999999999 TB"));
        assertThrows(IllegalArgumentException.class, () -> processGroup.setStatelessContentMaxHeap("50%"));
        assertThrows(IllegalArgumentException.class, () -> processGroup.setStatelessContentMaxHeapPercentage(91));
        assertThrows(IllegalArgumentException.class, () -> processGroup.setStatelessContentMaxHeapPercentage(-1));
        assertThrows(IllegalArgumentException.class, () -> processGroup.verifyCanSetStatelessContentMaxHeap("not a size"));
        processGroup.verifyCanSetStatelessContentMaxHeapPercentage(0);
        assertEquals("1 MB", processGroup.getStatelessContentMaxHeap());
        assertNull(processGroup.getStatelessContentMaxHeapPercentage());
        assertEquals(1024L * 1024L, processGroup.resolveStatelessContentMaxHeap());
    }

    @Test
    void testSetStatelessContentMaxHeapWhileRunningAllowsSameByteCount() {
        final StatelessGroupNode statelessGroupNode = mock(StatelessGroupNode.class);
        when(statelessGroupNodeFactory.createStatelessGroupNode(any())).thenReturn(statelessGroupNode);
        final StandardProcessGroup runningGroup = createStandardProcessGroup("running");
        runningGroup.setStatelessContentMaxHeapPercentage(null);
        runningGroup.setStatelessContentMaxHeap("1 MB");
        runningGroup.setExecutionEngine(ExecutionEngine.STATELESS);
        when(statelessGroupNode.getCurrentState()).thenReturn(ScheduledState.RUNNING);

        runningGroup.verifyCanSetStatelessContentMaxHeap("1024 KB");
        runningGroup.setStatelessContentMaxHeap("1024 KB");
        assertEquals("1024 KB", runningGroup.getStatelessContentMaxHeap());
        assertThrows(IllegalStateException.class, () -> runningGroup.verifyCanSetStatelessContentMaxHeap("2 MB"));
        assertThrows(IllegalStateException.class, () -> runningGroup.setStatelessContentMaxHeap("2 MB"));
        assertEquals(1024L * 1024L, runningGroup.resolveStatelessContentMaxHeap());

        final StandardProcessGroup childGroup = createStandardProcessGroup("child");
        childGroup.setName("Child");
        runningGroup.addProcessGroup(childGroup);
        assertThrows(IllegalStateException.class, () -> childGroup.verifyCanSetStatelessContentMaxHeap("2 MB"));
        assertThrows(IllegalStateException.class, () -> childGroup.setStatelessContentMaxHeap("2 MB"));

        when(statelessGroupNode.getCurrentState()).thenReturn(ScheduledState.STOPPED);
        runningGroup.setStatelessContentMaxHeap("2 MB");
        assertEquals(2L * 1024L * 1024L, runningGroup.resolveStatelessContentMaxHeap());
        runningGroup.setStatelessContentMaxHeap(null);
        runningGroup.setStatelessContentMaxHeapPercentage(90);

        when(statelessGroupNode.getCurrentState()).thenReturn(ScheduledState.RUNNING);
        runningGroup.verifyCanSetStatelessContentMaxHeapPercentage(90);
        runningGroup.setStatelessContentMaxHeapPercentage(90);
        assertThrows(IllegalStateException.class, () -> runningGroup.verifyCanSetStatelessContentMaxHeapPercentage(50));
        assertThrows(IllegalStateException.class, () -> runningGroup.setStatelessContentMaxHeapPercentage(50));

        when(statelessGroupNode.getCurrentState()).thenReturn(ScheduledState.STOPPED);
        runningGroup.setStatelessContentMaxHeapPercentage(50);
        assertEquals(50, runningGroup.getStatelessContentMaxHeapPercentage());
    }

    @Test
    void testFindReferencedControllerServicesHonorsPropertyDependencies() {
        final PropertyDescriptor servicePropertyDependsOnDefault = new PropertyDescriptor.Builder()
            .name("service-dep-default")
            .displayName("Service (Depends on Default)")
            .identifiesControllerService(ControllerService.class)
            .dependsOn(MODE_PROPERTY, VALUE_A)
            .build();

        final ControllerServiceNode noDependencyService = createService(SERVICE_ID);
        final ControllerServiceNode dependsOnModeBService = createService(SERVICE_ID_2);
        final ControllerServiceNode dependsOnDefaultModeService = createService(SERVICE_ID_3);

        // The Mode property is not set, so its default value of A determines which dependencies are satisfied
        final List<PropertyDescriptor> descriptors = List.of(MODE_PROPERTY, SERVICE_PROPERTY_NO_DEPENDENCY, SERVICE_PROPERTY_WITH_DEPENDENCY, servicePropertyDependsOnDefault);
        processGroup.addProcessor(createProcessor("default-mode", processGroup, ScheduledState.STOPPED, descriptors,
            Map.of(SERVICE_PROPERTY_NO_DEPENDENCY, SERVICE_ID, SERVICE_PROPERTY_WITH_DEPENDENCY, SERVICE_ID_2, servicePropertyDependsOnDefault, SERVICE_ID_3)));

        final StandardProcessGroup childGroup = createStandardProcessGroup("child");
        childGroup.setName("Child");
        processGroup.addProcessGroup(childGroup);
        childGroup.addProcessor(createProcessor("mode-b", childGroup, ScheduledState.STOPPED, List.of(MODE_PROPERTY, SERVICE_PROPERTY_WITH_DEPENDENCY),
            Map.of(MODE_PROPERTY, VALUE_B.getValue(), SERVICE_PROPERTY_WITH_DEPENDENCY, SERVICE_ID_2)));

        assertEquals(Set.of(noDependencyService, dependsOnDefaultModeService), processGroup.findReferencedControllerServices(ComponentHierarchyScope.IMMEDIATE_GROUP_ONLY));
        assertEquals(Set.of(noDependencyService, dependsOnModeBService, dependsOnDefaultModeService), processGroup.findReferencedControllerServices(ComponentHierarchyScope.INCLUDE_CHILD_GROUPS));
        assertEquals(Set.of(dependsOnModeBService), childGroup.findReferencedControllerServices(ComponentHierarchyScope.IMMEDIATE_GROUP_ONLY));
    }

    @Test
    void testFindReferencedControllerServicesIncludesRequiredServices() {
        final ControllerServiceNode referencedService = createService(SERVICE_ID);
        final ControllerServiceNode requiredService = createService(SERVICE_ID_2);
        final ControllerServiceNode transitivelyRequiredService = createService(SERVICE_ID_3);
        when(referencedService.getRequiredControllerServices()).thenReturn(List.of(requiredService));
        when(requiredService.getRequiredControllerServices()).thenReturn(List.of(transitivelyRequiredService));

        processGroup.addProcessor(createProcessor("processor", processGroup, ScheduledState.STOPPED, List.of(SERVICE_PROPERTY_NO_DEPENDENCY),
            Map.of(SERVICE_PROPERTY_NO_DEPENDENCY, SERVICE_ID)));

        assertEquals(Set.of(referencedService, requiredService, transitivelyRequiredService), processGroup.findReferencedControllerServices(ComponentHierarchyScope.IMMEDIATE_GROUP_ONLY));
    }

    @Test
    void testDrainStopsSourcesStartsOtherComponentsAndRestrictsScheduling() {
        final StandardProcessGroup rootGroup = createDrainParent();
        final ControllerServiceNode service = createService(SERVICE_ID);
        when(controllerServiceProvider.enableControllerServiceAndDependencies(service)).thenReturn(CompletableFuture.completedFuture(null));

        // A Processor whose only incoming connection is a self-loop is a source
        final ProcessorNode sourceProcessor = createProcessor("source", processGroup, ScheduledState.RUNNING);
        final Connection selfLoop = createConnection("self-loop", sourceProcessor, sourceProcessor, 0);
        when(sourceProcessor.getIncomingConnections()).thenReturn(List.of(selfLoop));

        final ProcessorNode downstreamProcessor = createProcessor("downstream", processGroup, ScheduledState.STOPPED, List.of(SERVICE_PROPERTY_NO_DEPENDENCY),
            Map.of(SERVICE_PROPERTY_NO_DEPENDENCY, SERVICE_ID));
        final Connection sourceToDownstream = createConnection("source-to-downstream", sourceProcessor, downstreamProcessor, 3);
        when(downstreamProcessor.getIncomingConnections()).thenReturn(List.of(sourceToDownstream));

        final Port inputPort = createPort("input", ConnectableType.INPUT_PORT, processGroup, ScheduledState.RUNNING);
        processGroup.addProcessor(sourceProcessor);
        processGroup.addProcessor(downstreamProcessor);
        processGroup.addInputPort(inputPort);
        processGroup.addConnection(sourceToDownstream);

        final StandardProcessGroup childGroup = createStandardProcessGroup("child");
        childGroup.setName("Child");
        processGroup.addProcessGroup(childGroup);
        final ProcessorNode childSourceProcessor = createProcessor("child-source", childGroup, ScheduledState.STOPPED);
        final Port childInputPort = createPort("child-input", ConnectableType.INPUT_PORT, childGroup, ScheduledState.STOPPED);
        childGroup.addProcessor(childSourceProcessor);
        childGroup.addInputPort(childInputPort);

        // A Stateless child group with no source is started only if data can reach it through one of its Input Ports
        when(statelessGroupNodeFactory.createStatelessGroupNode(any())).thenAnswer(invocation -> {
            final StatelessGroupNode statelessGroupNode = mock(StatelessGroupNode.class, withSettings().strictness(Strictness.LENIENT));
            when(statelessGroupNode.getCurrentState()).thenReturn(ScheduledState.STOPPED);
            return statelessGroupNode;
        });
        final StandardProcessGroup connectedStatelessGroup = createStandardProcessGroup("connected-stateless");
        connectedStatelessGroup.setName("Connected Stateless");
        processGroup.addProcessGroup(connectedStatelessGroup);
        connectedStatelessGroup.setExecutionEngine(ExecutionEngine.STATELESS);
        final Port statelessInputPort = createPort("stateless-input", ConnectableType.INPUT_PORT, connectedStatelessGroup, ScheduledState.STOPPED);
        when(statelessInputPort.hasIncomingConnection()).thenReturn(true);
        connectedStatelessGroup.addInputPort(statelessInputPort);
        final StandardProcessGroup emptyStatelessGroup = createStandardProcessGroup("empty-stateless");
        emptyStatelessGroup.setName("Empty Stateless");
        processGroup.addProcessGroup(emptyStatelessGroup);
        emptyStatelessGroup.setExecutionEngine(ExecutionEngine.STATELESS);

        final RemoteGroupPort remoteOutputPort = createRemotePort("remote-output", ConnectableType.REMOTE_OUTPUT_PORT, ScheduledState.STOPPED, true);
        final RemoteGroupPort remoteInputPort = createRemotePort("remote-input", ConnectableType.REMOTE_INPUT_PORT, ScheduledState.STOPPED, true);
        final RemoteProcessGroup remoteProcessGroup = createRemoteProcessGroup(Set.of(remoteInputPort));
        processGroup.addRemoteProcessGroup(remoteProcessGroup);
        final ProcessorNode processorOutsideDrain = createProcessor("outside", rootGroup, ScheduledState.STOPPED);

        // The source is still running when it is asked to stop, so the drain is not prepared until the source has stopped
        final CompletableFuture<Void> sourceStopFuture = new CompletableFuture<>();
        when(processScheduler.stopProcessor(sourceProcessor, ProcessorStopLifecycleMethods.TRIGGER_ALL)).thenReturn(sourceStopFuture);

        final ProcessGroupDrain drain = processGroup.beginDrain(DRAIN_ID);
        assertEquals(ProcessGroupDrainState.DRAINING, drain.getState());
        assertEquals(Optional.of(drain), childGroup.findActiveDrain());
        assertEquals(Optional.empty(), rootGroup.findActiveDrain());
        assertFalse(drain.isPrepared());
        sourceStopFuture.complete(null);
        assertTrue(drain.isPrepared());

        // If no Controller Service needs to wait to be enabled, the drain is initiated before beginDrain returns
        verify(childSourceProcessor).invokeDrainLifecycleMethods(eq(OnDrainInitiated.class), any(ProcessContext.class));
        final InOrder inOrder = inOrder(processScheduler, controllerServiceProvider, service, downstreamProcessor);
        inOrder.verify(processScheduler).stopProcessor(sourceProcessor, ProcessorStopLifecycleMethods.TRIGGER_ALL);
        inOrder.verify(processScheduler).stopPort(inputPort);
        inOrder.verify(controllerServiceProvider).enableControllerServiceAndDependencies(service);
        inOrder.verify(processScheduler).startProcessor(downstreamProcessor, false);
        inOrder.verify(service).invokeDrainLifecycleMethods(OnDrainInitiated.class);
        inOrder.verify(downstreamProcessor).invokeDrainLifecycleMethods(eq(OnDrainInitiated.class), any(ProcessContext.class));

        verify(processScheduler).startPort(childInputPort);
        verify(processScheduler).startStatelessGroup(connectedStatelessGroup.getStatelessGroupNode().orElseThrow());
        verify(processScheduler, never()).startStatelessGroup(emptyStatelessGroup.getStatelessGroupNode().orElseThrow());
        verify(remoteProcessGroup).startTransmitting(remoteInputPort);
        verify(processScheduler, never()).startProcessor(eq(childSourceProcessor), anyBoolean());
        verify(processScheduler, never()).startProcessor(eq(sourceProcessor), anyBoolean());
        verify(sourceProcessor).invokeDrainLifecycleMethods(eq(OnDrainInitiated.class), any(ProcessContext.class));

        // While data is queued, the sources may not be started and the other components may not be stopped
        final List<Connectable> sources = List.of(sourceProcessor, childSourceProcessor, inputPort, remoteOutputPort);
        final List<Connectable> nonSources = List.of(downstreamProcessor, childInputPort, remoteInputPort);
        for (final Connectable source : sources) {
            assertTrue(drain.keepsStopped(source));
            assertFalse(drain.keepsRunning(source));
            drain.verifyCanStop(source);
            assertTrue(assertIllegalStateThrown(() -> drain.verifyCanStart(source)).contains("still has 3 FlowFiles queued"));
        }

        for (final Connectable nonSource : nonSources) {
            assertTrue(drain.keepsRunning(nonSource));
            assertFalse(drain.keepsStopped(nonSource));
            drain.verifyCanStart(nonSource);
            assertTrue(assertIllegalStateThrown(() -> drain.verifyCanStop(nonSource)).contains("still has 3 FlowFiles queued"));
        }

        assertFalse(drain.keepsStopped(processorOutsideDrain));
        assertFalse(drain.keepsRunning(processorOutsideDrain));
        drain.verifyCanStart(processorOutsideDrain);
        drain.verifyCanStop(processorOutsideDrain);

        assertTrue(drain.keepsRunning(connectedStatelessGroup));
        assertTrue(assertIllegalStateThrown(() -> drain.verifyCanStop(connectedStatelessGroup)).contains("still has 3 FlowFiles queued"));
        assertFalse(drain.keepsStopped(emptyStatelessGroup));
        assertFalse(drain.keepsRunning(emptyStatelessGroup));
        drain.verifyCanStart(emptyStatelessGroup);
        drain.verifyCanStop(emptyStatelessGroup);

        // Once no data is queued, the drain still keeps the components stopped or running but no longer prevents a user from starting or stopping them
        when(sourceToDownstream.getFlowFileQueue().size()).thenReturn(new QueueSize(0, 0L));
        for (final Connectable source : sources) {
            assertTrue(drain.keepsStopped(source));
            drain.verifyCanStart(source);
        }

        for (final Connectable nonSource : nonSources) {
            assertTrue(drain.keepsRunning(nonSource));
            drain.verifyCanStop(nonSource);
        }

        // The group keeps the ended drain so that its final status can be read, and ending it again changes nothing
        clearInvocations(processScheduler);
        final ProcessGroupDrain endedDrain = processGroup.endDrain().orElseThrow();
        assertEquals(drain, endedDrain);
        assertEquals(ProcessGroupDrainState.ENDED, endedDrain.getState());
        assertEquals(Optional.of(drain), processGroup.getDrain());
        assertEquals(Optional.empty(), childGroup.findActiveDrain());
        assertEquals(Optional.of(drain), processGroup.endDrain());

        verify(service).invokeDrainLifecycleMethods(OnDrainCompleted.class);
        verify(downstreamProcessor).invokeDrainLifecycleMethods(eq(OnDrainCompleted.class), any(ProcessContext.class));
        verify(childSourceProcessor).invokeDrainLifecycleMethods(eq(OnDrainCompleted.class), any(ProcessContext.class));
        verifyNoInteractions(processScheduler);
        verify(controllerServiceProvider, never()).disableControllerService(any());
        verify(controllerServiceProvider, never()).disableControllerServicesAsync(any());

        assertFalse(drain.keepsStopped(sourceProcessor));
        assertFalse(drain.keepsRunning(downstreamProcessor));
    }

    @Test
    void testDrainFailsOrEndsDuringPreparation() {
        createDrainParent();
        final ControllerServiceNode service = createService(SERVICE_ID);
        final ProcessorNode sourceProcessor = createProcessor("source", processGroup, ScheduledState.RUNNING);
        when(processScheduler.stopProcessor(sourceProcessor, ProcessorStopLifecycleMethods.TRIGGER_ALL))
            .thenThrow(new IllegalStateException("Processor could not be stopped"))
            .thenReturn(CompletableFuture.completedFuture(null));

        final ProcessorNode downstreamProcessor = createProcessor("downstream", processGroup, ScheduledState.STOPPED, List.of(SERVICE_PROPERTY_NO_DEPENDENCY),
            Map.of(SERVICE_PROPERTY_NO_DEPENDENCY, SERVICE_ID));
        final Connection sourceToDownstream = createConnection("source-to-downstream", sourceProcessor, downstreamProcessor, 0);
        when(downstreamProcessor.getIncomingConnections()).thenReturn(List.of(sourceToDownstream));
        final Port outputPort = createPort("output", ConnectableType.OUTPUT_PORT, processGroup, ScheduledState.STOPPED);
        processGroup.addProcessor(sourceProcessor);
        processGroup.addProcessor(downstreamProcessor);
        processGroup.addOutputPort(outputPort);

        // No drain is recorded if a source cannot be stopped
        assertTrue(assertIllegalStateThrown(() -> processGroup.beginDrain(DRAIN_ID)).contains("Processor could not be stopped"));
        assertEquals(Optional.empty(), processGroup.getDrain());

        // A drain that ends while a Controller Service is being enabled starts nothing once the service is enabled
        final CompletableFuture<Void> enableFuture = new CompletableFuture<>();
        when(controllerServiceProvider.enableControllerServiceAndDependencies(service)).thenReturn(enableFuture);
        final ProcessGroupDrain endedDrain = processGroup.beginDrain(DRAIN_ID);
        verify(processScheduler, times(2)).stopProcessor(sourceProcessor, ProcessorStopLifecycleMethods.TRIGGER_ALL);
        assertEquals(endedDrain, processGroup.endDrain().orElseThrow());
        enableFuture.complete(null);
        assertEquals(ProcessGroupDrainState.ENDED, endedDrain.getState());
        verify(processScheduler, never()).startProcessor(any(ProcessorNode.class), anyBoolean());
        verify(processScheduler, never()).startPort(any(Port.class));
        verify(service, never()).invokeDrainLifecycleMethods(any());
        verify(downstreamProcessor, never()).invokeDrainLifecycleMethods(any(), any());

        // A drain fails if a Controller Service cannot be enabled
        final CompletableFuture<Void> failedEnableFuture = new CompletableFuture<>();
        when(controllerServiceProvider.enableControllerServiceAndDependencies(service)).thenReturn(failedEnableFuture);
        final ProcessGroupDrain failedDrain = processGroup.beginDrain("drain-2");
        failedEnableFuture.completeExceptionally(new IllegalStateException("Service could not be enabled"));
        assertEquals(ProcessGroupDrainState.FAILED, failedDrain.getState());
        assertTrue(failedDrain.getFailureReason().orElseThrow().contains("Service could not be enabled"));
        assertEquals(Optional.of(failedDrain), processGroup.getDrain());
        assertEquals(Optional.empty(), processGroup.findActiveDrain());
        verify(processScheduler, never()).startProcessor(any(ProcessorNode.class), anyBoolean());
        failedDrain.verifyCanStart(sourceProcessor);
        failedDrain.verifyCanStop(downstreamProcessor);

        // A failed drain is replaced without being ended, and a drain also fails if a component cannot be started
        when(controllerServiceProvider.enableControllerServiceAndDependencies(service)).thenReturn(CompletableFuture.completedFuture(null));
        doThrow(new IllegalStateException("Output Port could not be started")).when(processScheduler).startPort(outputPort);
        final ProcessGroupDrain replacementDrain = processGroup.beginDrain("drain-3");
        assertEquals(Optional.of(replacementDrain), processGroup.getDrain());
        assertEquals(ProcessGroupDrainState.FAILED, failedDrain.getState());
        assertEquals(ProcessGroupDrainState.FAILED, replacementDrain.getState());
        assertTrue(replacementDrain.getFailureReason().orElseThrow().contains("Output Port could not be started"));

        // Ending a failed drain keeps its failure so that it can still be reported
        assertEquals(Optional.of(replacementDrain), processGroup.endDrain());
        assertEquals(ProcessGroupDrainState.FAILED, replacementDrain.getState());

        verify(service, never()).invokeDrainLifecycleMethods(any());
        verify(downstreamProcessor, never()).invokeDrainLifecycleMethods(any(), any());
    }

    @Test
    void testVerifyCanBeginDrain() {
        final StandardProcessGroup rootGroup = createDrainParent();
        final StandardProcessGroup childGroup = createStandardProcessGroup("child");
        childGroup.setName("Child");
        processGroup.addProcessGroup(childGroup);

        final ProcessGroupDrain drain = processGroup.beginDrain(DRAIN_ID);
        for (final ProcessGroup group : List.of(rootGroup, processGroup, childGroup)) {
            assertIllegalStateThrown(() -> group.beginDrain("other-drain"));
        }

        assertEquals(Optional.of(drain), processGroup.getDrain());
        assertEquals(Optional.empty(), rootGroup.getDrain());
        assertEquals(Optional.empty(), childGroup.getDrain());

        processGroup.endDrain();
        rootGroup.verifyCanBeginDrain();
        processGroup.verifyCanBeginDrain();
        childGroup.verifyCanBeginDrain();

        childGroup.setExecutionEngine(ExecutionEngine.STATELESS);
        assertIllegalStateThrown(childGroup::verifyCanBeginDrain);
    }

    @Test
    void testDrainReportsBlockingComponentsAndQueueSizes() {
        createDrainParent();
        final ProcessorNode upstreamProcessor = createProcessor("upstream", processGroup, ScheduledState.RUNNING);

        final ValidationResult validationResult = new ValidationResult.Builder().subject("Directory").valid(false).explanation("Directory does not exist").build();
        final ProcessorNode invalidProcessor = createProcessor("invalid", processGroup, ScheduledState.STOPPED);
        when(invalidProcessor.getValidationStatus()).thenReturn(ValidationStatus.INVALID);
        when(invalidProcessor.performValidation()).thenReturn(ValidationStatus.INVALID);
        when(invalidProcessor.getValidationErrors()).thenReturn(List.of(validationResult));

        final ProcessorNode disabledProcessor = createProcessor("disabled", processGroup, ScheduledState.DISABLED);
        final ProcessorNode invalidProcessorWithEmptyQueue = createProcessor("invalid-empty", processGroup, ScheduledState.STOPPED);
        when(invalidProcessorWithEmptyQueue.getValidationStatus()).thenReturn(ValidationStatus.INVALID);
        when(invalidProcessorWithEmptyQueue.performValidation()).thenReturn(ValidationStatus.INVALID);

        for (final ProcessorNode processor : List.of(upstreamProcessor, invalidProcessor, disabledProcessor, invalidProcessorWithEmptyQueue)) {
            processGroup.addProcessor(processor);
        }

        final RemoteGroupPort disabledRemoteInputPort = createRemotePort("remote-input-disabled", ConnectableType.REMOTE_INPUT_PORT, ScheduledState.DISABLED, true);
        // The Remote Process Group is invalid, which makes its ports invalid even though the ports report no validation errors of their own
        final RemoteGroupPort invalidRemoteInputPort = createRemotePort("remote-input-invalid", ConnectableType.REMOTE_INPUT_PORT, ScheduledState.STOPPED, false);
        final RemoteProcessGroup remoteProcessGroup = createRemoteProcessGroup(Set.of(disabledRemoteInputPort, invalidRemoteInputPort));
        final ValidationResult remoteGroupValid = new ValidationResult.Builder().subject("Transport Protocol").valid(true).build();
        final ValidationResult remoteGroupInvalid = new ValidationResult.Builder().subject("Network Interface").valid(false).explanation("Interface does not exist").build();
        when(remoteProcessGroup.validate()).thenReturn(List.of(remoteGroupValid, remoteGroupInvalid));
        processGroup.addRemoteProcessGroup(remoteProcessGroup);

        final Connection toInvalid = createConnection("to-invalid", upstreamProcessor, invalidProcessor, 3);
        final Connection toDisabled = createConnection("to-disabled", upstreamProcessor, disabledProcessor, 2);
        final Connection toInvalidWithEmptyQueue = createConnection("to-invalid-empty", upstreamProcessor, invalidProcessorWithEmptyQueue, 0);
        final Connection toDisabledRemote = createConnection("to-remote-disabled", upstreamProcessor, disabledRemoteInputPort, 1);
        final Connection toInvalidRemote = createConnection("to-remote-invalid", upstreamProcessor, invalidRemoteInputPort, 4);
        when(invalidProcessor.getIncomingConnections()).thenReturn(List.of(toInvalid));
        when(disabledProcessor.getIncomingConnections()).thenReturn(List.of(toDisabled));
        when(invalidProcessorWithEmptyQueue.getIncomingConnections()).thenReturn(List.of(toInvalidWithEmptyQueue));
        when(disabledRemoteInputPort.getIncomingConnections()).thenReturn(List.of(toDisabledRemote));
        when(invalidRemoteInputPort.getIncomingConnections()).thenReturn(List.of(toInvalidRemote));
        for (final Connection connection : List.of(toInvalid, toDisabled, toInvalidWithEmptyQueue, toDisabledRemote, toInvalidRemote)) {
            processGroup.addConnection(connection);
        }

        final CompletableFuture<Void> upstreamStopFuture = new CompletableFuture<>();
        when(processScheduler.stopProcessor(upstreamProcessor, ProcessorStopLifecycleMethods.TRIGGER_ALL)).thenReturn(upstreamStopFuture);
        final ProcessGroupDrain drain = processGroup.beginDrain(DRAIN_ID);
        verify(upstreamProcessor).invokeDrainLifecycleMethods(eq(OnDrainInitiated.class), any(ProcessContext.class));
        verify(processScheduler, never()).startProcessor(eq(invalidProcessor), anyBoolean());
        verify(processScheduler, never()).startProcessor(eq(disabledProcessor), anyBoolean());
        verify(remoteProcessGroup, never()).startTransmitting(any(RemoteGroupPort.class));

        final List<DrainBlockingComponent> expectedBlockingComponents = List.of(
            new DrainBlockingComponent("invalid", "invalid", ConnectableType.PROCESSOR, ID, null, DrainBlockingComponent.Reason.INVALID, List.of(validationResult.toString()), 3),
            new DrainBlockingComponent("disabled", "disabled", ConnectableType.PROCESSOR, ID, null, DrainBlockingComponent.Reason.DISABLED, List.of(), 2),
            new DrainBlockingComponent("remote-input-disabled", "remote-input-disabled", ConnectableType.REMOTE_INPUT_PORT, ID, REMOTE_GROUP_ID, DrainBlockingComponent.Reason.DISABLED,
                List.of(remoteGroupInvalid.toString()), 1),
            new DrainBlockingComponent("remote-input-invalid", "remote-input-invalid", ConnectableType.REMOTE_INPUT_PORT, ID, REMOTE_GROUP_ID, DrainBlockingComponent.Reason.INVALID,
                List.of(remoteGroupInvalid.toString()), 4));
        assertEquals(Set.copyOf(expectedBlockingComponents), Set.copyOf(drain.getBlockingComponents()));
        assertEquals(new QueueSize(10, 100L), drain.getOriginalQueueSize());
        assertEquals(new QueueSize(10, 100L), drain.getCurrentQueueSize());

        // Until the sources have stopped, the original queue size is the current queue size, because the sources may still add data
        when(toInvalid.getFlowFileQueue().size()).thenReturn(new QueueSize(5, 50L));
        assertEquals(new QueueSize(12, 120L), drain.getOriginalQueueSize());

        // The original queue size is recorded once the sources have stopped, while the current queue size follows the queues
        upstreamStopFuture.complete(null);
        when(toInvalid.getFlowFileQueue().size()).thenReturn(new QueueSize(1, 10L));
        assertEquals(new QueueSize(8, 80L), drain.getCurrentQueueSize());
        assertEquals(new QueueSize(12, 120L), drain.getOriginalQueueSize());
    }

    private static String assertIllegalStateThrown(final Runnable action) {
        try {
            action.run();
        } catch (final IllegalStateException expected) {
            return expected.getMessage();
        }

        return fail("Expected an IllegalStateException");
    }

    /**
     * Makes the Process Group under test a child of a new root group, as a drain applies to a group within the flow. The background tasks of a drain
     * run on the calling thread, so each step of a drain is finished when the call that triggers it returns.
     *
     * @return the new root group
     */
    private StandardProcessGroup createDrainParent() {
        when(processScheduler.submitFrameworkTask(any(Runnable.class))).thenAnswer(invocation -> {
            invocation.getArgument(0, Runnable.class).run();
            return CompletableFuture.completedFuture(null);
        });

        processGroup.setName(NAME);
        final StandardProcessGroup rootGroup = createStandardProcessGroup("root");
        rootGroup.addProcessGroup(processGroup);
        return rootGroup;
    }

    private ProcessorNode createProcessor(final String id, final ProcessGroup group, final ScheduledState scheduledState) {
        return createProcessor(id, group, scheduledState, List.of(), Map.of());
    }

    private ProcessorNode createProcessor(final String id, final ProcessGroup group, final ScheduledState scheduledState, final List<PropertyDescriptor> descriptors,
                                          final Map<PropertyDescriptor, String> effectiveValues) {
        final ProcessorNode processor = mock(ProcessorNode.class, withSettings().strictness(Strictness.LENIENT));
        when(processor.getIdentifier()).thenReturn(id);
        when(processor.getName()).thenReturn(id);
        when(processor.getConnectableType()).thenReturn(ConnectableType.PROCESSOR);
        when(processor.getProcessGroup()).thenReturn(group);
        when(processor.getScheduledState()).thenReturn(scheduledState);
        when(processor.isRunning()).thenReturn(scheduledState == ScheduledState.RUNNING);
        when(processor.getValidationStatus()).thenReturn(ValidationStatus.VALID);
        when(processor.performValidation()).thenReturn(ValidationStatus.VALID);
        when(processor.getPropertyDescriptors()).thenReturn(descriptors);

        final Map<PropertyDescriptor, String> effectivePropertyValues = new LinkedHashMap<>();
        for (final PropertyDescriptor descriptor : descriptors) {
            final String value = effectiveValues.get(descriptor);
            when(processor.getPropertyDescriptor(descriptor.getName())).thenReturn(descriptor);
            when(processor.getEffectivePropertyValue(descriptor)).thenReturn(value);
            if (value != null) {
                effectivePropertyValues.put(descriptor, value);
            }
        }

        when(processor.getEffectivePropertyValues()).thenReturn(effectivePropertyValues);

        // The real isDependencySatisfied method resolves property values through getProperty
        final ValidationContext validationContext = mock(ValidationContext.class, withSettings().strictness(Strictness.LENIENT));
        when(validationContext.isDependencySatisfied(any(PropertyDescriptor.class), any())).thenCallRealMethod();
        for (final Map.Entry<PropertyDescriptor, String> entry : effectivePropertyValues.entrySet()) {
            final PropertyValue propertyValue = mock(PropertyValue.class, withSettings().strictness(Strictness.LENIENT));
            when(propertyValue.getValue()).thenReturn(entry.getValue());
            when(validationContext.getProperty(entry.getKey())).thenReturn(propertyValue);
        }

        when(processor.getParameterLookup()).thenReturn(ParameterLookup.EMPTY);
        when(processor.createValidationContext(any(), any(), any(), anyBoolean())).thenReturn(validationContext);
        return processor;
    }

    private ControllerServiceNode createService(final String serviceId) {
        final ControllerServiceNode service = mock(ControllerServiceNode.class, withSettings().strictness(Strictness.LENIENT));
        when(service.getIdentifier()).thenReturn(serviceId);
        when(service.getValidationStatus()).thenReturn(ValidationStatus.VALID);
        lenient().when(controllerServiceProvider.getControllerServiceNode(serviceId)).thenReturn(service);
        return service;
    }

    private Port createPort(final String id, final ConnectableType portType, final ProcessGroup group, final ScheduledState scheduledState) {
        final Port port = mock(Port.class, withSettings().strictness(Strictness.LENIENT));
        when(port.getIdentifier()).thenReturn(id);
        when(port.getName()).thenReturn(id);
        when(port.getConnectableType()).thenReturn(portType);
        when(port.getProcessGroup()).thenReturn(group);
        when(port.getScheduledState()).thenReturn(scheduledState);
        when(port.isRunning()).thenReturn(scheduledState == ScheduledState.RUNNING);
        when(port.isValid()).thenReturn(true);
        return port;
    }

    private RemoteGroupPort createRemotePort(final String id, final ConnectableType portType, final ScheduledState scheduledState, final boolean valid) {
        final RemoteGroupPort port = mock(RemoteGroupPort.class, withSettings().strictness(Strictness.LENIENT));
        when(port.getIdentifier()).thenReturn(id);
        when(port.getName()).thenReturn(id);
        when(port.getConnectableType()).thenReturn(portType);
        when(port.getProcessGroup()).thenReturn(processGroup);
        when(port.getScheduledState()).thenReturn(scheduledState);
        when(port.isValid()).thenReturn(valid);
        when(port.hasIncomingConnection()).thenReturn(true);
        return port;
    }

    private RemoteProcessGroup createRemoteProcessGroup(final Set<RemoteGroupPort> inputPorts) {
        final RemoteProcessGroup remoteProcessGroup = mock(RemoteProcessGroup.class, withSettings().strictness(Strictness.LENIENT));
        when(remoteProcessGroup.getIdentifier()).thenReturn(REMOTE_GROUP_ID);
        when(remoteProcessGroup.getInputPorts()).thenReturn(inputPorts);
        return remoteProcessGroup;
    }

    private Connection createConnection(final String id, final Connectable source, final Connectable destination, final int queuedCount) {
        final FlowFileQueue flowFileQueue = mock(FlowFileQueue.class, withSettings().strictness(Strictness.LENIENT));
        when(flowFileQueue.size()).thenReturn(new QueueSize(queuedCount, queuedCount * 10L));

        final Connection connection = mock(Connection.class, withSettings().strictness(Strictness.LENIENT));
        when(connection.getIdentifier()).thenReturn(id);
        when(connection.getSource()).thenReturn(source);
        when(connection.getDestination()).thenReturn(destination);
        when(connection.getFlowFileQueue()).thenReturn(flowFileQueue);
        return connection;
    }

    private StandardProcessGroup createStandardProcessGroup(final String id) {
        return createStandardProcessGroup(id, null);
    }

    private StandardProcessGroup createStandardProcessGroup(final String id, final String connectorId) {
        return new StandardProcessGroup(
                id,
                controllerServiceProvider,
                processScheduler,
                propertyEncryptionProvider,
                extensionManager,
                stateManagerProvider,
                flowManager,
                reloadComponent,
                nodeTypeProvider,
                clusterTopologyProvider,
                properties,
                statelessGroupNodeFactory,
                assetManager,
                connectorId
        );
    }

    private void addConnectionWithDropStatus(final String connectionId, final DropFlowFileStatus dropStatus) {
        final Connectable source = mock(Connectable.class);
        final Connectable destination = mock(Connectable.class);
        when(source.getConnectableType()).thenReturn(ConnectableType.PROCESSOR);
        when(destination.getConnectableType()).thenReturn(ConnectableType.PROCESSOR);
        when(source.getProcessGroup()).thenReturn(processGroup);
        when(destination.getProcessGroup()).thenReturn(processGroup);

        final FlowFileQueue flowFileQueue = mock(FlowFileQueue.class);
        when(flowFileQueue.dropFlowFiles(eq("drop-all"), eq("test-user"))).thenReturn(dropStatus);

        final Connection connection = mock(Connection.class);
        when(connection.getIdentifier()).thenReturn(connectionId);
        when(connection.getSource()).thenReturn(source);
        when(connection.getDestination()).thenReturn(destination);
        when(connection.getFlowFileQueue()).thenReturn(flowFileQueue);
        when(connection.getVersionedComponentId()).thenReturn(Optional.empty());

        processGroup.addConnection(connection);
    }

}
