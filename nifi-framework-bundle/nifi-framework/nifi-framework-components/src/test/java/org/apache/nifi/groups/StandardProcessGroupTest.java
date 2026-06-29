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

import org.apache.nifi.asset.AssetManager;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.controller.ClusterTopologyProvider;
import org.apache.nifi.controller.NodeTypeProvider;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ReloadComponent;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.encrypt.PropertyEncryptor;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.registry.flow.VersionControlInformation;
import org.apache.nifi.registry.flow.VersionedFlowStatus;
import org.apache.nifi.util.NiFiProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

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

    @Mock
    private ControllerServiceProvider controllerServiceProvider;

    @Mock
    private ProcessScheduler processScheduler;

    @Mock
    private PropertyEncryptor propertyEncryptor;

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
                propertyEncryptor,
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

    private StandardProcessGroup createStandardProcessGroup(final String id) {
        return new StandardProcessGroup(
                id,
                controllerServiceProvider,
                processScheduler,
                propertyEncryptor,
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

}
