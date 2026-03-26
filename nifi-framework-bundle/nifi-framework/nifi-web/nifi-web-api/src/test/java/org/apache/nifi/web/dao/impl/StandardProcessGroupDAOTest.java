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
package org.apache.nifi.web.dao.impl;

import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.web.ResourceNotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class StandardProcessGroupDAOTest {

    private StandardProcessGroupDAO processGroupDAO;

    @Mock
    private FlowController flowController;

    @Mock
    private FlowManager flowManager;

    @Mock
    private ProcessGroup rootGroup;

    @Mock
    private ProcessGroup connectorManagedGroup;

    private static final String ROOT_GROUP_ID = "root-group-id";
    private static final String CONNECTOR_GROUP_ID = "connector-group-id";
    private static final String NON_EXISTENT_ID = "non-existent-id";

    @BeforeEach
    void setUp() {
        processGroupDAO = new StandardProcessGroupDAO();
        processGroupDAO.setFlowController(flowController);

        when(flowController.getFlowManager()).thenReturn(flowManager);

        // Setup root group lookup (non-connector managed)
        when(flowManager.getGroup(ROOT_GROUP_ID, null)).thenReturn(rootGroup);
        when(flowManager.getGroup(CONNECTOR_GROUP_ID, null)).thenReturn(null);
        when(flowManager.getGroup(NON_EXISTENT_ID, null)).thenReturn(null);

        // Setup connector-managed group lookup (includes all groups)
        when(flowManager.getGroup(ROOT_GROUP_ID)).thenReturn(rootGroup);
        when(flowManager.getGroup(CONNECTOR_GROUP_ID)).thenReturn(connectorManagedGroup);
        when(flowManager.getGroup(NON_EXISTENT_ID)).thenReturn(null);
    }

    @Test
    void testGetProcessGroupFromRootHierarchy() {
        final ProcessGroup result = processGroupDAO.getProcessGroup(ROOT_GROUP_ID);

        assertEquals(rootGroup, result);
    }

    @Test
    void testGetProcessGroupFromRootHierarchyWithIncludeConnectorManagedFalse() {
        final ProcessGroup result = processGroupDAO.getProcessGroup(ROOT_GROUP_ID, false);

        assertEquals(rootGroup, result);
    }

    @Test
    void testGetProcessGroupFromRootHierarchyWithIncludeConnectorManagedTrue() {
        final ProcessGroup result = processGroupDAO.getProcessGroup(ROOT_GROUP_ID, true);

        assertEquals(rootGroup, result);
    }

    @Test
    void testGetProcessGroupFromConnectorManagedWithIncludeConnectorManagedTrue() {
        final ProcessGroup result = processGroupDAO.getProcessGroup(CONNECTOR_GROUP_ID, true);

        assertEquals(connectorManagedGroup, result);
    }

    @Test
    void testGetProcessGroupFromConnectorManagedWithIncludeConnectorManagedFalseThrows() {
        assertThrows(ResourceNotFoundException.class, () ->
            processGroupDAO.getProcessGroup(CONNECTOR_GROUP_ID, false)
        );
    }

    @Test
    void testGetProcessGroupWithDefaultDoesNotFindConnectorManagedGroup() {
        assertThrows(ResourceNotFoundException.class, () ->
            processGroupDAO.getProcessGroup(CONNECTOR_GROUP_ID)
        );
    }

    @Test
    void testGetProcessGroupWithNonExistentIdThrows() {
        assertThrows(ResourceNotFoundException.class, () ->
            processGroupDAO.getProcessGroup(NON_EXISTENT_ID, true)
        );
    }

    @Test
    void testGetProcessGroupWithNonExistentIdAndIncludeConnectorManagedFalseThrows() {
        assertThrows(ResourceNotFoundException.class, () ->
            processGroupDAO.getProcessGroup(NON_EXISTENT_ID, false)
        );
    }
}
