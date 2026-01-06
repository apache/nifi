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

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.bundle.BundleDetails;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.dao.ComponentStateDAO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StandardProcessorDAOTest {

    private static final String BUNDLE_GROUP_ID = "org.apache.nifi";

    private static final String BUNDLE_VERSION = "1.0.0";

    @Mock
    private FlowController flowController;

    @Mock
    private ComponentStateDAO componentStateDAO;

    @Mock
    private ProcessorNode processorNode;

    @Mock
    private Processor processor;

    @Mock
    private ProcessGroup processGroup;

    @Mock
    private FlowManager flowManager;

    @Mock
    private ExtensionManager extensionManager;

    @Mock
    private StateManagerProvider stateManagerProvider;

    @Mock
    private StateManager stateManager;

    private StandardProcessorDAO dao;

    @BeforeEach
    void setUp() {
        dao = new StandardProcessorDAO();
        dao.setFlowController(flowController);
        dao.setComponentStateDAO(componentStateDAO);

        // Set up lenient mocks for common interactions
        lenient().when(flowController.getFlowManager()).thenReturn(flowManager);
        lenient().when(flowManager.getRootGroup()).thenReturn(processGroup);
        lenient().when(processGroup.findProcessor(anyString())).thenReturn(processorNode);
        lenient().when(processorNode.getProcessGroup()).thenReturn(processGroup);
        lenient().when(processorNode.getIdentifier()).thenReturn("test-processor-id");
    }

    @Test
    void testVerifyUpdate_NormalStateChange() {
        // Processor in STOPPED state, trying to change to RUNNING
        ProcessorDTO processorDTO = new ProcessorDTO();
        processorDTO.setId("test-processor-id");
        processorDTO.setState("RUNNING");

        when(processorNode.getScheduledState()).thenReturn(ScheduledState.STOPPED);

        // Should not throw exception
        assertDoesNotThrow(() -> dao.verifyUpdate(processorDTO));
    }

    @Test
    void testVerifyUpdate_StopStartingProcessor() {
        // Processor in STOPPED logical state but STARTING physical state
        ProcessorDTO processorDTO = new ProcessorDTO();
        processorDTO.setId("test-processor-id");
        processorDTO.setState("STOPPED");

        when(processorNode.getScheduledState()).thenReturn(ScheduledState.STOPPED);
        when(processorNode.getPhysicalScheduledState()).thenReturn(ScheduledState.STARTING);

        // Should not throw exception and should verify stop permissions
        assertDoesNotThrow(() -> dao.verifyUpdate(processorDTO));
        verify(processorNode).verifyCanStop();
    }

    @Test
    void testVerifyUpdate_StopStartingProcessor_NoPermission() {
        // Processor in STARTING physical state but no permission to stop
        ProcessorDTO processorDTO = new ProcessorDTO();
        processorDTO.setId("test-processor-id");
        processorDTO.setState("STOPPED");

        when(processorNode.getScheduledState()).thenReturn(ScheduledState.STOPPED);
        when(processorNode.getPhysicalScheduledState()).thenReturn(ScheduledState.STARTING);
        doThrow(new IllegalStateException("Cannot stop")).when(processorNode).verifyCanStop();

        // Should throw exception
        assertThrows(IllegalStateException.class, () -> dao.verifyUpdate(processorDTO));
    }

    @Test
    void testVerifyUpdate_NoSpecialCaseForDisabled() {
        // Processor in DISABLED physical state (not STARTING)
        ProcessorDTO processorDTO = new ProcessorDTO();
        processorDTO.setId("test-processor-id");
        processorDTO.setState("STOPPED");

        when(processorNode.getScheduledState()).thenReturn(ScheduledState.DISABLED);

        // Should not call special case verification
        assertDoesNotThrow(() -> dao.verifyUpdate(processorDTO));
        verify(processorNode, never()).verifyCanStop();
    }

    @Test
    void testVerifyUpdate_TransitionStatesNotAllowed() {
        // User trying to set state to STARTING (internal transition state)
        ProcessorDTO processorDTO = new ProcessorDTO();
        processorDTO.setId("test-processor-id");
        processorDTO.setState("STARTING");

        // Should throw IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> dao.verifyUpdate(processorDTO));
    }

    @Test
    void testVerifyUpdate_StoppingStateNotAllowed() {
        // User trying to set state to STOPPING (internal transition state)
        ProcessorDTO processorDTO = new ProcessorDTO();
        processorDTO.setId("test-processor-id");
        processorDTO.setState("STOPPING");

        // Should throw IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> dao.verifyUpdate(processorDTO));
    }

    @Test
    void testVerifyUpdate_ProcessorNotFound() {
        // ProcessorDTO with ID that doesn't exist
        ProcessorDTO processorDTO = new ProcessorDTO();
        processorDTO.setId("non-existent-id");
        processorDTO.setState("RUNNING");

        when(processGroup.findProcessor("non-existent-id")).thenReturn(null);

        // Should throw ResourceNotFoundException
        assertThrows(ResourceNotFoundException.class, () -> dao.verifyUpdate(processorDTO));
    }

    @Test
    void testCreateProcessor(@TempDir final File tempDir) {
        final String id = ProcessorDTO.class.getSimpleName();
        final String groupId = ProcessGroup.class.getSimpleName();
        final String processorType = ProcessorDTO.class.getCanonicalName();

        final ProcessorDTO processorDTO = new ProcessorDTO();
        processorDTO.setId(id);
        processorDTO.setType(processorType);

        final BundleCoordinate bundleCoordinate = new BundleCoordinate(BUNDLE_GROUP_ID, processorType, BUNDLE_VERSION);
        final BundleDetails bundleDetails = new BundleDetails.Builder().coordinate(bundleCoordinate).workingDir(tempDir).build();
        final Bundle bundle = new Bundle(bundleDetails, getClass().getClassLoader());
        final List<Bundle> bundles = List.of(bundle);

        when(flowManager.getGroup(eq(groupId))).thenReturn(processGroup);
        when(flowController.getExtensionManager()).thenReturn(extensionManager);
        when(flowManager.createProcessor(eq(processorType), eq(id), eq(bundleCoordinate))).thenReturn(processorNode);
        when(extensionManager.getBundles(eq(processorType))).thenReturn(bundles);
        when(processorNode.getProcessor()).thenReturn(processor);
        when(processor.getIdentifier()).thenReturn(id);
        when(flowController.getStateManagerProvider()).thenReturn(stateManagerProvider);
        when(stateManagerProvider.getStateManager(eq(id), any())).thenReturn(stateManager);

        final ProcessorNode createdProcessorNode = dao.createProcessor(groupId, processorDTO);

        assertEquals(processorNode, createdProcessorNode);
        verify(processorNode).setProcessGroup(processGroup);
    }
}
