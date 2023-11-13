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
package org.apache.nifi.controller;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.cluster.protocol.StandardDataFlow;
import org.apache.nifi.controller.flow.VersionedDataflow;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.serialization.FlowSerializationException;
import org.apache.nifi.controller.serialization.FlowSerializer;
import org.apache.nifi.controller.serialization.ScheduledStateLookup;
import org.apache.nifi.controller.serialization.VersionedFlowSerializer;
import org.apache.nifi.controller.status.history.StatusHistoryRepository;
import org.apache.nifi.encrypt.PropertyEncryptor;
import org.apache.nifi.events.VolatileBulletinRepository;
import org.apache.nifi.nar.ExtensionDiscoveringManager;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.api.dto.ConnectableDTO;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.LabelDTO;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.revision.RevisionManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

/**
 */
@Disabled
public class StandardFlowServiceTest {

    private StandardFlowService flowService;
    private FlowController flowController;
    private NiFiProperties properties;
    private FlowFileEventRepository mockFlowFileEventRepository;
    private Authorizer authorizer;
    private AuditService mockAuditService;
    private PropertyEncryptor mockEncryptor;
    private RevisionManager revisionManager;
    private ExtensionDiscoveringManager extensionManager;
    private StatusHistoryRepository statusHistoryRepository;

    @BeforeAll
    public static void setupSuite() {
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, StandardFlowServiceTest.class.getResource("/conf/nifi.properties").getFile());
    }

    @BeforeEach
    public void setup() throws Exception {
        properties = NiFiProperties.createBasicNiFiProperties(null);
        mockFlowFileEventRepository = mock(FlowFileEventRepository.class);
        authorizer = mock(Authorizer.class);
        mockAuditService = mock(AuditService.class);
        revisionManager = mock(RevisionManager.class);
        extensionManager = mock(ExtensionDiscoveringManager.class);
        flowController = FlowController.createStandaloneInstance(mockFlowFileEventRepository, properties, authorizer, mockAuditService, mockEncryptor,
                                        new VolatileBulletinRepository(), extensionManager, statusHistoryRepository, null);
        flowService = StandardFlowService.createStandaloneInstance(flowController, properties, revisionManager, authorizer);
        statusHistoryRepository = mock(StatusHistoryRepository.class);
    }

    @Test
    public void testLoadWithFlow() throws IOException {
        byte[] flowBytes = IOUtils.toByteArray(StandardFlowServiceTest.class.getResourceAsStream("/conf/all-flow.json"));
        flowService.load(new StandardDataFlow(flowBytes, null, null, new HashSet<>()));

        final FlowSerializer<VersionedDataflow> serializer = new VersionedFlowSerializer(extensionManager);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final VersionedDataflow doc = serializer.transform(flowController, ScheduledStateLookup.IDENTITY_LOOKUP);
        serializer.serialize(doc, baos);

        String expectedFlow = new String(flowBytes).trim();
        String actualFlow = new String(baos.toByteArray()).trim();

        Assertions.assertEquals(expectedFlow, actualFlow);
    }

    @Test
    public void testLoadWithCorruptFlow() throws IOException {
        byte[] flowBytes = IOUtils.toByteArray(StandardFlowServiceTest.class.getResourceAsStream("/conf/all-flow-corrupt.xml"));

        assertThrows(FlowSerializationException.class, () ->
                flowService.load(new StandardDataFlow(flowBytes, null,
                        null, new HashSet<>())));
    }

    @Test
    public void testLoadExistingFlow() throws IOException {
        byte[] flowBytes = IOUtils.toByteArray(StandardFlowServiceTest.class.getResourceAsStream("/conf/all-flow.json"));
        flowService.load(new StandardDataFlow(flowBytes, null, null, new HashSet<>()));

        flowBytes = IOUtils.toByteArray(StandardFlowServiceTest.class.getResourceAsStream("/conf/all-flow-inheritable.json"));
        flowService.load(new StandardDataFlow(flowBytes, null, null, new HashSet<>()));

        final FlowSerializer<VersionedDataflow> serializer = new VersionedFlowSerializer(extensionManager);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final VersionedDataflow doc = serializer.transform(flowController, ScheduledStateLookup.IDENTITY_LOOKUP);
        serializer.serialize(doc, baos);

        String expectedFlow = new String(flowBytes).trim();
        String actualFlow = new String(baos.toByteArray()).trim();
        Assertions.assertEquals(expectedFlow, actualFlow);
    }

    @Test
    public void testLoadExistingFlowWithUninheritableFlow() throws IOException {
        byte[] originalBytes = IOUtils.toByteArray(StandardFlowServiceTest.class.getResourceAsStream("/conf/all-flow.json"));
        flowService.load(new StandardDataFlow(originalBytes, null, null, new HashSet<>()));

        try {
            byte[] updatedBytes = IOUtils.toByteArray(StandardFlowServiceTest.class.getResourceAsStream("/conf/all-flow-uninheritable.json"));
            flowService.load(new StandardDataFlow(updatedBytes, null, null, new HashSet<>()));
            fail("should have thrown " + UninheritableFlowException.class);
        } catch (UninheritableFlowException ufe) {
            final FlowSerializer<VersionedDataflow> serializer = new VersionedFlowSerializer(extensionManager);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final VersionedDataflow doc = serializer.transform(flowController, ScheduledStateLookup.IDENTITY_LOOKUP);
            serializer.serialize(doc, baos);

            String expectedFlow = new String(originalBytes).trim();
            String actualFlow = new String(baos.toByteArray()).trim();

            Assertions.assertEquals(expectedFlow, actualFlow);
        }
    }

    @Test
    public void testLoadExistingFlowWithCorruptFlow() throws IOException {
        byte[] originalBytes = IOUtils.toByteArray(StandardFlowServiceTest.class.getResourceAsStream("/conf/all-flow.json"));
        flowService.load(new StandardDataFlow(originalBytes, null, null, new HashSet<>()));

        try {
            byte[] updatedBytes = IOUtils.toByteArray(StandardFlowServiceTest.class.getResourceAsStream("/conf/all-flow-corrupt.xml"));
            flowService.load(new StandardDataFlow(updatedBytes, null, null, new HashSet<>()));
            fail("should have thrown " + FlowSerializationException.class);
        } catch (FlowSerializationException ufe) {
            final FlowSerializer<VersionedDataflow> serializer = new VersionedFlowSerializer(extensionManager);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final VersionedDataflow doc = serializer.transform(flowController, ScheduledStateLookup.IDENTITY_LOOKUP);
            serializer.serialize(doc, baos);

            String expectedFlow = new String(originalBytes).trim();
            String actualFlow = new String(baos.toByteArray()).trim();

            Assertions.assertEquals(expectedFlow, actualFlow);
        }
    }

    private void assertEquals(ProcessGroupDTO expected, ProcessGroupDTO actual) {
        if (expected == null && actual == null) {
            return;
        }

        Assertions.assertEquals(expected.getComments(), actual.getComments());
        assertEquals(expected.getContents(), actual.getContents());
    }

    private void assertEquals(FlowSnippetDTO expected, FlowSnippetDTO actual) {
        if (expected == null && actual == null) {
            return;
        }

        // check connections
        Assertions.assertEquals(expected.getConnections().size(), actual.getConnections().size());
        List<ConnectionDTO> expectedConnections = new ArrayList<>(expected.getConnections());
        List<ConnectionDTO> actualConnections = new ArrayList<>(actual.getConnections());
        for (int i = 0; i < expectedConnections.size(); i++) {
            assertEquals(expectedConnections.get(i), actualConnections.get(i));
        }

        // check groups
        Assertions.assertEquals(expected.getProcessGroups().size(), actual.getProcessGroups().size());
        List<ProcessGroupDTO> expectedProcessGroups = new ArrayList<>(expected.getProcessGroups());
        List<ProcessGroupDTO> actualProcessGroups = new ArrayList<>(actual.getProcessGroups());
        for (int i = 0; i < expectedProcessGroups.size(); i++) {
            assertEquals(expectedProcessGroups.get(i), actualProcessGroups.get(i));
        }

        // check input ports
        Assertions.assertEquals(expected.getInputPorts().size(), actual.getInputPorts().size());
        List<PortDTO> expectedInputPorts = new ArrayList<>(expected.getInputPorts());
        List<PortDTO> actualInputPort = new ArrayList<>(actual.getInputPorts());
        for (int i = 0; i < expectedInputPorts.size(); i++) {
            assertEquals(expectedInputPorts.get(i), actualInputPort.get(i));
        }

        // check labels
        Assertions.assertEquals(expected.getLabels().size(), actual.getLabels().size());
        List<LabelDTO> expectedLabels = new ArrayList<>(expected.getLabels());
        List<LabelDTO> actualLabels = new ArrayList<>(actual.getLabels());
        for (int i = 0; i < expectedLabels.size(); i++) {
            assertEquals(expectedLabels.get(i), actualLabels.get(i));
        }

        // check output ports
        Assertions.assertEquals(expected.getOutputPorts().size(), actual.getOutputPorts().size());
        List<PortDTO> expectedOutputPorts = new ArrayList<>(expected.getOutputPorts());
        List<PortDTO> actualOutputPort = new ArrayList<>(actual.getOutputPorts());
        for (int i = 0; i < expectedOutputPorts.size(); i++) {
            assertEquals(expectedOutputPorts.get(i), actualOutputPort.get(i));
        }

        // check processors
        Assertions.assertEquals(expected.getProcessors().size(), actual.getProcessors().size());
        List<ProcessorDTO> expectedProcessors = new ArrayList<>(expected.getProcessors());
        List<ProcessorDTO> actualProcessors = new ArrayList<>(actual.getProcessors());
        for (int i = 0; i < expectedProcessors.size(); i++) {
            assertEquals(expectedProcessors.get(i), actualProcessors.get(i));
        }
    }

    private void assertEquals(ConnectionDTO expected, ConnectionDTO actual) {
        if (expected == null && actual == null) {
            return;
        }

        Assertions.assertEquals(expected.getAvailableRelationships(), actual.getAvailableRelationships());
        assertEquals(expected.getDestination(), actual.getDestination());
        Assertions.assertEquals(expected.getId(), actual.getId());
        Assertions.assertEquals(expected.getName(), actual.getName());
        Assertions.assertEquals(expected.getParentGroupId(), actual.getParentGroupId());
        Assertions.assertEquals(expected.getSelectedRelationships(), actual.getSelectedRelationships());
        assertEquals(expected.getSource(), actual.getSource());
    }

    private void assertEquals(ConnectableDTO expected, ConnectableDTO actual) {
        if (expected == null && actual == null) {
            return;
        }

        Assertions.assertEquals(expected.getGroupId(), actual.getGroupId());
        Assertions.assertEquals(expected.getId(), actual.getId());
        Assertions.assertEquals(expected.getName(), actual.getName());
        Assertions.assertEquals(expected.getType(), actual.getType());
    }

    private void assertEquals(PortDTO expected, PortDTO actual) {
        if (expected == null && actual == null) {
            return;
        }

        Assertions.assertEquals(expected.getId(), actual.getId());
        Assertions.assertEquals(expected.getName(), actual.getName());
        Assertions.assertEquals(expected.getParentGroupId(), actual.getParentGroupId());
    }

    private void assertEquals(LabelDTO expected, LabelDTO actual) {
        if (expected == null && actual == null) {
            return;
        }

        Assertions.assertEquals(expected.getId(), actual.getId());
        Assertions.assertEquals(expected.getLabel(), actual.getLabel());
        Assertions.assertEquals(expected.getParentGroupId(), actual.getParentGroupId());
        Assertions.assertEquals(expected.getStyle(), actual.getStyle());
    }

    private void assertEquals(ProcessorDTO expected, ProcessorDTO actual) {
        if (expected == null && actual == null) {
            return;
        }

        Assertions.assertEquals(expected.getId(), actual.getId());
        Assertions.assertEquals(expected.getName(), actual.getName());
        Assertions.assertEquals(expected.getParentGroupId(), actual.getParentGroupId());
        Assertions.assertEquals(expected.getStyle(), actual.getStyle());
        Assertions.assertEquals(expected.getType(), actual.getType());
        Assertions.assertEquals(expected.getState(), actual.getState());
        Assertions.assertEquals(expected.getRelationships(), actual.getRelationships());
        Assertions.assertEquals(expected.getValidationErrors(), actual.getValidationErrors());
        assertEquals(expected.getConfig(), actual.getConfig());
    }

    private void assertEquals(ProcessorConfigDTO expected, ProcessorConfigDTO actual) {
        if (expected == null && actual == null) {
            return;
        }

        Assertions.assertEquals(expected.getAnnotationData(), actual.getAnnotationData());
        Assertions.assertEquals(expected.getComments(), actual.getComments());
        Assertions.assertEquals(expected.getConcurrentlySchedulableTaskCount(), actual.getConcurrentlySchedulableTaskCount());
        Assertions.assertEquals(expected.getCustomUiUrl(), actual.getCustomUiUrl());
        Assertions.assertEquals(expected.getDescriptors(), actual.getDescriptors());
        Assertions.assertEquals(expected.getProperties(), actual.getProperties());
        Assertions.assertEquals(expected.getSchedulingPeriod(), actual.getSchedulingPeriod());
    }
}
