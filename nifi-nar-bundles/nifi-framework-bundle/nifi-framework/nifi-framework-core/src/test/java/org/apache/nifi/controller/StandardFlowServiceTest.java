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
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.serialization.FlowSerializationException;
import org.apache.nifi.controller.serialization.ScheduledStateLookup;
import org.apache.nifi.controller.serialization.StandardFlowSerializer;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.events.VolatileBulletinRepository;
import org.apache.nifi.nar.ExtensionDiscoveringManager;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.registry.flow.FlowRegistryClient;
import org.apache.nifi.registry.variable.FileBasedVariableRegistry;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.w3c.dom.Document;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

/**
 */
@Ignore
public class StandardFlowServiceTest {

    private StandardFlowService flowService;
    private FlowController flowController;
    private NiFiProperties properties;
    private FlowFileEventRepository mockFlowFileEventRepository;
    private Authorizer authorizer;
    private AuditService mockAuditService;
    private StringEncryptor mockEncryptor;
    private RevisionManager revisionManager;
    private VariableRegistry variableRegistry;
    private ExtensionManager extensionManager;

    @BeforeClass
    public static void setupSuite() {
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, StandardFlowServiceTest.class.getResource("/conf/nifi.properties").getFile());
    }

    @Before
    public void setup() throws Exception {
        properties = NiFiProperties.createBasicNiFiProperties(null, null);



        variableRegistry = new FileBasedVariableRegistry(properties.getVariableRegistryPropertiesPaths());
        mockFlowFileEventRepository = mock(FlowFileEventRepository.class);
        authorizer = mock(Authorizer.class);
        mockAuditService = mock(AuditService.class);
        revisionManager = mock(RevisionManager.class);
        extensionManager = mock(ExtensionDiscoveringManager.class);
        flowController = FlowController.createStandaloneInstance(mockFlowFileEventRepository, properties, authorizer, mockAuditService, mockEncryptor,
                                        new VolatileBulletinRepository(), variableRegistry, mock(FlowRegistryClient.class), extensionManager);
        flowService = StandardFlowService.createStandaloneInstance(flowController, properties, mockEncryptor, revisionManager, authorizer);
    }

    @Test
    public void testLoadWithFlow() throws IOException {
        byte[] flowBytes = IOUtils.toByteArray(StandardFlowServiceTest.class.getResourceAsStream("/conf/all-flow.xml"));
        flowService.load(new StandardDataFlow(flowBytes, null, null, new HashSet<>()));

        StandardFlowSerializer serializer = new StandardFlowSerializer(mockEncryptor);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final Document doc = serializer.transform(flowController, ScheduledStateLookup.IDENTITY_LOOKUP);
        serializer.serialize(doc, baos);

        String expectedFlow = new String(flowBytes).trim();
        String actualFlow = new String(baos.toByteArray()).trim();

        Assert.assertEquals(expectedFlow, actualFlow);
    }

    @Test(expected = FlowSerializationException.class)
    public void testLoadWithCorruptFlow() throws IOException {
        byte[] flowBytes = IOUtils.toByteArray(StandardFlowServiceTest.class.getResourceAsStream("/conf/all-flow-corrupt.xml"));
        flowService.load(new StandardDataFlow(flowBytes, null, null, new HashSet<>()));
    }

    @Test
    public void testLoadExistingFlow() throws IOException {
        byte[] flowBytes = IOUtils.toByteArray(StandardFlowServiceTest.class.getResourceAsStream("/conf/all-flow.xml"));
        flowService.load(new StandardDataFlow(flowBytes, null, null, new HashSet<>()));

        flowBytes = IOUtils.toByteArray(StandardFlowServiceTest.class.getResourceAsStream("/conf/all-flow-inheritable.xml"));
        flowService.load(new StandardDataFlow(flowBytes, null, null, new HashSet<>()));

        StandardFlowSerializer serializer = new StandardFlowSerializer(mockEncryptor);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final Document doc = serializer.transform(flowController, ScheduledStateLookup.IDENTITY_LOOKUP);
        serializer.serialize(doc, baos);

        String expectedFlow = new String(flowBytes).trim();
        String actualFlow = new String(baos.toByteArray()).trim();
        Assert.assertEquals(expectedFlow, actualFlow);
    }

    @Test
    public void testLoadExistingFlowWithUninheritableFlow() throws IOException {
        byte[] originalBytes = IOUtils.toByteArray(StandardFlowServiceTest.class.getResourceAsStream("/conf/all-flow.xml"));
        flowService.load(new StandardDataFlow(originalBytes, null, null, new HashSet<>()));

        try {
            byte[] updatedBytes = IOUtils.toByteArray(StandardFlowServiceTest.class.getResourceAsStream("/conf/all-flow-uninheritable.xml"));
            flowService.load(new StandardDataFlow(updatedBytes, null, null, new HashSet<>()));
            fail("should have thrown " + UninheritableFlowException.class);
        } catch (UninheritableFlowException ufe) {

            StandardFlowSerializer serializer = new StandardFlowSerializer(mockEncryptor);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final Document doc = serializer.transform(flowController, ScheduledStateLookup.IDENTITY_LOOKUP);
            serializer.serialize(doc, baos);

            String expectedFlow = new String(originalBytes).trim();
            String actualFlow = new String(baos.toByteArray()).trim();

            Assert.assertEquals(expectedFlow, actualFlow);
        }
    }

    @Test
    public void testLoadExistingFlowWithCorruptFlow() throws IOException {
        byte[] originalBytes = IOUtils.toByteArray(StandardFlowServiceTest.class.getResourceAsStream("/conf/all-flow.xml"));
        flowService.load(new StandardDataFlow(originalBytes, null, null, new HashSet<>()));

        try {
            byte[] updatedBytes = IOUtils.toByteArray(StandardFlowServiceTest.class.getResourceAsStream("/conf/all-flow-corrupt.xml"));
            flowService.load(new StandardDataFlow(updatedBytes, null, null, new HashSet<>()));
            fail("should have thrown " + FlowSerializationException.class);
        } catch (FlowSerializationException ufe) {

            StandardFlowSerializer serializer = new StandardFlowSerializer(mockEncryptor);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final Document doc = serializer.transform(flowController, ScheduledStateLookup.IDENTITY_LOOKUP);
            serializer.serialize(doc, baos);

            String expectedFlow = new String(originalBytes).trim();
            String actualFlow = new String(baos.toByteArray()).trim();

            Assert.assertEquals(expectedFlow, actualFlow);
        }
    }

    private void assertEquals(ProcessGroupDTO expected, ProcessGroupDTO actual) {
        if (expected == null && actual == null) {
            return;
        }

        Assert.assertEquals(expected.getComments(), actual.getComments());
        assertEquals(expected.getContents(), actual.getContents());
    }

    private void assertEquals(FlowSnippetDTO expected, FlowSnippetDTO actual) {
        if (expected == null && actual == null) {
            return;
        }

        // check connections
        Assert.assertEquals(expected.getConnections().size(), actual.getConnections().size());
        List<ConnectionDTO> expectedConnections = new ArrayList<>(expected.getConnections());
        List<ConnectionDTO> actualConnections = new ArrayList<>(actual.getConnections());
        for (int i = 0; i < expectedConnections.size(); i++) {
            assertEquals(expectedConnections.get(i), actualConnections.get(i));
        }

        // check groups
        Assert.assertEquals(expected.getProcessGroups().size(), actual.getProcessGroups().size());
        List<ProcessGroupDTO> expectedProcessGroups = new ArrayList<>(expected.getProcessGroups());
        List<ProcessGroupDTO> actualProcessGroups = new ArrayList<>(actual.getProcessGroups());
        for (int i = 0; i < expectedProcessGroups.size(); i++) {
            assertEquals(expectedProcessGroups.get(i), actualProcessGroups.get(i));
        }

        // check input ports
        Assert.assertEquals(expected.getInputPorts().size(), actual.getInputPorts().size());
        List<PortDTO> expectedInputPorts = new ArrayList<>(expected.getInputPorts());
        List<PortDTO> actualInputPort = new ArrayList<>(actual.getInputPorts());
        for (int i = 0; i < expectedInputPorts.size(); i++) {
            assertEquals(expectedInputPorts.get(i), actualInputPort.get(i));
        }

        // check labels
        Assert.assertEquals(expected.getLabels().size(), actual.getLabels().size());
        List<LabelDTO> expectedLabels = new ArrayList<>(expected.getLabels());
        List<LabelDTO> actualLabels = new ArrayList<>(actual.getLabels());
        for (int i = 0; i < expectedLabels.size(); i++) {
            assertEquals(expectedLabels.get(i), actualLabels.get(i));
        }

        // check output ports
        Assert.assertEquals(expected.getOutputPorts().size(), actual.getOutputPorts().size());
        List<PortDTO> expectedOutputPorts = new ArrayList<>(expected.getOutputPorts());
        List<PortDTO> actualOutputPort = new ArrayList<>(actual.getOutputPorts());
        for (int i = 0; i < expectedOutputPorts.size(); i++) {
            assertEquals(expectedOutputPorts.get(i), actualOutputPort.get(i));
        }

        // check processors
        Assert.assertEquals(expected.getProcessors().size(), actual.getProcessors().size());
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

        Assert.assertEquals(expected.getAvailableRelationships(), actual.getAvailableRelationships());
        assertEquals(expected.getDestination(), actual.getDestination());
        Assert.assertEquals(expected.getId(), actual.getId());
        Assert.assertEquals(expected.getName(), actual.getName());
        Assert.assertEquals(expected.getParentGroupId(), actual.getParentGroupId());
        Assert.assertEquals(expected.getSelectedRelationships(), actual.getSelectedRelationships());
        assertEquals(expected.getSource(), actual.getSource());
    }

    private void assertEquals(ConnectableDTO expected, ConnectableDTO actual) {
        if (expected == null && actual == null) {
            return;
        }

        Assert.assertEquals(expected.getGroupId(), actual.getGroupId());
        Assert.assertEquals(expected.getId(), actual.getId());
        Assert.assertEquals(expected.getName(), actual.getName());
        Assert.assertEquals(expected.getType(), actual.getType());
    }

    private void assertEquals(PortDTO expected, PortDTO actual) {
        if (expected == null && actual == null) {
            return;
        }

        Assert.assertEquals(expected.getId(), actual.getId());
        Assert.assertEquals(expected.getName(), actual.getName());
        Assert.assertEquals(expected.getParentGroupId(), actual.getParentGroupId());
    }

    private void assertEquals(LabelDTO expected, LabelDTO actual) {
        if (expected == null && actual == null) {
            return;
        }

        Assert.assertEquals(expected.getId(), actual.getId());
        Assert.assertEquals(expected.getLabel(), actual.getLabel());
        Assert.assertEquals(expected.getParentGroupId(), actual.getParentGroupId());
        Assert.assertEquals(expected.getStyle(), actual.getStyle());
    }

    private void assertEquals(ProcessorDTO expected, ProcessorDTO actual) {
        if (expected == null && actual == null) {
            return;
        }

        Assert.assertEquals(expected.getId(), actual.getId());
        Assert.assertEquals(expected.getName(), actual.getName());
        Assert.assertEquals(expected.getParentGroupId(), actual.getParentGroupId());
        Assert.assertEquals(expected.getStyle(), actual.getStyle());
        Assert.assertEquals(expected.getType(), actual.getType());
        Assert.assertEquals(expected.getState(), actual.getState());
        Assert.assertEquals(expected.getRelationships(), actual.getRelationships());
        Assert.assertEquals(expected.getValidationErrors(), actual.getValidationErrors());
        assertEquals(expected.getConfig(), actual.getConfig());
    }

    private void assertEquals(ProcessorConfigDTO expected, ProcessorConfigDTO actual) {
        if (expected == null && actual == null) {
            return;
        }

        Assert.assertEquals(expected.getAnnotationData(), actual.getAnnotationData());
        Assert.assertEquals(expected.getComments(), actual.getComments());
        Assert.assertEquals(expected.getConcurrentlySchedulableTaskCount(), actual.getConcurrentlySchedulableTaskCount());
        Assert.assertEquals(expected.getCustomUiUrl(), actual.getCustomUiUrl());
        Assert.assertEquals(expected.getDescriptors(), actual.getDescriptors());
        Assert.assertEquals(expected.getProperties(), actual.getProperties());
        Assert.assertEquals(expected.getSchedulingPeriod(), actual.getSchedulingPeriod());
    }
}
