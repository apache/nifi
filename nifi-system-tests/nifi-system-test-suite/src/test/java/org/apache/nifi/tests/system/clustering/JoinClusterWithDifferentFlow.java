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
package org.apache.nifi.tests.system.clustering;

import org.apache.nifi.controller.serialization.FlowEncodingVersion;
import org.apache.nifi.controller.serialization.FlowFromDOMFactory;
import org.apache.nifi.encrypt.PropertyEncryptor;
import org.apache.nifi.encrypt.PropertyEncryptorFactory;
import org.apache.nifi.security.xml.XmlUtils;
import org.apache.nifi.tests.system.InstanceConfiguration;
import org.apache.nifi.tests.system.NiFiInstance;
import org.apache.nifi.tests.system.NiFiInstanceFactory;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.tests.system.SpawnedClusterNiFiInstanceFactory;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.api.dto.AffectedComponentDTO;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.NodeDTO;
import org.apache.nifi.web.api.dto.ParameterContextDTO;
import org.apache.nifi.web.api.dto.ParameterContextReferenceDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.flow.FlowDTO;
import org.apache.nifi.web.api.dto.flow.ProcessGroupFlowDTO;
import org.apache.nifi.web.api.entity.AffectedComponentEntity;
import org.apache.nifi.web.api.entity.ClusterEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ControllerServicesEntity;
import org.apache.nifi.web.api.entity.NodeEntity;
import org.apache.nifi.web.api.entity.ParameterEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class JoinClusterWithDifferentFlow extends NiFiSystemIT {
    @Override
    protected NiFiInstanceFactory getInstanceFactory() {
        return new SpawnedClusterNiFiInstanceFactory(
            new InstanceConfiguration.Builder()
                .bootstrapConfig("src/test/resources/conf/clustered/node1/bootstrap.conf")
                .instanceDirectory("target/node1")
                .flowXml(new File("src/test/resources/flows/mismatched-flows/flow1.xml.gz"))
                .build(),
            new InstanceConfiguration.Builder()
                .bootstrapConfig("src/test/resources/conf/clustered/node2/bootstrap.conf")
                .instanceDirectory("target/node2")
                .flowXml(new File("src/test/resources/flows/mismatched-flows/flow2.xml.gz"))
                .build()
        );
    }


    @Test
    public void testStartupWithDifferentFlow() throws IOException, SAXException, ParserConfigurationException, NiFiClientException, InterruptedException {
        final NiFiInstance node2 = getNiFiInstance().getNodeInstance(2);
        final File node2ConfDir = new File(node2.getInstanceDirectory(), "conf");

        final File backupFile = getBackupFile(node2ConfDir);
        final NodeDTO node2Dto = getNodeDTO(5672);

        verifyFlowContentsOnDisk(backupFile);
        disconnectNode(node2Dto);
        verifyInMemoryFlowContents();

        // Reconnect the node so that we can properly shutdown
        reconnectNode(node2Dto);
    }


    private List<File> getFlowXmlFiles(final File confDir) {
        final File[] flowXmlFileArray = confDir.listFiles(file -> file.getName().startsWith("flow") && file.getName().endsWith(".xml.gz"));
        final List<File> flowXmlFiles = new ArrayList<>(Arrays.asList(flowXmlFileArray));
        return flowXmlFiles;
    }

    private File getBackupFile(final File confDir) throws InterruptedException {
        waitFor(() -> getFlowXmlFiles(confDir).size() == 2);

        final List<File> flowXmlFiles = getFlowXmlFiles(confDir);
        assertEquals(2, flowXmlFiles.size());

        flowXmlFiles.removeIf(file -> file.getName().equals("flow.xml.gz"));

        assertEquals(1, flowXmlFiles.size());
        final File backupFile = flowXmlFiles.get(0);
        return backupFile;
    }

    private void verifyFlowContentsOnDisk(final File backupFile) throws IOException, SAXException, ParserConfigurationException {
        // Read the flow and make sure that the backup looks the same as the original. We don't just do a byte comparison because the compression may result in different
        // gzipped bytes and because if the two flows do differ, we want to have the String representation so that we can compare to see how they are different.
        final String flowXml = readFlow(backupFile);
        final String expectedFlow = readFlow(new File("src/test/resources/flows/mismatched-flows/flow2.xml.gz"));

        assertEquals(expectedFlow, flowXml);

        // Verify some of the values that were persisted to disk
        final File confDir = backupFile.getParentFile();
        final String loadedFlow = readFlow(new File(confDir, "flow.xml.gz"));

        final DocumentBuilder documentBuilder = XmlUtils.createSafeDocumentBuilder(false);
        final Document document = documentBuilder.parse(new InputSource(new StringReader(loadedFlow)));
        final Element rootElement = (Element) document.getElementsByTagName("flowController").item(0);
        final FlowEncodingVersion encodingVersion = FlowEncodingVersion.parse(rootElement);

        final NiFiInstance node2 = getNiFiInstance().getNodeInstance(2);
        final PropertyEncryptor encryptor = createEncryptorFromProperties(node2.getProperties());
        final Element rootGroupElement = (Element) rootElement.getElementsByTagName("rootGroup").item(0);

        final ProcessGroupDTO groupDto = FlowFromDOMFactory.getProcessGroup(null, rootGroupElement, encryptor, encodingVersion);
        final Set<ProcessGroupDTO> childGroupDtos = groupDto.getContents().getProcessGroups();
        assertEquals(1, childGroupDtos.size());

        final ProcessGroupDTO childGroup = childGroupDtos.iterator().next();
        assertFalse(childGroup.getId().endsWith("00"));
        final FlowSnippetDTO childContents = childGroup.getContents();

        final Set<ProcessorDTO> childProcessors = childContents.getProcessors();
        assertEquals(1, childProcessors.size());

        final ProcessorDTO procDto = childProcessors.iterator().next();
        assertFalse(procDto.getId().endsWith("00"));
        assertFalse(procDto.getName().endsWith("00"));
    }


    private NodeDTO getNodeDTO(final int apiPort) throws NiFiClientException, IOException {
        final ClusterEntity clusterEntity = getNifiClient().getControllerClient().getNodes();
        final NodeDTO node2Dto = clusterEntity.getCluster().getNodes().stream()
            .filter(nodeDto -> nodeDto.getApiPort() == apiPort)
            .findAny()
            .orElseThrow(() -> new RuntimeException("Could not locate Node 2"));

        return node2Dto;
    }

    private void disconnectNode(final NodeDTO nodeDto) throws NiFiClientException, IOException, InterruptedException {
        // Disconnect Node 2 so that we can go to the node directly via the REST API and ensure that the flow is correct.
        final NodeEntity nodeEntity = new NodeEntity();
        nodeEntity.setNode(nodeDto);

        getNifiClient().getControllerClient().disconnectNode(nodeDto.getNodeId(), nodeEntity);

        // Give the node a second to disconnect
        Thread.sleep(1000L);
    }

    private void reconnectNode(final NodeDTO nodeDto) throws NiFiClientException, IOException {
        final NodeEntity nodeEntity = new NodeEntity();
        nodeEntity.setNode(nodeDto);
        getNifiClient().getControllerClient().connectNode(nodeDto.getNodeId(), nodeEntity);
        waitForAllNodesConnected();
    }

    private void verifyInMemoryFlowContents() throws NiFiClientException, IOException, InterruptedException {
        final NiFiClient node2Client = createClient(5672);
        final ProcessGroupFlowDTO rootGroupFlow = node2Client.getFlowClient().getProcessGroup("root").getProcessGroupFlow();
        final FlowDTO flowDto = rootGroupFlow.getFlow();
        assertEquals(1, flowDto.getProcessGroups().size());

        final ParameterContextReferenceDTO paramContextReference = flowDto.getProcessGroups().iterator().next().getParameterContext().getComponent();
        assertEquals("65b6403c-016e-1000-900b-357b13fcc7c4", paramContextReference.getId());
        assertEquals("Context 1", paramContextReference.getName());

        ProcessorEntity generateFlowFileEntity = node2Client.getProcessorClient().getProcessor("65b8f293-016e-1000-7b8f-6c6752fa921b");
        final Map<String, String> generateProperties = generateFlowFileEntity.getComponent().getConfig().getProperties();
        assertEquals("01 B", generateProperties.get("File Size"));
        assertEquals("1", generateProperties.get("Batch Size"));

        assertEquals("1 hour", generateFlowFileEntity.getComponent().getConfig().getSchedulingPeriod());

        String currentState = null;
        while ("RUNNING".equals(currentState)) {
            Thread.sleep(50L);
            generateFlowFileEntity = node2Client.getProcessorClient().getProcessor("65b8f293-016e-1000-7b8f-6c6752fa921b");
            currentState = generateFlowFileEntity.getComponent().getState();
        }

        final ParameterContextDTO contextDto = node2Client.getParamContextClient().getParamContext(paramContextReference.getId()).getComponent();
        assertEquals(2, contextDto.getBoundProcessGroups().size());
        assertEquals(1, contextDto.getParameters().size());
        final ParameterEntity parameterEntity = contextDto.getParameters().iterator().next();
        assertEquals("ABC", parameterEntity.getParameter().getName());
        assertEquals("XYZ", parameterEntity.getParameter().getValue());

        final Set<AffectedComponentEntity> affectedComponentEntities = parameterEntity.getParameter().getReferencingComponents();
        assertEquals(1, affectedComponentEntities.size());
        final AffectedComponentDTO affectedComponent = affectedComponentEntities.iterator().next().getComponent();
        assertEquals("65b8f293-016e-1000-7b8f-6c6752fa921b", affectedComponent.getId());
        assertEquals(AffectedComponentDTO.COMPONENT_TYPE_PROCESSOR, affectedComponent.getReferenceType());

        // The original Controller Service, whose UUID ended with 00 should be removed and a new one inherited.
        final ControllerServicesEntity controllerLevelServices = node2Client.getFlowClient().getControllerServices();
        assertEquals(1, controllerLevelServices.getControllerServices().size());

        final ControllerServiceEntity firstService = controllerLevelServices.getControllerServices().iterator().next();
        assertFalse(firstService.getId().endsWith("00"));
    }

    private PropertyEncryptor createEncryptorFromProperties(Properties properties) {
        final NiFiProperties niFiProperties = NiFiProperties.createBasicNiFiProperties(null, properties);
        return PropertyEncryptorFactory.getPropertyEncryptor(niFiProperties);
    }

    private String readFlow(final File file) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try (final InputStream fis = new FileInputStream(file);
             final InputStream gzipIn = new GZIPInputStream(fis)) {

            final byte[] buffer = new byte[4096];
            int len;
            while ((len = gzipIn.read(buffer)) > 0) {
                baos.write(buffer, 0, len);
            }
        }

        final byte[] bytes = baos.toByteArray();
        return new String(bytes, StandardCharsets.UTF_8);
    }
}