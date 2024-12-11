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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.controller.flow.VersionedDataflow;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.tests.system.InstanceConfiguration;
import org.apache.nifi.tests.system.NiFiInstance;
import org.apache.nifi.tests.system.NiFiInstanceFactory;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.tests.system.SpawnedClusterNiFiInstanceFactory;
import org.apache.nifi.toolkit.client.FlowClient;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.toolkit.client.ReportingTasksClient;
import org.apache.nifi.web.api.dto.AffectedComponentDTO;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.ParameterContextDTO;
import org.apache.nifi.web.api.dto.ParameterContextReferenceDTO;
import org.apache.nifi.web.api.dto.flow.FlowDTO;
import org.apache.nifi.web.api.dto.flow.ProcessGroupFlowDTO;
import org.apache.nifi.web.api.entity.AffectedComponentEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ControllerServicesEntity;
import org.apache.nifi.web.api.entity.ParameterEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.ReportingTaskEntity;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class JoinClusterWithDifferentFlowIT extends NiFiSystemIT {
    @Override
    public NiFiInstanceFactory getInstanceFactory() {
        final Map<String, String> propertyOverrides = Collections.singletonMap("nifi.cluster.flow.serialization.format", "JSON");

        return new SpawnedClusterNiFiInstanceFactory(
                new InstanceConfiguration.Builder()
                        .bootstrapConfig("src/test/resources/conf/clustered/node1/bootstrap.conf")
                        .instanceDirectory("target/node1")
                        .overrideNifiProperties(propertyOverrides)
                        .flowJson(new File("src/test/resources/flows/mismatched-flows/flow1.json.gz"))
                        .build(),
                new InstanceConfiguration.Builder()
                        .bootstrapConfig("src/test/resources/conf/clustered/node2/bootstrap.conf")
                        .instanceDirectory("target/node2")
                        .flowJson(new File("src/test/resources/flows/mismatched-flows/flow1.json.gz"))
                        .overrideNifiProperties(propertyOverrides)
                        .build());
    }

    @Test
    public void testStartupWithDifferentFlow() throws IOException, NiFiClientException, InterruptedException {
        // Ensure that the root-level controller service is enabled
        waitFor(() -> {
            final ControllerServiceDTO rootService = getNifiClient().getControllerServicesClient().getControllerService("65b6f2b0-016e-1000-1bfa-6bc482d8cd2b").getComponent();
            return "ENABLED".equals(rootService.getState());
        });

        // Once we've started up, we want to have node 2 startup with a different flow.
        // We cannot simply startup both nodes at the same time with
        // different flows because then either flow could be elected the "correct flow"
        // and as a result, we don't know which node to look at to ensure
        // that the proper flow resolution occurred.
        // To avoid that situation, we let both nodes startup with flow 1. Then we
        // shutdown node 2, delete its flow, replace it with flow2.xml.gz from our
        // mismatched-flows
        // directory, and restart, which will ensure that Node 1 will be elected primary
        // and hold the "correct" copy of the flow.
        final NiFiInstance node2 = getNiFiInstance().getNodeInstance(2);
        node2.stop();

        final File node2ConfDir = new File(node2.getInstanceDirectory(), "conf");
        final File flowJsonFile = new File(node2ConfDir, "flow.json.gz");
        Files.deleteIfExists(flowJsonFile.toPath());
        Files.copy(Paths.get("src/test/resources/flows/mismatched-flows/flow2.json.gz"), flowJsonFile.toPath());

        node2.start(true);

        waitForAllNodesConnected();
        switchClientToNode(2);

        final File backupFile = getBackupFile(node2ConfDir);
        verifyFlowContentsOnDisk(backupFile);
        verifyInMemoryFlowContents();
    }

    private List<File> getFlowJsonFiles(final File confDir) {
        final File[] flowJsonFileArray = confDir.listFiles(file -> file.getName().startsWith("flow") && file.getName().endsWith(".json.gz"));
        assertNotNull(flowJsonFileArray);
        final List<File> flowJsonFiles = Arrays.asList(flowJsonFileArray);
        return flowJsonFiles;
    }

    private File getBackupFile(final File confDir) throws InterruptedException {
        waitFor(() -> getFlowJsonFiles(confDir).size() == 1);

        final List<File> flowJsonFiles = getFlowJsonFiles(confDir);
        assertEquals(1, flowJsonFiles.size());

        return flowJsonFiles.get(0);
    }

    private void verifyFlowContentsOnDisk(final File backupFile) throws IOException {
        // Verify some of the values that were persisted to disk
        final File confDir = backupFile.getParentFile();
        final String loadedFlow = readFlow(new File(confDir, "flow.json.gz"));

        final ObjectMapper objectMapper = new ObjectMapper();
        final VersionedDataflow versionedDataflow = objectMapper.readValue(loadedFlow, VersionedDataflow.class);
        final VersionedProcessGroup rootGroup = versionedDataflow.getRootGroup();
        assertEquals(1, rootGroup.getProcessGroups().size());

        final VersionedProcessGroup childGroup = rootGroup.getProcessGroups().iterator().next();
        assertFalse(childGroup.getIdentifier().endsWith("00"));

        assertEquals(1, childGroup.getProcessors().size());
        final VersionedProcessor childProcessor = childGroup.getProcessors().iterator().next();

        assertFalse(childProcessor.getIdentifier().endsWith("00"));
        assertFalse(childProcessor.getName().endsWith("00"));
    }

    private void verifyInMemoryFlowContents() throws NiFiClientException, IOException {
        final FlowClient flowClient = getNifiClient().getFlowClient(DO_NOT_REPLICATE);
        final ProcessGroupFlowDTO rootGroupFlow = flowClient.getProcessGroup("root").getProcessGroupFlow();
        final FlowDTO flowDto = rootGroupFlow.getFlow();
        assertEquals(1, flowDto.getProcessGroups().size());

        final ParameterContextReferenceDTO paramContextReference = flowDto.getProcessGroups().iterator().next().getParameterContext().getComponent();
        assertEquals("65b6403c-016e-1000-900b-357b13fcc7c4", paramContextReference.getId());
        assertEquals("Context 1", paramContextReference.getName());

        final ProcessorEntity generateFlowFileEntity = getNifiClient().getProcessorClient(DO_NOT_REPLICATE).getProcessor("65b8f293-016e-1000-7b8f-6c6752fa921b");
        final Map<String, String> generateProperties = generateFlowFileEntity.getComponent().getConfig().getProperties();
        assertEquals("01 B", generateProperties.get("File Size"));
        assertEquals("1", generateProperties.get("Batch Size"));
        assertEquals("1 hour", generateFlowFileEntity.getComponent().getConfig().getSchedulingPeriod());

        assertEquals(1, getNifiClient().getParamContextClient(DO_NOT_REPLICATE).getParamContexts().getParameterContexts().size());
        final ParameterContextDTO contextDto = getNifiClient().getParamContextClient(DO_NOT_REPLICATE).getParamContext(paramContextReference.getId(), false).getComponent();
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

        // The original Controller Service, whose UUID ended with 00 should be removed
        // and a new one inherited.
        final ControllerServicesEntity controllerLevelServices = getNifiClient().getFlowClient(DO_NOT_REPLICATE).getControllerServices();
        assertEquals(1, controllerLevelServices.getControllerServices().size());

        final ControllerServiceEntity firstService = controllerLevelServices.getControllerServices().iterator().next();
        assertFalse(firstService.getId().endsWith("00"));

        final ReportingTasksClient reportingTasksClient = getNifiClient().getReportingTasksClient(DO_NOT_REPLICATE);
        final ReportingTaskEntity taskEntity = reportingTasksClient.getReportingTask("65b75baf-016e-1000-13f9-cbcfa0a26576");
        assertNotNull(taskEntity);

        // Service with ID ending in 00 should no longer exist
        assertThrows(NiFiClientException.class, () -> reportingTasksClient.getReportingTask("65b75baf-016e-1000-13f9-cbcfa0a2657600"));
    }

    private String readFlow(final File file) throws IOException {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                final InputStream fis = new FileInputStream(file);
                final InputStream gzipIn = new GZIPInputStream(fis)) {

            final byte[] buffer = new byte[4096];
            int len;
            while ((len = gzipIn.read(buffer)) > 0) {
                baos.write(buffer, 0, len);
            }

            return baos.toString(StandardCharsets.UTF_8);
        }
    }
}