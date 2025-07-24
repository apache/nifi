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
import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.apache.nifi.controller.flow.VersionedDataflow;
import org.apache.nifi.controller.queue.LoadBalanceCompression;
import org.apache.nifi.controller.queue.LoadBalanceStrategy;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.tests.system.NiFiInstanceFactory;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.flow.FlowDTO;
import org.apache.nifi.web.api.entity.AffectedComponentEntity;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.FlowFileEntity;
import org.apache.nifi.web.api.entity.ParameterContextEntity;
import org.apache.nifi.web.api.entity.ParameterContextUpdateRequestEntity;
import org.apache.nifi.web.api.entity.ParameterEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.ReportingTaskEntity;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FlowSynchronizationIT extends NiFiSystemIT {
    private static final Logger logger = LoggerFactory.getLogger(FlowSynchronizationIT.class);
    private static final String RUNNING_STATE = "RUNNING";
    private static final String ENABLED_STATE = "ENABLED";
    private static final String SENSITIVE_VALUE_MASK = "********";

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public NiFiInstanceFactory getInstanceFactory() {
        return createTwoNodeInstanceFactory();
    }


    @Test
    public void testParameterUpdateWhileNodeDisconnected() throws NiFiClientException, IOException, InterruptedException {
        // Add Parameter context with Param1 = 1
        final ParameterContextEntity parameterContextEntity = getClientUtil().createParameterContext("testParameterUpdateWhileNodeDisconnected", Collections.singletonMap("Param1", "1"));
        getClientUtil().setParameterContext("root", parameterContextEntity);

        // Create a GenerateFlowFile that adds an attribute with name 'attr' and a value that references the parameter
        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        getClientUtil().updateProcessorProperties(generate, Collections.singletonMap("attr", "#{Param1}"));

        // Connect GenerateFlowFile to another processor so we can examine its output
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");
        final ConnectionEntity connection = getClientUtil().createConnection(generate, terminate, "success");

        // Start the generator and ensure that we have the expected output
        getClientUtil().startProcessor(generate);
        waitForQueueCount(connection.getId(), getNumberOfNodes());

        for (int i = 0; i < 2; i++) {
            final FlowFileEntity flowFile = getClientUtil().getQueueFlowFile(connection.getId(), i);
            assertEquals("1", flowFile.getFlowFile().getAttributes().get("attr"));
        }

        // Disconnect Node 2, then update Parameter Context on Node 1.
        // This should generate a new FlowFile on Node 1.
        final String node2Id = getNodeEntity(2).getNode().getNodeId();
        getClientUtil().disconnectNode(node2Id);
        waitForNodeState(2, NodeConnectionState.DISCONNECTED);

        final ParameterContextUpdateRequestEntity updateRequestEntity = getClientUtil().updateParameterContext(parameterContextEntity, Collections.singletonMap("Param1", "updated"));
        getClientUtil().waitForParameterContextRequestToComplete(parameterContextEntity.getId(), updateRequestEntity.getRequest().getRequestId());

        waitForQueueCount(connection.getId(), 2);

        final FlowFileEntity flowFile0 = getClientUtil().getQueueFlowFile(connection.getId(), 0);
        final FlowFileEntity flowFile1 = getClientUtil().getQueueFlowFile(connection.getId(), 1);
        assertEquals("1", flowFile0.getFlowFile().getAttributes().get("attr"));
        assertEquals("updated", flowFile1.getFlowFile().getAttributes().get("attr"));

        // Reconnect the node2. This should result in Node 2 also restarting the GenerateFlowFile processor, which should also produce a new FlowFile with updated attributes
        getClientUtil().connectNode(node2Id);
        waitForAllNodesConnected();

        waitForQueueCount(connection.getId(), 4);

        for (int i = 0; i < 2; i++) {
            final FlowFileEntity flowFile = getClientUtil().getQueueFlowFile(connection.getId(), i);
            assertEquals("1", flowFile.getFlowFile().getAttributes().get("attr"));
        }

        for (int i = 2; i < 4; i++) {
            final FlowFileEntity flowFile = getClientUtil().getQueueFlowFile(connection.getId(), i);
            assertEquals("updated", flowFile.getFlowFile().getAttributes().get("attr"));
        }
    }

    @Test
    public void testSensitivePropertiesInherited() throws NiFiClientException, IOException, InterruptedException {
        // Create 3 CountEvents processors. We use this processor because it has a Sensitive property descriptor.
        final ProcessorEntity countEvents1 = getClientUtil().createProcessor("CountEvents");
        final ProcessorEntity countEvents2 = getClientUtil().createProcessor("CountEvents");
        final ProcessorEntity countEvents3 = getClientUtil().createProcessor("CountEvents");

        // Create parameter context with a sensitive parameter and set that on the root group
        final ParameterContextEntity paramContext = getClientUtil().createParameterContext("testSensitivePropertiesInherited", "MyParameter", "Our Secret", true);
        getClientUtil().setParameterContext("root", paramContext);

        // Set sensitive property of 1 processor to an explicit value and sensitive property of another to a sensitive parameter.
        getClientUtil().updateProcessorProperties(countEvents1, Collections.singletonMap("Sensitive", "My Secret"));
        getClientUtil().updateProcessorProperties(countEvents3, Collections.singletonMap("Sensitive", "#{MyParameter}"));

        disconnectNode(2);

        // With Node 2 disconnected, update processor 2 to have a sensitive value
        getClientUtil().updateProcessorProperties(countEvents2, Collections.singletonMap("Sensitive", "Your Secret"));

        // Reconnect node and wait for it to fully connect
        reconnectNode(2);
        waitForAllNodesConnected();


        // Make sure all processors on Node 2 have a sensitive value set
        switchClientToNode(2);
        final Map<String, String> proc1Properties = getNifiClient().getProcessorClient(DO_NOT_REPLICATE).getProcessor(countEvents1.getId()).getComponent().getConfig().getProperties();
        final Map<String, String> proc2Properties = getNifiClient().getProcessorClient(DO_NOT_REPLICATE).getProcessor(countEvents2.getId()).getComponent().getConfig().getProperties();
        final Map<String, String> proc3Properties = getNifiClient().getProcessorClient(DO_NOT_REPLICATE).getProcessor(countEvents3.getId()).getComponent().getConfig().getProperties();

        assertEquals(SENSITIVE_VALUE_MASK, proc1Properties.get("Sensitive"));
        assertEquals(SENSITIVE_VALUE_MASK, proc2Properties.get("Sensitive"));
        assertEquals(SENSITIVE_VALUE_MASK, proc3Properties.get("Sensitive"));

        // Make sure that the sensitive parameter is being referenced.
        final ParameterEntity parameter = getNifiClient().getParamContextClient(DO_NOT_REPLICATE).getParamContext(paramContext.getId(), false).getComponent().getParameters().iterator().next();
        final Set<AffectedComponentEntity> referencingComponents = parameter.getParameter().getReferencingComponents();
        assertEquals(1, referencingComponents.size());
        assertEquals(countEvents3.getId(), referencingComponents.iterator().next().getComponent().getId());
    }

    @Test
    public void testComponentsRecreatedOnRejoinCluster() throws NiFiClientException, IOException, InterruptedException {
        final ProcessGroupEntity topLevel = getClientUtil().createProcessGroup("testComponentsRecreatedOnRejoinCluster", "root");
        // Build dataflow with processors at root level and an inner group that contains an input port, output port, and a processor, as well as a Controller Service that the processor will use.
        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile", topLevel.getId());
        final ProcessGroupEntity group = getClientUtil().createProcessGroup("Inner Group", topLevel.getId());
        final PortEntity inPort = getClientUtil().createInputPort("In", group.getId());
        final PortEntity outPort = getClientUtil().createOutputPort("Out", group.getId());
        final ProcessorEntity count = getClientUtil().createProcessor("CountFlowFiles", group.getId());
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile", topLevel.getId());
        getClientUtil().updateProcessorSchedulingPeriod(generate, "60 sec");

        final ControllerServiceEntity countService = getClientUtil().createControllerService("StandardCountService", group.getId());
        getClientUtil().updateProcessorProperties(count, Collections.singletonMap("Count Service", countService.getId()));

        // Connect components together
        getClientUtil().createConnection(generate, inPort, "success");
        getClientUtil().createConnection(inPort, count);
        getClientUtil().createConnection(count, outPort, "success");
        getClientUtil().createConnection(outPort, terminate);

        // Create controller-level service & reporting task
        final ControllerServiceEntity sleepService = getClientUtil().createControllerService("StandardSleepService", null);
        final ReportingTaskEntity reportingTask = getClientUtil().createReportingTask("org.apache.nifi.reporting.WriteToFileReportingTask", getClientUtil().getTestBundle());
        final File file = new File("target/1.txt");
        assertTrue(file.createNewFile() || file.exists());
        final Map<String, String> reportingTaskProperties = new HashMap<>();
        reportingTaskProperties.put("Filename", file.getAbsolutePath());
        reportingTaskProperties.put("Text", "${now():toNumber()}");
        getClientUtil().updateReportingTaskProperties(reportingTask, reportingTaskProperties);

        final ParameterContextEntity context = getClientUtil().createParameterContext("testComponentsRecreatedOnRejoinCluster", "abc", "hello", false);

        // Disconnect Node 2
        disconnectNode(2);

        // Switch client to Node 2 and destroy everything.
        switchClientToNode(2);
        destroyFlow();

        // Start everything up on Node 1.
        switchClientToNode(1);
        getClientUtil().enableControllerService(countService);
        getClientUtil().enableControllerService(sleepService);
        getClientUtil().startReportingTask(reportingTask);
        getClientUtil().waitForValidProcessor(count.getId()); // Now that service was enabled, wait for processor to become valid.
        getClientUtil().startProcessGroupComponents(group.getId());
        getClientUtil().startProcessor(terminate);
        getClientUtil().startProcessor(generate);

        final ParameterContextUpdateRequestEntity updateRequestEntity = getClientUtil().updateParameterContext(context, Collections.singletonMap("abc", "good-bye"));
        getClientUtil().waitForParameterContextRequestToComplete(context.getComponent().getId(), updateRequestEntity.getRequest().getRequestId());

        // Switch client back to Node 1, reconnect Node 2, and wait for that to complete.
        reconnectNode(2);
        waitForAllNodesConnected();

        // Verify that components exist on node 2, by switching client to Node 2 and not replicating requests, so that we see exactly what
        // is on Node 2.
        switchClientToNode(2);

        final ProcessGroupFlowEntity flow = getNifiClient().getFlowClient(DO_NOT_REPLICATE).getProcessGroup(topLevel.getId());
        final FlowDTO flowDto = flow.getProcessGroupFlow().getFlow();
        assertEquals(2, flowDto.getConnections().size());
        assertEquals(2, flowDto.getProcessors().size());

        final ProcessorEntity node2Generate = flowDto.getProcessors().stream().filter(proc -> proc.getId().equals(generate.getId())).findAny().orElse(null);
        assertNotNull(node2Generate);
        assertEquals("60 sec", node2Generate.getComponent().getConfig().getSchedulingPeriod());
        assertEquals(1, flowDto.getProcessGroups().size());

        final ProcessGroupEntity node2Group = flowDto.getProcessGroups().iterator().next();
        assertEquals(1, node2Group.getInputPortCount().intValue());
        assertEquals(1, node2Group.getOutputPortCount().intValue());

        final ProcessGroupFlowEntity childGroupFlow = getNifiClient().getFlowClient(DO_NOT_REPLICATE).getProcessGroup(group.getId());
        final FlowDTO node2GroupContents = childGroupFlow.getProcessGroupFlow().getFlow();
        assertEquals(2, node2GroupContents.getConnections().size());
        assertEquals(1, node2GroupContents.getProcessors().size());

        final Set<ControllerServiceEntity> groupServices = getNifiClient().getFlowClient(DO_NOT_REPLICATE).getControllerServices(group.getId()).getControllerServices();
        assertEquals(1, groupServices.size());

        final ProcessorDTO node2CountProc = node2GroupContents.getProcessors().iterator().next().getComponent();
        final Map<String, String> procProperties = node2CountProc.getConfig().getProperties();
        final String serviceId = groupServices.iterator().next().getId();
        assertEquals(serviceId, procProperties.get("Count Service"));
        assertEquals(countService.getId(), serviceId);
        assertEquals(count.getId(), node2CountProc.getId());
        waitFor(() -> {
            final ProcessorDTO updatedNode2CountProc = getNifiClient().getProcessorClient(DO_NOT_REPLICATE).getProcessor(node2CountProc.getId()).getComponent();
            return updatedNode2CountProc.getState().equals(RUNNING_STATE);
        });

        final PortDTO node2InputPort = node2GroupContents.getInputPorts().iterator().next().getComponent();
        assertEquals(inPort.getId(), node2InputPort.getId());
        assertEquals(inPort.getComponent().getName(), node2InputPort.getName());
        waitFor(() -> {
            final PortDTO updatedNode2InputPort = getNifiClient().getInputPortClient(DO_NOT_REPLICATE).getInputPort(node2InputPort.getId()).getComponent();
            return updatedNode2InputPort.getState().equals(RUNNING_STATE);
        });

        final PortDTO node2OutputPort = node2GroupContents.getOutputPorts().iterator().next().getComponent();
        assertEquals(outPort.getId(), node2OutputPort.getId());
        assertEquals(outPort.getComponent().getName(), node2OutputPort.getName());
        waitFor(() -> {
            final PortDTO updatedNode2OutputPort = getNifiClient().getOutputPortClient(DO_NOT_REPLICATE).getOutputPort(node2OutputPort.getId()).getComponent();
            return updatedNode2OutputPort.getState().equals(RUNNING_STATE);
        });

        final ControllerServiceEntity node2SleepService = getNifiClient().getControllerServicesClient(DO_NOT_REPLICATE).getControllerService(sleepService.getId());
        assertEquals(sleepService.getId(), node2SleepService.getId());
        waitFor(() -> {
            final ControllerServiceEntity updatedNode2SleepService = getNifiClient().getControllerServicesClient(DO_NOT_REPLICATE).getControllerService(sleepService.getId());
            return updatedNode2SleepService.getComponent().getState().equals(ENABLED_STATE);
        });

        waitFor(() -> {
            final ReportingTaskEntity updatedNode2ReportingTask = getNifiClient().getReportingTasksClient(DO_NOT_REPLICATE).getReportingTask(reportingTask.getId());
            return updatedNode2ReportingTask.getComponent().getState().equals(RUNNING_STATE);
        });

        final ParameterContextEntity node2Context = getNifiClient().getParamContextClient(DO_NOT_REPLICATE).getParamContext(context.getId(), false);
        final String node2ParamValue = node2Context.getComponent().getParameters().stream()
            .filter(param -> param.getParameter().getName().equals("abc"))
            .map(param -> param.getParameter().getValue())
            .findAny()
            .orElse(null);
        assertEquals("good-bye", node2ParamValue);
    }


    @Test
    public void testReconnectionWithUpdatedConnection() throws NiFiClientException, IOException, InterruptedException {
        // Create connection between two processors
        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        getClientUtil().updateProcessorSchedulingPeriod(generate, "60 sec");

        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");
        final ConnectionEntity connection = getClientUtil().createConnection(generate, terminate, "success");

        // Disconnect Node 2.
        disconnectNode(2);

        // Update the connection and start both its source & destination.
        getClientUtil().updateConnectionLoadBalancing(connection, LoadBalanceStrategy.ROUND_ROBIN, LoadBalanceCompression.DO_NOT_COMPRESS, null);
        getClientUtil().startProcessor(generate);
        getClientUtil().startProcessor(terminate);

        // Reconnect Node 2 and ensure that the connection is updated and that its processors are running
        reconnectNode(2);
        waitForAllNodesConnected();

        // Make sure the connection is configured for round robin
        final ConnectionEntity rejoinedConnection = getNifiClient().getConnectionClient(DO_NOT_REPLICATE).getConnection(connection.getId());
        assertEquals(LoadBalanceStrategy.ROUND_ROBIN.name(), rejoinedConnection.getComponent().getLoadBalanceStrategy());

        // Ensure that the processors are running
        waitFor(() -> {
            final Set<ProcessorEntity> processors = getNifiClient().getFlowClient(DO_NOT_REPLICATE).getProcessGroup("root").getProcessGroupFlow().getFlow().getProcessors();
            if (processors.size() != 2) {
                return false;
            }

            for (final ProcessorEntity processor : processors) {
                if (!processor.getComponent().getState().equals(RUNNING_STATE)) {
                    return false;
                }
            }

            return true;
        });
    }


    @Test
    public void testCannotRemoveComponentsWhileNodeDisconnected() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");
        final ConnectionEntity connection = getClientUtil().createConnection(generate, terminate, "success");

        // Shut down node 2
        disconnectNode(2);
        waitForNodeState(2, NodeConnectionState.DISCONNECTED);

        // Attempt to delete connection. It should throw an Exception.
        assertThrows(Exception.class, () -> getNifiClient().getConnectionClient().deleteConnection(connection));

        // Attempt to delete processor. It should throw an Exception.
        assertThrows(Exception.class, () -> getNifiClient().getProcessorClient().deleteProcessor(generate));

        // Reconnect the node
        reconnectNode(2);
        waitForAllNodesConnected();

        // Ensure that we can delete the connection and the processors
        getNifiClient().getConnectionClient().deleteConnection(connection);
        getNifiClient().getProcessorClient().deleteProcessor(generate);
        getNifiClient().getProcessorClient().deleteProcessor(terminate);
    }


    @Test
    public void testComponentStatesRestoredOnReconnect() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");
        final ConnectionEntity connection = getClientUtil().createConnection(generate, terminate, "success");

        getClientUtil().startProcessor(generate);
        waitForQueueCount(connection.getId(), 2);

        // Shut down node 2
        disconnectNode(2);

        getClientUtil().stopProcessor(generate);
        getClientUtil().startProcessor(terminate);

        waitForQueueCount(connection.getId(), 0);

        reconnectNode(2);
        waitForAllNodesConnected();

        getClientUtil().waitForStoppedProcessor(generate.getId());
        waitForQueueCount(connection.getId(), 0);

        switchClientToNode(2);

        // Ensure that Node 2 has the correct state for each processor.
        waitFor(() -> {
            final ProcessorEntity latestTerminate = getNifiClient().getProcessorClient(DO_NOT_REPLICATE).getProcessor(terminate.getId());
            return "RUNNING".equalsIgnoreCase(latestTerminate.getComponent().getState());
        });

        waitFor(() -> {
            final ProcessorEntity latestGenerate = getNifiClient().getProcessorClient(DO_NOT_REPLICATE).getProcessor(generate.getId());
            return "STOPPED".equalsIgnoreCase(latestGenerate.getComponent().getState());
        });
    }


    @Test
    public void testComponentsRecreatedOnRestart() throws NiFiClientException, IOException, InterruptedException {
        // Build dataflow with processors at root level and an inner group that contains an input port, output port, and a processor, as well as a Controller Service that the processor will use.
        final ProcessGroupEntity topLevelGroup = getClientUtil().createProcessGroup("testComponentsRecreatedOnRestart", "root");
        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile", topLevelGroup.getId());
        final ProcessGroupEntity group = getClientUtil().createProcessGroup("Inner Group", topLevelGroup.getId());
        final PortEntity inPort = getClientUtil().createInputPort("In", group.getId());
        final PortEntity outPort = getClientUtil().createOutputPort("Out", group.getId());
        final ProcessorEntity count = getClientUtil().createProcessor("CountFlowFiles", group.getId());
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile", topLevelGroup.getId());
        getClientUtil().updateProcessorSchedulingPeriod(generate, "60 sec");

        final ControllerServiceEntity countService = getClientUtil().createControllerService("StandardCountService", group.getId());
        getClientUtil().updateProcessorProperties(count, Collections.singletonMap("Count Service", countService.getId()));

        // Connect components together
        getClientUtil().createConnection(generate, inPort, "success");
        getClientUtil().createConnection(inPort, count);
        getClientUtil().createConnection(count, outPort, "success");
        getClientUtil().createConnection(outPort, terminate);

        // Create controller-level service & reporting task
        final ControllerServiceEntity sleepService = getClientUtil().createControllerService("StandardSleepService", null);
        final ReportingTaskEntity reportingTask = getClientUtil().createReportingTask("org.apache.nifi.reporting.WriteToFileReportingTask", getClientUtil().getTestBundle());
        final File file = new File("target/1.txt");
        assertTrue(file.createNewFile() || file.exists());
        final Map<String, String> reportingTaskProperties = new HashMap<>();
        reportingTaskProperties.put("Filename", file.getAbsolutePath());
        reportingTaskProperties.put("Text", "${now():toNumber()}");
        getClientUtil().updateReportingTaskProperties(reportingTask, reportingTaskProperties);

        // Start everything up on Node 1.
        getClientUtil().enableControllerService(sleepService);
        getClientUtil().enableControllerService(countService);
        getClientUtil().startReportingTask(reportingTask);
        getClientUtil().waitForValidProcessor(count.getId()); // Now that service was enabled, wait for processor to become valid.
        getClientUtil().startProcessGroupComponents(group.getId());
        getClientUtil().startProcessor(terminate);
        getClientUtil().startProcessor(generate);
        getClientUtil().waitForProcessorState(count.getId(), RUNNING_STATE);

        // Stop & restart Node 2.
        getNiFiInstance().getNodeInstance(2).stop();
        getNiFiInstance().getNodeInstance(2).start(true);
        waitForAllNodesConnected();

        // Verify that components exist on node 2, by switching client to Node 2 and not replicating requests, so that we see exactly what
        // is on Node 2.
        switchClientToNode(2);

        final ProcessGroupFlowEntity flow = getNifiClient().getFlowClient(DO_NOT_REPLICATE).getProcessGroup(topLevelGroup.getId());
        final FlowDTO flowDto = flow.getProcessGroupFlow().getFlow();
        assertEquals(2, flowDto.getConnections().size());
        assertEquals(2, flowDto.getProcessors().size());

        final ProcessorEntity node2Generate = flowDto.getProcessors().stream().filter(proc -> proc.getId().equals(generate.getId())).findAny().orElse(null);
        assertNotNull(node2Generate);
        assertEquals("60 sec", node2Generate.getComponent().getConfig().getSchedulingPeriod());
        assertEquals(1, flowDto.getProcessGroups().size());

        final ProcessGroupEntity node2Group = flowDto.getProcessGroups().iterator().next();
        assertEquals(1, node2Group.getInputPortCount().intValue());
        assertEquals(1, node2Group.getOutputPortCount().intValue());

        final ProcessGroupFlowEntity childGroupFlow = getNifiClient().getFlowClient(DO_NOT_REPLICATE).getProcessGroup(group.getId());
        final FlowDTO node2GroupContents = childGroupFlow.getProcessGroupFlow().getFlow();
        assertEquals(2, node2GroupContents.getConnections().size());
        assertEquals(1, node2GroupContents.getProcessors().size());

        final Set<ControllerServiceEntity> groupServices = getNifiClient().getFlowClient(DO_NOT_REPLICATE).getControllerServices(group.getId()).getControllerServices();
        assertEquals(1, groupServices.size());

        final ProcessorDTO node2CountProc = node2GroupContents.getProcessors().iterator().next().getComponent();
        final Map<String, String> procProperties = node2CountProc.getConfig().getProperties();
        final String serviceId = groupServices.iterator().next().getId();
        assertEquals(serviceId, procProperties.get("Count Service"));
        assertEquals(countService.getId(), serviceId);
        assertEquals(count.getId(), node2CountProc.getId());
        waitFor(() -> {
            final ProcessorDTO updatedNode2CountProc = getNifiClient().getProcessorClient(DO_NOT_REPLICATE).getProcessor(node2CountProc.getId()).getComponent();
            return updatedNode2CountProc.getState().equals(RUNNING_STATE);
        });

        final PortDTO node2InputPort = node2GroupContents.getInputPorts().iterator().next().getComponent();
        assertEquals(inPort.getId(), node2InputPort.getId());
        assertEquals(inPort.getComponent().getName(), node2InputPort.getName());
        waitFor(() -> {
            final PortDTO updatedNode2InputPort = getNifiClient().getInputPortClient(DO_NOT_REPLICATE).getInputPort(node2InputPort.getId()).getComponent();
            return updatedNode2InputPort.getState().equals(RUNNING_STATE);
        });

        final PortDTO node2OutputPort = node2GroupContents.getOutputPorts().iterator().next().getComponent();
        assertEquals(outPort.getId(), node2OutputPort.getId());
        assertEquals(outPort.getComponent().getName(), node2OutputPort.getName());
        waitFor(() -> {
            final PortDTO updatedNode2OutputPort = getNifiClient().getOutputPortClient(DO_NOT_REPLICATE).getOutputPort(node2OutputPort.getId()).getComponent();
            return updatedNode2OutputPort.getState().equals(RUNNING_STATE);
        });

        final ControllerServiceEntity node2SleepService = getNifiClient().getControllerServicesClient(DO_NOT_REPLICATE).getControllerService(sleepService.getId());
        assertEquals(sleepService.getId(), node2SleepService.getId());
        waitFor(() -> {
            final ControllerServiceDTO updatedNode2SleepService = getNifiClient().getControllerServicesClient(DO_NOT_REPLICATE).getControllerService(sleepService.getId()).getComponent();
            return updatedNode2SleepService.getState().equals("ENABLED");
        });

        waitFor(() -> {
            final ReportingTaskEntity updatedNode2ReportingTask = getNifiClient().getReportingTasksClient(DO_NOT_REPLICATE).getReportingTask(reportingTask.getId());
            return updatedNode2ReportingTask.getComponent().getState().equals(RUNNING_STATE);
        });
    }


    @Test
    public void testReconnectAddsProcessor() throws NiFiClientException, IOException, InterruptedException {
        // Create GenerateFlowFile processor
        final ProcessorEntity generateFlowFile = getClientUtil().createProcessor("GenerateFlowFile");

        // Disconnect Node 2. Switch client to direct requests to Node 2 so that we can update the node while it's disconnected.
        disconnectNode(2);
        switchClientToNode(2);

        // Delete the Processor
        generateFlowFile.setDisconnectedNodeAcknowledged(true);
        getNifiClient().getProcessorClient().deleteProcessor(generateFlowFile);

        // Wait until the node saves its flow showing that the processor has been removed
        waitFor(() -> getNode2Flow().getRootGroup().getProcessors().isEmpty());

        // Switch client to direct requests to Node 1 and ask Node 1 to reconnect Node 2.
        switchClientToNode(1);
        reconnectNode(2);
        waitForAllNodesConnected();

        // Redirect client to send requests to Node 2.
        switchClientToNode(2);

        // Make a request to Node 2 using special headers that cause it not to replicate the request. We do this because we want to get the flow
        // exactly as it appears on Node 2 instead of merging responses. Wait until there is 1 Processor in the flow, as reconnecting to the cluster
        // should have restored it.
        waitFor(() -> {
            final ProcessGroupFlowEntity flow = getNifiClient().getFlowClient(DO_NOT_REPLICATE).getProcessGroup("root");
            return flow.getProcessGroupFlow().getFlow().getProcessors().size() == 1;
        });
    }

    @Test
    public void testAddControllerServiceReferencingExistingService() throws NiFiClientException, IOException, InterruptedException {
        // Generate -> CountFlowFiles -/->
        // CountFlowFiles depends on countA, depends on countB, depends on countC
        final ProcessorEntity generateFlowFile = getClientUtil().createProcessor("GenerateFlowFile");
        ProcessorEntity countFlowFiles = getClientUtil().createProcessor("CountFlowFiles");
        ControllerServiceEntity countA = getClientUtil().createControllerService("StandardCountService");
        ControllerServiceEntity countB = getClientUtil().createControllerService("StandardCountService");
        ControllerServiceEntity countC = getClientUtil().createControllerService("StandardCountService");

        getClientUtil().setAutoTerminatedRelationships(countFlowFiles, "success");
        final ConnectionEntity connection = getClientUtil().createConnection(generateFlowFile, countFlowFiles, "success");

        countA = getClientUtil().updateControllerServiceProperties(countA, Collections.singletonMap("Dependent Service", countB.getId()));
        countB = getClientUtil().updateControllerServiceProperties(countB, Collections.singletonMap("Dependent Service", countC.getId()));
        countFlowFiles = getClientUtil().updateProcessorProperties(countFlowFiles, Collections.singletonMap("Count Service", countA.getId()));

        getClientUtil().enableControllerService(countC);
        getClientUtil().enableControllerService(countB);
        getClientUtil().enableControllerService(countA);
        getClientUtil().waitForControllerServicesEnabled(countC.getParentGroupId(), List.of(countC.getId(), countB.getId(), countA.getId()));

        getClientUtil().startProcessor(countFlowFiles);

        // Disconnect Node 2. Switch client to direct requests to Node 2 so that we can update the node while it's disconnected.
        disconnectNode(2);
        switchClientToNode(2);

        generateFlowFile.setDisconnectedNodeAcknowledged(true);
        countFlowFiles.setDisconnectedNodeAcknowledged(true);
        countA.setDisconnectedNodeAcknowledged(true);
        countB.setDisconnectedNodeAcknowledged(true);
        countC.setDisconnectedNodeAcknowledged(true);
        connection.setDisconnectedNodeAcknowledged(true);

        // Delete the CountFlowFiles processor, and countB and countC services, disable A.
        getClientUtil().stopProcessor(countFlowFiles);
        getNifiClient().getConnectionClient().deleteConnection(connection);
        getNifiClient().getProcessorClient().deleteProcessor(countFlowFiles);
        getClientUtil().disableControllerServices("root", true);
        getNifiClient().getControllerServicesClient().deleteControllerService(countC);
        getNifiClient().getControllerServicesClient().deleteControllerService(countB);

        // Wait until the node saves its flow showing the updates
        waitFor(() -> getNode2Flow().getRootGroup().getControllerServices().size() == 1);

        // Switch client to direct requests to Node 1 and ask Node 1 to reconnect Node 2.
        switchClientToNode(1);
        reconnectNode(2);
        waitForAllNodesConnected();

        // Redirect client to send requests to Node 2.
        switchClientToNode(2);

        final ProcessorEntity countFlowFilesEntity = countFlowFiles; // assign to final variable so it can be referenced within lambda
        final ControllerServiceEntity countAEntity = countA;
        final ControllerServiceEntity countBEntity = countB;
        final ControllerServiceEntity countCEntity = countC;

        // Make a request to Node 2 using special headers that cause it not to replicate the request. We do this because we want to get the flow
        // exactly as it appears on Node 2 instead of merging responses. Wait until there is 1 Processor in the flow, as reconnecting to the cluster
        // should have restored it.
        waitFor(() -> {
            final ProcessGroupFlowEntity flow = getNifiClient().getFlowClient(DO_NOT_REPLICATE).getProcessGroup("root");
            final FlowDTO flowDto = flow.getProcessGroupFlow().getFlow();
            if (flowDto.getProcessors().size() != 2) {
                logger.info("Currently {} processors, waiting for 2", flowDto.getProcessors().size());
                return false;
            }

            final Set<ControllerServiceEntity> services = getNifiClient().getFlowClient(DO_NOT_REPLICATE).getControllerServices("root").getControllerServices();
            if (services.size() != 3) {
                logger.info("Currently {} services, waiting for 3", services.size());
                return false;
            }

            boolean foundCountFlowFiles = false;
            for (final ProcessorEntity entity : flowDto.getProcessors()) {
                if (!entity.getComponent().getId().equals(countFlowFilesEntity.getId())) {
                    continue;
                }

                foundCountFlowFiles = true;
                if (!entity.getComponent().getConfig().getProperties().equals(countFlowFilesEntity.getComponent().getConfig().getProperties())) {
                    logger.info("CountFlowFiles doesn't yet have correct config");
                    return false;
                }
            }

            assertTrue(foundCountFlowFiles);

            // Ensure services are of the correct type, enabled, and have the correct properties set.
            for (final ControllerServiceEntity serviceEntity : services) {
                final ControllerServiceDTO dto = serviceEntity.getComponent();
                assertEquals(countAEntity.getComponent().getType(), dto.getType());

                if (!ControllerServiceState.ENABLED.name().equals(dto.getState())) {
                    logger.info("Service with ID {} is not yet enabled", dto.getId());
                    return false;
                }

                if (dto.getId().equals(countAEntity.getId()) && !dto.getProperties().equals(countAEntity.getComponent().getProperties())) {
                    logger.info("CountA does not currently have correct properties");
                    return false;
                }
                if (dto.getId().equals(countBEntity.getId()) && !dto.getProperties().equals(countBEntity.getComponent().getProperties())) {
                    logger.info("CountB does not currently have correct properties");
                    return false;
                }
                if (dto.getId().equals(countCEntity.getId()) && !dto.getProperties().equals(countCEntity.getComponent().getProperties())) {
                    logger.info("CountC does not currently have correct properties");
                    return false;
                }
            }

            return true;
        });
    }


    @Test
    public void testUnnecessaryProcessorsAndConnectionsRemoved() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        getClientUtil().updateProcessorSchedulingPeriod(generate, "10 mins");

        // Disconnect Node 2. Switch client to direct requests to Node 2 so that we can update the node while it's disconnected.
        disconnectNode(2);
        switchClientToNode(2);

        // Create a TerminateFlowFile processor, connect Generate to it, and start them both.
        getNifiClient().getProcessorClient().acknowledgeDisconnectedNode();
        getNifiClient().getConnectionClient().acknowledgeDisconnectedNode();

        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");
        final ConnectionEntity connection = getClientUtil().createConnection(generate, terminate, "success");
        getClientUtil().startProcessor(generate);
        waitForMinQueueCount(connection.getId(), 1);
        getClientUtil().startProcessor(terminate);

        waitForQueueCount(connection.getId(), 0);

        // Reconnect the node to the cluster
        switchClientToNode(1);
        reconnectNode(2);
        waitForAllNodesConnected();

        // Redirect client to send requests to Node 2.
        switchClientToNode(2);

        waitFor(() -> {
            final ProcessGroupFlowEntity flow = getNifiClient().getFlowClient(DO_NOT_REPLICATE).getProcessGroup("root");
            final FlowDTO flowDto = flow.getProcessGroupFlow().getFlow();

            if (flowDto.getProcessors().size() != 1) {
                return false;
            }

            return flowDto.getConnections().isEmpty();
        });
    }

    @Test
    public void testRejoinAfterControllerServiceEnabled() throws NiFiClientException, IOException, InterruptedException {
        final ControllerServiceEntity controllerService = getClientUtil().createControllerService("StandardCountService");
        disconnectNode(2);

        getClientUtil().enableControllerService(controllerService);
        reconnectNode(2);
        waitForAllNodesConnected();

        switchClientToNode(2);
        waitFor(() -> {
            final ControllerServiceEntity currentService = getNifiClient().getControllerServicesClient(DO_NOT_REPLICATE).getControllerService(controllerService.getId());
            return ControllerServiceState.ENABLED.name().equals(currentService.getComponent().getState());
        });
    }

    @Test
    public void testRejoinAfterControllerServiceDisabled() throws NiFiClientException, IOException, InterruptedException {
        final ControllerServiceEntity controllerService = getClientUtil().createControllerService("StandardCountService");
        getClientUtil().enableControllerService(controllerService);

        disconnectNode(2);
        getClientUtil().disableControllerService(controllerService);

        reconnectNode(2);
        waitForAllNodesConnected();

        switchClientToNode(2);
        waitFor(() -> {
            final ControllerServiceEntity currentService = getNifiClient().getControllerServicesClient(DO_NOT_REPLICATE).getControllerService(controllerService.getId());
            return ControllerServiceState.DISABLED.name().equals(currentService.getComponent().getState());
        });
    }


    @Test
    public void testReconnectWithRunningProcessorUnchanged() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity generateFlowFile = getClientUtil().createProcessor("GenerateFlowFile");
        final ProcessorEntity reverseContents = getClientUtil().createProcessor("ReverseContents");
        final ProcessorEntity terminateFlowFile = getClientUtil().createProcessor("TerminateFlowFile");
        getClientUtil().createConnection(generateFlowFile, reverseContents, "success");
        getClientUtil().createConnection(reverseContents, terminateFlowFile, "success");

        getClientUtil().waitForValidProcessor(generateFlowFile.getId());
        getClientUtil().waitForValidProcessor(reverseContents.getId());
        getClientUtil().waitForValidProcessor(terminateFlowFile.getId());

        getClientUtil().startProcessor(reverseContents);

        disconnectNode(2);
        reconnectNode(2);
        waitForAllNodesConnected();
    }

    @Test
    public void testReconnectWithRunningProcessorUnchangedInChildGroup() throws NiFiClientException, IOException, InterruptedException {
        final ProcessGroupEntity group = getClientUtil().createProcessGroup("testReconnectWithRunningProcessorUnchangedInChildGroup", "root");
        final ProcessorEntity generateFlowFile = getClientUtil().createProcessor("GenerateFlowFile", group.getId());
        final ProcessorEntity reverseContents = getClientUtil().createProcessor("ReverseContents", group.getId());
        final ProcessorEntity terminateFlowFile = getClientUtil().createProcessor("TerminateFlowFile", group.getId());
        getClientUtil().createConnection(generateFlowFile, reverseContents, "success", group.getId());
        getClientUtil().createConnection(reverseContents, terminateFlowFile, "success", group.getId());

        getClientUtil().waitForValidProcessor(generateFlowFile.getId());
        getClientUtil().waitForValidProcessor(reverseContents.getId());
        getClientUtil().waitForValidProcessor(terminateFlowFile.getId());

        getClientUtil().startProcessor(reverseContents);

        disconnectNode(2);
        reconnectNode(2);
        waitForAllNodesConnected();
    }


    private VersionedDataflow getNode2Flow() throws IOException {
        final File instanceDir = getNiFiInstance().getNodeInstance(2).getInstanceDirectory();
        final File conf = new File(instanceDir, "conf");
        final File flow = new File(conf, "flow.json.gz");

        // Buffer into memory so that it's easy to set breakpoints & debug when necessary
        final byte[] bytes;
        try (final InputStream fis = new FileInputStream(flow);
             final InputStream in = new GZIPInputStream(fis);
             final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            StreamUtils.copy(in, baos);
            bytes = baos.toByteArray();
        }

        try (final InputStream in = new ByteArrayInputStream(bytes)) {
            return objectMapper.readValue(in, VersionedDataflow.class);
        }
    }

}
