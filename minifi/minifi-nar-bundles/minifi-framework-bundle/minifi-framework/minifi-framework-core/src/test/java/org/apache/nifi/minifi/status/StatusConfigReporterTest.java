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

package org.apache.nifi.minifi.status;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.RemoteProcessGroupStatus;
import org.apache.nifi.controller.status.RunStatus;
import org.apache.nifi.controller.status.TransmissionStatus;
import org.apache.nifi.diagnostics.GarbageCollection;
import org.apache.nifi.diagnostics.StorageUsage;
import org.apache.nifi.diagnostics.SystemDiagnostics;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.minifi.commons.status.FlowStatusReport;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.BulletinQuery;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.reporting.UserAwareEventAccess;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.minifi.commons.status.util.StatusReportPopulator.addConnectionStatus;
import static org.apache.nifi.minifi.commons.status.util.StatusReportPopulator.addControllerServiceStatus;
import static org.apache.nifi.minifi.commons.status.util.StatusReportPopulator.addExpectedRemoteProcessGroupStatus;
import static org.apache.nifi.minifi.commons.status.util.StatusReportPopulator.addInstanceStatus;
import static org.apache.nifi.minifi.commons.status.util.StatusReportPopulator.addProcessorStatus;
import static org.apache.nifi.minifi.commons.status.util.StatusReportPopulator.addReportingTaskStatus;
import static org.apache.nifi.minifi.commons.status.util.StatusReportPopulator.addSystemDiagnosticStatus;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StatusConfigReporterTest {
    private FlowController mockFlowController;
    private FlowManager mockFlowManager;
    private UserAwareEventAccess mockEventAccess;
    private ProcessGroupStatus rootGroupStatus;
    private BulletinRepository bulletinRepo;
    private ProcessGroup processGroup;

    @Before
    public void setup() {
        mockFlowController = mock(FlowController.class);
        mockFlowManager = mock(FlowManager.class);
        mockEventAccess = mock(UserAwareEventAccess.class);
        rootGroupStatus = mock(ProcessGroupStatus.class);
        bulletinRepo = mock(BulletinRepository.class);
        processGroup = mock(ProcessGroup.class);

        when(mockFlowController.getFlowManager()).thenReturn(mockFlowManager);
        when(mockFlowManager.getRootGroupId()).thenReturn("root");
        when(mockFlowController.getEventAccess()).thenReturn(mockEventAccess);
        when(mockEventAccess.getGroupStatus("root")).thenReturn(rootGroupStatus);
        when(mockEventAccess.getControllerStatus()).thenReturn(rootGroupStatus);
        when(mockFlowController.getBulletinRepository()).thenReturn(bulletinRepo);
        when(mockFlowManager.getGroup(mockFlowController.getFlowManager().getRootGroupId())).thenReturn(processGroup);
    }

    @Test
    public void processorStatusHealth() throws Exception {
        populateProcessor(false, false);

        String statusRequest = "processor:all:health";
        FlowStatusReport actual = StatusConfigReporter.getStatus(mockFlowController, statusRequest, LoggerFactory.getLogger(StatusConfigReporterTest.class));

        FlowStatusReport expected = new FlowStatusReport();
        expected.setErrorsGeneratingReport(Collections.EMPTY_LIST);

        addProcessorStatus(expected, true, false, false, false, false);

        assertEquals(expected, actual);
    }

    @Test
    public void individualProcessorStatusHealth() throws Exception {
        populateProcessor(false, false);

        String statusRequest = "processor:UpdateAttributeProcessorId:health";
        FlowStatusReport actual = StatusConfigReporter.getStatus(mockFlowController, statusRequest, LoggerFactory.getLogger(StatusConfigReporterTest.class));

        FlowStatusReport expected = new FlowStatusReport();
        expected.setErrorsGeneratingReport(Collections.EMPTY_LIST);

        addProcessorStatus(expected, true, false, false, false, false);

        assertEquals(expected, actual);
    }

    @Test
    public void processorStatusWithValidationErrors() throws Exception {
        populateProcessor(true, false);

        String statusRequest = "processor:all:health";
        FlowStatusReport actual = StatusConfigReporter.getStatus(mockFlowController, statusRequest, LoggerFactory.getLogger(StatusConfigReporterTest.class));

        FlowStatusReport expected = new FlowStatusReport();
        expected.setErrorsGeneratingReport(Collections.EMPTY_LIST);

        addProcessorStatus(expected, true, true, false, false, false);

        assertEquals(expected, actual);
    }

    @Test
    public void processorStatusAll() throws Exception {
        populateProcessor(true, true);

        String statusRequest = "processor:all:health, stats, bulletins";
        FlowStatusReport actual = StatusConfigReporter.getStatus(mockFlowController, statusRequest, LoggerFactory.getLogger(StatusConfigReporterTest.class));

        FlowStatusReport expected = new FlowStatusReport();
        expected.setErrorsGeneratingReport(Collections.EMPTY_LIST);

        addProcessorStatus(expected, true, true, true, true, true);

        assertEquals(expected, actual);
    }

    @Test
    public void connectionStatusHealth() throws Exception {
        populateConnection();

        String statusRequest = "connection:all:health";
        FlowStatusReport status = StatusConfigReporter.getStatus(mockFlowController, statusRequest, LoggerFactory.getLogger(StatusConfigReporterTest.class));

        FlowStatusReport expected = new FlowStatusReport();
        expected.setErrorsGeneratingReport(Collections.EMPTY_LIST);

        addConnectionStatus(expected, true, false);

        assertEquals(expected, status);
    }


    @Test
    public void connectionStatusAll() throws Exception {
        populateConnection();

        String statusRequest = "connection:all:health, stats";
        FlowStatusReport actual = StatusConfigReporter.getStatus(mockFlowController, statusRequest, LoggerFactory.getLogger(StatusConfigReporterTest.class));

        FlowStatusReport expected = new FlowStatusReport();
        expected.setErrorsGeneratingReport(Collections.EMPTY_LIST);
        addConnectionStatus(expected, true, true);

        assertEquals(expected, actual);
    }

    @Test
    public void connectionAndProcessorStatusHealth() throws Exception {

        populateConnection();

        populateProcessor(false, false);

        String statusRequest = "connection:connectionId:health; processor:UpdateAttributeProcessorId:health";
        FlowStatusReport actual = StatusConfigReporter.getStatus(mockFlowController, statusRequest, LoggerFactory.getLogger(StatusConfigReporterTest.class));

        FlowStatusReport expected = new FlowStatusReport();
        addConnectionStatus(expected, true, false);

        addProcessorStatus(expected, true, false, false, false, false);

        expected.setErrorsGeneratingReport(Collections.EMPTY_LIST);

        assertEquals(expected, actual);
    }

    @Test
    public void provenanceReportingTaskStatusHealth() throws Exception {
        populateReportingTask(false, false);

        String statusRequest = "provenanceReporting:health";
        FlowStatusReport actual = StatusConfigReporter.getStatus(mockFlowController, statusRequest, LoggerFactory.getLogger(StatusConfigReporterTest.class));

        FlowStatusReport expected = new FlowStatusReport();
        expected.setErrorsGeneratingReport(Collections.EMPTY_LIST);
        addReportingTaskStatus(expected, true, false, false, false);

        assertEquals(expected, actual);
    }


    @Test
    public void provenanceReportingTaskStatusBulletins() throws Exception {
        populateReportingTask(true, false);

        String statusRequest = "provenanceReporting:bulletins";
        FlowStatusReport actual = StatusConfigReporter.getStatus(mockFlowController, statusRequest, LoggerFactory.getLogger(StatusConfigReporterTest.class));

        FlowStatusReport expected = new FlowStatusReport();
        expected.setErrorsGeneratingReport(Collections.EMPTY_LIST);

        addReportingTaskStatus(expected, false, false, true, true);

        assertEquals(expected, actual);
    }

    @Test
    public void provenanceReportingTaskStatusAll() throws Exception {
        populateReportingTask(true, true);

        String statusRequest = "provenanceReporting:health,bulletins";
        FlowStatusReport actual = StatusConfigReporter.getStatus(mockFlowController, statusRequest, LoggerFactory.getLogger(StatusConfigReporterTest.class));

        FlowStatusReport expected = new FlowStatusReport();
        expected.setErrorsGeneratingReport(Collections.EMPTY_LIST);

        addReportingTaskStatus(expected, true, true, true, true);

        assertEquals(expected, actual);
    }

    @Test
    public void systemDiagnosticHeap() throws Exception {
        populateSystemDiagnostics();

        String statusRequest = "systemDiagnostics:heap";
        FlowStatusReport actual = StatusConfigReporter.getStatus(mockFlowController, statusRequest, LoggerFactory.getLogger(StatusConfigReporterTest.class));

        FlowStatusReport expected = new FlowStatusReport();
        expected.setErrorsGeneratingReport(Collections.EMPTY_LIST);

        addSystemDiagnosticStatus(expected, true, false, false, false, false);

        assertEquals(expected, actual);
    }

    @Test
    public void systemDiagnosticProcessorStats() throws Exception {
        populateSystemDiagnostics();

        String statusRequest = "systemDiagnostics:processorStats";
        FlowStatusReport actual = StatusConfigReporter.getStatus(mockFlowController, statusRequest, LoggerFactory.getLogger(StatusConfigReporterTest.class));

        FlowStatusReport expected = new FlowStatusReport();
        expected.setErrorsGeneratingReport(Collections.EMPTY_LIST);

        addSystemDiagnosticStatus(expected, false, true, false, false, false);

        assertEquals(expected, actual);
    }

    @Test
    public void systemDiagnosticFlowFileRepo() throws Exception {
        populateSystemDiagnostics();

        String statusRequest = "systemDiagnostics:flowfilerepositoryusage";
        FlowStatusReport actual = StatusConfigReporter.getStatus(mockFlowController, statusRequest, LoggerFactory.getLogger(StatusConfigReporterTest.class));

        FlowStatusReport expected = new FlowStatusReport();
        expected.setErrorsGeneratingReport(Collections.EMPTY_LIST);

        addSystemDiagnosticStatus(expected, false, false, true, false, false);

        assertEquals(expected, actual);
    }

    @Test
    public void systemDiagnosticContentRepo() throws Exception {
        populateSystemDiagnostics();

        String statusRequest = "systemDiagnostics:contentrepositoryusage";
        FlowStatusReport actual = StatusConfigReporter.getStatus(mockFlowController, statusRequest, LoggerFactory.getLogger(StatusConfigReporterTest.class));

        FlowStatusReport expected = new FlowStatusReport();
        expected.setErrorsGeneratingReport(Collections.EMPTY_LIST);

        addSystemDiagnosticStatus(expected, false, false, false, true, false);

        assertEquals(expected, actual);
    }

    @Test
    public void systemDiagnosticGarbageCollection() throws Exception {
        populateSystemDiagnostics();

        String statusRequest = "systemDiagnostics:garbagecollection";
        FlowStatusReport actual = StatusConfigReporter.getStatus(mockFlowController, statusRequest, LoggerFactory.getLogger(StatusConfigReporterTest.class));

        FlowStatusReport expected = new FlowStatusReport();
        expected.setErrorsGeneratingReport(Collections.EMPTY_LIST);

        addSystemDiagnosticStatus(expected, false, false, false, false, true);

        assertEquals(expected, actual);
    }


    @Test
    public void systemDiagnosticAll() throws Exception {
        populateSystemDiagnostics();

        String statusRequest = "systemDiagnostics:garbagecollection, heap, processorstats, contentrepositoryusage, flowfilerepositoryusage";
        FlowStatusReport actual = StatusConfigReporter.getStatus(mockFlowController, statusRequest, LoggerFactory.getLogger(StatusConfigReporterTest.class));

        FlowStatusReport expected = new FlowStatusReport();
        expected.setErrorsGeneratingReport(Collections.EMPTY_LIST);

        addSystemDiagnosticStatus(expected, true, true, true, true, true);

        assertEquals(expected, actual);
    }

    @Test
    public void instanceStatusHealth() throws Exception {
        populateInstance(false);

        String statusRequest = "instance:health";
        FlowStatusReport actual = StatusConfigReporter.getStatus(mockFlowController, statusRequest, LoggerFactory.getLogger(StatusConfigReporterTest.class));

        FlowStatusReport expected = new FlowStatusReport();
        expected.setErrorsGeneratingReport(Collections.EMPTY_LIST);
        addInstanceStatus(expected, true, false, false, false);

        assertEquals(expected, actual);
    }

    @Test
    public void instanceStatusBulletins() throws Exception {
        populateInstance(true);

        String statusRequest = "instance:bulletins";
        FlowStatusReport actual = StatusConfigReporter.getStatus(mockFlowController, statusRequest, LoggerFactory.getLogger(StatusConfigReporterTest.class));

        FlowStatusReport expected = new FlowStatusReport();
        expected.setErrorsGeneratingReport(Collections.EMPTY_LIST);

        addInstanceStatus(expected, false, false, true, true);

        assertEquals(expected, actual);
    }

    @Test
    public void instanceStatusStats() throws Exception {
        populateInstance(false);

        String statusRequest = "instance:stats";
        FlowStatusReport actual = StatusConfigReporter.getStatus(mockFlowController, statusRequest, LoggerFactory.getLogger(StatusConfigReporterTest.class));

        FlowStatusReport expected = new FlowStatusReport();
        expected.setErrorsGeneratingReport(Collections.EMPTY_LIST);

        addInstanceStatus(expected, false, true, false, false);

        assertEquals(expected, actual);
    }

    @Test
    public void instanceStatusAll() throws Exception {
        populateInstance(true);

        String statusRequest = "instance:stats, bulletins, health";
        FlowStatusReport actual = StatusConfigReporter.getStatus(mockFlowController, statusRequest, LoggerFactory.getLogger(StatusConfigReporterTest.class));

        FlowStatusReport expected = new FlowStatusReport();
        expected.setErrorsGeneratingReport(Collections.EMPTY_LIST);

        addInstanceStatus(expected, true, true, true, true);

        assertEquals(expected, actual);
    }

    @Test
    public void controllerServiceStatusHealth() throws Exception {
        populateControllerService(false, false);

        String statusRequest = "controllerServices:health";
        FlowStatusReport actual = StatusConfigReporter.getStatus(mockFlowController, statusRequest, LoggerFactory.getLogger(StatusConfigReporterTest.class));

        FlowStatusReport expected = new FlowStatusReport();
        expected.setErrorsGeneratingReport(Collections.EMPTY_LIST);

        addControllerServiceStatus(expected, true, false, false, false);

        assertEquals(expected, actual);
    }

    @Test
    public void controllerServiceStatusBulletins() throws Exception {
        populateControllerService(false, true);

        String statusRequest = "controllerServices:bulletins";
        FlowStatusReport actual = StatusConfigReporter.getStatus(mockFlowController, statusRequest, LoggerFactory.getLogger(StatusConfigReporterTest.class));

        FlowStatusReport expected = new FlowStatusReport();
        expected.setErrorsGeneratingReport(Collections.EMPTY_LIST);

        addControllerServiceStatus(expected, false, false, true, true);

        assertEquals(expected, actual);
    }

    @Test
    public void controllerServiceStatusAll() throws Exception {
        populateControllerService(true, true);

        String statusRequest = "controllerServices:bulletins, health";

        FlowStatusReport actual = StatusConfigReporter.getStatus(mockFlowController, statusRequest, LoggerFactory.getLogger(StatusConfigReporterTest.class));

        FlowStatusReport expected = new FlowStatusReport();
        expected.setErrorsGeneratingReport(Collections.EMPTY_LIST);

        addControllerServiceStatus(expected, true, true, true, true);

        assertEquals(expected, actual);
    }

    @Test
    public void remoteProcessGroupStatusHealth() throws Exception {
        populateRemoteProcessGroup(false, false);

        String statusRequest = "remoteProcessGroup:all:health";
        FlowStatusReport actual = StatusConfigReporter.getStatus(mockFlowController, statusRequest, LoggerFactory.getLogger(StatusConfigReporterTest.class));

        FlowStatusReport expected = new FlowStatusReport();
        expected.setErrorsGeneratingReport(Collections.EMPTY_LIST);

        addExpectedRemoteProcessGroupStatus(expected, true, false, false, false, false, false);

        assertEquals(expected, actual);
    }

    @Test
    public void remoteProcessGroupStatusBulletins() throws Exception {
        populateRemoteProcessGroup(true, false);

        String statusRequest = "remoteProcessGroup:all:bulletins";
        FlowStatusReport actual = StatusConfigReporter.getStatus(mockFlowController, statusRequest, LoggerFactory.getLogger(StatusConfigReporterTest.class));

        FlowStatusReport expected = new FlowStatusReport();
        expected.setErrorsGeneratingReport(Collections.EMPTY_LIST);

        addExpectedRemoteProcessGroupStatus(expected, false, false, false, false, true, true);

        assertEquals(expected, actual);
    }

    @Test
    public void remoteProcessGroupStatusInputPorts() throws Exception {
        populateRemoteProcessGroup(false, false);

        String statusRequest = "remoteProcessGroup:all:inputPorts";
        FlowStatusReport actual = StatusConfigReporter.getStatus(mockFlowController, statusRequest, LoggerFactory.getLogger(StatusConfigReporterTest.class));

        FlowStatusReport expected = new FlowStatusReport();
        expected.setErrorsGeneratingReport(Collections.EMPTY_LIST);

        addExpectedRemoteProcessGroupStatus(expected, false, true, false, false, false, false);

        assertEquals(expected, actual);
    }

    @Test
    public void remoteProcessGroupStatusOutputPorts() throws Exception {
        populateRemoteProcessGroup(false, false);

        String statusRequest = "remoteProcessGroup:all:outputPorts";
        FlowStatusReport actual = StatusConfigReporter.getStatus(mockFlowController, statusRequest, LoggerFactory.getLogger(StatusConfigReporterTest.class));

        FlowStatusReport expected = new FlowStatusReport();
        expected.setErrorsGeneratingReport(Collections.EMPTY_LIST);

        addExpectedRemoteProcessGroupStatus(expected, false, false, true, false, false, false);

        assertEquals(expected, actual);
    }

    @Test
    public void remoteProcessGroupStatusStats() throws Exception {
        populateRemoteProcessGroup(false, false);

        String statusRequest = "remoteProcessGroup:all:stats";
        FlowStatusReport actual = StatusConfigReporter.getStatus(mockFlowController, statusRequest, LoggerFactory.getLogger(StatusConfigReporterTest.class));

        FlowStatusReport expected = new FlowStatusReport();
        expected.setErrorsGeneratingReport(Collections.EMPTY_LIST);

        addExpectedRemoteProcessGroupStatus(expected, false, false, false, true, false, false);

        assertEquals(expected, actual);
    }


    @Test
    public void remoteProcessGroupStatusAll() throws Exception {
        populateRemoteProcessGroup(true, true);

        String statusRequest = "remoteProcessGroup:all:health, bulletins, inputPorts, outputPorts, stats";
        FlowStatusReport actual = StatusConfigReporter.getStatus(mockFlowController, statusRequest, LoggerFactory.getLogger(StatusConfigReporterTest.class));

        FlowStatusReport expected = new FlowStatusReport();
        expected.setErrorsGeneratingReport(Collections.EMPTY_LIST);

        addExpectedRemoteProcessGroupStatus(expected, true, true, true, true, true, true);

        assertEquals(expected, actual);
    }

    @Test
    public void statusEverything() throws Exception {
        when(bulletinRepo.findBulletins(anyObject())).thenReturn(Collections.emptyList());

        populateControllerService(true, false);
        populateInstance(true);
        populateSystemDiagnostics();
        populateReportingTask(false, true);
        populateConnection();
        populateProcessor(true, false);
        populateRemoteProcessGroup(false, true);

        String statusRequest = "controllerServices:bulletins,health; processor:all:health,stats,bulletins; instance:bulletins,health,stats ; systemDiagnostics:garbagecollection, heap, " +
                "processorstats, contentrepositoryusage, flowfilerepositoryusage; connection:all:health,stats; provenanceReporting:health,bulletins; remoteProcessGroup:all:health, " +
                "bulletins, inputPorts, outputPorts, stats";

        FlowStatusReport actual = StatusConfigReporter.getStatus(mockFlowController, statusRequest, LoggerFactory.getLogger(StatusConfigReporterTest.class));

        FlowStatusReport expected = new FlowStatusReport();
        expected.setErrorsGeneratingReport(Collections.EMPTY_LIST);

        addControllerServiceStatus(expected, true, true, true, false);
        addInstanceStatus(expected, true, true, true, true);
        addSystemDiagnosticStatus(expected, true, true, true, true, true);
        addReportingTaskStatus(expected, true, true, true, false);
        addConnectionStatus(expected, true, true);
        addProcessorStatus(expected, true, true, true, true, false);
        addExpectedRemoteProcessGroupStatus(expected, true, true, true, true, true, false);

        assertEquals(expected, actual);
    }


    /***************************
     * Populator methods
     *************************/

    private void addBulletinsToInstance() {
        Bulletin bulletin = mock(Bulletin.class);
        when(bulletin.getTimestamp()).thenReturn(new Date(1464019245000L));
        when(bulletin.getMessage()).thenReturn("Bulletin message");

        List<Bulletin> bulletinList = new ArrayList<>();
        bulletinList.add(bulletin);

        when(bulletinRepo.findBulletinsForController()).thenReturn(bulletinList);
    }

    private void populateSystemDiagnostics() {
        SystemDiagnostics systemDiagnostics = new SystemDiagnostics();
        addGarbageCollectionToSystemDiagnostics(systemDiagnostics);
        addHeapSystemDiagnostics(systemDiagnostics);
        addContentRepoToSystemDiagnostics(systemDiagnostics);
        addFlowFileRepoToSystemDiagnostics(systemDiagnostics);
        addProcessorInfoToSystemDiagnostics(systemDiagnostics);
        when(mockFlowController.getSystemDiagnostics()).thenReturn(systemDiagnostics);
    }

    private void populateControllerService(boolean validationErrors, boolean addBulletins) {
        ControllerServiceNode controllerServiceNode = mock(ControllerServiceNode.class);
        addControllerServiceHealth(controllerServiceNode);
        if (validationErrors) {
            addValidationErrors(controllerServiceNode);
        }

        if (addBulletins) {
            addBulletins("Bulletin message", controllerServiceNode.getIdentifier());
        }
        HashSet<ControllerServiceNode> controllerServiceNodes = new HashSet<>();
        controllerServiceNodes.add(controllerServiceNode);
        when(mockFlowController.getFlowManager().getAllControllerServices()).thenReturn(controllerServiceNodes);
    }

    private void populateInstance(boolean addBulletins) {
        setRootGroupStatusVariables();
        if (addBulletins) {
            addBulletinsToInstance();
        }
    }

    private void populateReportingTask(boolean addBulletins, boolean validationErrors) {
        if (addBulletins) {
            addBulletins("Bulletin message", "ReportProvenance");
        }

        ReportingTaskNode reportingTaskNode = mock(ReportingTaskNode.class);
        addReportingTaskNodeVariables(reportingTaskNode);

        HashSet<ReportingTaskNode> reportingTaskNodes = new HashSet<>();
        reportingTaskNodes.add(reportingTaskNode);

        when(mockFlowController.getAllReportingTasks()).thenReturn(reportingTaskNodes);

        if (validationErrors) {
            ValidationResult validationResult = new ValidationResult.Builder()
                    .input("input")
                    .subject("subject")
                    .explanation("is not valid")
                    .build();

            ValidationResult validationResult2 = new ValidationResult.Builder()
                    .input("input2")
                    .subject("subject2")
                    .explanation("is not valid too")
                    .build();

            List<ValidationResult> validationResultList = new ArrayList<>();
            validationResultList.add(validationResult);
            validationResultList.add(validationResult2);

            when(reportingTaskNode.getValidationErrors()).thenReturn(validationResultList);
        } else {
            when(reportingTaskNode.getValidationErrors()).thenReturn(Collections.EMPTY_LIST);
        }
    }

    private void populateConnection() {
        ConnectionStatus connectionStatus = new ConnectionStatus();
        connectionStatus.setQueuedBytes(100);
        connectionStatus.setId("connectionId");
        connectionStatus.setName("connectionName");
        connectionStatus.setQueuedCount(10);
        connectionStatus.setInputCount(1);
        connectionStatus.setInputBytes(2);
        connectionStatus.setOutputCount(3);
        connectionStatus.setOutputBytes(4);

        Collection<ConnectionStatus> statusCollection = new ArrayList<>();
        statusCollection.add(connectionStatus);

        when(rootGroupStatus.getConnectionStatus()).thenReturn(statusCollection);
    }

    private void populateProcessor(boolean validationErrors, boolean addBulletins) {
        if (addBulletins) {
            addBulletins("Bulletin message", "UpdateAttributeProcessorId");
        }

        ProcessorStatus processorStatus = new ProcessorStatus();
        processorStatus.setType("org.apache.nifi.processors.attributes.UpdateAttribute");
        processorStatus.setId("UpdateAttributeProcessorId");
        processorStatus.setName("UpdateAttributeProcessorName");
        processorStatus.setRunStatus(RunStatus.Stopped);
        processorStatus.setActiveThreadCount(1);
        processorStatus.setFlowFilesReceived(2);
        processorStatus.setBytesRead(3);
        processorStatus.setBytesWritten(4);
        processorStatus.setFlowFilesSent(5);
        processorStatus.setInvocations(6);
        processorStatus.setProcessingNanos(7);

        Collection<ProcessorStatus> statusCollection = new ArrayList<>();
        statusCollection.add(processorStatus);

        mockProcessorEmptyValidation(processorStatus.getId(), processGroup);
        when(rootGroupStatus.getProcessorStatus()).thenReturn(statusCollection);

        ProcessorNode processorNode = mock(ProcessorNode.class);
        when(processGroup.getProcessor(processorStatus.getId())).thenReturn(processorNode);

        if (validationErrors) {
            ValidationResult validationResult = new ValidationResult.Builder()
                    .input("input")
                    .subject("subject")
                    .explanation("is not valid")
                    .build();

            ValidationResult validationResult2 = new ValidationResult.Builder()
                    .input("input2")
                    .subject("subject2")
                    .explanation("is not valid too")
                    .build();

            List<ValidationResult> validationResultList = new ArrayList<>();
            validationResultList.add(validationResult);
            validationResultList.add(validationResult2);

            when(processorNode.getValidationErrors()).thenReturn(validationResultList);
        } else {
            when(processorNode.getValidationErrors()).thenReturn(Collections.EMPTY_LIST);
        }
    }

    private void populateRemoteProcessGroup(boolean addBulletins, boolean addAuthIssues) {
        when(mockFlowController.getFlowManager().getGroup(mockFlowController.getFlowManager().getRootGroupId())).thenReturn(processGroup);

        RemoteProcessGroup remoteProcessGroup = mock(RemoteProcessGroup.class);
        when(processGroup.getRemoteProcessGroup(any())).thenReturn(remoteProcessGroup);

        RemoteGroupPort remoteGroupPort = mock(RemoteGroupPort.class);
        when(remoteGroupPort.getName()).thenReturn("inputPort");
        when(remoteGroupPort.getTargetExists()).thenReturn(true);
        when(remoteGroupPort.isTargetRunning()).thenReturn(false);

        when(remoteProcessGroup.getInputPorts()).thenReturn(Collections.singleton(remoteGroupPort));

        remoteGroupPort = mock(RemoteGroupPort.class);
        when(remoteGroupPort.getName()).thenReturn("outputPort");
        when(remoteGroupPort.getTargetExists()).thenReturn(true);
        when(remoteGroupPort.isTargetRunning()).thenReturn(false);

        when(remoteProcessGroup.getOutputPorts()).thenReturn(Collections.singleton(remoteGroupPort));

        RemoteProcessGroupStatus remoteProcessGroupStatus = new RemoteProcessGroupStatus();
        addRemoteProcessGroupStatus(remoteProcessGroupStatus);
        if (addBulletins) {
            addBulletins("Bulletin message", remoteProcessGroupStatus.getId());
        }
        when(rootGroupStatus.getRemoteProcessGroupStatus()).thenReturn(Collections.singletonList(remoteProcessGroupStatus));
    }


    private void setRootGroupStatusVariables() {
        when(rootGroupStatus.getQueuedContentSize()).thenReturn(1L);
        when(rootGroupStatus.getQueuedCount()).thenReturn(2);
        when(rootGroupStatus.getActiveThreadCount()).thenReturn(3);
        when(rootGroupStatus.getBytesRead()).thenReturn(1L);
        when(rootGroupStatus.getBytesWritten()).thenReturn(2L);
        when(rootGroupStatus.getBytesSent()).thenReturn(3L);
        when(rootGroupStatus.getFlowFilesSent()).thenReturn(4);
        when(rootGroupStatus.getBytesTransferred()).thenReturn(5L);
        when(rootGroupStatus.getFlowFilesTransferred()).thenReturn(6);
        when(rootGroupStatus.getBytesReceived()).thenReturn(7L);
        when(rootGroupStatus.getFlowFilesReceived()).thenReturn(8);
    }

    private void addGarbageCollectionToSystemDiagnostics(SystemDiagnostics systemDiagnostics) {
        Map<String, GarbageCollection> garbageCollectionMap = new HashMap<>();

        GarbageCollection garbageCollection1 = new GarbageCollection();
        garbageCollection1.setCollectionCount(1);
        garbageCollection1.setCollectionTime(10);
        garbageCollection1.setName("garbage 1");
        garbageCollectionMap.put(garbageCollection1.getName(), garbageCollection1);

        systemDiagnostics.setGarbageCollection(garbageCollectionMap);
    }

    private void addContentRepoToSystemDiagnostics(SystemDiagnostics systemDiagnostics) {
        Map<String, StorageUsage> stringStorageUsageMap = new HashMap<>();

        StorageUsage repoUsage1 = new StorageUsage();
        repoUsage1.setFreeSpace(30);
        repoUsage1.setTotalSpace(100);
        repoUsage1.setIdentifier("Content repo1");
        stringStorageUsageMap.put(repoUsage1.getIdentifier(), repoUsage1);

        systemDiagnostics.setContentRepositoryStorageUsage(stringStorageUsageMap);
    }

    private void addFlowFileRepoToSystemDiagnostics(SystemDiagnostics systemDiagnostics) {
        StorageUsage repoUsage = new StorageUsage();
        repoUsage.setFreeSpace(30);
        repoUsage.setTotalSpace(100);
        repoUsage.setIdentifier("FlowFile repo");
        systemDiagnostics.setFlowFileRepositoryStorageUsage(repoUsage);
    }

    private void addHeapSystemDiagnostics(SystemDiagnostics systemDiagnostics) {
        systemDiagnostics.setMaxHeap(5);
        systemDiagnostics.setTotalHeap(3);
        systemDiagnostics.setUsedHeap(2);
        systemDiagnostics.setMaxNonHeap(9);
        systemDiagnostics.setTotalNonHeap(8);
        systemDiagnostics.setUsedNonHeap(6);
    }

    private void addProcessorInfoToSystemDiagnostics(SystemDiagnostics systemDiagnostics) {
        systemDiagnostics.setProcessorLoadAverage(80.9);
        systemDiagnostics.setAvailableProcessors(5);
    }

    private void mockProcessorEmptyValidation(String id, ProcessGroup processGroup) {
        ProcessorNode processorNode = mock(ProcessorNode.class);
        when(processGroup.getProcessor(id)).thenReturn(processorNode);
        when(processorNode.getValidationErrors()).thenReturn(Collections.emptyList());
    }

    private void addControllerServiceHealth(ControllerServiceNode controllerServiceNode) {
        when(controllerServiceNode.getName()).thenReturn("mockControllerService");
        when(controllerServiceNode.getIdentifier()).thenReturn("mockControllerService");
        when(controllerServiceNode.getState()).thenReturn(ControllerServiceState.ENABLED);
        when(controllerServiceNode.getValidationErrors()).thenReturn(Collections.emptyList());
    }

    private void addReportingTaskNodeVariables(ReportingTaskNode reportingTaskNode) {
        when(reportingTaskNode.getValidationErrors()).thenReturn(Collections.emptyList());
        when(reportingTaskNode.getActiveThreadCount()).thenReturn(1);
        when(reportingTaskNode.getScheduledState()).thenReturn(ScheduledState.RUNNING);
        when(reportingTaskNode.getIdentifier()).thenReturn("ReportProvenance");
        when(reportingTaskNode.getName()).thenReturn("ReportProvenance");

    }

    private void addRemoteProcessGroupStatus(RemoteProcessGroupStatus remoteProcessGroupStatus) {
        remoteProcessGroupStatus.setName("rpg1");
        remoteProcessGroupStatus.setId("rpg1");
        remoteProcessGroupStatus.setTransmissionStatus(TransmissionStatus.Transmitting);
        remoteProcessGroupStatus.setActiveRemotePortCount(1);
        remoteProcessGroupStatus.setInactiveRemotePortCount(2);

        remoteProcessGroupStatus.setActiveThreadCount(3);
        remoteProcessGroupStatus.setSentContentSize(4L);
        remoteProcessGroupStatus.setSentCount(5);
    }

    private void addBulletins(String message, String sourceId) {
        Bulletin bulletin = mock(Bulletin.class);
        when(bulletin.getTimestamp()).thenReturn(new Date(1464019245000L));
        when(bulletin.getMessage()).thenReturn(message);

        List<Bulletin> bulletinList = new ArrayList<>();
        bulletinList.add(bulletin);

        BulletinQueryAnswer bulletinQueryAnswer = new BulletinQueryAnswer(sourceId, bulletinList);
        when(bulletinRepo.findBulletins(anyObject())).then(bulletinQueryAnswer);
    }

    private void addValidationErrors(ComponentNode connectable) {
        ValidationResult validationResult = new ValidationResult.Builder()
                .input("input")
                .subject("subject")
                .explanation("is not valid")
                .build();

        ValidationResult validationResult2 = new ValidationResult.Builder()
                .input("input2")
                .subject("subject2")
                .explanation("is not valid too")
                .build();

        List<ValidationResult> validationResultList = new ArrayList<>();
        validationResultList.add(validationResult);
        validationResultList.add(validationResult2);
        when(connectable.getValidationErrors()).thenReturn(validationResultList);
    }

    private class BulletinQueryAnswer implements Answer {

        final List<Bulletin> bulletinList;
        String idToMatch = "";

        private BulletinQueryAnswer(String idToMatch, List<Bulletin> bulletinList) {
            this.idToMatch = idToMatch;
            this.bulletinList = bulletinList;
        }

        @Override
        public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
            BulletinQuery bulletinQuery = (BulletinQuery) invocationOnMock.getArguments()[0];
            if (idToMatch.equals(bulletinQuery.getSourceIdPattern().toString())) {
                return bulletinList;
            }
            return Collections.emptyList();
        }
    }
}
