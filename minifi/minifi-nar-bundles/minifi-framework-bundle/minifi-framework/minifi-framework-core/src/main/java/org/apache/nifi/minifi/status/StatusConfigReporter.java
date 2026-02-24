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
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.RemoteProcessGroupStatus;
import org.apache.nifi.minifi.commons.status.FlowStatusReport;
import org.apache.nifi.minifi.commons.status.connection.ConnectionStatusBean;
import org.apache.nifi.minifi.commons.status.controllerservice.ControllerServiceStatus;
import org.apache.nifi.minifi.commons.status.instance.InstanceStatus;
import org.apache.nifi.minifi.commons.status.processor.ProcessorStatusBean;
import org.apache.nifi.minifi.commons.status.reportingTask.ReportingTaskStatus;
import org.apache.nifi.minifi.commons.status.rpg.RemoteProcessGroupStatusBean;
import org.apache.nifi.minifi.commons.status.system.SystemDiagnosticsStatus;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.nifi.minifi.status.StatusRequestParser.parseConnectionStatusRequest;
import static org.apache.nifi.minifi.status.StatusRequestParser.parseControllerServiceStatusRequest;
import static org.apache.nifi.minifi.status.StatusRequestParser.parseInstanceRequest;
import static org.apache.nifi.minifi.status.StatusRequestParser.parseProcessorStatusRequest;
import static org.apache.nifi.minifi.status.StatusRequestParser.parseRemoteProcessGroupStatusRequest;
import static org.apache.nifi.minifi.status.StatusRequestParser.parseReportingTaskStatusRequest;
import static org.apache.nifi.minifi.status.StatusRequestParser.parseSystemDiagnosticsRequest;

public final class StatusConfigReporter {

    private StatusConfigReporter() {
    }

    public static FlowStatusReport getStatus(final FlowController flowController, final String statusRequest, final Logger logger) throws StatusRequestException {
        if (statusRequest == null) {
            logger.error("Received a status request which was null");
            throw new StatusRequestException("Cannot complete status request because the statusRequest is null");
        }

        if (flowController == null) {
            logger.error("Received a status but the Flow Controller is null");
            throw new StatusRequestException("Cannot complete status request because the Flow Controller is null");
        }
        final FlowStatusReport flowStatusReport = new FlowStatusReport();
        final List<String> errorsGeneratingReport = new LinkedList<>();
        flowStatusReport.setErrorsGeneratingReport(errorsGeneratingReport);

        final String[] itemsToReport = statusRequest.split(";");

        final ProcessGroupStatus rootGroupStatus = flowController.getEventAccess().getControllerStatus();

        final Map<String, ProcessorStatus> processorStatusMap = null;
        final Map<String, ConnectionStatus> connectionStatusMap = null;
        final Map<String, RemoteProcessGroupStatus> remoteProcessGroupStatusMap = null;

        for (final String item : itemsToReport) {
            try {
                final RequestItem requestItem = new RequestItem(item);
                switch (requestItem.queryType.toLowerCase().trim()) {
                    case "systemdiagnostics":
                        final SystemDiagnosticsStatus systemDiagnosticsStatus = parseSystemDiagnosticsRequest(flowController.getSystemDiagnostics(), requestItem.options);
                        flowStatusReport.setSystemDiagnosticsStatus(systemDiagnosticsStatus);
                        break;
                    case "instance":
                        final InstanceStatus instanceStatus = parseInstanceRequest(requestItem.options, flowController, rootGroupStatus);
                        flowStatusReport.setInstanceStatus(instanceStatus);
                        break;
                    case "remoteprocessgroup":
                        if (flowStatusReport.getRemoteProcessGroupStatusList() == null) {
                            final List<RemoteProcessGroupStatusBean> remoteProcessGroupStatusList = new LinkedList<>();
                            flowStatusReport.setRemoteProcessGroupStatusList(remoteProcessGroupStatusList);
                        }
                        handleRemoteProcessGroupRequest(requestItem, rootGroupStatus, flowController, flowStatusReport.getRemoteProcessGroupStatusList(), remoteProcessGroupStatusMap, logger);
                        break;
                    case "processor":
                        if (flowStatusReport.getProcessorStatusList() == null) {
                            final List<ProcessorStatusBean> processorStatusList = new LinkedList<>();
                            flowStatusReport.setProcessorStatusList(processorStatusList);
                        }
                        handleProcessorRequest(requestItem, rootGroupStatus, flowController, flowStatusReport.getProcessorStatusList(), processorStatusMap, logger);
                        break;
                    case "connection":
                        if (flowStatusReport.getConnectionStatusList() == null) {
                            final List<ConnectionStatusBean> connectionStatusList = new LinkedList<>();
                            flowStatusReport.setConnectionStatusList(connectionStatusList);
                        }
                        handleConnectionRequest(requestItem, rootGroupStatus, flowStatusReport.getConnectionStatusList(), connectionStatusMap, logger);
                        break;
                    case "provenancereporting":
                        if (flowStatusReport.getRemoteProcessGroupStatusList() == null) {
                            final List<ReportingTaskStatus> reportingTaskStatusList = new LinkedList<>();
                            flowStatusReport.setReportingTaskStatusList(reportingTaskStatusList);
                        }
                        handleReportingTaskRequest(requestItem, flowController, flowStatusReport.getReportingTaskStatusList(), logger);
                        break;
                    case "controllerservices":
                        if (flowStatusReport.getControllerServiceStatusList() == null) {
                            final List<ControllerServiceStatus> controllerServiceStatusList = new LinkedList<>();
                            flowStatusReport.setControllerServiceStatusList(controllerServiceStatusList);
                        }
                        handleControllerServices(requestItem, flowController, flowStatusReport.getControllerServiceStatusList(), logger);
                        break;
                }
            } catch (final Exception e) {
                logger.error("Hit exception while requesting status for item '{}'", item, e);
                errorsGeneratingReport.add("Unable to get status for request '" + item + "' due to:" + e);
            }
        }
        return flowStatusReport;
    }

    private static void handleControllerServices(final RequestItem requestItem, final FlowController flowController,
            final List<ControllerServiceStatus> controllerServiceStatusList, final Logger logger) {

        final Collection<ControllerServiceNode> controllerServiceNodeSet = flowController.getFlowManager().getAllControllerServices();

        if (!controllerServiceNodeSet.isEmpty()) {
            for (final ControllerServiceNode controllerServiceNode : controllerServiceNodeSet) {
                controllerServiceStatusList.add(parseControllerServiceStatusRequest(controllerServiceNode, requestItem.options, flowController, logger));
            }
        }
    }

    private static void handleProcessorRequest(final RequestItem requestItem, final ProcessGroupStatus rootGroupStatus,
            final FlowController flowController, final List<ProcessorStatusBean> processorStatusBeanList,
            final Map<String, ProcessorStatus> processorStatusMapArg, final Logger logger) throws StatusRequestException {
        final Map<String, ProcessorStatus> processorStatusMap = processorStatusMapArg != null
                ? processorStatusMapArg : transformStatusCollection(rootGroupStatus.getProcessorStatus());

        final String rootGroupId = flowController.getFlowManager().getRootGroupId();
        if (requestItem.identifier.equalsIgnoreCase("all")) {
            if (!processorStatusMap.isEmpty()) {
                for (final ProcessorStatus processorStatus : new HashSet<>(processorStatusMap.values())) {
                    final Collection<ValidationResult> validationResults = flowController.getFlowManager().getGroup(rootGroupId).getProcessor(processorStatus.getId()).getValidationErrors();
                    processorStatusBeanList.add(parseProcessorStatusRequest(processorStatus, requestItem.options, flowController, validationResults));
                }
            }
        } else {

            if (processorStatusMap.containsKey(requestItem.identifier)) {
                final ProcessorStatus processorStatus = processorStatusMap.get(requestItem.identifier);
                final Collection<ValidationResult> validationResults = flowController.getFlowManager().getGroup(rootGroupId).getProcessor(processorStatus.getId()).getValidationErrors();
                processorStatusBeanList.add(parseProcessorStatusRequest(processorStatus, requestItem.options, flowController, validationResults));
            } else {
                logger.warn("Status for processor with key {} was requested but one does not exist", requestItem.identifier);
                throw new StatusRequestException("No processor with key " + requestItem.identifier + " to report status on");
            }
        }
    }

    private static void handleConnectionRequest(final RequestItem requestItem, final ProcessGroupStatus rootGroupStatus,
            final List<ConnectionStatusBean> connectionStatusList, final Map<String, ConnectionStatus> connectionStatusMapArg,
            final Logger logger) throws StatusRequestException {
        final Map<String, ConnectionStatus> connectionStatusMap = connectionStatusMapArg != null
                ? connectionStatusMapArg : transformStatusCollection(rootGroupStatus.getConnectionStatus());

        if (requestItem.identifier.equalsIgnoreCase("all")) {
            if (!connectionStatusMap.isEmpty()) {
                for (final ConnectionStatus connectionStatus : new HashSet<>(connectionStatusMap.values())) {
                    connectionStatusList.add(parseConnectionStatusRequest(connectionStatus, requestItem.options, logger));
                }
            }
        } else {
            if (connectionStatusMap.containsKey(requestItem.identifier)) {
                connectionStatusList.add(parseConnectionStatusRequest(connectionStatusMap.get(requestItem.identifier), requestItem.options, logger));
            } else {
                logger.warn("Status for connection with key {} was requested but one does not exist", requestItem.identifier);
                throw new StatusRequestException("No connection with key " + requestItem.identifier + " to report status on");
            }
        }

    }

    private static void handleRemoteProcessGroupRequest(final RequestItem requestItem, final ProcessGroupStatus rootGroupStatus,
            final FlowController flowController, final List<RemoteProcessGroupStatusBean> remoteProcessGroupStatusList,
            final Map<String, RemoteProcessGroupStatus> remoteProcessGroupStatusMapArg, final Logger logger) throws StatusRequestException {
        final Map<String, RemoteProcessGroupStatus> remoteProcessGroupStatusMap = remoteProcessGroupStatusMapArg != null
                ? remoteProcessGroupStatusMapArg : transformStatusCollection(rootGroupStatus.getRemoteProcessGroupStatus());

        if (requestItem.identifier.equalsIgnoreCase("all")) {
            if (!remoteProcessGroupStatusMap.isEmpty()) {
                for (final RemoteProcessGroupStatus remoteProcessGroupStatus : new HashSet<>(remoteProcessGroupStatusMap.values())) {
                    remoteProcessGroupStatusList.add(parseRemoteProcessGroupStatusRequest(remoteProcessGroupStatus, requestItem.options, flowController));
                }
            }
        } else {

            if (remoteProcessGroupStatusMap.containsKey(requestItem.identifier)) {
                final RemoteProcessGroupStatus remoteProcessGroupStatus = remoteProcessGroupStatusMap.get(requestItem.identifier);
                remoteProcessGroupStatusList.add(parseRemoteProcessGroupStatusRequest(remoteProcessGroupStatus, requestItem.options, flowController));
            } else {
                logger.warn("Status for Remote Process Group with key {} was requested but one does not exist", requestItem.identifier);
                throw new StatusRequestException("No Remote Process Group with key " + requestItem.identifier + " to report status on");
            }
        }
    }

    private static void handleReportingTaskRequest(final RequestItem requestItem, final FlowController flowController, final List<ReportingTaskStatus> reportingTaskStatusList, final Logger logger) {
        final Set<ReportingTaskNode> reportingTaskNodes = flowController.getAllReportingTasks();

        if (!reportingTaskNodes.isEmpty()) {
            for (final ReportingTaskNode reportingTaskNode : reportingTaskNodes) {
                reportingTaskStatusList.add(parseReportingTaskStatusRequest(reportingTaskNode.getIdentifier(), reportingTaskNode, requestItem.options, flowController, logger));
            }
        }
    }

    private static <E> Map<String, E> transformStatusCollection(final Collection<E> statusCollection) {
        final Map<String, E> statusMap = new HashMap<>();
        for (final E status : statusCollection) {
            if (status instanceof ProcessorStatus) {
                statusMap.put(((ProcessorStatus) status).getId(), status);
                if (((ProcessorStatus) status).getName() != null) {
                    statusMap.put(((ProcessorStatus) status).getName(), status);
                }
            } else if (status instanceof ConnectionStatus) {
                statusMap.put(((ConnectionStatus) status).getId(), status);
                if (((ConnectionStatus) status).getName() != null) {
                    statusMap.put(((ConnectionStatus) status).getName(), status);
                }
            } else if (status instanceof RemoteProcessGroupStatus) {
                statusMap.put(((RemoteProcessGroupStatus) status).getId(), status);
                if (((RemoteProcessGroupStatus) status).getName() != null) {
                    statusMap.put(((RemoteProcessGroupStatus) status).getName(), status);
                }
            }
        }
        return statusMap;
    }

    private static class RequestItem {

        private final int EXPECTED_REQUEST_COMPONENTS = 2;

        private String queryType;
        private String identifier;
        private String options;

        public RequestItem(final String requestString) {
            final List<String> reqComponents = Arrays.asList(requestString.split(":"));
            final int numComponents = reqComponents.size();
            if (numComponents < EXPECTED_REQUEST_COMPONENTS) {
                throw new IllegalArgumentException(String.format("Cannot perform a FlowStatusQuery request for '%s'.  Expected at least %d components but got %d.",
                        requestString, EXPECTED_REQUEST_COMPONENTS, numComponents));
            }
            this.queryType = reqComponents.get(0).toLowerCase();
            if (numComponents == 2) {
                this.options = reqComponents.get(1);
            } else {
                this.identifier = reqComponents.get(1);
                this.options = reqComponents.get(2);
            }
            // normalize options
            this.options = this.options.toLowerCase();
        }
    }

}
