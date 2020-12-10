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
import org.apache.nifi.diagnostics.GarbageCollection;
import org.apache.nifi.diagnostics.StorageUsage;
import org.apache.nifi.diagnostics.SystemDiagnostics;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.minifi.commons.status.common.BulletinStatus;
import org.apache.nifi.minifi.commons.status.common.ValidationError;
import org.apache.nifi.minifi.commons.status.connection.ConnectionHealth;
import org.apache.nifi.minifi.commons.status.connection.ConnectionStats;
import org.apache.nifi.minifi.commons.status.connection.ConnectionStatusBean;
import org.apache.nifi.minifi.commons.status.controllerservice.ControllerServiceHealth;
import org.apache.nifi.minifi.commons.status.controllerservice.ControllerServiceStatus;
import org.apache.nifi.minifi.commons.status.instance.InstanceHealth;
import org.apache.nifi.minifi.commons.status.instance.InstanceStats;
import org.apache.nifi.minifi.commons.status.instance.InstanceStatus;
import org.apache.nifi.minifi.commons.status.processor.ProcessorHealth;
import org.apache.nifi.minifi.commons.status.processor.ProcessorStats;
import org.apache.nifi.minifi.commons.status.processor.ProcessorStatusBean;
import org.apache.nifi.minifi.commons.status.reportingTask.ReportingTaskHealth;
import org.apache.nifi.minifi.commons.status.reportingTask.ReportingTaskStatus;
import org.apache.nifi.minifi.commons.status.rpg.PortStatus;
import org.apache.nifi.minifi.commons.status.rpg.RemoteProcessGroupHealth;
import org.apache.nifi.minifi.commons.status.rpg.RemoteProcessGroupStats;
import org.apache.nifi.minifi.commons.status.rpg.RemoteProcessGroupStatusBean;
import org.apache.nifi.minifi.commons.status.system.ContentRepositoryUsage;
import org.apache.nifi.minifi.commons.status.system.FlowfileRepositoryUsage;
import org.apache.nifi.minifi.commons.status.system.GarbageCollectionStatus;
import org.apache.nifi.minifi.commons.status.system.HeapStatus;
import org.apache.nifi.minifi.commons.status.system.SystemDiagnosticsStatus;
import org.apache.nifi.minifi.commons.status.system.SystemProcessorStats;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.BulletinQuery;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class StatusRequestParser {
    private StatusRequestParser() {
    }

    static ProcessorStatusBean parseProcessorStatusRequest(ProcessorStatus inputProcessorStatus, String statusTypes, FlowController flowController, Collection<ValidationResult> validationResults) {
        ProcessorStatusBean processorStatusBean = new ProcessorStatusBean();
        processorStatusBean.setId(inputProcessorStatus.getId());
        processorStatusBean.setName(inputProcessorStatus.getName());

        String[] statusSplits = statusTypes.split(",");
        List<Bulletin> bulletinList = flowController.getBulletinRepository().findBulletins(
                new BulletinQuery.Builder()
                        .sourceIdMatches(inputProcessorStatus.getId())
                        .build());

        for (String statusType : statusSplits) {
            switch (statusType.toLowerCase().trim()) {
                case "health":
                    ProcessorHealth processorHealth = new ProcessorHealth();

                    processorHealth.setRunStatus(inputProcessorStatus.getRunStatus().name());
                    processorHealth.setHasBulletins(!bulletinList.isEmpty());
                    processorHealth.setValidationErrorList(transformValidationResults(validationResults));

                    processorStatusBean.setProcessorHealth(processorHealth);
                    break;
                case "bulletins":
                    processorStatusBean.setBulletinList(transformBulletins(bulletinList));
                    break;
                case "stats":
                    ProcessorStats processorStats = new ProcessorStats();

                    processorStats.setActiveThreads(inputProcessorStatus.getActiveThreadCount());
                    processorStats.setFlowfilesReceived(inputProcessorStatus.getFlowFilesReceived());
                    processorStats.setBytesRead(inputProcessorStatus.getBytesRead());
                    processorStats.setBytesWritten(inputProcessorStatus.getBytesWritten());
                    processorStats.setFlowfilesSent(inputProcessorStatus.getFlowFilesSent());
                    processorStats.setInvocations(inputProcessorStatus.getInvocations());
                    processorStats.setProcessingNanos(inputProcessorStatus.getProcessingNanos());

                    processorStatusBean.setProcessorStats(processorStats);
                    break;
            }
        }
        return processorStatusBean;
    }

    static RemoteProcessGroupStatusBean parseRemoteProcessGroupStatusRequest(RemoteProcessGroupStatus inputRemoteProcessGroupStatus, String statusTypes, FlowController flowController) {
        RemoteProcessGroupStatusBean remoteProcessGroupStatusBean = new RemoteProcessGroupStatusBean();
        remoteProcessGroupStatusBean.setName(inputRemoteProcessGroupStatus.getName());

        String rootGroupId = flowController.getFlowManager().getRootGroupId();
        String[] statusSplits = statusTypes.split(",");

        List<Bulletin> bulletinList = flowController.getBulletinRepository().findBulletins(
                new BulletinQuery.Builder()
                        .sourceIdMatches(inputRemoteProcessGroupStatus.getId())
                        .build());

        for (String statusType : statusSplits) {
            switch (statusType.toLowerCase().trim()) {
                case "health":
                    RemoteProcessGroupHealth remoteProcessGroupHealth = new RemoteProcessGroupHealth();

                    remoteProcessGroupHealth.setTransmissionStatus(inputRemoteProcessGroupStatus.getTransmissionStatus().name());
                    remoteProcessGroupHealth.setActivePortCount(inputRemoteProcessGroupStatus.getActiveRemotePortCount());
                    remoteProcessGroupHealth.setInactivePortCount(inputRemoteProcessGroupStatus.getInactiveRemotePortCount());
                    remoteProcessGroupHealth.setHasBulletins(!bulletinList.isEmpty());

                    remoteProcessGroupStatusBean.setRemoteProcessGroupHealth(remoteProcessGroupHealth);
                    break;
                case "bulletins":
                    remoteProcessGroupStatusBean.setBulletinList(transformBulletins(bulletinList));
                    break;
                case "inputports":
                    remoteProcessGroupStatusBean.setInputPortStatusList(getPortStatusList(inputRemoteProcessGroupStatus, flowController, rootGroupId, RemoteProcessGroup::getInputPorts));
                    break;
                case "outputports":
                    remoteProcessGroupStatusBean.setOutputPortStatusList(getPortStatusList(inputRemoteProcessGroupStatus, flowController, rootGroupId, RemoteProcessGroup::getOutputPorts));
                    break;
                case "stats":
                    RemoteProcessGroupStats remoteProcessGroupStats = new RemoteProcessGroupStats();

                    remoteProcessGroupStats.setActiveThreads(inputRemoteProcessGroupStatus.getActiveThreadCount());
                    remoteProcessGroupStats.setSentContentSize(inputRemoteProcessGroupStatus.getSentContentSize());
                    remoteProcessGroupStats.setSentCount(inputRemoteProcessGroupStatus.getSentCount());

                    remoteProcessGroupStatusBean.setRemoteProcessGroupStats(remoteProcessGroupStats);
                    break;
            }
        }
        return remoteProcessGroupStatusBean;
    }

    private static List<PortStatus> getPortStatusList(RemoteProcessGroupStatus inputRemoteProcessGroupStatus, FlowController flowController, String rootGroupId,
                                                      Function<RemoteProcessGroup, Set<RemoteGroupPort>> portFunction) {
        return portFunction.apply(flowController.getFlowManager().getGroup(rootGroupId).getRemoteProcessGroup(inputRemoteProcessGroupStatus.getId())).stream().map(r -> {
            PortStatus portStatus = new PortStatus();

            portStatus.setName(r.getName());
            portStatus.setTargetExists(r.getTargetExists());
            portStatus.setTargetRunning(r.isTargetRunning());

            return portStatus;
        }).collect(Collectors.toList());
    }

    static ConnectionStatusBean parseConnectionStatusRequest(ConnectionStatus inputConnectionStatus, String statusTypes, Logger logger) {
        ConnectionStatusBean connectionStatusBean = new ConnectionStatusBean();
        connectionStatusBean.setId(inputConnectionStatus.getId());
        connectionStatusBean.setName(inputConnectionStatus.getName());

        String[] statusSplits = statusTypes.split(",");
        for (String statusType : statusSplits) {
            switch (statusType.toLowerCase().trim()) {
                case "health":
                    ConnectionHealth connectionHealth = new ConnectionHealth();

                    connectionHealth.setQueuedBytes(inputConnectionStatus.getQueuedBytes());
                    connectionHealth.setQueuedCount(inputConnectionStatus.getQueuedCount());

                    connectionStatusBean.setConnectionHealth(connectionHealth);
                    break;
                case "stats":
                    ConnectionStats connectionStats = new ConnectionStats();

                    connectionStats.setInputBytes(inputConnectionStatus.getInputBytes());
                    connectionStats.setInputCount(inputConnectionStatus.getInputCount());
                    connectionStats.setOutputCount(inputConnectionStatus.getOutputCount());
                    connectionStats.setOutputBytes(inputConnectionStatus.getOutputBytes());

                    connectionStatusBean.setConnectionStats(connectionStats);
                    break;
            }
        }
        return connectionStatusBean;
    }

    static ReportingTaskStatus parseReportingTaskStatusRequest(String id, ReportingTaskNode reportingTaskNode, String statusTypes, FlowController flowController, Logger logger) {
        ReportingTaskStatus reportingTaskStatus = new ReportingTaskStatus();
        reportingTaskStatus.setName(id);

        String[] statusSplits = statusTypes.split(",");
        List<Bulletin> bulletinList = flowController.getBulletinRepository().findBulletins(
                new BulletinQuery.Builder()
                        .sourceIdMatches(id)
                        .build());
        for (String statusType : statusSplits) {
            switch (statusType.toLowerCase().trim()) {
                case "health":
                    ReportingTaskHealth reportingTaskHealth = new ReportingTaskHealth();

                    reportingTaskHealth.setScheduledState(reportingTaskNode.getScheduledState().name());
                    reportingTaskHealth.setActiveThreads(reportingTaskNode.getActiveThreadCount());
                    reportingTaskHealth.setHasBulletins(!bulletinList.isEmpty());

                    Collection<ValidationResult> validationResults = reportingTaskNode.getValidationErrors();
                    reportingTaskHealth.setValidationErrorList(transformValidationResults(validationResults));

                    reportingTaskStatus.setReportingTaskHealth(reportingTaskHealth);
                    break;
                case "bulletins":
                    reportingTaskStatus.setBulletinList(transformBulletins(bulletinList));
                    break;
            }
        }
        return reportingTaskStatus;
    }

    static ControllerServiceStatus parseControllerServiceStatusRequest(ControllerServiceNode controllerServiceNode, String statusTypes, FlowController flowController, Logger logger) {
        ControllerServiceStatus controllerServiceStatus = new ControllerServiceStatus();
        String id = controllerServiceNode.getIdentifier();
        controllerServiceStatus.setName(id);

        String[] statusSplits = statusTypes.split(",");
        List<Bulletin> bulletinList = flowController.getBulletinRepository().findBulletins(
                new BulletinQuery.Builder()
                        .sourceIdMatches(id)
                        .build());
        for (String statusType : statusSplits) {
            switch (statusType.toLowerCase().trim()) {
                case "health":
                    ControllerServiceHealth controllerServiceHealth = new ControllerServiceHealth();

                    controllerServiceHealth.setState(controllerServiceNode.getState().name());
                    controllerServiceHealth.setHasBulletins(!bulletinList.isEmpty());

                    Collection<ValidationResult> validationResults = controllerServiceNode.getValidationErrors();
                    controllerServiceHealth.setValidationErrorList(transformValidationResults(validationResults));

                    controllerServiceStatus.setControllerServiceHealth(controllerServiceHealth);
                    break;
                case "bulletins":
                    controllerServiceStatus.setBulletinList(transformBulletins(bulletinList));
                    break;
            }
        }
        return controllerServiceStatus;
    }

    static SystemDiagnosticsStatus parseSystemDiagnosticsRequest(SystemDiagnostics inputSystemDiagnostics, String statusTypes) throws StatusRequestException {
        if (inputSystemDiagnostics == null) {
            throw new StatusRequestException("Unable to get system diagnostics");
        }

        SystemDiagnosticsStatus systemDiagnosticsStatus = new SystemDiagnosticsStatus();
        String[] statusSplits = statusTypes.split(",");

        for (String statusType : statusSplits) {
            switch (statusType.toLowerCase().trim()) {
                case "heap":
                    HeapStatus heapStatus = new HeapStatus();
                    heapStatus.setTotalHeap(inputSystemDiagnostics.getTotalHeap());
                    heapStatus.setMaxHeap(inputSystemDiagnostics.getMaxHeap());
                    heapStatus.setFreeHeap(inputSystemDiagnostics.getFreeHeap());
                    heapStatus.setUsedHeap(inputSystemDiagnostics.getUsedHeap());
                    heapStatus.setHeapUtilization(inputSystemDiagnostics.getHeapUtilization());
                    heapStatus.setTotalNonHeap(inputSystemDiagnostics.getTotalNonHeap());
                    heapStatus.setMaxNonHeap(inputSystemDiagnostics.getMaxNonHeap());
                    heapStatus.setFreeNonHeap(inputSystemDiagnostics.getFreeNonHeap());
                    heapStatus.setUsedNonHeap(inputSystemDiagnostics.getUsedNonHeap());
                    heapStatus.setNonHeapUtilization(inputSystemDiagnostics.getNonHeapUtilization());
                    systemDiagnosticsStatus.setHeapStatus(heapStatus);
                    break;
                case "processorstats":
                    SystemProcessorStats systemProcessorStats = new SystemProcessorStats();
                    systemProcessorStats.setAvailableProcessors(inputSystemDiagnostics.getAvailableProcessors());
                    systemProcessorStats.setLoadAverage(inputSystemDiagnostics.getProcessorLoadAverage());
                    systemDiagnosticsStatus.setProcessorStatus(systemProcessorStats);
                    break;
                case "contentrepositoryusage":
                    List<ContentRepositoryUsage> contentRepositoryUsageList = new LinkedList<>();
                    Map<String, StorageUsage> contentRepoStorage = inputSystemDiagnostics.getContentRepositoryStorageUsage();

                    for (Map.Entry<String, StorageUsage> stringStorageUsageEntry : contentRepoStorage.entrySet()) {
                        ContentRepositoryUsage contentRepositoryUsage = new ContentRepositoryUsage();
                        StorageUsage storageUsage = stringStorageUsageEntry.getValue();

                        contentRepositoryUsage.setName(storageUsage.getIdentifier());
                        contentRepositoryUsage.setFreeSpace(storageUsage.getFreeSpace());
                        contentRepositoryUsage.setTotalSpace(storageUsage.getTotalSpace());
                        contentRepositoryUsage.setDiskUtilization(storageUsage.getDiskUtilization());
                        contentRepositoryUsage.setUsedSpace(storageUsage.getUsedSpace());

                        contentRepositoryUsageList.add(contentRepositoryUsage);
                    }
                    systemDiagnosticsStatus.setContentRepositoryUsageList(contentRepositoryUsageList);
                    break;
                case "flowfilerepositoryusage":
                    FlowfileRepositoryUsage flowfileRepositoryUsage = new FlowfileRepositoryUsage();
                    StorageUsage flowFileRepoStorage = inputSystemDiagnostics.getFlowFileRepositoryStorageUsage();

                    flowfileRepositoryUsage.setFreeSpace(flowFileRepoStorage.getFreeSpace());
                    flowfileRepositoryUsage.setTotalSpace(flowFileRepoStorage.getTotalSpace());
                    flowfileRepositoryUsage.setDiskUtilization(flowFileRepoStorage.getDiskUtilization());
                    flowfileRepositoryUsage.setUsedSpace(flowFileRepoStorage.getUsedSpace());

                    systemDiagnosticsStatus.setFlowfileRepositoryUsage(flowfileRepositoryUsage);
                    break;
                case "garbagecollection":
                    List<GarbageCollectionStatus> garbageCollectionStatusList = new LinkedList<>();
                    Map<String, GarbageCollection> garbageCollectionMap = inputSystemDiagnostics.getGarbageCollection();

                    for (Map.Entry<String, GarbageCollection> stringGarbageCollectionEntry : garbageCollectionMap.entrySet()) {
                        GarbageCollectionStatus garbageCollectionStatus = new GarbageCollectionStatus();
                        GarbageCollection garbageCollection = stringGarbageCollectionEntry.getValue();

                        garbageCollectionStatus.setName(garbageCollection.getName());
                        garbageCollectionStatus.setCollectionCount(garbageCollection.getCollectionCount());
                        garbageCollectionStatus.setCollectionTime(garbageCollection.getCollectionTime());

                        garbageCollectionStatusList.add(garbageCollectionStatus);
                    }
                    systemDiagnosticsStatus.setGarbageCollectionStatusList(garbageCollectionStatusList);
                    break;
            }
        }
        return systemDiagnosticsStatus;
    }

    static InstanceStatus parseInstanceRequest(String statusTypes, FlowController flowController, ProcessGroupStatus rootGroupStatus) {
        InstanceStatus instanceStatus = new InstanceStatus();

        flowController.getFlowManager().getAllControllerServices();
        List<Bulletin> bulletinList = flowController.getBulletinRepository().findBulletinsForController();
        String[] statusSplits = statusTypes.split(",");

        for (String statusType : statusSplits) {
            switch (statusType.toLowerCase().trim()) {
                case "health":
                    InstanceHealth instanceHealth = new InstanceHealth();

                    instanceHealth.setQueuedCount(rootGroupStatus.getQueuedCount());
                    instanceHealth.setQueuedContentSize(rootGroupStatus.getQueuedContentSize());
                    instanceHealth.setHasBulletins(!bulletinList.isEmpty());
                    instanceHealth.setActiveThreads(rootGroupStatus.getActiveThreadCount());

                    instanceStatus.setInstanceHealth(instanceHealth);
                    break;
                case "bulletins":
                    instanceStatus.setBulletinList(transformBulletins(flowController.getBulletinRepository().findBulletinsForController()));
                    break;
                case "stats":
                    InstanceStats instanceStats = new InstanceStats();

                    instanceStats.setBytesRead(rootGroupStatus.getBytesRead());
                    instanceStats.setBytesWritten(rootGroupStatus.getBytesWritten());
                    instanceStats.setBytesSent(rootGroupStatus.getBytesSent());
                    instanceStats.setFlowfilesSent(rootGroupStatus.getFlowFilesSent());
                    instanceStats.setBytesTransferred(rootGroupStatus.getBytesTransferred());
                    instanceStats.setFlowfilesTransferred(rootGroupStatus.getFlowFilesTransferred());
                    instanceStats.setBytesReceived(rootGroupStatus.getBytesReceived());
                    instanceStats.setFlowfilesReceived(rootGroupStatus.getFlowFilesReceived());

                    instanceStatus.setInstanceStats(instanceStats);
                    break;
            }
        }
        return instanceStatus;
    }

    private static List<ValidationError> transformValidationResults(Collection<ValidationResult> validationResults) {
        List<ValidationError> validationErrorList = new LinkedList<>();
        for (ValidationResult validationResult : validationResults) {
            if (!validationResult.isValid()) {
                ValidationError validationError = new ValidationError();
                validationError.setSubject(validationResult.getSubject());
                validationError.setInput(validationResult.getInput());
                validationError.setReason(validationResult.getExplanation());

                validationErrorList.add(validationError);
            }
        }
        return validationErrorList;
    }

    private static List<BulletinStatus> transformBulletins(List<Bulletin> bulletinList) {
        List<BulletinStatus> bulletinStatusList = new LinkedList<>();
        if (!bulletinList.isEmpty()) {
            for (Bulletin bulletin : bulletinList) {
                BulletinStatus bulletinStatus = new BulletinStatus();
                bulletinStatus.setMessage(bulletin.getMessage());
                bulletinStatus.setTimestamp(bulletin.getTimestamp());
                bulletinStatusList.add(bulletinStatus);
            }
        }
        return bulletinStatusList;
    }
}
