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

package org.apache.nifi.minifi.commons.status.util;

import org.apache.nifi.minifi.commons.status.FlowStatusReport;
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

import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class StatusReportPopulator {
    private StatusReportPopulator() {
    }

    public static void addProcessorStatus(FlowStatusReport flowStatusReport, boolean addHealth, boolean validationErrors, boolean addStats, boolean addBulletins, boolean populateBulletins) {
        ProcessorStatusBean expectedProcessorStatus = new ProcessorStatusBean();
        expectedProcessorStatus.setId("UpdateAttributeProcessorId");
        expectedProcessorStatus.setName("UpdateAttributeProcessorName");

        if (addHealth) {
            ProcessorHealth processorHealth = new ProcessorHealth();
            processorHealth.setHasBulletins(populateBulletins);
            processorHealth.setRunStatus("Stopped");
            if (validationErrors) {
                List<ValidationError> validationErrorList = new LinkedList<>();
                ValidationError validationError1 = new ValidationError();
                validationError1.setInput("input");
                validationError1.setSubject("subject");
                validationError1.setReason("is not valid");
                validationErrorList.add(validationError1);

                ValidationError validationError2 = new ValidationError();
                validationError2.setInput("input2");
                validationError2.setSubject("subject2");
                validationError2.setReason("is not valid too");
                validationErrorList.add(validationError2);

                processorHealth.setValidationErrorList(validationErrorList);
            } else {
                processorHealth.setValidationErrorList(Collections.EMPTY_LIST);
            }
            expectedProcessorStatus.setProcessorHealth(processorHealth);
        }

        if (addStats) {
            ProcessorStats expectedProcessorStats = new ProcessorStats();

            expectedProcessorStats.setActiveThreads(1);
            expectedProcessorStats.setFlowfilesReceived(2);
            expectedProcessorStats.setBytesRead(3);
            expectedProcessorStats.setBytesWritten(4);
            expectedProcessorStats.setFlowfilesSent(5);
            expectedProcessorStats.setInvocations(6);
            expectedProcessorStats.setProcessingNanos(7);

            expectedProcessorStatus.setProcessorStats(expectedProcessorStats);
        }

        if (addBulletins) {
            if (populateBulletins) {
                BulletinStatus bulletinStatus = new BulletinStatus();
                bulletinStatus.setMessage("Bulletin message");
                bulletinStatus.setTimestamp(new Date(1464019245000L));

                expectedProcessorStatus.setBulletinList(Collections.singletonList(bulletinStatus));
            } else {
                expectedProcessorStatus.setBulletinList(Collections.EMPTY_LIST);
            }
        }
        flowStatusReport.setProcessorStatusList(Collections.singletonList(expectedProcessorStatus));
    }

    public static void addConnectionStatus(FlowStatusReport flowStatusReport, boolean addHealth, boolean addStats) {

        ConnectionStatusBean expectedConnectionStatus = new ConnectionStatusBean();

        expectedConnectionStatus.setId("connectionId");
        expectedConnectionStatus.setName("connectionName");
        if (addHealth) {
            ConnectionHealth connectionHealth = new ConnectionHealth();
            connectionHealth.setQueuedCount(10);
            connectionHealth.setQueuedBytes(100);
            expectedConnectionStatus.setConnectionHealth(connectionHealth);
        }

        if (addStats) {
            ConnectionStats expectedConnectionStats = new ConnectionStats();
            expectedConnectionStats.setInputCount(1);
            expectedConnectionStats.setInputBytes(2);
            expectedConnectionStats.setOutputCount(3);
            expectedConnectionStats.setOutputBytes(4);
            expectedConnectionStatus.setConnectionStats(expectedConnectionStats);
        }

        flowStatusReport.setConnectionStatusList(Collections.singletonList(expectedConnectionStatus));
    }

    public static void addExpectedRemoteProcessGroupStatus(FlowStatusReport flowStatusReport, boolean addHealth, boolean addInputPort, boolean addOutputPort,
                                                           boolean addStats, boolean addBulletins, boolean populateBulletins) {
        RemoteProcessGroupStatusBean expectedRemoteProcessGroupStatus = new RemoteProcessGroupStatusBean();
        expectedRemoteProcessGroupStatus.setName("rpg1");

        if (addHealth) {
            RemoteProcessGroupHealth remoteProcessGroupHealth = new RemoteProcessGroupHealth();
            remoteProcessGroupHealth.setTransmissionStatus("Transmitting");
            remoteProcessGroupHealth.setHasBulletins(populateBulletins);
            remoteProcessGroupHealth.setActivePortCount(1);
            remoteProcessGroupHealth.setInactivePortCount(2);
            expectedRemoteProcessGroupStatus.setRemoteProcessGroupHealth(remoteProcessGroupHealth);
        }

        if (addBulletins) {
            if (populateBulletins) {
                BulletinStatus bulletinStatus = new BulletinStatus();
                bulletinStatus.setMessage("Bulletin message");
                bulletinStatus.setTimestamp(new Date(1464019245000L));
                expectedRemoteProcessGroupStatus.setBulletinList(Collections.singletonList(bulletinStatus));
            } else {
                expectedRemoteProcessGroupStatus.setBulletinList(Collections.EMPTY_LIST);
            }
        }

        if (addInputPort) {
            PortStatus expectedInputPortStatus = new PortStatus();
            expectedInputPortStatus.setName("inputPort");
            expectedInputPortStatus.setTargetExists(true);
            expectedInputPortStatus.setTargetRunning(false);
            expectedRemoteProcessGroupStatus.setInputPortStatusList(Collections.singletonList(expectedInputPortStatus));
        }

        if (addOutputPort) {
            PortStatus expectedOutputPortStatus = new PortStatus();
            expectedOutputPortStatus.setName("outputPort");
            expectedOutputPortStatus.setTargetExists(true);
            expectedOutputPortStatus.setTargetRunning(false);
            expectedRemoteProcessGroupStatus.setOutputPortStatusList(Collections.singletonList(expectedOutputPortStatus));
        }

        if (addStats) {
            RemoteProcessGroupStats remoteProcessGroupStats = new RemoteProcessGroupStats();
            remoteProcessGroupStats.setActiveThreads(3);
            remoteProcessGroupStats.setSentContentSize(4L);
            remoteProcessGroupStats.setSentCount(5);
            expectedRemoteProcessGroupStatus.setRemoteProcessGroupStats(remoteProcessGroupStats);
        }

        flowStatusReport.setRemoteProcessGroupStatusList(Collections.singletonList(expectedRemoteProcessGroupStatus));
    }

    public static void addControllerServiceStatus(FlowStatusReport flowStatusReport, boolean addHealth, boolean addValidationErrors, boolean addBulletins, boolean populateBulletins) {
        ControllerServiceStatus controllerServiceStatus = new ControllerServiceStatus();
        controllerServiceStatus.setName("mockControllerService");

        if (addBulletins) {
            if (populateBulletins) {
                BulletinStatus bulletinStatus = new BulletinStatus();
                bulletinStatus.setMessage("Bulletin message");
                bulletinStatus.setTimestamp(new Date(1464019245000L));
                controllerServiceStatus.setBulletinList(Collections.singletonList(bulletinStatus));
            } else {
                controllerServiceStatus.setBulletinList(Collections.EMPTY_LIST);
            }
        }

        if (addHealth) {
            ControllerServiceHealth controllerServiceHealth = new ControllerServiceHealth();
            controllerServiceHealth.setState("ENABLED");
            controllerServiceHealth.setHasBulletins(populateBulletins);
            controllerServiceStatus.setControllerServiceHealth(controllerServiceHealth);

            if (addValidationErrors) {
                List<ValidationError> validationErrorList = new LinkedList<>();
                ValidationError validationError1 = new ValidationError();
                validationError1.setInput("input");
                validationError1.setSubject("subject");
                validationError1.setReason("is not valid");
                validationErrorList.add(validationError1);

                ValidationError validationError2 = new ValidationError();
                validationError2.setInput("input2");
                validationError2.setSubject("subject2");
                validationError2.setReason("is not valid too");
                validationErrorList.add(validationError2);

                controllerServiceHealth.setValidationErrorList(validationErrorList);
            } else {
                controllerServiceHealth.setValidationErrorList(Collections.EMPTY_LIST);
            }
        }

        flowStatusReport.setControllerServiceStatusList(Collections.singletonList(controllerServiceStatus));
    }

    public static void addInstanceStatus(FlowStatusReport flowStatusReport, boolean addHealth, boolean addStats, boolean addBulletins, boolean populateBulletins) {
        InstanceStatus instanceStatus = new InstanceStatus();

        if (addHealth) {
            InstanceHealth instanceHealth = new InstanceHealth();
            instanceHealth.setQueuedContentSize(1L);
            instanceHealth.setQueuedCount(2);
            instanceHealth.setActiveThreads(3);
            instanceHealth.setHasBulletins(populateBulletins);
            instanceStatus.setInstanceHealth(instanceHealth);
        }

        if (addBulletins) {
            if (populateBulletins) {
                BulletinStatus bulletinStatus = new BulletinStatus();
                bulletinStatus.setMessage("Bulletin message");
                bulletinStatus.setTimestamp(new Date(1464019245000L));
                instanceStatus.setBulletinList(Collections.singletonList(bulletinStatus));
            } else {
                instanceStatus.setBulletinList(Collections.EMPTY_LIST);
            }
        }

        if (addStats) {
            InstanceStats instanceStats = new InstanceStats();
            instanceStats.setBytesRead(1L);
            instanceStats.setBytesWritten(2L);
            instanceStats.setBytesSent(3L);
            instanceStats.setFlowfilesSent(4);
            instanceStats.setBytesTransferred(5L);
            instanceStats.setFlowfilesTransferred(6);
            instanceStats.setBytesReceived(7L);
            instanceStats.setFlowfilesReceived(8);
            instanceStatus.setInstanceStats(instanceStats);
        }

        flowStatusReport.setInstanceStatus(instanceStatus);
    }

    public static void addSystemDiagnosticStatus(FlowStatusReport flowStatusReport, boolean addHeap, boolean addProcessorStats, boolean addFlowFileRepoUsage, boolean addContentRepoUsage,
                                                 boolean addGarbageCollectionStatus) {

        SystemDiagnosticsStatus expectedSystemDiagnosticStatus = new SystemDiagnosticsStatus();

        if (addHeap) {
            HeapStatus heapStatus = new HeapStatus();
            heapStatus.setMaxHeap(5);
            heapStatus.setTotalHeap(3);
            heapStatus.setUsedHeap(2);
            heapStatus.setFreeHeap(1);
            heapStatus.setHeapUtilization(40);
            heapStatus.setMaxNonHeap(9);
            heapStatus.setTotalNonHeap(8);
            heapStatus.setUsedNonHeap(6);
            heapStatus.setFreeNonHeap(2);
            heapStatus.setNonHeapUtilization(67);
            expectedSystemDiagnosticStatus.setHeapStatus(heapStatus);
        }

        if (addProcessorStats) {
            SystemProcessorStats systemProcessorStats = new SystemProcessorStats();
            systemProcessorStats.setLoadAverage(80.9);
            systemProcessorStats.setAvailableProcessors(5);
            expectedSystemDiagnosticStatus.setProcessorStatus(systemProcessorStats);
        }

        if (addFlowFileRepoUsage) {
            FlowfileRepositoryUsage flowfileRepositoryUsage = new FlowfileRepositoryUsage();
            flowfileRepositoryUsage.setFreeSpace(30);
            flowfileRepositoryUsage.setTotalSpace(100);
            flowfileRepositoryUsage.setUsedSpace(70);
            flowfileRepositoryUsage.setDiskUtilization(70);
            expectedSystemDiagnosticStatus.setFlowfileRepositoryUsage(flowfileRepositoryUsage);
        }

        if (addContentRepoUsage) {
            List<ContentRepositoryUsage> contentRepositoryUsageList = new LinkedList<>();
            ContentRepositoryUsage contentRepositoryUsage = new ContentRepositoryUsage();
            contentRepositoryUsage.setFreeSpace(30);
            contentRepositoryUsage.setTotalSpace(100);
            contentRepositoryUsage.setName("Content repo1");
            contentRepositoryUsage.setUsedSpace(70);
            contentRepositoryUsage.setDiskUtilization(70);
            contentRepositoryUsageList.add(contentRepositoryUsage);
            expectedSystemDiagnosticStatus.setContentRepositoryUsageList(contentRepositoryUsageList);
        }

        if (addGarbageCollectionStatus) {
            List<GarbageCollectionStatus> garbageCollectionStatusList = new LinkedList<>();
            GarbageCollectionStatus garbageCollectionStatus1 = new GarbageCollectionStatus();
            garbageCollectionStatus1.setCollectionCount(1);
            garbageCollectionStatus1.setCollectionTime(10);
            garbageCollectionStatus1.setName("garbage 1");
            garbageCollectionStatusList.add(garbageCollectionStatus1);
            expectedSystemDiagnosticStatus.setGarbageCollectionStatusList(garbageCollectionStatusList);
        }

        flowStatusReport.setSystemDiagnosticsStatus(expectedSystemDiagnosticStatus);
    }

    public static void addReportingTaskStatus(FlowStatusReport flowStatusReport, boolean addHealth, boolean addValidationErrors, boolean addBulletins, boolean populateBulletins) {
        ReportingTaskStatus reportingTaskStatus = new ReportingTaskStatus();

        reportingTaskStatus.setName("ReportProvenance");

        if (addHealth) {
            ReportingTaskHealth reportingTaskHealth = new ReportingTaskHealth();

            reportingTaskHealth.setActiveThreads(1);
            reportingTaskHealth.setScheduledState("RUNNING");
            reportingTaskHealth.setHasBulletins(populateBulletins);

            if (addValidationErrors) {
                List<ValidationError> validationErrorList = new LinkedList<>();
                ValidationError validationError1 = new ValidationError();
                validationError1.setInput("input");
                validationError1.setSubject("subject");
                validationError1.setReason("is not valid");
                validationErrorList.add(validationError1);

                ValidationError validationError2 = new ValidationError();
                validationError2.setInput("input2");
                validationError2.setSubject("subject2");
                validationError2.setReason("is not valid too");
                validationErrorList.add(validationError2);

                reportingTaskHealth.setValidationErrorList(validationErrorList);
            } else {
                reportingTaskHealth.setValidationErrorList(Collections.EMPTY_LIST);
            }
            reportingTaskStatus.setReportingTaskHealth(reportingTaskHealth);
        }

        if (addBulletins) {
            if (populateBulletins) {
                BulletinStatus bulletinStatus = new BulletinStatus();
                bulletinStatus.setMessage("Bulletin message");
                bulletinStatus.setTimestamp(new Date(1464019245000L));
                reportingTaskStatus.setBulletinList(Collections.singletonList(bulletinStatus));
            } else {
                reportingTaskStatus.setBulletinList(Collections.EMPTY_LIST);
            }
        }

        flowStatusReport.setReportingTaskStatusList(Collections.singletonList(reportingTaskStatus));
    }
}
