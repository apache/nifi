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
package org.apache.nifi.controller.serialization;

import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.encrypt.PropertyEncryptor;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedReportingTask;
import org.apache.nifi.flow.VersionedReportingTaskSnapshot;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.scheduling.SchedulingStrategy;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.nifi.controller.serialization.FlowSynchronizationUtils.createBundleCoordinate;
import static org.apache.nifi.controller.serialization.FlowSynchronizationUtils.decryptProperties;
import static org.apache.nifi.controller.serialization.FlowSynchronizationUtils.getSensitiveDynamicPropertyNames;

public class StandardVersionedReportingTaskImporter implements VersionedReportingTaskImporter {

    final FlowController flowController;

    public StandardVersionedReportingTaskImporter(final FlowController flowController) {
        this.flowController = flowController;
    }

    @Override
    public VersionedReportingTaskImportResult importSnapshot(final VersionedReportingTaskSnapshot reportingTaskSnapshot) {
        final List<VersionedControllerService> controllerServices = Optional.ofNullable(reportingTaskSnapshot.getControllerServices()).orElse(Collections.emptyList());
        final Set<ControllerServiceNode> controllerServiceNodes = importControllerServices(controllerServices);

        final List<VersionedReportingTask> reportingTasks = Optional.ofNullable(reportingTaskSnapshot.getReportingTasks()).orElse(Collections.emptyList());
        final Set<ReportingTaskNode> reportingTaskNodes = importReportingTasks(reportingTasks);

        return new VersionedReportingTaskImportResult(reportingTaskNodes, controllerServiceNodes);
    }

    private Set<ReportingTaskNode> importReportingTasks(final List<VersionedReportingTask> reportingTasks) {
        final Set<ReportingTaskNode> taskNodes = new HashSet<>();
        for (final VersionedReportingTask reportingTask : reportingTasks) {
            final ReportingTaskNode taskNode = addReportingTask(reportingTask);
            taskNodes.add(taskNode);
        }
        return taskNodes;
    }

    private ReportingTaskNode addReportingTask(final VersionedReportingTask reportingTask) {
        final ExtensionManager extensionManager = flowController.getExtensionManager();
        final BundleCoordinate coordinate = createBundleCoordinate(extensionManager, reportingTask.getBundle(), reportingTask.getType());

        final ReportingTaskNode taskNode = flowController.createReportingTask(reportingTask.getType(), reportingTask.getInstanceIdentifier(), coordinate, false);
        taskNode.setName(reportingTask.getName());
        taskNode.setComments(reportingTask.getComments());
        taskNode.setSchedulingPeriod(reportingTask.getSchedulingPeriod());
        taskNode.setSchedulingStrategy(SchedulingStrategy.valueOf(reportingTask.getSchedulingStrategy()));
        taskNode.setAnnotationData(reportingTask.getAnnotationData());

        final Set<String> sensitiveDynamicPropertyNames = getSensitiveDynamicPropertyNames(taskNode, reportingTask);
        final Map<String, String> decryptedProperties = decryptProperties(reportingTask.getProperties(), flowController.getEncryptor());
        taskNode.setProperties(decryptedProperties, false, sensitiveDynamicPropertyNames);
        return taskNode;
    }

    private Set<ControllerServiceNode> importControllerServices(final List<VersionedControllerService> controllerServices) {
        final Set<ControllerServiceNode> controllerServicesAdded = new HashSet<>();
        for (final VersionedControllerService versionedControllerService : controllerServices) {
            final ControllerServiceNode serviceNode = flowController.getFlowManager().getRootControllerService(versionedControllerService.getInstanceIdentifier());
            if (serviceNode == null) {
                final ControllerServiceNode added = addRootControllerService(versionedControllerService);
                controllerServicesAdded.add(added);
            }
        }

        for (final VersionedControllerService versionedControllerService : controllerServices) {
            final ControllerServiceNode serviceNode = flowController.getFlowManager().getRootControllerService(versionedControllerService.getInstanceIdentifier());
            if (controllerServicesAdded.contains(serviceNode)) {
                updateRootControllerService(serviceNode, versionedControllerService, flowController.getEncryptor());
            }
        }

        return controllerServicesAdded;
    }

    private ControllerServiceNode addRootControllerService(final VersionedControllerService controllerService) {
        final FlowManager flowManager = flowController.getFlowManager();
        final ExtensionManager extensionManager = flowController.getExtensionManager();

        final BundleCoordinate bundleCoordinate = createBundleCoordinate(extensionManager, controllerService.getBundle(), controllerService.getType());
        final ControllerServiceNode serviceNode = flowManager.createControllerService(controllerService.getType(), controllerService.getInstanceIdentifier(),
                bundleCoordinate, Collections.emptySet(), true, true, null);
        serviceNode.setVersionedComponentId(controllerService.getIdentifier());
        flowManager.addRootControllerService(serviceNode);

        return serviceNode;
    }

    private void updateRootControllerService(final ControllerServiceNode serviceNode, final VersionedControllerService controllerService,
                                             final PropertyEncryptor encryptor) {
        serviceNode.pauseValidationTrigger();
        try {
            serviceNode.setName(controllerService.getName());
            serviceNode.setAnnotationData(controllerService.getAnnotationData());
            serviceNode.setComments(controllerService.getComments());

            if (controllerService.getBulletinLevel() != null) {
                serviceNode.setBulletinLevel(LogLevel.valueOf(controllerService.getBulletinLevel()));
            } else {
                // this situation exists for backward compatibility with nifi 1.16 and earlier where controller services do not have bulletinLevels set in flow.xml/flow.json
                // and bulletinLevels are at the WARN level by default
                serviceNode.setBulletinLevel(LogLevel.WARN);
            }

            final Set<String> sensitiveDynamicPropertyNames = getSensitiveDynamicPropertyNames(serviceNode, controllerService);
            final Map<String, String> decryptedProperties = decryptProperties(controllerService.getProperties(), encryptor);
            serviceNode.setProperties(decryptedProperties, false, sensitiveDynamicPropertyNames);
        } finally {
            serviceNode.resumeValidationTrigger();
        }
    }

}
