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

import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedReportingTask;
import org.apache.nifi.flow.VersionedReportingTaskSnapshot;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.registry.flow.mapping.NiFiRegistryFlowMapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class VersionedReportingTaskSnapshotMapper {

    private final NiFiRegistryFlowMapper flowMapper;
    private final ControllerServiceProvider controllerServiceProvider;

    public VersionedReportingTaskSnapshotMapper(final ExtensionManager extensionManager, final ControllerServiceProvider controllerServiceProvider) {
        this.flowMapper = new NiFiRegistryFlowMapper(extensionManager);
        this.controllerServiceProvider = controllerServiceProvider;
    }

    public VersionedReportingTaskSnapshot createMapping(final Set<ReportingTaskNode> reportingTaskNodes, final Set<ControllerServiceNode> controllerServiceNodes) {
        final VersionedReportingTaskSnapshot versionedReportingTaskSnapshot = new VersionedReportingTaskSnapshot();
        versionedReportingTaskSnapshot.setReportingTasks(mapReportingTasks(reportingTaskNodes));
        versionedReportingTaskSnapshot.setControllerServices(mapControllerServices(controllerServiceNodes));
        return versionedReportingTaskSnapshot;
    }

    private List<VersionedReportingTask> mapReportingTasks(final Set<ReportingTaskNode> reportingTaskNodes) {
        final List<VersionedReportingTask> reportingTasks = new ArrayList<>();

        for (final ReportingTaskNode taskNode : reportingTaskNodes) {
            final VersionedReportingTask versionedReportingTask = flowMapper.mapReportingTask(taskNode, controllerServiceProvider);
            reportingTasks.add(versionedReportingTask);
        }

        return reportingTasks;
    }

    private List<VersionedControllerService> mapControllerServices(final Set<ControllerServiceNode> controllerServiceNodes) {
        final List<VersionedControllerService> controllerServices = new ArrayList<>();

        for (final ControllerServiceNode serviceNode : controllerServiceNodes) {
            final VersionedControllerService versionedControllerService = flowMapper.mapControllerService(
                    serviceNode, controllerServiceProvider, Collections.emptySet(), Collections.emptyMap());
            controllerServices.add(versionedControllerService);
        }

        return controllerServices;
    }
}
