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

import java.util.Collections;
import java.util.Set;

public class VersionedReportingTaskImportResult {

    private final Set<ReportingTaskNode> reportingTaskNodes;
    private final Set<ControllerServiceNode> controllerServiceNodes;

    public VersionedReportingTaskImportResult(final Set<ReportingTaskNode> reportingTaskNodes,
                                              final Set<ControllerServiceNode> controllerServiceNodes) {
        this.reportingTaskNodes = Collections.unmodifiableSet(reportingTaskNodes == null ? Collections.emptySet() : reportingTaskNodes);
        this.controllerServiceNodes = Collections.unmodifiableSet(controllerServiceNodes == null ? Collections.emptySet() : controllerServiceNodes);
    }

    public Set<ReportingTaskNode> getReportingTaskNodes() {
        return reportingTaskNodes;
    }

    public Set<ControllerServiceNode> getControllerServiceNodes() {
        return controllerServiceNodes;
    }
}

