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
package org.apache.nifi.diagnostics.bootstrap.tasks;

import org.apache.nifi.controller.FlowController;
import org.apache.nifi.diagnostics.DiagnosticTask;
import org.apache.nifi.diagnostics.DiagnosticsDumpElement;
import org.apache.nifi.diagnostics.StandardDiagnosticsDumpElement;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.ProcessGroupCounts;

import java.util.ArrayList;
import java.util.List;

public class FlowConfigurationDiagnosticTask implements DiagnosticTask {
    private final FlowController flowController;

    public FlowConfigurationDiagnosticTask(final FlowController flowController) {
        this.flowController = flowController;
    }

    @Override
    public DiagnosticsDumpElement captureDump(final boolean verbose) {
        final List<String> details = new ArrayList<>();

        final ProcessGroup rootGroup = flowController.getFlowManager().getRootGroup();
        final FlowController.GroupStatusCounts statusCounts = flowController.getGroupStatusCounts(rootGroup);
        details.add("Active Thread Count: " + statusCounts.getActiveThreadCount());
        details.add("Terminated Thread Count: " + statusCounts.getTerminatedThreadCount());
        details.add("Queued FlowFiles: " + statusCounts.getQueuedCount());
        details.add("Queued Bytes: " + statusCounts.getQueuedContentSize());

        final ProcessGroupCounts counts = rootGroup.getCounts();
        details.add("Running Components: " + counts.getRunningCount());
        details.add("Stopped Components: " + counts.getStoppedCount());
        details.add("Invalid Components: " + counts.getInvalidCount());
        details.add("Disabled Components: " + counts.getDisabledCount());
        details.add("Local Input Ports: " + counts.getLocalInputPortCount());
        details.add("Local Output Ports: " + counts.getLocalOutputPortCount());
        details.add("Site-to-Site Input Ports: " + counts.getPublicInputPortCount());
        details.add("Site-to-Site Input Ports: " + counts.getPublicOutputPortCount());
        details.add("Active RPG Ports: " + counts.getActiveRemotePortCount());
        details.add("Inactive RPG Ports: " + counts.getInactiveRemotePortCount());
        details.add("");
        details.add("Total Process Groups: " + rootGroup.findAllProcessGroups().size());
        details.add("Locally Modified and Stale Count: " + counts.getLocallyModifiedAndStaleCount());
        details.add("Locally Modified Count: " + counts.getLocallyModifiedCount());
        details.add("Stale Count: " + counts.getStaleCount());
        details.add("Sync Failure Count: " + counts.getSyncFailureCount());
        details.add("Up-to-Date Count: " + counts.getUpToDateCount());

        return new StandardDiagnosticsDumpElement("Flow Configuration", details);
    }
}
