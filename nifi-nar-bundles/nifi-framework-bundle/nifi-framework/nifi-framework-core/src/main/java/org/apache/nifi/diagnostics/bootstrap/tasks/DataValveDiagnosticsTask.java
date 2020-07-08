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

import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.diagnostics.DiagnosticTask;
import org.apache.nifi.diagnostics.DiagnosticsDumpElement;
import org.apache.nifi.diagnostics.StandardDiagnosticsDumpElement;
import org.apache.nifi.groups.DataValveDiagnostics;
import org.apache.nifi.groups.ProcessGroup;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DataValveDiagnosticsTask implements DiagnosticTask {
    private final FlowManager flowManager;

    public DataValveDiagnosticsTask(final FlowManager flowManager) {
        this.flowManager = flowManager;
    }

    @Override
    public DiagnosticsDumpElement captureDump(final boolean verbose) {
        final ProcessGroup rootGroup = flowManager.getRootGroup();
        final List<ProcessGroup> allGroups = rootGroup.findAllProcessGroups();
        allGroups.add(rootGroup);

        final List<String> details = new ArrayList<>();
        for (final ProcessGroup group : allGroups) {
            final DataValveDiagnostics valveDiagnostics = group.getDataValve().getDiagnostics();

            details.add("Process Group " + group.getIdentifier() + ", Name = " + group.getName());
            details.add("Currently Have Data Flowing In: " + valveDiagnostics.getGroupsWithDataFlowingIn());
            details.add("Currently Have Data Flowing out: " + valveDiagnostics.getGroupsWithDataFlowingOut());
            details.add("Reason for Not allowing data to flow in:");

            for (final Map.Entry<String, List<ProcessGroup>> entry : valveDiagnostics.getReasonForInputNotAllowed().entrySet()) {
                details.add("    " + entry.getKey() + ":");
                entry.getValue().forEach(gr -> details.add("        " + gr));
            }

            details.add("Reason for Not allowing data to flow out:");
            for (final Map.Entry<String, List<ProcessGroup>> entry : valveDiagnostics.getReasonForOutputNotAllowed().entrySet()) {
                details.add("    " + entry.getKey() + ":");
                entry.getValue().forEach(gr -> details.add("        " + gr));
            }

            details.add("");
        }

        return new StandardDiagnosticsDumpElement("DataValve Diagnostics", details);
    }
}
