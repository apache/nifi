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
package org.apache.nifi.diagnostics.bootstrap;

import org.apache.nifi.controller.FlowController;
import org.apache.nifi.diagnostics.DiagnosticTask;
import org.apache.nifi.diagnostics.DiagnosticsDump;
import org.apache.nifi.diagnostics.DiagnosticsDumpElement;
import org.apache.nifi.diagnostics.DiagnosticsFactory;
import org.apache.nifi.diagnostics.StandardDiagnosticsDump;
import org.apache.nifi.diagnostics.bootstrap.tasks.ClusterDiagnosticTask;
import org.apache.nifi.diagnostics.bootstrap.tasks.ComponentCountTask;
import org.apache.nifi.diagnostics.bootstrap.tasks.ContentRepositoryScanTask;
import org.apache.nifi.diagnostics.bootstrap.tasks.DataValveDiagnosticsTask;
import org.apache.nifi.diagnostics.bootstrap.tasks.DiagnosticAnalysisTask;
import org.apache.nifi.diagnostics.bootstrap.tasks.FlowConfigurationDiagnosticTask;
import org.apache.nifi.diagnostics.bootstrap.tasks.GarbageCollectionDiagnosticTask;
import org.apache.nifi.diagnostics.bootstrap.tasks.JVMDiagnosticTask;
import org.apache.nifi.diagnostics.bootstrap.tasks.LongRunningProcessorTask;
import org.apache.nifi.diagnostics.bootstrap.tasks.MemoryPoolPeakUsageTask;
import org.apache.nifi.diagnostics.bootstrap.tasks.NarsDiagnosticTask;
import org.apache.nifi.diagnostics.bootstrap.tasks.NiFiPropertiesDiagnosticTask;
import org.apache.nifi.diagnostics.bootstrap.tasks.OperatingSystemDiagnosticTask;
import org.apache.nifi.diagnostics.bootstrap.tasks.RepositoryDiagnosticTask;
import org.apache.nifi.diagnostics.ThreadDumpTask;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class BootstrapDiagnosticsFactory implements DiagnosticsFactory {
    private static final Logger logger = LoggerFactory.getLogger(BootstrapDiagnosticsFactory.class);

    private FlowController flowController;
    private NiFiProperties nifiProperties;

    @Override
    public DiagnosticsDump create(final boolean verbose) {
        final List<DiagnosticsDumpElement> dumpElements = new ArrayList<>();
        for (final DiagnosticTask dumpTask : getDiagnosticTasks()) {
            try {
                final DiagnosticsDumpElement dumpElement = dumpTask.captureDump(verbose);
                if (dumpElement != null) {
                    dumpElements.add(dumpElement);
                }
            } catch (final Exception e) {
                logger.error("Failed to obtain diagnostics information from " + dumpTask.getClass(), e);
            }
        }

        return new StandardDiagnosticsDump(dumpElements, System.currentTimeMillis());
    }

    public List<DiagnosticTask> getDiagnosticTasks() {
        final List<DiagnosticTask> tasks = new ArrayList<>();
        tasks.add(new DiagnosticAnalysisTask(flowController));
        tasks.add(new JVMDiagnosticTask());
        tasks.add(new OperatingSystemDiagnosticTask());
        tasks.add(new NarsDiagnosticTask(flowController.getExtensionManager()));
        tasks.add(new FlowConfigurationDiagnosticTask(flowController));
        tasks.add(new LongRunningProcessorTask(flowController));
        tasks.add(new ClusterDiagnosticTask(flowController));
        tasks.add(new GarbageCollectionDiagnosticTask(flowController));
        tasks.add(new MemoryPoolPeakUsageTask());
        tasks.add(new RepositoryDiagnosticTask(flowController));
        tasks.add(new ComponentCountTask(flowController));
        tasks.add(new NiFiPropertiesDiagnosticTask(nifiProperties));
        tasks.add(new ContentRepositoryScanTask(flowController));
        tasks.add(new DataValveDiagnosticsTask(flowController.getFlowManager()));
        tasks.add(new ThreadDumpTask());
        return tasks;
    }

    public void setFlowController(final FlowController flowController) {
        this.flowController = flowController;
    }

    public void setNifiProperties(final NiFiProperties nifiProperties) {
        this.nifiProperties = nifiProperties;
    }
}
