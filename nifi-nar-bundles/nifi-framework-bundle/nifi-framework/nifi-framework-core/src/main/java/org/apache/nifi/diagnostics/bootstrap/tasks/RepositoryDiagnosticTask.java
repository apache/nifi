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
import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.scheduling.RepositoryContextFactory;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.diagnostics.DiagnosticTask;
import org.apache.nifi.diagnostics.DiagnosticsDumpElement;
import org.apache.nifi.diagnostics.StandardDiagnosticsDumpElement;
import org.apache.nifi.provenance.ProvenanceRepository;
import org.apache.nifi.util.FormatUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RepositoryDiagnosticTask implements DiagnosticTask {
    private final FlowController flowController;

    public RepositoryDiagnosticTask(final FlowController flowController) {
        this.flowController = flowController;
    }

    @Override
    public DiagnosticsDumpElement captureDump(final boolean verbose) {
        final List<String> details = new ArrayList<>();

        final RepositoryContextFactory contextFactory = flowController.getRepositoryContextFactory();

        final ProcessGroupStatus rootGroupStatus = flowController.getEventAccess().getGroupStatus(flowController.getFlowManager().getRootGroupId());

        try {
            captureDiagnostics(contextFactory.getFlowFileRepository(), details);
        } catch (final IOException ioe) {
            details.add("Failed to gather details about FlowFile Repository");
        }

        try {
            details.add("");
            captureDiagnostics(contextFactory.getContentRepository(), rootGroupStatus, details);
        } catch (final IOException ioe) {
            details.add("Failed to gather details about Content Repository");
        }

        try {
            details.add("");
            captureDiagnostics(contextFactory.getProvenanceRepository(), details);
        } catch (final IOException ioe) {
            details.add("Failed to gather details about Provenance Repository");
        }

        return new StandardDiagnosticsDumpElement("NiFi Repositories", details);
    }

    private void captureDiagnostics(final FlowFileRepository repository, final List<String> details) throws IOException {
        details.add("FlowFile Repository Implementation: " + repository.getClass().getName());
        details.add("FlowFile Repository File Store: " + repository.getFileStoreName());
        details.add("FlowFile Repository Storage Capacity: " + FormatUtils.formatDataSize(repository.getStorageCapacity()));
        details.add("FlowFile Repository Usable Space: " + FormatUtils.formatDataSize(repository.getUsableStorageSpace()));
    }

    private void captureDiagnostics(final ContentRepository repository, final ProcessGroupStatus status, final List<String> details) throws IOException {
        details.add("Content Repository Implementation: " + repository.getClass().getName());
        for (final String containerName : repository.getContainerNames()) {
            details.add("Content Repository <" + containerName + "> File Store: " + repository.getContainerFileStoreName(containerName));
            details.add("Content Repository <" + containerName + "> Storage Capacity: " + FormatUtils.formatDataSize(repository.getContainerCapacity(containerName)));
            details.add("Content Repository <" + containerName + "> Usable Space: " + FormatUtils.formatDataSize(repository.getContainerUsableSpace(containerName)));
        }

        details.add("Bytes Read (Last 5 mins): " + FormatUtils.formatDataSize(status.getBytesRead()));
        details.add("Bytes Written (Last 5 mins): " + FormatUtils.formatDataSize(status.getBytesWritten()));
    }

    private void captureDiagnostics(final ProvenanceRepository repository, final List<String> details) throws IOException {
        details.add("Provenance Repository Implementation: " + repository.getClass().getName());
        for (final String containerName : repository.getContainerNames()) {
            details.add("Provenance Repository <" + containerName + "> File Store: " + repository.getContainerFileStoreName(containerName));
            details.add("Provenance Repository <" + containerName + "> Storage Capacity: " + FormatUtils.formatDataSize(repository.getContainerCapacity(containerName)));
            details.add("Provenance Repository <" + containerName + "> Usable Space: " + FormatUtils.formatDataSize(repository.getContainerUsableSpace(containerName)));
        }
    }

}
