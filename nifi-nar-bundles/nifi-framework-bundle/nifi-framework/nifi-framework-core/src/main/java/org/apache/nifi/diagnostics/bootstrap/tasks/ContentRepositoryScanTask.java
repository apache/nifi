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

import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.FlowFileSwapManager;
import org.apache.nifi.controller.repository.ResourceClaimReference;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.diagnostics.DiagnosticTask;
import org.apache.nifi.diagnostics.DiagnosticsDumpElement;
import org.apache.nifi.diagnostics.StandardDiagnosticsDumpElement;
import org.apache.nifi.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ContentRepositoryScanTask implements DiagnosticTask {
    private static final Logger logger = LoggerFactory.getLogger(ContentRepositoryScanTask.class);

    private final FlowController flowController;

    public ContentRepositoryScanTask(final FlowController flowController) {
        this.flowController = flowController;
    }

    @Override
    public DiagnosticsDumpElement captureDump(final boolean verbose) {
        if (!verbose) {
            // This task is very expensive, as it must scan the contents of the Content Repository. As such, it will not
            // run at all if verbose output is disabled.
            return null;
        }

        final ContentRepository contentRepository = flowController.getRepositoryContextFactory().getContentRepository();
        if (!contentRepository.isActiveResourceClaimsSupported()) {
            return new StandardDiagnosticsDumpElement("Content Repository Scan", Collections.singletonList("Current Content Repository does not support scanning for in-use content"));
        }

        final FlowFileRepository flowFileRepository = flowController.getRepositoryContextFactory().getFlowFileRepository();
        final ResourceClaimManager resourceClaimManager = flowController.getResourceClaimManager();
        final FlowFileSwapManager swapManager = flowController.createSwapManager();

        final List<String> details = new ArrayList<>();

        final Map<String, RetainedFileSet> retainedFileSetsByQueue = new HashMap<>();
        final FlowManager flowManager = flowController.getFlowManager();

        final NumberFormat numberFormat = NumberFormat.getNumberInstance();
        for (final String containerName : contentRepository.getContainerNames()) {
            try {
                final Set<ResourceClaim> resourceClaims = contentRepository.getActiveResourceClaims(containerName);

                final Map<ResourceClaim, Set<ResourceClaimReference>> referenceMap = flowFileRepository.findResourceClaimReferences(resourceClaims, swapManager);

                for (final ResourceClaim resourceClaim : resourceClaims) {
                    final int claimCount = resourceClaimManager.getClaimantCount(resourceClaim);
                    final boolean inUse = resourceClaim.isInUse();
                    final boolean destructable = resourceClaimManager.isDestructable(resourceClaim);

                    final Set<ResourceClaimReference> references = referenceMap == null ? Collections.emptySet() : referenceMap.getOrDefault(resourceClaim, Collections.emptySet());

                    final String path = resourceClaim.getContainer() + "/" + resourceClaim.getSection() + "/" + resourceClaim.getId();
                    final long fileSize = contentRepository.size(resourceClaim);
                    details.add(String.format("%1$s; Size = %2$s bytes; Claimant Count = %3$d; In Use = %4$b; Awaiting Destruction = %5$b; References (%6$d) = %7$s",
                        path, numberFormat.format(fileSize), claimCount, inUse, destructable,  references.size(), references));

                    for (final ResourceClaimReference claimReference : references) {
                        final String queueId = claimReference.getQueueIdentifier();
                        final Connection connection = flowManager.getConnection(queueId);
                        QueueSize queueSize = new QueueSize(0, 0L);
                        if (connection != null) {
                            queueSize = connection.getFlowFileQueue().size();
                        }

                        final RetainedFileSet retainedFileSet = retainedFileSetsByQueue.computeIfAbsent(queueId, RetainedFileSet::new);
                        retainedFileSet.addFile(path, fileSize);
                        retainedFileSet.setQueueSize(queueSize);
                    }
                }
            } catch (final Exception e) {
                logger.error("Failed to obtain listing of Active Resource Claims for container {}", containerName, e);
                details.add("Failed to obtain listing of Active Resource Claims in container " + containerName);
            }
        }

        details.add(""); // Insert empty detail lines to make output more readable.

        final Set<ResourceClaim> orphanedResourceClaims = flowFileRepository.findOrphanedResourceClaims();
        if (orphanedResourceClaims == null || orphanedResourceClaims.isEmpty()) {
            details.add("No Resource Claims were referenced by orphaned FlowFiles.");
        } else {
            details.add("The following Resource Claims were referenced by orphaned FlowFiles (FlowFiles that exist in the FlowFile Repository but for which the FlowFile's connection/queue" +
                " did not exist when NiFi started):");

            for (final ResourceClaim claim : orphanedResourceClaims) {
                details.add(claim.toString());
            }
        }

        details.add("");

        final List<RetainedFileSet> retainedFileSets = new ArrayList<>(retainedFileSetsByQueue.values());
        retainedFileSets.sort(Comparator.comparing(RetainedFileSet::getByteCount).reversed());
        details.add("The following queues retain data in the Content Repository:");
        if (retainedFileSets.isEmpty()) {
            details.add("No queues retain any files in the Content Repository");
        } else {
            for (final RetainedFileSet retainedFileSet : retainedFileSets) {
                final String formatted = String.format("Queue ID = %s; Queue Size = %s FlowFiles / %s; Retained Files = %d; Retained Size = %s bytes (%s)",
                    retainedFileSet.getQueueId(), numberFormat.format(retainedFileSet.getQueueSize().getObjectCount()), FormatUtils.formatDataSize(retainedFileSet.getQueueSize().getByteCount()),
                    retainedFileSet.getFilenames().size(), numberFormat.format(retainedFileSet.getByteCount()), FormatUtils.formatDataSize(retainedFileSet.getByteCount()));

                details.add(formatted);
            }
        }

        return new StandardDiagnosticsDumpElement("Content Repository Scan", details);
    }

    private static class RetainedFileSet {
        private final String queueId;
        private final Set<String> filenames = new HashSet<>();
        private long byteCount;
        private QueueSize queueSize;

        public RetainedFileSet(final String queueId) {
            this.queueId = queueId;
        }

        public String getQueueId() {
            return queueId;
        }

        public void addFile(final String filename, final long bytes) {
            if (filenames.add(filename)) {
                byteCount += bytes;
            }
        }

        public Set<String> getFilenames() {
            return filenames;
        }

        public long getByteCount() {
            return byteCount;
        }

        public QueueSize getQueueSize() {
            return queueSize;
        }

        public void setQueueSize(final QueueSize queueSize) {
            this.queueSize = queueSize;
        }
    }
}
