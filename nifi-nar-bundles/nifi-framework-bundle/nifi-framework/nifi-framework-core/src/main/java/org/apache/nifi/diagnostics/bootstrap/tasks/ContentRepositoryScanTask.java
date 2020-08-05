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
import org.apache.nifi.controller.repository.FlowFileSwapManager;
import org.apache.nifi.controller.repository.ResourceClaimReference;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.diagnostics.DiagnosticTask;
import org.apache.nifi.diagnostics.DiagnosticsDumpElement;
import org.apache.nifi.diagnostics.StandardDiagnosticsDumpElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
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
                    details.add(String.format("%1$s, Claimant Count = %2$d, In Use = %3$b, Awaiting Destruction = %4$b, References (%5$d) = %6$s",
                        path, claimCount, inUse, destructable,  references.size(), references.toString()));
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

        return new StandardDiagnosticsDumpElement("Content Repository Scan", details);
    }
}
