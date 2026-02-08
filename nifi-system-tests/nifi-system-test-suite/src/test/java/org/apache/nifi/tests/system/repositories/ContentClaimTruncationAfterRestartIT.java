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
package org.apache.nifi.tests.system.repositories;

import org.apache.nifi.tests.system.NiFiInstance;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * System test that verifies the truncation feature works correctly after a NiFi restart.
 * <p>
 * During the first run, NiFi is configured with very conservative truncation settings (99% archive
 * usage threshold), so truncation never activates. FlowFiles are generated but not deleted.
 * </p>
 * <p>
 * NiFi is then stopped, reconfigured with aggressive truncation settings (1% archive usage threshold),
 * and restarted. On recovery, {@code WriteAheadFlowFileRepository.restoreFlowFiles()} re-derives
 * truncation candidates by analyzing the recovered FlowFiles' ContentClaims. After restart, the large
 * FlowFiles are deleted, and the test verifies that the content repository files are truncated on disk.
 * </p>
 */
public class ContentClaimTruncationAfterRestartIT extends NiFiSystemIT {

    @Override
    protected Map<String, String> getNifiPropertiesOverrides() {
        // Phase 1: Conservative settings — truncation should NOT occur
        final Map<String, String> overrides = new HashMap<>();
        overrides.put("nifi.flowfile.repository.checkpoint.interval", "1 sec");
        overrides.put("nifi.content.claim.max.appendable.size", "50 KB");
        // Very high archive threshold means no disk pressure, so truncation never activates
        overrides.put("nifi.content.repository.archive.max.usage.percentage", "99%");
        overrides.put("nifi.content.repository.archive.cleanup.frequency", "1 sec");
        return overrides;
    }

    @Override
    protected boolean isAllowFactoryReuse() {
        return false;
    }

    @Test
    public void testTruncationOccursAfterRestartWithRecoveredCandidates() throws NiFiClientException, IOException, InterruptedException {
        // === Phase 1: Generate FlowFiles with conservative settings (no truncation) ===

        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateTruncatableFlowFiles");
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");

        final Map<String, String> generateProps = Map.of(
            "Batch Count", "10",
            "Small File Size", "1 KB",
            "Large File Size", "10 MB",
            "Small Files Per Batch", "9"
        );
        getClientUtil().updateProcessorProperties(generate, generateProps);

        ConnectionEntity connection = getClientUtil().createConnection(generate, terminate, "success");
        connection = getClientUtil().updateConnectionPrioritizer(connection, "PriorityAttributePrioritizer");
        connection = getClientUtil().updateConnectionBackpressure(connection, 10000, 100L * 1024 * 1024);

        // Generate all 100 FlowFiles (90 small @ 1 KB + 10 large @ 10 MB)
        getClientUtil().startProcessor(generate);
        waitForQueueCount(connection.getId(), 100);
        getClientUtil().stopProcessor(generate);
        getClientUtil().waitForStoppedProcessor(generate.getId());

        // Verify the content repository is large — the 10 MB FlowFiles are on disk
        final File contentRepoDir = new File(getNiFiInstance().getInstanceDirectory(), "content_repository");
        final long thresholdBytes = 1024 * 1024; // 1 MB
        final long sizeBeforeRestart = getContentRepoSize(contentRepoDir);
        assertTrue(sizeBeforeRestart > thresholdBytes,
            "Content repository should be large before restart, but was " + sizeBeforeRestart + " bytes");

        // === Phase 2: Stop NiFi, reconfigure for aggressive truncation, and restart ===

        final NiFiInstance nifiInstance = getNiFiInstance();
        nifiInstance.stop();

        // Switch archive threshold to 1% so truncation activates under disk pressure
        nifiInstance.setProperties(Map.of(
            "nifi.content.repository.archive.max.usage.percentage", "1%"
        ));

        nifiInstance.start(true);

        // After restart, WriteAheadFlowFileRepository.restoreFlowFiles() should have re-derived
        // that the 10 large tail claims are truncation candidates.

        // Run TerminateFlowFile 10 times. Due to PriorityAttributePrioritizer, the 10 large
        // FlowFiles (priority=1) are dequeued first.
        for (int i = 0; i < 10; i++) {
            final ProcessorEntity terminateAfterRestart = getNifiClient().getProcessorClient().getProcessor(terminate.getId());
            getNifiClient().getProcessorClient().runProcessorOnce(terminateAfterRestart);
            getClientUtil().waitForStoppedProcessor(terminateAfterRestart.getId());
        }

        waitForQueueCount(connection.getId(), 90);

        // Wait for the content repository files to be truncated.
        // Before truncation: ~10 files of ~10 MB each = ~100 MB total.
        // After truncation: ~10 files of ~9 KB each = ~90 KB total.
        waitFor(() -> {
            try {
                return getContentRepoSize(contentRepoDir) < thresholdBytes;
            } catch (final Exception e) {
                return false;
            }
        });

        final long finalSize = getContentRepoSize(contentRepoDir);
        assertTrue(finalSize < thresholdBytes,
            "Content repository total size should be below " + thresholdBytes + " bytes after truncation, but was " + finalSize);
    }

    private long getContentRepoSize(final File dir) {
        if (dir == null || !dir.exists()) {
            return 0;
        }

        final File[] children = dir.listFiles();
        if (children == null) {
            return 0L;
        }

        long total = 0;
        for (final File child : children) {
            if (child.isDirectory()) {
                if (child.getName().equals("archive")) {
                    continue; // Skip archive directories
                }

                total += getContentRepoSize(child);
            } else {
                total += child.length();
            }
        }

        return total;
    }
}
