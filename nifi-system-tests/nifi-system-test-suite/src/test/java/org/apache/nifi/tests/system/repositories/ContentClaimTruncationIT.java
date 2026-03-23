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

import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * System test that verifies the end-to-end truncation feature. It generates FlowFiles with a pattern
 * of 9 small (1 KB) + 1 large (10 MB) per batch, removes only the large FlowFiles via priority-based
 * ordering, and then verifies that the content repository files are truncated on disk.
 */
public class ContentClaimTruncationIT extends NiFiSystemIT {

    @Override
    protected Map<String, String> getNifiPropertiesOverrides() {
        final Map<String, String> overrides = new HashMap<>();
        // Use a short checkpoint interval so truncatable claims are flushed to the ResourceClaimManager promptly
        overrides.put("nifi.flowfile.repository.checkpoint.interval", "1 sec");
        overrides.put("nifi.content.repository.archive.cleanup.frequency", "1 sec");
        // Explicitly set the max appendable claim size (same as system test default, but explicit for clarity)
        overrides.put("nifi.content.claim.max.appendable.size", "50 KB");
        // Set archive threshold extremely low so that truncation occurs quickly
        overrides.put("nifi.content.repository.archive.max.usage.percentage", "1%");
        return overrides;
    }

    @Override
    protected boolean isAllowFactoryReuse() {
        // Don't reuse the NiFi instance since we override checkpoint interval
        return false;
    }

    @Test
    public void testLargeFlowFileTruncation() throws NiFiClientException, IOException, InterruptedException {
        // Create the processors
        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateTruncatableFlowFiles");
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");

        // Configure GenerateTruncatableFlowFiles with 10 batches (100 FlowFiles total)
        final Map<String, String> generateProps = Map.of(
            "Batch Count", "10",
            "Small File Size", "1 KB",
            "Large File Size", "10 MB",
            "Small Files Per Batch", "9"
        );
        getClientUtil().updateProcessorProperties(generate, generateProps);

        // Create connection with PriorityAttributePrioritizer and 100 MB backpressure
        ConnectionEntity connection = getClientUtil().createConnection(generate, terminate, "success");
        connection = getClientUtil().updateConnectionPrioritizer(connection, "PriorityAttributePrioritizer");
        connection = getClientUtil().updateConnectionBackpressure(connection, 10000, 100L * 1024 * 1024);

        // Start the generator and wait for 100 FlowFiles to be queued
        getClientUtil().startProcessor(generate);
        waitForQueueCount(connection.getId(), 100);

        // Stop the generator
        getClientUtil().stopProcessor(generate);
        getClientUtil().waitForStoppedProcessor(generate.getId());

        // Run TerminateFlowFile 10 times. Due to PriorityAttributePrioritizer,
        // the 10 large FlowFiles (priority=1) will be dequeued first.
        for (int i = 0; i < 10; i++) {
            getNifiClient().getProcessorClient().runProcessorOnce(terminate);
            getClientUtil().waitForStoppedProcessor(terminate.getId());
        }

        // Wait for 90 FlowFiles remaining (the 10 large ones have been removed)
        waitForQueueCount(connection.getId(), 90);

        // Wait for the content repository files to be truncated.
        // Before truncation: ~10 files of ~10 MB each = ~100 MB total.
        // After truncation: ~10 files of ~9 KB each = ~90 KB total.
        // We set a generous threshold of 1 MB.
        final File contentRepoDir = new File(getNiFiInstance().getInstanceDirectory(), "content_repository");
        final long thresholdBytes = 1024 * 1024; // 1 MB

        waitFor(() -> {
            try {
                final long totalSize = getContentRepoSize(contentRepoDir.toPath());
                return totalSize < thresholdBytes;
            } catch (final IOException e) {
                return false;
            }
        });

        // Final assertion
        final long finalSize = getContentRepoSize(contentRepoDir.toPath());
        assertTrue(finalSize < thresholdBytes,
                "Content repository total size should be below " + thresholdBytes + " bytes after truncation, but was " + finalSize);
    }

    /**
     * Walks the content repository directory (excluding any "archive" subdirectories)
     * and returns the total size of all regular files.
     */
    private long getContentRepoSize(final Path contentRepoPath) throws IOException {
        if (!Files.exists(contentRepoPath)) {
            return 0;
        }

        final AtomicLong totalSize = new AtomicLong(0);
        Files.walkFileTree(contentRepoPath, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) {
                // Skip archive directories
                if (dir.getFileName() != null && "archive".equals(dir.getFileName().toString())) {
                    return FileVisitResult.SKIP_SUBTREE;
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) {
                totalSize.addAndGet(attrs.size());
                return FileVisitResult.CONTINUE;
            }
        });

        return totalSize.get();
    }
}
