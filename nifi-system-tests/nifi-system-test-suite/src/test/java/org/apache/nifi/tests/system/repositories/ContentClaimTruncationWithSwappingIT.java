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
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * System test that verifies content claim truncation does not prematurely truncate data when FlowFiles
 * are swapped out to disk. SwappablePriorityQueue requires at least 10,000 FlowFiles in the swap queue
 * before writing a swap file (SWAP_RECORD_POLL_SIZE = 10,000). Combined with a swap threshold of 20,
 * we generate 10,020 FlowFiles per connection so that 10,000 are written to a swap file on disk.
 * Each test asserts that swap files actually exist on disk before proceeding, confirming that real
 * disk-based swapping is exercised.
 */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
public class ContentClaimTruncationWithSwappingIT extends NiFiSystemIT {

    private static final long ONE_MEGABYTE_IN_BYTES = 1024L * 1024L;
    private static final long NOT_TRUNCATED_MIN_BYTES = 10L * ONE_MEGABYTE_IN_BYTES;
    private static final long BACKPRESSURE_BYTES = 200L * ONE_MEGABYTE_IN_BYTES;

    private static final int BATCH_COUNT = 10;
    private static final int SMALL_FILES_PER_BATCH = 1001;
    private static final int SMALL_FILE_SIZE_BYTES = 1;
    private static final int TOTAL_FLOWFILES_PER_CONNECTION = BATCH_COUNT * (SMALL_FILES_PER_BATCH + 1);

    private static final long EXPECTED_CONTENT_SIZE_AFTER_TRUNCATION = (long) BATCH_COUNT * SMALL_FILES_PER_BATCH * SMALL_FILE_SIZE_BYTES;

    private static final Map<String, String> GENERATE_TRUNCATABLE_PROPS = Map.of(
        "Batch Count", String.valueOf(BATCH_COUNT),
        "Small File Size", "1 B",
        "Large File Size", "10 MB",
        "Small Files Per Batch", String.valueOf(SMALL_FILES_PER_BATCH));

    @Override
    protected Map<String, String> getNifiPropertiesOverrides() {
        return Map.of(
            "nifi.flowfile.repository.checkpoint.interval", "1 sec",
            "nifi.content.repository.archive.cleanup.frequency", "1 sec",
            "nifi.content.claim.max.appendable.size", "50 KB",
            "nifi.content.claim.truncation.enabled", "true",
            "nifi.content.repository.archive.max.usage.percentage", "1%",
            "nifi.queue.swap.threshold", "20");
    }

    @Override
    protected boolean isAllowFactoryReuse() {
        return false;
    }

    @Override
    protected boolean isDestroyEnvironmentAfterEachTest() {
        return true;
    }

    @Test
    public void testSwapInWithoutRestartAllowsTruncation() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity generator = getClientUtil().createProcessor("GenerateTruncatableFlowFiles");
        final ProcessorEntity terminateFlowFile = getClientUtil().createProcessor("TerminateFlowFile");
        getClientUtil().updateProcessorProperties(generator, GENERATE_TRUNCATABLE_PROPS);

        ConnectionEntity connection = getClientUtil().createConnection(generator, terminateFlowFile, "success");
        connection = getClientUtil().updateConnectionPrioritizer(connection, "PriorityAttributePrioritizer");
        connection = getClientUtil().updateConnectionBackpressure(connection, TOTAL_FLOWFILES_PER_CONNECTION + 1000, BACKPRESSURE_BYTES);

        ProcessorEntity currentGenerator = generator;
        currentGenerator = getNifiClient().getProcessorClient().runProcessorOnce(currentGenerator);
        getClientUtil().waitForStoppedProcessor(currentGenerator.getId());
        waitForQueueCount(connection.getId(), TOTAL_FLOWFILES_PER_CONNECTION);

        assertSwapFilesExist();

        final File contentRepositoryDirectory = new File(getNiFiInstance().getInstanceDirectory(), "content_repository");
        final long contentSizeBeforeDrain = getContentRepositorySize(contentRepositoryDirectory.toPath());
        assertTrue(contentSizeBeforeDrain >= 50L * ONE_MEGABYTE_IN_BYTES,
                "Content repository should hold at least 50 MB before draining; was " + contentSizeBeforeDrain);

        ProcessorEntity currentTerminate = terminateFlowFile;
        for (int i = 0; i < BATCH_COUNT; i++) {
            currentTerminate = getNifiClient().getProcessorClient().runProcessorOnce(currentTerminate);
            getClientUtil().waitForStoppedProcessor(currentTerminate.getId());
        }
        waitForQueueCount(connection.getId(), TOTAL_FLOWFILES_PER_CONNECTION - BATCH_COUNT);

        waitFor(() -> {
            try {
                return getContentRepositorySize(contentRepositoryDirectory.toPath()) == EXPECTED_CONTENT_SIZE_AFTER_TRUNCATION;
            } catch (final IOException e) {
                return false;
            }
        });

        final long finalSize = getContentRepositorySize(contentRepositoryDirectory.toPath());
        assertEquals(EXPECTED_CONTENT_SIZE_AFTER_TRUNCATION, finalSize,
                "Content repository should contain only small file data after truncation");
    }

    @Test
    public void testNoTruncationAfterRestartWithSwappedFlowFiles() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity generator = getClientUtil().createProcessor("GenerateTruncatableFlowFiles");
        ProcessorEntity firstTerminateFlowFile = getClientUtil().createProcessor("TerminateFlowFile");
        ProcessorEntity secondTerminateFlowFile = getClientUtil().createProcessor("TerminateFlowFile");
        getClientUtil().updateProcessorProperties(generator, GENERATE_TRUNCATABLE_PROPS);

        ConnectionEntity firstConnection = getClientUtil().createConnection(generator, firstTerminateFlowFile, "success");
        ConnectionEntity secondConnection = getClientUtil().createConnection(generator, secondTerminateFlowFile, "success");
        firstConnection = getClientUtil().updateConnectionBackpressure(firstConnection, TOTAL_FLOWFILES_PER_CONNECTION + 1000, BACKPRESSURE_BYTES);
        secondConnection = getClientUtil().updateConnectionBackpressure(secondConnection, TOTAL_FLOWFILES_PER_CONNECTION + 1000, BACKPRESSURE_BYTES);

        ProcessorEntity currentGenerator = generator;
        currentGenerator = getNifiClient().getProcessorClient().runProcessorOnce(currentGenerator);
        getClientUtil().waitForStoppedProcessor(currentGenerator.getId());
        waitForQueueCount(firstConnection.getId(), TOTAL_FLOWFILES_PER_CONNECTION);
        waitForQueueCount(secondConnection.getId(), TOTAL_FLOWFILES_PER_CONNECTION);

        assertSwapFilesExist();

        final NiFiInstance nifiInstance = getNiFiInstance();
        nifiInstance.stop();
        nifiInstance.start(true);
        firstTerminateFlowFile = getNifiClient().getProcessorClient().getProcessor(firstTerminateFlowFile.getId());
        secondTerminateFlowFile = getNifiClient().getProcessorClient().getProcessor(secondTerminateFlowFile.getId());

        assertSwapFilesExist();

        final File contentRepositoryDirectory = new File(getNiFiInstance().getInstanceDirectory(), "content_repository");

        drainTerminateQueue(firstTerminateFlowFile, firstConnection.getId());
        Thread.sleep(1000);

        final long sizeAfterFirstConnectionDrained = getContentRepositorySize(contentRepositoryDirectory.toPath());
        assertTrue(sizeAfterFirstConnectionDrained >= NOT_TRUNCATED_MIN_BYTES,
                "Content should not be truncated while the other connection still references the same claims and swap files exist; size was " + sizeAfterFirstConnectionDrained);

        drainTerminateQueue(secondTerminateFlowFile, secondConnection.getId());

        waitFor(() -> {
            try {
                return getContentRepositorySize(contentRepositoryDirectory.toPath()) == 0;
            } catch (final IOException e) {
                return false;
            }
        });
    }

    // Parameterized over checkpoint timing and drain order. drainFirstConnectionFirst=true drains the
    // first connection before the second; drainFirstConnectionFirst=false drains in the opposite order
    // to verify that the truncation defense is symmetric regardless of which copy is removed first.
    @ParameterizedTest(name = "waitForCheckpoint={0}, drainFirstConnectionFirst={1}")
    @CsvSource({"false, true", "true, true", "true, false"})
    public void testCloneSwapRestartContentIntact(final boolean waitForCheckpoint, final boolean drainFirstConnectionFirst)
            throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity generator = getClientUtil().createProcessor("GenerateTruncatableFlowFiles");
        ProcessorEntity firstTerminate = getClientUtil().createProcessor("TerminateFlowFile");
        ProcessorEntity secondTerminate = getClientUtil().createProcessor("TerminateFlowFile");
        getClientUtil().updateProcessorProperties(generator, GENERATE_TRUNCATABLE_PROPS);

        ConnectionEntity firstConnection = getClientUtil().createConnection(generator, firstTerminate, "success");
        ConnectionEntity secondConnection = getClientUtil().createConnection(generator, secondTerminate, "success");
        firstConnection = getClientUtil().updateConnectionBackpressure(firstConnection, TOTAL_FLOWFILES_PER_CONNECTION + 1000, BACKPRESSURE_BYTES);
        secondConnection = getClientUtil().updateConnectionBackpressure(secondConnection, TOTAL_FLOWFILES_PER_CONNECTION + 1000, BACKPRESSURE_BYTES);

        ProcessorEntity currentGenerator = generator;
        currentGenerator = getNifiClient().getProcessorClient().runProcessorOnce(currentGenerator);
        getClientUtil().waitForStoppedProcessor(currentGenerator.getId());
        waitForQueueCount(firstConnection.getId(), TOTAL_FLOWFILES_PER_CONNECTION);
        waitForQueueCount(secondConnection.getId(), TOTAL_FLOWFILES_PER_CONNECTION);

        assertSwapFilesExist();

        if (waitForCheckpoint) {
            Thread.sleep(5_000);
        }

        final NiFiInstance nifiInstance = getNiFiInstance();
        nifiInstance.stop();
        nifiInstance.start(true);
        firstTerminate = getNifiClient().getProcessorClient().getProcessor(firstTerminate.getId());
        secondTerminate = getNifiClient().getProcessorClient().getProcessor(secondTerminate.getId());

        assertSwapFilesExist();

        final File contentRepositoryDirectory = new File(getNiFiInstance().getInstanceDirectory(), "content_repository");

        final byte[] firstContent = getClientUtil().getFlowFileContentAsByteArray(firstConnection.getId(), 0);
        assertNotNull(firstContent);
        assertTrue(firstContent.length > 0, "FlowFile content from first connection should be accessible after restart");

        final byte[] secondContent = getClientUtil().getFlowFileContentAsByteArray(secondConnection.getId(), 0);
        assertNotNull(secondContent);
        assertTrue(secondContent.length > 0, "FlowFile content from second connection should be accessible after restart");

        final ProcessorEntity terminateToDrainFirst = drainFirstConnectionFirst ? firstTerminate : secondTerminate;
        final String connectionToDrainFirst = drainFirstConnectionFirst ? firstConnection.getId() : secondConnection.getId();
        final ProcessorEntity terminateToDrainSecond = drainFirstConnectionFirst ? secondTerminate : firstTerminate;
        final String connectionToDrainSecond = drainFirstConnectionFirst ? secondConnection.getId() : firstConnection.getId();

        drainTerminateQueue(terminateToDrainFirst, connectionToDrainFirst);
        Thread.sleep(1_000);

        final long sizeAfterFirstDrained = getContentRepositorySize(contentRepositoryDirectory.toPath());
        assertTrue(sizeAfterFirstDrained >= NOT_TRUNCATED_MIN_BYTES,
                "Content should not be truncated while the other connection still references the same claims; size was " + sizeAfterFirstDrained);

        final byte[] remainingContent = getClientUtil().getFlowFileContentAsByteArray(connectionToDrainSecond, 0);
        assertNotNull(remainingContent);
        assertTrue(remainingContent.length > 0, "FlowFile content should be accessible after draining the other connection");

        drainTerminateQueue(terminateToDrainSecond, connectionToDrainSecond);

        waitFor(() -> {
            try {
                return getContentRepositorySize(contentRepositoryDirectory.toPath()) == 0;
            } catch (final IOException e) {
                return false;
            }
        });
    }

    private void assertSwapFilesExist() {
        final File flowFileRepositoryDirectory = new File(getNiFiInstance().getInstanceDirectory(), "flowfile_repository");
        final File swapDirectory = new File(flowFileRepositoryDirectory, "swap");
        assertTrue(swapDirectory.exists(), "Swap directory should exist: " + swapDirectory.getAbsolutePath());

        final File[] swapFiles = swapDirectory.listFiles((directory, name) -> name.endsWith(".swap"));
        assertNotNull(swapFiles, "Should be able to list files in swap directory: " + swapDirectory.getAbsolutePath());
        assertTrue(swapFiles.length > 0, "At least one swap file should exist in " + swapDirectory.getAbsolutePath()
                + " to confirm that FlowFiles were actually swapped to disk");
    }

    private void drainTerminateQueue(final ProcessorEntity terminateFlowFileProcessor, final String connectionId) throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity latestProcessor = getNifiClient().getProcessorClient().getProcessor(terminateFlowFileProcessor.getId());
        final ProcessorEntity startedProcessor = getClientUtil().startProcessor(latestProcessor);
        waitForQueueCount(connectionId, 0);
        getClientUtil().stopProcessor(startedProcessor);
    }

    private long getContentRepositorySize(final Path contentRepositoryPath) throws IOException {
        if (!Files.exists(contentRepositoryPath)) {
            return 0;
        }

        final AtomicLong totalSize = new AtomicLong(0);
        Files.walkFileTree(contentRepositoryPath, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) {
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
