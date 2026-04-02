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
import org.apache.nifi.toolkit.client.ProvenanceClient.ReplayEventNodes;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.ReplayLastEventResponseEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * System test that verifies the end-to-end content claim truncation feature. Generates FlowFiles with a pattern
 * of 9 small (1 KB) + 1 large (10 MB) per batch, removes only the large FlowFiles via priority-based ordering,
 * and then verifies that the content repository files are truncated on disk.
 */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
public class ContentClaimTruncationIT extends NiFiSystemIT {

    private static final long ONE_MEGABYTE_IN_BYTES = 1024L * 1024L;
    private static final long NOT_TRUNCATED_MIN_BYTES = 10L * ONE_MEGABYTE_IN_BYTES;
    private static final long BACKPRESSURE_BYTES = 100L * ONE_MEGABYTE_IN_BYTES;

    private static final int BATCH_COUNT = 10;
    private static final int SMALL_FILES_PER_BATCH = 9;
    private static final int SMALL_FILE_SIZE_BYTES = 1024;

    private static final Map<String, String> GENERATE_TRUNCATABLE_PROPS = Map.of(
        "Batch Count", String.valueOf(BATCH_COUNT),
        "Small File Size", "1 KB",
        "Large File Size", "10 MB",
        "Small Files Per Batch", String.valueOf(SMALL_FILES_PER_BATCH));

    private static final long EXPECTED_CONTENT_SIZE_AFTER_TRUNCATION = (long) BATCH_COUNT * SMALL_FILES_PER_BATCH * SMALL_FILE_SIZE_BYTES;

    private static final String REPLACEMENT_CONTENT = "small replacement";
    private static final long EXPECTED_CONTENT_SIZE_AFTER_OVERWRITE_TRUNCATION =
            EXPECTED_CONTENT_SIZE_AFTER_TRUNCATION + (long) BATCH_COUNT * REPLACEMENT_CONTENT.getBytes(StandardCharsets.UTF_8).length;

    @Override
    protected Map<String, String> getNifiPropertiesOverrides() {
        return Map.of(
            "nifi.flowfile.repository.checkpoint.interval", "1 sec",
            "nifi.content.repository.archive.cleanup.frequency", "1 sec",
            "nifi.content.claim.max.appendable.size", "50 KB",
            "nifi.content.claim.truncation.enabled", "true",
            "nifi.content.repository.archive.max.usage.percentage", "1%");
    }

    @Override
    protected boolean isAllowFactoryReuse() {
        return false;
    }

    @Override
    protected boolean isDestroyEnvironmentAfterEachTest() {
        return true;
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testLargeFlowFileTruncation(final boolean restartBeforeDroppingData) throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity generator = getClientUtil().createProcessor("GenerateTruncatableFlowFiles");
        ProcessorEntity terminateFlowFile = getClientUtil().createProcessor("TerminateFlowFile");
        getClientUtil().updateProcessorProperties(generator, GENERATE_TRUNCATABLE_PROPS);
        getClientUtil().updateProcessorSchedulingPeriod(generator, "0 sec");

        ConnectionEntity connection = getClientUtil().createConnection(generator, terminateFlowFile, "success");
        connection = getClientUtil().updateConnectionPrioritizer(connection, "PriorityAttributePrioritizer");
        connection = getClientUtil().updateConnectionBackpressure(connection, 10000, BACKPRESSURE_BYTES);

        getClientUtil().startProcessor(generator);
        waitForQueueCount(connection.getId(), 100);
        getClientUtil().stopProcessor(generator);
        getClientUtil().waitForStoppedProcessor(generator.getId());

        final File contentRepositoryDirectory = new File(getNiFiInstance().getInstanceDirectory(), "content_repository");
        waitFor(() -> {
            try {
                return getContentRepositorySize(contentRepositoryDirectory.toPath()) >= 100L * ONE_MEGABYTE_IN_BYTES;
            } catch (final IOException e) {
                return false;
            }
        });

        if (restartBeforeDroppingData) {
            final NiFiInstance nifiInstance = getNiFiInstance();
            nifiInstance.stop();
            nifiInstance.start(true);
            terminateFlowFile = getNifiClient().getProcessorClient().getProcessor(terminateFlowFile.getId());
        }

        for (int i = 0; i < BATCH_COUNT; i++) {
            terminateFlowFile = getNifiClient().getProcessorClient().runProcessorOnce(terminateFlowFile);
            getClientUtil().waitForStoppedProcessor(terminateFlowFile.getId());
        }
        waitForQueueCount(connection.getId(), BATCH_COUNT * SMALL_FILES_PER_BATCH);

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

    // Draining one of two success connections must not truncate shared content claims while the other connection still holds FlowFiles.
    // After both connections are drained, the content repository should be empty.
    // When restartBeforeDroppingData is true, NiFi is restarted after generating data to verify that loadFlowFiles()
    // correctly rebuilds the truncationEligibleClaimReferenceCounts so shared claims are not prematurely truncated.
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testClonedSuccessNoTruncationUntilBothConnectionsDrained(final boolean restartBeforeDroppingData) throws Exception {
        final ProcessorEntity generator = getClientUtil().createProcessor("GenerateTruncatableFlowFiles");
        ProcessorEntity firstTerminateFlowFile = getClientUtil().createProcessor("TerminateFlowFile");
        ProcessorEntity secondTerminateFlowFile = getClientUtil().createProcessor("TerminateFlowFile");
        getClientUtil().updateProcessorProperties(generator, GENERATE_TRUNCATABLE_PROPS);
        getClientUtil().updateProcessorSchedulingPeriod(generator, "0 sec");

        ConnectionEntity firstConnection = getClientUtil().createConnection(generator, firstTerminateFlowFile, "success");
        ConnectionEntity secondConnection = getClientUtil().createConnection(generator, secondTerminateFlowFile, "success");
        firstConnection = getClientUtil().updateConnectionBackpressure(firstConnection, 10000, BACKPRESSURE_BYTES);
        secondConnection = getClientUtil().updateConnectionBackpressure(secondConnection, 10000, BACKPRESSURE_BYTES);

        getClientUtil().startProcessor(generator);
        waitForQueueCount(firstConnection.getId(), 100);
        waitForQueueCount(secondConnection.getId(), 100);

        getClientUtil().stopProcessor(generator);
        getClientUtil().waitForStoppedProcessor(generator.getId());

        if (restartBeforeDroppingData) {
            final NiFiInstance nifiInstance = getNiFiInstance();
            nifiInstance.stop();
            nifiInstance.start(true);
            firstTerminateFlowFile = getNifiClient().getProcessorClient().getProcessor(firstTerminateFlowFile.getId());
            secondTerminateFlowFile = getNifiClient().getProcessorClient().getProcessor(secondTerminateFlowFile.getId());
        }

        final File contentRepositoryDirectory = new File(getNiFiInstance().getInstanceDirectory(), "content_repository");

        drainTerminateQueue(firstTerminateFlowFile, firstConnection.getId());
        Thread.sleep(1000);

        final long sizeAfterFirstConnectionDrained = getContentRepositorySize(contentRepositoryDirectory.toPath());
        assertTrue(sizeAfterFirstConnectionDrained >= NOT_TRUNCATED_MIN_BYTES,
                "Content should not be truncated while the other connection still references the same claims; size was " + sizeAfterFirstConnectionDrained);

        drainTerminateQueue(secondTerminateFlowFile, secondConnection.getId());

        waitFor(() -> {
            try {
                return getContentRepositorySize(contentRepositoryDirectory.toPath()) == 0;
            } catch (final IOException e) {
                return false;
            }
        });
    }

    // Same scenario as testClonedSuccessNoTruncationUntilBothConnectionsDrained, but the second connection uses
    // PriorityAttributePrioritizer. After the first connection is drained, only the large FlowFiles are removed
    // from the second connection so that tail truncation of content repository files can occur.
    // When restartBeforeDroppingData is true, NiFi is restarted after generating data to verify that loadFlowFiles()
    // correctly rebuilds the truncationEligibleClaimReferenceCounts and truncation still works after recovery.
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testClonedSuccessTruncatesAfterLargeFlowFilesRemovedFromSecondConnection(final boolean restartBeforeDroppingData) throws Exception {
        final ProcessorEntity generator = getClientUtil().createProcessor("GenerateTruncatableFlowFiles");
        ProcessorEntity firstTerminateFlowFile = getClientUtil().createProcessor("TerminateFlowFile");
        ProcessorEntity secondTerminateFlowFile = getClientUtil().createProcessor("TerminateFlowFile");
        getClientUtil().updateProcessorProperties(generator, GENERATE_TRUNCATABLE_PROPS);
        getClientUtil().updateProcessorSchedulingPeriod(generator, "0 sec");

        ConnectionEntity firstConnection = getClientUtil().createConnection(generator, firstTerminateFlowFile, "success");
        ConnectionEntity secondConnection = getClientUtil().createConnection(generator, secondTerminateFlowFile, "success");
        firstConnection = getClientUtil().updateConnectionBackpressure(firstConnection, 10000, BACKPRESSURE_BYTES);
        secondConnection = getClientUtil().updateConnectionBackpressure(secondConnection, 10000, BACKPRESSURE_BYTES);
        secondConnection = getClientUtil().updateConnectionPrioritizer(secondConnection, "PriorityAttributePrioritizer");

        getClientUtil().startProcessor(generator);
        waitForQueueCount(firstConnection.getId(), 100);
        waitForQueueCount(secondConnection.getId(), 100);

        getClientUtil().stopProcessor(generator);
        getClientUtil().waitForStoppedProcessor(generator.getId());

        if (restartBeforeDroppingData) {
            final NiFiInstance nifiInstance = getNiFiInstance();
            nifiInstance.stop();
            nifiInstance.start(true);
            firstTerminateFlowFile = getNifiClient().getProcessorClient().getProcessor(firstTerminateFlowFile.getId());
            secondTerminateFlowFile = getNifiClient().getProcessorClient().getProcessor(secondTerminateFlowFile.getId());
        }

        final File contentRepositoryDirectory = new File(getNiFiInstance().getInstanceDirectory(), "content_repository");

        drainTerminateQueue(firstTerminateFlowFile, firstConnection.getId());
        Thread.sleep(1000);

        final long sizeAfterFirstConnectionDrained = getContentRepositorySize(contentRepositoryDirectory.toPath());
        assertTrue(sizeAfterFirstConnectionDrained >= NOT_TRUNCATED_MIN_BYTES,
                "Content should not be truncated while the other connection still references the same claims; size was " + sizeAfterFirstConnectionDrained);

        ProcessorEntity secondTerminate = secondTerminateFlowFile;
        for (int i = 0; i < BATCH_COUNT; i++) {
            secondTerminate = getNifiClient().getProcessorClient().runProcessorOnce(secondTerminate);
            getClientUtil().waitForStoppedProcessor(secondTerminate.getId());
        }
        waitForQueueCount(secondConnection.getId(), BATCH_COUNT * SMALL_FILES_PER_BATCH);

        waitFor(() -> {
            try {
                return getContentRepositorySize(contentRepositoryDirectory.toPath()) == EXPECTED_CONTENT_SIZE_AFTER_TRUNCATION;
            } catch (final IOException e) {
                return false;
            }
        });
    }

    @Test
    public void testOverwriteContentAllowsTruncationOfOldClaim() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity generator = getClientUtil().createProcessor("GenerateTruncatableFlowFiles");
        final ProcessorEntity updateContent = getClientUtil().createProcessor("UpdateContent");
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");

        getClientUtil().updateProcessorProperties(generator, GENERATE_TRUNCATABLE_PROPS);
        getClientUtil().updateProcessorSchedulingPeriod(generator, "0 sec");
        getClientUtil().updateProcessorProperties(updateContent, Map.of("Content", REPLACEMENT_CONTENT));

        ConnectionEntity generatorToUpdate = getClientUtil().createConnection(generator, updateContent, "success");
        generatorToUpdate = getClientUtil().updateConnectionPrioritizer(generatorToUpdate, "PriorityAttributePrioritizer");
        generatorToUpdate = getClientUtil().updateConnectionBackpressure(generatorToUpdate, 10000, BACKPRESSURE_BYTES);

        getClientUtil().createConnection(updateContent, terminate, "success");

        getClientUtil().startProcessor(generator);
        waitForQueueCount(generatorToUpdate.getId(), 100);
        getClientUtil().stopProcessor(generator);
        getClientUtil().waitForStoppedProcessor(generator.getId());

        final File contentRepositoryDirectory = new File(getNiFiInstance().getInstanceDirectory(), "content_repository");
        waitFor(() -> {
            try {
                return getContentRepositorySize(contentRepositoryDirectory.toPath()) >= 100L * ONE_MEGABYTE_IN_BYTES;
            } catch (final IOException e) {
                return false;
            }
        });

        ProcessorEntity currentUpdateContent = updateContent;
        for (int i = 0; i < BATCH_COUNT; i++) {
            currentUpdateContent = getNifiClient().getProcessorClient().runProcessorOnce(currentUpdateContent);
            getClientUtil().waitForStoppedProcessor(currentUpdateContent.getId());
        }
        waitForQueueCount(generatorToUpdate.getId(), BATCH_COUNT * SMALL_FILES_PER_BATCH);

        waitFor(() -> {
            try {
                return getContentRepositorySize(contentRepositoryDirectory.toPath()) == EXPECTED_CONTENT_SIZE_AFTER_OVERWRITE_TRUNCATION;
            } catch (final IOException e) {
                return false;
            }
        });

        final long finalSize = getContentRepositorySize(contentRepositoryDirectory.toPath());
        assertEquals(EXPECTED_CONTENT_SIZE_AFTER_OVERWRITE_TRUNCATION, finalSize,
                "Content repository should contain truncated original data plus replacement content");
    }

    @Test
    public void testReplayedFlowFileContentNotTruncated() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity generator = getClientUtil().createProcessor("GenerateTruncatableFlowFiles");
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");

        final Map<String, String> generateProps = Map.of(
            "Batch Count", "1",
            "Small File Size", "1 KB",
            "Large File Size", "10 MB",
            "Small Files Per Batch", "9");
        getClientUtil().updateProcessorProperties(generator, generateProps);
        getClientUtil().updateProcessorSchedulingPeriod(generator, "0 sec");

        ConnectionEntity connection = getClientUtil().createConnection(generator, terminate, "success");
        connection = getClientUtil().updateConnectionPrioritizer(connection, "PriorityAttributePrioritizer");
        connection = getClientUtil().updateConnectionBackpressure(connection, 10000, BACKPRESSURE_BYTES);

        getClientUtil().startProcessor(generator);
        waitForQueueCount(connection.getId(), 10);
        getClientUtil().stopProcessor(generator);
        getClientUtil().waitForStoppedProcessor(generator.getId());

        ProcessorEntity currentTerminate = terminate;
        while (getConnectionQueueSize(connection.getId()) > 0) {
            currentTerminate = getNifiClient().getProcessorClient().runProcessorOnce(currentTerminate);
            getClientUtil().waitForStoppedProcessor(currentTerminate.getId());
        }
        waitForQueueCount(connection.getId(), 0);

        final ReplayLastEventResponseEntity replayResponse = getNifiClient().getProvenanceClient().replayLastEvent(currentTerminate.getId(), ReplayEventNodes.PRIMARY);
        assertNull(replayResponse.getAggregateSnapshot().getFailureExplanation());
        assertNotNull(replayResponse.getAggregateSnapshot().getEventsReplayed());

        waitForQueueCount(connection.getId(), 1);

        final byte[] replayedContent = getClientUtil().getFlowFileContentAsByteArray(connection.getId(), 0);
        assertNotNull(replayedContent);
        assertTrue(replayedContent.length > 0,
                "Replayed FlowFile content should not be empty — truncation must not have destroyed the content");
    }

    private void drainTerminateQueue(final ProcessorEntity terminateFlowFileProcessor, final String connectionId) throws NiFiClientException, IOException, InterruptedException {
        ProcessorEntity currentProcessor = terminateFlowFileProcessor;
        while (getConnectionQueueSize(connectionId) > 0) {
            currentProcessor = getNifiClient().getProcessorClient().runProcessorOnce(currentProcessor);
            getClientUtil().waitForStoppedProcessor(currentProcessor.getId());
        }
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
