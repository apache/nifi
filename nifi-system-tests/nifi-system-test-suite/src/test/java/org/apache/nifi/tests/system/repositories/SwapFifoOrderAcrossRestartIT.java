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
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Verifies that a FIFO connection holding active, swapped-out and in-memory swap-queue FlowFiles
 * still delivers the oldest FlowFiles first after NiFi is restarted.
 */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
public class SwapFifoOrderAcrossRestartIT extends NiFiSystemIT {

    // One batch of FLOWFILE_COUNT leaves SWAP_THRESHOLD FlowFiles active, 63 full swap files and 63 FlowFiles in the
    // in-memory swap queue. The in-memory FlowFiles are 4096 ids newer than the active ones, so they share hash buckets
    // with the active FlowFiles in the recovered repository and are interleaved with them on restart.
    private static final int SWAP_THRESHOLD = 64;
    private static final int FLOWFILE_COUNT = 4_159;
    private static final String INDEX_ATTRIBUTE = "index";

    @Override
    protected Map<String, String> getNifiPropertiesOverrides() {
        return Map.of(
            "nifi.queue.swap.threshold", String.valueOf(SWAP_THRESHOLD),
            "nifi.flowfile.repository.checkpoint.interval", "1 sec");
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
    public void testFifoOrderPreservedAcrossRestartWithSwappedFlowFiles() throws NiFiClientException, IOException, InterruptedException {
        ProcessorEntity generator = getClientUtil().createProcessor("GenerateFlowFile");
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");
        getClientUtil().updateProcessorProperties(generator, Map.of(
            "Batch Size", String.valueOf(FLOWFILE_COUNT),
            INDEX_ATTRIBUTE, "${nextInt()}"));

        ConnectionEntity connection = getClientUtil().createConnection(generator, terminate, "success");
        connection = getClientUtil().updateConnectionBackpressure(connection, FLOWFILE_COUNT * 2L, 1024L * 1024L * 1024L);

        generator = getClientUtil().runProcessorOnce(generator);
        getClientUtil().waitForStoppedProcessor(generator.getId());
        waitForQueueCount(connection.getId(), FLOWFILE_COUNT);
        assertSwapFilesExist();
        waitForCheckpoint();

        final long firstIndex = getIndex(connection.getId(), 0);

        getNiFiInstance().stop();
        getNiFiInstance().start(true);
        waitForQueueCount(connection.getId(), FLOWFILE_COUNT);
        assertSwapFilesExist();

        for (int position = 0; position < SWAP_THRESHOLD; position++) {
            final long index = getIndex(connection.getId(), position);
            if (index >= firstIndex + SWAP_THRESHOLD) {
                fail("FIFO order not preserved after restart: queue position " + position + " holds FlowFile with index " + index
                    + " but only indexes " + firstIndex + ".." + (firstIndex + SWAP_THRESHOLD - 1) + " were enqueued first");
            }
        }
    }

    private long getIndex(final String connectionId, final int position) throws NiFiClientException, IOException {
        final String index = getClientUtil().getQueueFlowFile(connectionId, position).getFlowFile().getAttributes().get(INDEX_ATTRIBUTE);
        assertNotNull(index, "FlowFile at position " + position + " lacks attribute " + INDEX_ATTRIBUTE);
        return Long.parseLong(index);
    }

    // NiFi does not checkpoint on shutdown, so the restart must recover from a checkpoint taken after the swap-out
    private void waitForCheckpoint() throws InterruptedException {
        final File checkpoint = new File(getFlowFileRepositoryDirectory(), "checkpoint");
        final long lastSwapOutMillis = Arrays.stream(getSwapFiles()).mapToLong(File::lastModified).max().orElseThrow();
        waitFor(() -> checkpoint.lastModified() > lastSwapOutMillis);
    }

    private File getFlowFileRepositoryDirectory() {
        return new File(getNiFiInstance().getInstanceDirectory(), "flowfile_repository");
    }

    private void assertSwapFilesExist() {
        assertTrue(getSwapFiles().length > 0, "Expected at least one swap file in " + getFlowFileRepositoryDirectory().getAbsolutePath());
    }

    private File[] getSwapFiles() {
        final File swapDirectory = new File(getFlowFileRepositoryDirectory(), "swap");
        final File[] swapFiles = swapDirectory.listFiles((directory, name) -> name.endsWith(".swap"));
        assertNotNull(swapFiles, "Cannot list swap directory " + swapDirectory.getAbsolutePath());
        return swapFiles;
    }
}
