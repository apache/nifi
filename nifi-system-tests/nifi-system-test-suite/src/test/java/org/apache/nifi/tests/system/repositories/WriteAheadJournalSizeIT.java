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
import org.apache.nifi.web.api.dto.StateMapDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that the FlowFile Repository and the local State Provider are checkpointed when their write-ahead log's journal reaches the
 * configured maximum size. Both are configured with a checkpoint interval that cannot elapse while the test is running, so a checkpoint
 * that the test observes can only have been triggered by the size of the journal.
 */
public class WriteAheadJournalSizeIT extends NiFiSystemIT {
    private static final String MAXIMUM_JOURNAL_SIZE = "20 KB";
    private static final long MAXIMUM_JOURNAL_BYTES = 20 * 1024L;

    private static final int FLOWFILE_COUNT = 1000;
    private static final int STATE_UPDATE_COUNT = 500;
    private static final int STATE_VALUE_SIZE = 64;

    @Override
    protected Map<String, String> getNifiPropertiesOverrides() {
        return Map.of("nifi.flowfile.repository.checkpoint.interval", "1 hour",
            "nifi.flowfile.repository.checkpoint.max.journal.size", MAXIMUM_JOURNAL_SIZE,
            "nifi.state.management.configuration.file", "conf/state-management-bounded-journal.xml");
    }

    @Test
    public void testFlowFileRepositoryCheckpointedWhenMaximumJournalSizeReached() throws NiFiClientException, IOException, InterruptedException {
        final File flowFileRepositoryDirectory = new File(getNiFiInstance().getInstanceDirectory(), "flowfile_repository");
        final long initialTransactionId = getMaximumJournalTransactionId(flowFileRepositoryDirectory);

        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");
        getClientUtil().updateProcessorProperties(generate, Map.of("Batch Size", "1", "File Size", "0 B", "Max FlowFiles", String.valueOf(FLOWFILE_COUNT)));
        getClientUtil().updateProcessorSchedulingPeriod(generate, "0 sec");

        final ConnectionEntity connection = getClientUtil().createConnection(generate, terminate, "success");

        // Each invocation of the Processor creates a single FlowFile and commits its session, so each invocation results in its own small
        // update to the FlowFile Repository.
        getClientUtil().startProcessor(generate);
        waitForQueueCount(connection.getId(), FLOWFILE_COUNT);
        getClientUtil().stopProcessor(generate);
        getClientUtil().waitForStoppedProcessor(generate.getId());

        assertJournalCheckpointed(flowFileRepositoryDirectory, initialTransactionId);

        // The FlowFiles that were written before and after the checkpoint must all still be accounted for.
        assertEquals(FLOWFILE_COUNT, getConnectionQueueSize(connection.getId()));
    }

    @Test
    public void testLocalStateProviderCheckpointedWhenMaximumJournalSizeReached() throws NiFiClientException, IOException, InterruptedException {
        final File localStateDirectory = new File(getNiFiInstance().getInstanceDirectory(), "state/local");
        final long initialTransactionId = getMaximumJournalTransactionId(localStateDirectory);

        final ProcessorEntity appendState = getClientUtil().createProcessor("AppendLocalState");
        getClientUtil().updateProcessorProperties(appendState, Map.of("Update Count", String.valueOf(STATE_UPDATE_COUNT), "Value Size", STATE_VALUE_SIZE + " B"));
        getClientUtil().setAutoTerminatedRelationships(appendState, "success");

        // Each of the Processor's state updates is written to the State Provider individually, so the Processor produces many small updates.
        getClientUtil().runProcessorOnce(appendState);
        getClientUtil().waitForStoppedProcessor(appendState.getId());

        assertJournalCheckpointed(localStateDirectory, initialTransactionId);

        final Map<String, String> expectedState = Map.of("update", String.valueOf(STATE_UPDATE_COUNT - 1), "value", "A".repeat(STATE_VALUE_SIZE));
        assertEquals(expectedState, getLocalState(appendState.getId()));

        // Restarting proves that the state that was checkpointed and the state that was written to the new journal afterward are both recovered.
        final NiFiInstance nifiInstance = getNiFiInstance();
        nifiInstance.stop();
        nifiInstance.start(true);
        setupClient();

        assertEquals(expectedState, getLocalState(appendState.getId()));
    }

    /**
     * Waits until the write-ahead log in the given directory has rolled over to a new journal and then asserts that the journals that were
     * checkpointed have been removed and that the storage consumed by the remaining journal is within the configured maximum. A journal is
     * named for the identifier of the first transaction that it holds, so a larger transaction identifier means a newer journal.
     *
     * @param storageDirectory the storage directory of the write-ahead log
     * @param initialTransactionId the largest journal transaction identifier that existed before the test performed any updates
     */
    private void assertJournalCheckpointed(final File storageDirectory, final long initialTransactionId) throws InterruptedException {
        waitFor(() -> getMaximumJournalTransactionId(storageDirectory) > initialTransactionId);

        final List<File> journalFiles = listJournalFiles(storageDirectory);
        assertEquals(1, journalFiles.size(), "Expected the journals that were checkpointed to be removed but found " + journalFiles);

        long journalBytes = 0L;
        for (final File journalFile : journalFiles) {
            journalBytes += journalFile.length();
        }

        assertTrue(journalBytes <= MAXIMUM_JOURNAL_BYTES, "Expected the journal storage to be bounded by " + MAXIMUM_JOURNAL_BYTES + " bytes but it consumed " + journalBytes + " bytes");
    }

    private long getMaximumJournalTransactionId(final File storageDirectory) {
        long maximumTransactionId = -1L;

        for (final File journalFile : listJournalFiles(storageDirectory)) {
            final String filename = journalFile.getName();
            final long transactionId = Long.parseLong(filename.substring(0, filename.indexOf(".")));
            maximumTransactionId = Math.max(maximumTransactionId, transactionId);
        }

        return maximumTransactionId;
    }

    private List<File> listJournalFiles(final File storageDirectory) {
        final File[] journalFiles = new File(storageDirectory, "journals").listFiles(file -> file.getName().endsWith(".journal"));
        return (journalFiles == null) ? List.of() : List.of(journalFiles);
    }

    private Map<String, String> getLocalState(final String processorId) throws NiFiClientException, IOException {
        final StateMapDTO localState = getNifiClient().getProcessorClient().getProcessorState(processorId).getComponentState().getLocalState();
        assertNotNull(localState);

        final Map<String, String> state = new HashMap<>();
        localState.getState().forEach(entry -> state.put(entry.getKey(), entry.getValue()));

        return state;
    }
}
