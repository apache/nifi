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
package org.apache.nifi.processor.util.list;

import org.apache.nifi.annotation.notification.PrimaryNodeState;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestWatcher;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.nifi.processor.util.list.AbstractListProcessor.PRECISION_MILLIS;
import static org.apache.nifi.processor.util.list.AbstractListProcessor.PRECISION_MINUTES;
import static org.apache.nifi.processor.util.list.AbstractListProcessor.PRECISION_SECONDS;
import static org.apache.nifi.processor.util.list.AbstractListProcessor.TARGET_SYSTEM_TIMESTAMP_PRECISION;
import org.apache.nifi.processor.util.list.TestAbstractListProcessor.ConcreteListProcessor;
import org.apache.nifi.processor.util.list.TestAbstractListProcessor.DistributedCache;
import static org.junit.Assert.assertEquals;

public class ITAbstractListProcessor {

    /**
     * @return current timestamp in milliseconds, but truncated at specified
     * target precision (e.g. SECONDS or MINUTES).
     */
    private static long getCurrentTimestampMillis(final TimeUnit targetPrecision) {
        final long timestampInTargetPrecision = targetPrecision.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        return TimeUnit.MILLISECONDS.convert(timestampInTargetPrecision, targetPrecision);
    }

    private static long getSleepMillis(final TimeUnit targetPrecision) {
        return AbstractListProcessor.LISTING_LAG_MILLIS.get(targetPrecision) * 2;
    }

    private static final long DEFAULT_SLEEP_MILLIS = getSleepMillis(TimeUnit.MILLISECONDS);

    private ConcreteListProcessor proc;
    private TestRunner runner;

    @Rule
    public TestWatcher dumpState = new ListProcessorTestWatcher(
            () -> {
                try {
                    return runner.getStateManager().getState(Scope.LOCAL).toMap();
                } catch (IOException e) {
                    throw new RuntimeException("Failed to retrieve state", e);
                }
            },
            () -> proc.getEntityList(),
            () -> runner.getFlowFilesForRelationship(AbstractListProcessor.REL_SUCCESS).stream().map(m -> (FlowFile) m).collect(Collectors.toList())
    );

    @Before
    public void setup() {
        proc = new ConcreteListProcessor();
        runner = TestRunners.newTestRunner(proc);
    }

    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder();

    /**
     * <p>
     * Ensures that files are listed when those are old enough:
     * <li>Files with last modified timestamp those are old enough to determine
     * that those are completely written and no further files are expected to be
     * added with the same timestamp.</li>
     * <li>This behavior is expected when a processor is scheduled less
     * frequently, such as hourly or daily.</li>
     * </p>
     */
    @Test
    public void testAllExistingEntriesEmittedOnFirstIteration() throws Exception {
        final long oldTimestamp = System.currentTimeMillis() - getSleepMillis(TimeUnit.MILLISECONDS);

        // These entries have existed before the processor runs at the first time.
        proc.addEntity("name", "id", oldTimestamp);
        proc.addEntity("name", "id2", oldTimestamp);

        // First run, the above listed entries should be emitted since it has existed.
        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 2);
        runner.clearTransferState();

        // Ensure we have covered the necessary lag period to avoid issues where the processor was immediately scheduled to run again
        Thread.sleep(DEFAULT_SLEEP_MILLIS);

        // Run again without introducing any new entries
        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 0);
    }

    private void testPreviouslySkippedEntriesEmmitedOnNextIteration(final TimeUnit targetPrecision) throws InterruptedException {
        runner.run();

        final long initialTimestamp = getCurrentTimestampMillis(targetPrecision);

        setTargetSystemTimestampPrecision(targetPrecision);

        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 0);
        proc.addEntity("name", "id", initialTimestamp);
        proc.addEntity("name", "id2", initialTimestamp);
        runner.run();

        // First run, the above listed entries would be skipped to avoid write synchronization issues
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 0);
        runner.clearTransferState();

        // Ensure we have covered the necessary lag period to avoid issues where the processor was immediately scheduled to run again
        Thread.sleep(getSleepMillis(targetPrecision));

        // Run again without introducing any new entries
        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 2);
    }

    /**
     * <p>
     * Ensures that newly created files should wait to confirm there is no more
     * files created with the same timestamp:
     * <li>If files have the latest modified timestamp at an iteration, then
     * those should be postponed to be listed</li>
     * <li>If those files still are the latest files at the next iteration, then
     * those should be listed</li>
     * </p>
     */
    @Test
    public void testPreviouslySkippedEntriesEmittedOnNextIterationMilliPrecision() throws Exception {
        testPreviouslySkippedEntriesEmmitedOnNextIteration(TimeUnit.MILLISECONDS);
    }

    /**
     * Same as
     * {@link #testPreviouslySkippedEntriesEmittedOnNextIterationMilliPrecision()}
     * but simulates that the target filesystem only provide timestamp precision
     * in Seconds.
     */
    @Test
    public void testPreviouslySkippedEntriesEmittedOnNextIterationSecondPrecision() throws Exception {
        testPreviouslySkippedEntriesEmmitedOnNextIteration(TimeUnit.SECONDS);
    }

    /**
     * Same as
     * {@link #testPreviouslySkippedEntriesEmittedOnNextIterationMilliPrecision()}
     * but simulates that the target filesystem only provide timestamp precision
     * in Minutes.
     */
    @Test
    public void testPreviouslySkippedEntriesEmittedOnNextIterationMinutesPrecision() throws Exception {
        testPreviouslySkippedEntriesEmmitedOnNextIteration(TimeUnit.MINUTES);
    }

    private void testOnlyNewEntriesEmitted(final TimeUnit targetPrecision) throws InterruptedException {

        final long initialTimestamp = getCurrentTimestampMillis(targetPrecision);

        setTargetSystemTimestampPrecision(targetPrecision);

        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 0);
        proc.addEntity("name", "id", initialTimestamp);
        proc.addEntity("name", "id2", initialTimestamp);
        runner.run();

        // First run, the above listed entries would be skipped to avoid write synchronization issues
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 0);
        runner.clearTransferState();

        // Ensure we have covered the necessary lag period to avoid issues where the processor was immediately scheduled to run again
        Thread.sleep(getSleepMillis(targetPrecision));

        // Running again, our two previously seen files are now cleared to be released
        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 2);
        runner.clearTransferState();

        // Verify no new old files show up
        proc.addEntity("name", "id2", initialTimestamp);
        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 0);
        runner.clearTransferState();

        // An entry that is older than already processed entry should not be listed.
        proc.addEntity("name", "id3", initialTimestamp - targetPrecision.toMillis(1));
        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 0);
        runner.clearTransferState();

        // If an entry whose timestamp is the same with the last processed timestamp should not be listed.
        proc.addEntity("name", "id2", initialTimestamp);
        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 0);
        runner.clearTransferState();

        // Now a new file beyond the current time enters
        proc.addEntity("name", "id2", initialTimestamp + targetPrecision.toMillis(1));

        // It should show up
        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 1);
        runner.clearTransferState();
    }

    private void setTargetSystemTimestampPrecision(TimeUnit targetPrecision) {
        switch (targetPrecision) {
            case MINUTES:
                runner.setProperty(TARGET_SYSTEM_TIMESTAMP_PRECISION, PRECISION_MINUTES);
                break;
            case SECONDS:
                runner.setProperty(TARGET_SYSTEM_TIMESTAMP_PRECISION, PRECISION_SECONDS);
                break;
            case MILLISECONDS:
                runner.setProperty(TARGET_SYSTEM_TIMESTAMP_PRECISION, PRECISION_MILLIS);
                break;
        }
    }

    @Test
    public void testOnlyNewEntriesEmittedMillisPrecision() throws Exception {
        testOnlyNewEntriesEmitted(TimeUnit.MILLISECONDS);
    }

    @Test
    public void testOnlyNewEntriesEmittedSecondPrecision() throws Exception {
        testOnlyNewEntriesEmitted(TimeUnit.SECONDS);
    }

    @Test
    public void testOnlyNewEntriesEmittedMinutesPrecision() throws Exception {
        testOnlyNewEntriesEmitted(TimeUnit.MINUTES);
    }

    @Test
    public void testHandleRestartWithEntriesAlreadyTransferredAndNoneNew() throws Exception {

        final long initialTimestamp = System.currentTimeMillis();

        proc.addEntity("name", "id", initialTimestamp);
        proc.addEntity("name", "id2", initialTimestamp);

        // Emulate having state but not having had the processor run such as in a restart
        final Map<String, String> preexistingState = new HashMap<>();
        preexistingState.put(AbstractListProcessor.LATEST_LISTED_ENTRY_TIMESTAMP_KEY, Long.toString(initialTimestamp));
        preexistingState.put(AbstractListProcessor.LAST_PROCESSED_LATEST_ENTRY_TIMESTAMP_KEY, Long.toString(initialTimestamp));
        preexistingState.put(AbstractListProcessor.IDENTIFIER_PREFIX + ".0", "id");
        preexistingState.put(AbstractListProcessor.IDENTIFIER_PREFIX + ".1", "id2");
        runner.getStateManager().setState(preexistingState, Scope.CLUSTER);

        // run for the first time
        runner.run();

        // First run, the above listed entries would be skipped
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 0);
        runner.clearTransferState();

        // Ensure we have covered the necessary lag period to avoid issues where the processor was immediately scheduled to run again
        Thread.sleep(DEFAULT_SLEEP_MILLIS);

        // Running again, these files should be eligible for transfer and again skipped
        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 0);
        runner.clearTransferState();

        // Verify no new old files show up
        proc.addEntity("name", "id2", initialTimestamp);
        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 0);
        runner.clearTransferState();

        proc.addEntity("name", "id3", initialTimestamp - 1);
        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 0);
        runner.clearTransferState();

        proc.addEntity("name", "id2", initialTimestamp);
        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 0);
        runner.clearTransferState();

        // Now a new file beyond the current time enters
        proc.addEntity("name", "id2", initialTimestamp + 1);

        // It should now show up
        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 1);
        runner.clearTransferState();
    }

    @Test
    public void testStateStoredInClusterStateManagement() throws Exception {

        final DistributedCache cache = new DistributedCache();
        runner.addControllerService("cache", cache);
        runner.enableControllerService(cache);
        runner.setProperty(AbstractListProcessor.DISTRIBUTED_CACHE_SERVICE, "cache");

        final long initialTimestamp = System.currentTimeMillis();

        proc.addEntity("name", "id", initialTimestamp);
        runner.run();

        final Map<String, String> expectedState = new HashMap<>();
        // Ensure only timestamp is migrated
        expectedState.put(AbstractListProcessor.LATEST_LISTED_ENTRY_TIMESTAMP_KEY, String.valueOf(initialTimestamp));
        expectedState.put(AbstractListProcessor.LAST_PROCESSED_LATEST_ENTRY_TIMESTAMP_KEY, "0");
        runner.getStateManager().assertStateEquals(expectedState, Scope.CLUSTER);

        Thread.sleep(DEFAULT_SLEEP_MILLIS);

        runner.run();
        // Ensure only timestamp is migrated
        expectedState.put(AbstractListProcessor.LATEST_LISTED_ENTRY_TIMESTAMP_KEY, String.valueOf(initialTimestamp));
        expectedState.put(AbstractListProcessor.LAST_PROCESSED_LATEST_ENTRY_TIMESTAMP_KEY, String.valueOf(initialTimestamp));
        expectedState.put(AbstractListProcessor.IDENTIFIER_PREFIX + ".0", "id");
        runner.getStateManager().assertStateEquals(expectedState, Scope.CLUSTER);
    }

    @Test
    public void testResumeListingAfterClearingState() throws Exception {

        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 0);

        final long initialEventTimestamp = System.currentTimeMillis();
        proc.addEntity("name", "id", initialEventTimestamp);
        proc.addEntity("name", "id2", initialEventTimestamp);

        // Add entities but these should not be transferred as they are the latest values
        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 0);

        // after providing a pause in listings, the files should now  transfer
        Thread.sleep(DEFAULT_SLEEP_MILLIS);

        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 2);
        runner.clearTransferState();

        // Verify entities are not transferred again for the given state
        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 0);
        runner.clearTransferState();

        // Clear state for this processor, eradicating timestamp
        runner.getStateManager().clear(Scope.CLUSTER);
        Assert.assertEquals("State is not empty for this component after clearing", 0, runner.getStateManager().getState(Scope.CLUSTER).toMap().size());

        // Ensure the original files are now transferred again.
        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 2);
        runner.clearTransferState();
    }

    @Test
    public void testResumeListingAfterBecamePrimary() throws Exception {
        final long initialTimestamp = System.currentTimeMillis();

        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 0);
        proc.addEntity("name", "id", initialTimestamp);
        proc.addEntity("name", "id2", initialTimestamp);

        // Add entities but these should not be transferred as they are the latest values
        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 0);
        runner.clearTransferState();

        // after providing a pause in listings, the files should now  transfer
        Thread.sleep(DEFAULT_SLEEP_MILLIS);

        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 2);
        runner.clearTransferState();

        // Emulate reelection process
        proc.onPrimaryNodeChange(PrimaryNodeState.ELECTED_PRIMARY_NODE);

        // Now a new file enters
        proc.addEntity("name", "id3", initialTimestamp + 1);;

        // First run skips the execution because determined timestamp is the same as last listing
        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 0);
        runner.clearTransferState();

        // Now the cluster state has been read, all set to perform next listing
        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 1);
        runner.clearTransferState();
    }

    @Test
    public void testOnlyNewStateStored() throws Exception {

        runner.run();

        final long initialTimestamp = System.currentTimeMillis();

        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 0);
        proc.addEntity("name", "id", initialTimestamp);
        proc.addEntity("name", "id2", initialTimestamp);

        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 0);
        runner.clearTransferState();

        Thread.sleep(DEFAULT_SLEEP_MILLIS);

        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 2);
        runner.clearTransferState();

        final StateMap stateMap = runner.getStateManager().getState(Scope.CLUSTER);
        assertEquals(2, stateMap.getVersion());

        final Map<String, String> map = stateMap.toMap();
        // Ensure timestamp and identifiers are migrated
        assertEquals(4, map.size());
        assertEquals(Long.toString(initialTimestamp), map.get(AbstractListProcessor.LATEST_LISTED_ENTRY_TIMESTAMP_KEY));
        assertEquals(Long.toString(initialTimestamp), map.get(AbstractListProcessor.LAST_PROCESSED_LATEST_ENTRY_TIMESTAMP_KEY));
        assertEquals("id", map.get(AbstractListProcessor.IDENTIFIER_PREFIX + ".0"));
        assertEquals("id2", map.get(AbstractListProcessor.IDENTIFIER_PREFIX + ".1"));

        proc.addEntity("new name", "new id", initialTimestamp + 1);
        runner.run();

        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 1);
        runner.clearTransferState();

        StateMap updatedStateMap = runner.getStateManager().getState(Scope.CLUSTER);
        assertEquals(3, updatedStateMap.getVersion());

        assertEquals(3, updatedStateMap.toMap().size());
        assertEquals(Long.toString(initialTimestamp + 1), updatedStateMap.get(AbstractListProcessor.LATEST_LISTED_ENTRY_TIMESTAMP_KEY));
        // Processed timestamp is now caught up
        assertEquals(Long.toString(initialTimestamp + 1), updatedStateMap.get(AbstractListProcessor.LAST_PROCESSED_LATEST_ENTRY_TIMESTAMP_KEY));
        assertEquals("new id", updatedStateMap.get(AbstractListProcessor.IDENTIFIER_PREFIX + ".0"));
    }

}
