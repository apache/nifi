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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.Charsets;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.list.AbstractListProcessor;
import org.apache.nifi.processor.util.list.ListableEntity;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestAbstractListProcessor {

    static final long DEFAULT_SLEEP_MILLIS = TimeUnit.NANOSECONDS.toMillis(AbstractListProcessor.LISTING_LAG_NANOS * 2);

    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder();

    @Test
    public void testAllExistingEntriesEmittedOnFirstIteration() throws Exception {
        final long oldTimestamp = System.nanoTime() - (AbstractListProcessor.LISTING_LAG_NANOS * 2);

        // These entries have existed before the processor runs at the first time.
        final ConcreteListProcessor proc = new ConcreteListProcessor();
        proc.addEntity("name", "id", oldTimestamp);
        proc.addEntity("name", "id2", oldTimestamp);

        // First run, the above listed entries should be emitted since it has existed.
        final TestRunner runner = TestRunners.newTestRunner(proc);

        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 2);
        runner.clearTransferState();

        // Ensure we have covered the necessary lag period to avoid issues where the processor was immediately scheduled to run again
        Thread.sleep(DEFAULT_SLEEP_MILLIS);

        // Run again without introducing any new entries
        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 0);
    }

    @Test
    public void testPreviouslySkippedEntriesEmittedOnNextIteration() throws Exception {
        final ConcreteListProcessor proc = new ConcreteListProcessor();
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.run();

        final long initialTimestamp = System.nanoTime();

        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 0);
        proc.addEntity("name", "id", initialTimestamp);
        proc.addEntity("name", "id2", initialTimestamp);
        runner.run();

        // First run, the above listed entries would be skipped to avoid write synchronization issues
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 0);
        runner.clearTransferState();

        // Ensure we have covered the necessary lag period to avoid issues where the processor was immediately scheduled to run again
        Thread.sleep(DEFAULT_SLEEP_MILLIS);

        // Run again without introducing any new entries
        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 2);
    }

    @Test
    public void testOnlyNewEntriesEmitted() throws Exception {
        final ConcreteListProcessor proc = new ConcreteListProcessor();
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.run();

        final long initialTimestamp = System.nanoTime();

        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 0);
        proc.addEntity("name", "id", initialTimestamp);
        proc.addEntity("name", "id2", initialTimestamp);
        runner.run();

        // First run, the above listed entries would be skipped to avoid write synchronization issues
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 0);
        runner.clearTransferState();

        // Ensure we have covered the necessary lag period to avoid issues where the processor was immediately scheduled to run again
        Thread.sleep(DEFAULT_SLEEP_MILLIS);

        // Running again, our two previously seen files are now cleared to be released
        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 2);
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

        // It should show up
        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 1);
        runner.clearTransferState();
    }

    @Test
    public void testHandleRestartWithEntriesAlreadyTransferredAndNoneNew() throws Exception {
        final ConcreteListProcessor proc = new ConcreteListProcessor();
        final TestRunner runner = TestRunners.newTestRunner(proc);

        final long initialTimestamp = System.nanoTime();

        proc.addEntity("name", "id", initialTimestamp);
        proc.addEntity("name", "id2", initialTimestamp);

        // Emulate having state but not having had the processor run such as in a restart
        final Map<String, String> preexistingState = new HashMap<>();
        preexistingState.put(AbstractListProcessor.LISTING_TIMESTAMP_KEY, Long.toString(initialTimestamp));
        preexistingState.put(AbstractListProcessor.PROCESSED_TIMESTAMP_KEY, Long.toString(initialTimestamp));
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
        final ConcreteListProcessor proc = new ConcreteListProcessor();
        final TestRunner runner = TestRunners.newTestRunner(proc);
        final DistributedCache cache = new DistributedCache();
        runner.addControllerService("cache", cache);
        runner.enableControllerService(cache);
        runner.setProperty(AbstractListProcessor.DISTRIBUTED_CACHE_SERVICE, "cache");

        final long initialTimestamp = System.nanoTime();

        proc.addEntity("name", "id", initialTimestamp);
        runner.run();

        final Map<String, String> expectedState = new HashMap<>();
        // Ensure only timestamp is migrated
        expectedState.put(AbstractListProcessor.LISTING_TIMESTAMP_KEY, String.valueOf(initialTimestamp));
        expectedState.put(AbstractListProcessor.PROCESSED_TIMESTAMP_KEY, "0");
        runner.getStateManager().assertStateEquals(expectedState, Scope.CLUSTER);

        Thread.sleep(DEFAULT_SLEEP_MILLIS);

        runner.run();
        // Ensure only timestamp is migrated
        expectedState.put(AbstractListProcessor.LISTING_TIMESTAMP_KEY, String.valueOf(initialTimestamp));
        expectedState.put(AbstractListProcessor.PROCESSED_TIMESTAMP_KEY, String.valueOf(initialTimestamp));
        runner.getStateManager().assertStateEquals(expectedState, Scope.CLUSTER);
    }

    @Test
    public void testStateMigratedFromCacheService() throws InitializationException {
        final ConcreteListProcessor proc = new ConcreteListProcessor();
        final TestRunner runner = TestRunners.newTestRunner(proc);
        final DistributedCache cache = new DistributedCache();
        runner.addControllerService("cache", cache);
        runner.enableControllerService(cache);
        runner.setProperty(AbstractListProcessor.DISTRIBUTED_CACHE_SERVICE, "cache");

        final String serviceState = "{\"latestTimestamp\":1492,\"matchingIdentifiers\":[\"id\"]}";
        final String cacheKey = runner.getProcessor().getIdentifier() + ".lastListingTime./path";
        cache.stored.put(cacheKey, serviceState);

        runner.run();

        final MockStateManager stateManager = runner.getStateManager();
        final Map<String, String> expectedState = new HashMap<>();
        // Ensure only timestamp is migrated
        expectedState.put(AbstractListProcessor.LISTING_TIMESTAMP_KEY, "1492");
        expectedState.put(AbstractListProcessor.PROCESSED_TIMESTAMP_KEY, "1492");
        stateManager.assertStateEquals(expectedState, Scope.CLUSTER);
    }

    @Test
    public void testNoStateToMigrate() throws Exception {
        final ConcreteListProcessor proc = new ConcreteListProcessor();
        final TestRunner runner = TestRunners.newTestRunner(proc);

        runner.run();

        final MockStateManager stateManager = runner.getStateManager();
        final Map<String, String> expectedState = new HashMap<>();
        stateManager.assertStateEquals(expectedState, Scope.CLUSTER);
    }

    @Test
    public void testStateMigratedFromLocalFile() throws Exception {
        final ConcreteListProcessor proc = new ConcreteListProcessor();
        final TestRunner runner = TestRunners.newTestRunner(proc);

        // Create a file that we will populate with the desired state
        File persistenceFile = testFolder.newFile(proc.persistenceFilename);
        // Override the processor's internal persistence file
        proc.persistenceFile = persistenceFile;

        // Local File persistence was a properties file format of <key>=<JSON entity listing representation>
        // Our ConcreteListProcessor is centered around files which are provided for a given path
        final String serviceState = proc.getPath(runner.getProcessContext()) + "={\"latestTimestamp\":1492,\"matchingIdentifiers\":[\"id\"]}";

        // Create a persistence file of the format anticipated
        try (FileOutputStream fos = new FileOutputStream(persistenceFile);) {
            fos.write(serviceState.getBytes(Charsets.UTF_8));
        }

        runner.run();

        // Verify the local persistence file is removed
        Assert.assertTrue("Failed to remove persistence file", !persistenceFile.exists());

        // Verify the state manager now maintains the associated state
        final Map<String, String> expectedState = new HashMap<>();
        // Ensure only timestamp is migrated
        expectedState.put(AbstractListProcessor.LISTING_TIMESTAMP_KEY, "1492");
        expectedState.put(AbstractListProcessor.PROCESSED_TIMESTAMP_KEY, "1492");
        runner.getStateManager().assertStateEquals(expectedState, Scope.CLUSTER);
    }

    @Test
    public void testResumeListingAfterClearingState() throws Exception {
        final ConcreteListProcessor proc = new ConcreteListProcessor();
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.run();

        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 0);

        final long initialEventTimestamp = System.nanoTime();
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
    public void testFetchOnStart() throws InitializationException {
        final ConcreteListProcessor proc = new ConcreteListProcessor();
        final TestRunner runner = TestRunners.newTestRunner(proc);
        final DistributedCache cache = new DistributedCache();
        runner.addControllerService("cache", cache);
        runner.enableControllerService(cache);
        runner.setProperty(AbstractListProcessor.DISTRIBUTED_CACHE_SERVICE, "cache");

        runner.run();

        assertEquals(1, cache.fetchCount);
    }

    @Test
    public void testOnlyNewStateStored() throws Exception {
        final ConcreteListProcessor proc = new ConcreteListProcessor();
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.run();

        final long initialTimestamp = System.nanoTime();

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
        // Ensure only timestamp is migrated
        assertEquals(2, map.size());
        assertEquals(Long.toString(initialTimestamp), map.get(AbstractListProcessor.LISTING_TIMESTAMP_KEY));
        assertEquals(Long.toString(initialTimestamp), map.get(AbstractListProcessor.PROCESSED_TIMESTAMP_KEY));

        proc.addEntity("new name", "new id", initialTimestamp + 1);
        runner.run();

        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 1);
        runner.clearTransferState();

        StateMap updatedStateMap = runner.getStateManager().getState(Scope.CLUSTER);
        assertEquals(3, updatedStateMap.getVersion());

        assertEquals(2, updatedStateMap.toMap().size());
        assertEquals(Long.toString(initialTimestamp + 1), updatedStateMap.get(AbstractListProcessor.LISTING_TIMESTAMP_KEY));
        // Processed timestamp is now caught up
        assertEquals(Long.toString(initialTimestamp + 1), updatedStateMap.get(AbstractListProcessor.PROCESSED_TIMESTAMP_KEY));
    }

    private static class DistributedCache extends AbstractControllerService implements DistributedMapCacheClient {
        private final Map<Object, Object> stored = new HashMap<>();
        private int fetchCount = 0;

        @Override
        public <K, V> boolean putIfAbsent(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
            return false;
        }

        @Override
        public <K, V> V getAndPutIfAbsent(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer, Deserializer<V> valueDeserializer) throws IOException {
            return null;
        }

        @Override
        public <K> boolean containsKey(K key, Serializer<K> keySerializer) throws IOException {
            return false;
        }

        @Override
        public <K, V> void put(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
            stored.put(key, value);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <K, V> V get(K key, Serializer<K> keySerializer, Deserializer<V> valueDeserializer) throws IOException {
            fetchCount++;
            return (V) stored.get(key);
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public <K> boolean remove(K key, Serializer<K> serializer) throws IOException {
            final Object value = stored.remove(key);
            return value != null;
        }

        @Override
        public long removeByPattern(String regex) throws IOException {
            final List<Object> removedRecords = new ArrayList<>();
            Pattern p = Pattern.compile(regex);
            for (Object key : stored.keySet()) {
                // Key must be backed by something that can be converted into a String
                Matcher m = p.matcher(key.toString());
                if (m.matches()) {
                    removedRecords.add(stored.get(key));
                }
            }
            final long numRemoved = removedRecords.size();
            removedRecords.forEach(stored::remove);
            return numRemoved;
        }
    }


    private static class ConcreteListProcessor extends AbstractListProcessor<ListableEntity> {
        private final List<ListableEntity> entities = new ArrayList<>();

        public final String persistenceFilename = "ListProcessor-local-state-" + UUID.randomUUID().toString() + ".json";
        public String persistenceFolder = "target/";
        public File persistenceFile = new File(persistenceFolder + persistenceFilename);

        @Override
        public File getPersistenceFile() {
            return persistenceFile;
        }

        public void addEntity(final String name, final String identifier, final long timestamp) {
            final ListableEntity entity = new ListableEntity() {
                @Override
                public String getName() {
                    return name;
                }

                @Override
                public String getIdentifier() {
                    return identifier;
                }

                @Override
                public long getTimestamp() {
                    return timestamp;
                }
            };

            entities.add(entity);
        }

        @Override
        protected Map<String, String> createAttributes(final ListableEntity entity, final ProcessContext context) {
            return Collections.emptyMap();
        }

        @Override
        protected String getPath(final ProcessContext context) {
            return "/path";
        }

        @Override
        protected List<ListableEntity> performListing(final ProcessContext context, final Long minTimestamp) throws IOException {
            return Collections.unmodifiableList(entities);
        }

        @Override
        protected boolean isListingResetNecessary(PropertyDescriptor property) {
            return false;
        }

        @Override
        protected Scope getStateScope(final ProcessContext context) {
            return Scope.CLUSTER;
        }
    }
}
