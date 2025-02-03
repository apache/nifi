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

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.glassfish.jersey.internal.guava.Predicates;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestAbstractListProcessor {

    private ConcreteListProcessor proc;
    private TestRunner runner;

    @BeforeEach
    public void setup() {
        proc = new ConcreteListProcessor();
        runner = TestRunners.newTestRunner(proc);
    }

    @Test
    public void testStateMigratedWhenPrimaryNodeSwitch() throws IOException {
        // add a few entities
        for (int i = 0; i < 5; i++) {
            proc.addEntity(String.valueOf(i), String.valueOf(i), 88888L);
        }

        // Add an entity with a later timestamp
        proc.addEntity("10", "10", 99999999L);

        // Run the processor. All 6 should be listed.
        runner.run();
        runner.assertAllFlowFilesTransferred(AbstractListProcessor.REL_SUCCESS, 6);

        // Now, we want to mimic Primary Node changing. To do so, we'll capture the Cluster State from the State Manager,
        // create a new Processor, and set the state to be the same, and update the processor in order to produce the same listing.
        final ConcreteListProcessor secondProc = new ConcreteListProcessor();
        // Add same listing to the new processor
        for (int i = 0; i < 5; i++) {
            secondProc.addEntity(String.valueOf(i), String.valueOf(i), 88888L);
        }
        secondProc.addEntity("10", "10", 99999999L);
        // Create new runner for the second processor and update its state to match that of the last TestRunner.
        final StateMap stateMap = runner.getStateManager().getState(Scope.CLUSTER);
        runner = TestRunners.newTestRunner(secondProc);
        runner.getStateManager().setState(stateMap.toMap(), Scope.CLUSTER);

        // Run several times, ensuring that nothing is emitted.
        for (int i = 0; i < 10; i++) {
            runner.run();
            runner.assertAllFlowFilesTransferred(AbstractListProcessor.REL_SUCCESS, 0);
        }

        // Add one more entry and ensure that it's emitted.
        secondProc.addEntity("new", "new", 999999990L);
        runner.run();
        runner.assertAllFlowFilesTransferred(AbstractListProcessor.REL_SUCCESS, 1);
    }

    @Test
    public void testNoStateToMigrate() {
        runner.run();

        final MockStateManager stateManager = runner.getStateManager();
        final Map<String, String> expectedState = new HashMap<>();
        stateManager.assertStateEquals(expectedState, Scope.CLUSTER);
    }

    @Test
    public void testWriteRecords() throws InitializationException {
        final RecordSetWriterFactory writerFactory = new MockRecordWriter("id,name,timestamp,size", false);
        runner.addControllerService("record-writer", writerFactory);
        runner.enableControllerService(writerFactory);

        runner.setProperty(AbstractListProcessor.RECORD_WRITER, "record-writer");

        runner.run();

        assertEquals(0, runner.getFlowFilesForRelationship(AbstractListProcessor.REL_SUCCESS).size());
        proc.addEntity("name", "identifier", 4L);
        proc.addEntity("name2", "identifier2", 8L);

        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractListProcessor.REL_SUCCESS, 1);

        final MockFlowFile flowfile = runner.getFlowFilesForRelationship(AbstractListProcessor.REL_SUCCESS).get(0);
        flowfile.assertAttributeEquals("record.count", "2");
        flowfile.assertContentEquals("id,name,timestamp,size\nidentifier,name,4,0\nidentifier2,name2,8,0\n");

        runner.clearTransferState();
        runner.run();
        runner.assertAllFlowFilesTransferred(AbstractListProcessor.REL_SUCCESS, 0);
    }

    @Test
    public void testNoTrackingEntityStrategy() throws IOException {

        // Firstly, choose Timestamp Strategy lists 2 entities and set state.
        // After that choose No Tracking Strategy to test if this strategy remove the state.
        ProcessSession session = runner.getProcessSessionFactory().createSession();
        ProcessContext context = runner.getProcessContext();

        runner.setProperty(AbstractListProcessor.LISTING_STRATEGY, AbstractListProcessor.BY_TIMESTAMPS);

        // two entities listed
        proc.addEntity("one", "firstFile", 1585344381476L);
        proc.addEntity("two", "secondFile", 1585344381475L);

        assertVerificationOutcome(Outcome.SUCCESSFUL, ".* Found 2 objects.  Of those, 2 match the filter.");

        runner.run();
        assertEquals(2, runner.getFlowFilesForRelationship(AbstractListProcessor.REL_SUCCESS).size());
        assertEquals(2, proc.entities.size());
        assertVerificationOutcome(Outcome.SUCCESSFUL, ".* Found 2 objects.  Of those, 2 match the filter.");

        final MockStateManager stateManager = runner.getStateManager();
        final Map<String, String> expectedState = new HashMap<>();
        final Map<String, String> realState = new HashMap<>();

        realState.put(AbstractListProcessor.LATEST_LISTED_ENTRY_TIMESTAMP_KEY, String.valueOf(proc.entities.get("firstFile").getTimestamp()));
        realState.put(AbstractListProcessor.LAST_PROCESSED_LATEST_ENTRY_TIMESTAMP_KEY, String.valueOf(proc.entities.get("secondFile").getTimestamp()));
        realState.put(AbstractListProcessor.IDENTIFIER_PREFIX + ".0", proc.entities.get("firstFile").getIdentifier());
        realState.put(AbstractListProcessor.IDENTIFIER_PREFIX + ".1", proc.entities.get("secondFile").getIdentifier());

        stateManager.setState(realState, Scope.CLUSTER);

        // Ensure timestamp and identifies are migrated
        expectedState.put(AbstractListProcessor.LATEST_LISTED_ENTRY_TIMESTAMP_KEY, String.valueOf(proc.entities.get("firstFile").getTimestamp()));
        expectedState.put(AbstractListProcessor.LAST_PROCESSED_LATEST_ENTRY_TIMESTAMP_KEY, String.valueOf(proc.entities.get("secondFile").getTimestamp()));
        expectedState.put(AbstractListProcessor.IDENTIFIER_PREFIX + ".0", proc.entities.get("firstFile").getIdentifier());
        expectedState.put(AbstractListProcessor.IDENTIFIER_PREFIX + ".1", proc.entities.get("secondFile").getIdentifier());

        runner.getStateManager().assertStateEquals(expectedState, Scope.CLUSTER);

        // Change listing strategy
        runner.setProperty(AbstractListProcessor.LISTING_STRATEGY, AbstractListProcessor.NO_TRACKING);

        // Clear any listed entities after choose No Tracking Strategy
        proc.entities.clear();
        assertVerificationOutcome(Outcome.SUCCESSFUL, ".* Found no objects.");

        // Add new entity
        proc.addEntity("one", "firstFile", 1585344381476L);
        proc.listByTrackingTimestamps(context, session);

        // Test if state cleared or not
        runner.getStateManager().assertStateNotEquals(expectedState, Scope.CLUSTER);
        assertEquals(1, proc.entities.size());
        assertVerificationOutcome(Outcome.SUCCESSFUL, ".* Found 1 object.  Of that, 1 matches the filter.");
    }

    @Test
    public void testEntityTrackingStrategy() throws InitializationException {
        runner.setProperty(AbstractListProcessor.LISTING_STRATEGY, AbstractListProcessor.BY_ENTITIES);
        // Require a cache service.
        runner.assertNotValid();

        final EphemeralMapCacheClientService trackingCache = new EphemeralMapCacheClientService();
        runner.addControllerService("tracking-cache", trackingCache);
        runner.enableControllerService(trackingCache);

        runner.setProperty(ListedEntityTracker.TRACKING_STATE_CACHE, "tracking-cache");
        runner.setProperty(ListedEntityTracker.TRACKING_TIME_WINDOW, "10ms");

        runner.assertValid();

        proc.currentTimestamp.set(0L);
        runner.run();
        assertEquals(0, runner.getFlowFilesForRelationship(AbstractListProcessor.REL_SUCCESS).size());

        // Should list one entity.
        proc.addEntity("one", "one", 1, 1);
        proc.currentTimestamp.set(1L);
        runner.clearTransferState();
        // Prior to running the processor, we should expect 3 objects during verification
        assertVerificationOutcome(Outcome.SUCCESSFUL, "Successfully listed contents of .*\\.\\s*" +
                "Found 1 object.  Of that, 1 matches the filter.");
        runner.run();
        assertEquals(1, runner.getFlowFilesForRelationship(AbstractListProcessor.REL_SUCCESS).size());
        runner.getFlowFilesForRelationship(AbstractListProcessor.REL_SUCCESS).get(0)
            .assertAttributeEquals(CoreAttributes.FILENAME.key(), "one");
        // The object is now tracked, so it's no longer considered new
        assertVerificationOutcome(Outcome.SUCCESSFUL, "Successfully listed contents of .*\\.\\s*" +
                "Found 1 object.  Of that, 1 matches the filter.");

        // Should not list any entity.
        proc.currentTimestamp.set(2L);
        runner.clearTransferState();
        assertVerificationOutcome(Outcome.SUCCESSFUL, "Successfully listed contents of .*\\.\\s*" +
                "Found 1 object.  Of that, 1 matches the filter.");
        runner.run();
        assertEquals(0, runner.getFlowFilesForRelationship(AbstractListProcessor.REL_SUCCESS).size());

        // Should list added entities.
        proc.currentTimestamp.set(10L);
        proc.addEntity("five", "five", 5, 5);
        proc.addEntity("six", "six", 6, 6);
        runner.clearTransferState();
        assertVerificationOutcome(Outcome.SUCCESSFUL, "Successfully listed contents of .*\\.\\s*" +
                "Found 3 objects.  Of those, 3 match the filter.");
        runner.run();
        assertEquals(2, runner.getFlowFilesForRelationship(AbstractListProcessor.REL_SUCCESS).size());
        runner.getFlowFilesForRelationship(AbstractListProcessor.REL_SUCCESS).get(0)
                .assertAttributeEquals(CoreAttributes.FILENAME.key(), "five");
        runner.getFlowFilesForRelationship(AbstractListProcessor.REL_SUCCESS).get(1)
                .assertAttributeEquals(CoreAttributes.FILENAME.key(), "six");

        // Should be able to list entities having older timestamp than the previously listed entity.
        // But if its timestamp is out of tracking window, then it won't be picked.
        // Current timestamp = 13, and window = 10ms, meaning it can pick entities having timestamp 3 to 13.
        proc.currentTimestamp.set(13L);
        proc.addEntity("two", "two", 2, 2);
        proc.addEntity("three", "three", 3, 3);
        proc.addEntity("four", "four", 4, 4);
        runner.clearTransferState();
        assertVerificationOutcome(Outcome.SUCCESSFUL, "Successfully listed contents of .*\\.\\s*" +
                "Found 6 objects.  Of those, 6 match the filter.");
        runner.run();
        assertEquals(2, runner.getFlowFilesForRelationship(AbstractListProcessor.REL_SUCCESS).size());
        runner.getFlowFilesForRelationship(AbstractListProcessor.REL_SUCCESS).get(0)
                .assertAttributeEquals(CoreAttributes.FILENAME.key(), "three");
        runner.getFlowFilesForRelationship(AbstractListProcessor.REL_SUCCESS).get(1)
                .assertAttributeEquals(CoreAttributes.FILENAME.key(), "four");

        // Can pick entity that has newer timestamp.
        // Can pick entity that has different size.
        proc.currentTimestamp.set(14L);
        proc.addEntity("five", "five", 7, 5);
        proc.addEntity("six", "six", 6, 16);
        runner.clearTransferState();
        assertVerificationOutcome(Outcome.SUCCESSFUL, "Successfully listed contents of .*\\.\\s*" +
                "Found 6 objects.  Of those, 6 match the filter.");
        runner.run();
        assertEquals(2, runner.getFlowFilesForRelationship(AbstractListProcessor.REL_SUCCESS).size());
        runner.getFlowFilesForRelationship(AbstractListProcessor.REL_SUCCESS).get(0)
                .assertAttributeEquals(CoreAttributes.FILENAME.key(), "six");
        runner.getFlowFilesForRelationship(AbstractListProcessor.REL_SUCCESS).get(1)
                .assertAttributeEquals(CoreAttributes.FILENAME.key(), "five");

        // Reset state.
        // Current timestamp = 15, and window = 11ms, meaning it can pick entities having timestamp 4 to 15.
        proc.currentTimestamp.set(15L);
        // ConcreteListProcessor can reset state with any property.
        runner.setProperty(ListedEntityTracker.TRACKING_TIME_WINDOW, "11ms");
        runner.setProperty(ConcreteListProcessor.RESET_STATE, "1");
        runner.setProperty(ListedEntityTracker.INITIAL_LISTING_TARGET, "window");
        runner.clearTransferState();

        // Prior to running the processor, we should expect 3 objects during verification
        assertVerificationOutcome(Outcome.SUCCESSFUL, "Successfully listed contents of .*\\.\\s*" +
                "Found 6 objects.  Of those, 6 match the filter.");
        runner.run();

        assertEquals(3, runner.getFlowFilesForRelationship(AbstractListProcessor.REL_SUCCESS).size());
        runner.getFlowFilesForRelationship(AbstractListProcessor.REL_SUCCESS).get(0)
                .assertAttributeEquals(CoreAttributes.FILENAME.key(), "four");
        runner.getFlowFilesForRelationship(AbstractListProcessor.REL_SUCCESS).get(1)
                .assertAttributeEquals(CoreAttributes.FILENAME.key(), "six");
        runner.getFlowFilesForRelationship(AbstractListProcessor.REL_SUCCESS).get(2)
                .assertAttributeEquals(CoreAttributes.FILENAME.key(), "five");

        // Reset state again.
        proc.currentTimestamp.set(20L);
        // ConcreteListProcessor can reset state with any property.
        runner.setProperty(ListedEntityTracker.INITIAL_LISTING_TARGET, "all");
        runner.setProperty(ConcreteListProcessor.RESET_STATE, "2");
        runner.clearTransferState();

        assertVerificationOutcome(Outcome.SUCCESSFUL, "Successfully listed contents of .*\\.\\s*" +
                "Found 6 objects.  Of those, 6 match the filter.");

        runner.run();
        // All entities should be picked, one to six.
        assertEquals(6, runner.getFlowFilesForRelationship(AbstractListProcessor.REL_SUCCESS).size());
        // Now all are tracked, so none are new
        assertVerificationOutcome(Outcome.SUCCESSFUL, "Successfully listed contents of .*\\.\\s*" +
                "Found 6 objects.  Of those, 6 match the filter.");

        // Reset state again.
        proc.currentTimestamp.set(25L);
        runner.setProperty(ListedEntityTracker.INITIAL_LISTING_TARGET, "window");
        runner.setProperty(ListedEntityTracker.TRACKING_TIME_WINDOW, "20ms");
        runner.setProperty(ConcreteListProcessor.LISTING_FILTER, "f[a-z]+"); // Match only four and five
        runner.setProperty(ConcreteListProcessor.RESET_STATE, "3");
        runner.clearTransferState();

        // Time window is now 5ms - 25ms, so only 5 and 6 fall in the window, so only 1 of the 2 filtered entities are considered 'new'
        assertVerificationOutcome(Outcome.SUCCESSFUL, "Successfully listed contents of .*\\.\\s*" +
                "Found 6 objects.  Of those, 2 match the filter.");
    }

    private void assertVerificationOutcome(final Outcome expectedOutcome, final String expectedExplanationRegex) {
        final List<ConfigVerificationResult> results = proc.verify(runner.getProcessContext(), runner.getLogger(), Collections.emptyMap());

        assertEquals(1, results.size());
        final ConfigVerificationResult result = results.get(0);
        assertEquals(expectedOutcome, result.getOutcome());
        assertTrue(result.getExplanation().matches(expectedExplanationRegex),
                String.format("Expected verification result to match pattern [%s].  Actual explanation was: %s", expectedExplanationRegex, result.getExplanation()));
    }

    static class EphemeralMapCacheClientService extends AbstractControllerService implements DistributedMapCacheClient {
        private final Map<Object, Object> stored = new HashMap<>();

        @Override
        public <K, V> boolean putIfAbsent(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
            return false;
        }

        @Override
        public <K, V> V getAndPutIfAbsent(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer, Deserializer<V> valueDeserializer) {
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
    }

    static class ConcreteListProcessor extends AbstractListProcessor<ListableEntity> {
        final Map<String, ListableEntity> entities = new HashMap<>();

        private static final PropertyDescriptor RESET_STATE = new PropertyDescriptor.Builder()
                .name("reset-state")
                .addValidator(Validator.VALID)
                .build();
        private static final PropertyDescriptor LISTING_FILTER = new PropertyDescriptor.Builder()
                .name("listing-filter")
                .displayName("Listing Filter")
                .description("Filters listed entities by name.")
                .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
                .build();

        final AtomicReference<Long> currentTimestamp = new AtomicReference<>();

        @Override
        protected ListedEntityTracker<ListableEntity> createListedEntityTracker() {
            return new ListedEntityTracker<>(getIdentifier(), getLogger(), currentTimestamp::get, null);
        }

        @Override
        protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            final List<PropertyDescriptor> properties = new ArrayList<>();
            properties.add(LISTING_STRATEGY);
            properties.add(RECORD_WRITER);
            properties.add(TARGET_SYSTEM_TIMESTAMP_PRECISION);
            properties.add(ListedEntityTracker.TRACKING_STATE_CACHE);
            properties.add(ListedEntityTracker.TRACKING_TIME_WINDOW);
            properties.add(ListedEntityTracker.INITIAL_LISTING_TARGET);
            properties.add(RESET_STATE);
            properties.add(LISTING_FILTER);
            return properties;
        }

        public void addEntity(final String name, final String identifier, final long timestamp) {
            addEntity(name, identifier, timestamp, 0);
        }

        public void addEntity(final String name, final String identifier, final long timestamp, long size) {
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

                @Override
                public long getSize() {
                    return size;
                }

                @Override
                public Record toRecord() {
                    final Map<String, Object> values = new HashMap<>(4);
                    values.put("id", identifier);
                    values.put("name", name);
                    values.put("timestamp", timestamp);
                    values.put("size", size);
                    return new MapRecord(getRecordSchema(), values);
                }
            };

            entities.put(entity.getIdentifier(), entity);
        }

        @Override
        protected Map<String, String> createAttributes(final ListableEntity entity, final ProcessContext context) {
            final Map<String, String> attributes = new HashMap<>();
            attributes.put(CoreAttributes.FILENAME.key(), entity.getIdentifier());
            return attributes;
        }

        @Override
        protected String getPath(final ProcessContext context) {
            return "/path";
        }

        @Override
        protected List<ListableEntity> performListing(final ProcessContext context, final Long minTimestamp, ListingMode listingMode) {
            final PropertyValue listingFilter = context.getProperty(LISTING_FILTER);
            Predicate<ListableEntity> filter = listingFilter.isSet()
                    ? entity -> entity.getName().matches(listingFilter.getValue())
                    : Predicates.alwaysTrue();
            return getEntityList().stream().filter(filter).collect(Collectors.toList());
        }

        @Override
        protected Integer countUnfilteredListing(final ProcessContext context) {
            return entities.size();
        }

        List<ListableEntity> getEntityList() {
            return entities.values().stream().sorted(Comparator.comparing(ListableEntity::getTimestamp)).collect(Collectors.toList());
        }

        @Override
        protected boolean isListingResetNecessary(PropertyDescriptor property) {
            return RESET_STATE.equals(property);
        }

        @Override
        protected String getListingContainerName(final ProcessContext context) {
            return String.format("In-memory entity collection [%s]", entities);
        }

        @Override
        protected Scope getStateScope(final PropertyContext context) {
            return Scope.CLUSTER;
        }

        @Override
        protected RecordSchema getRecordSchema() {
            final List<RecordField> fields = new ArrayList<>();
            fields.add(new RecordField("id", RecordFieldType.STRING.getDataType()));
            fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
            fields.add(new RecordField("timestamp", RecordFieldType.TIMESTAMP.getDataType()));
            fields.add(new RecordField("size", RecordFieldType.LONG.getDataType()));
            return new SimpleRecordSchema(fields);
        }
    }
}
