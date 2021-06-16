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

package org.apache.nifi.provenance.store;

import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.provenance.EventIdFirstSchemaRecordWriter;
import org.apache.nifi.provenance.IdentifierLookup;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.provenance.RepositoryConfiguration;
import org.apache.nifi.provenance.StandardProvenanceEventRecord;
import org.apache.nifi.provenance.authorization.EventAuthorizer;
import org.apache.nifi.provenance.authorization.EventTransformer;
import org.apache.nifi.provenance.serialization.RecordReaders;
import org.apache.nifi.provenance.serialization.RecordWriters;
import org.apache.nifi.provenance.serialization.StorageSummary;
import org.apache.nifi.provenance.store.iterator.EventIterator;
import org.apache.nifi.provenance.toc.StandardTocWriter;
import org.apache.nifi.provenance.toc.TocUtil;
import org.apache.nifi.provenance.toc.TocWriter;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestPartitionedWriteAheadEventStore {
    private static final RecordWriterFactory writerFactory = (file, idGen, compress, createToc) -> RecordWriters.newSchemaRecordWriter(file, idGen, compress, createToc);
    private static final RecordReaderFactory readerFactory = (file, logs, maxChars) -> RecordReaders.newRecordReader(file, logs, maxChars);

    private final AtomicLong idGenerator = new AtomicLong(0L);

    @Rule
    public TestName testName = new TestName();

    @Before
    public void resetIds() {
        idGenerator.set(0L);
    }


    @Test
    @Ignore
    public void testPerformanceOfAccessingEvents() throws Exception {
        final RecordWriterFactory recordWriterFactory = (file, idGenerator, compressed, createToc) -> {
            final TocWriter tocWriter = createToc ? new StandardTocWriter(TocUtil.getTocFile(file), false, false) : null;
            return new EventIdFirstSchemaRecordWriter(file, idGenerator, tocWriter, compressed, 1024 * 1024, IdentifierLookup.EMPTY);
        };

        final RecordReaderFactory recordReaderFactory = (file, logs, maxChars) -> RecordReaders.newRecordReader(file, logs, maxChars);
        final PartitionedWriteAheadEventStore store = new PartitionedWriteAheadEventStore(createConfig(),
            recordWriterFactory, recordReaderFactory, EventReporter.NO_OP, new EventFileManager());
        store.initialize();

        assertEquals(-1, store.getMaxEventId());
        for (int i = 0; i < 100_000; i++) {
            final ProvenanceEventRecord event1 = createEvent();
            store.addEvents(Collections.singleton(event1));
        }

        final List<Long> eventIdList = Arrays.asList(4L, 80L, 1024L, 40_000L, 80_000L, 99_000L);

        while (true) {
            for (int i = 0; i < 100; i++) {
                time(() -> store.getEvents(eventIdList, EventAuthorizer.GRANT_ALL, EventTransformer.EMPTY_TRANSFORMER), "Fetch Events");
            }

            Thread.sleep(1000L);
        }
    }

    private void time(final Callable<?> task, final String taskDescription) throws Exception {
        final long start = System.nanoTime();
        task.call();
        final long nanos = System.nanoTime() - start;
        final long millis = TimeUnit.NANOSECONDS.toMillis(nanos);
        System.out.println("Took " + millis + " ms to " + taskDescription);
    }

    @Test
    public void testSingleWriteThenRead() throws IOException {
        final PartitionedWriteAheadEventStore store = new PartitionedWriteAheadEventStore(createConfig(), writerFactory, readerFactory, EventReporter.NO_OP, new EventFileManager());
        store.initialize();

        assertEquals(-1, store.getMaxEventId());
        final ProvenanceEventRecord event1 = createEvent();
        final StorageResult result = store.addEvents(Collections.singleton(event1));

        final StorageSummary summary = result.getStorageLocations().values().iterator().next();
        final long eventId = summary.getEventId();
        final ProvenanceEventRecord eventWithId = addId(event1, eventId);

        assertEquals(0, store.getMaxEventId());

        final ProvenanceEventRecord read = store.getEvent(eventId).get();
        assertEquals(eventWithId, read);
    }

    @Test
    public void testMultipleWritesThenReads() throws IOException {
        final PartitionedWriteAheadEventStore store = new PartitionedWriteAheadEventStore(createConfig(), writerFactory, readerFactory, EventReporter.NO_OP, new EventFileManager());
        store.initialize();
        assertEquals(-1, store.getMaxEventId());

        final int numEvents = 20;
        final List<ProvenanceEventRecord> events = new ArrayList<>(numEvents);
        for (int i = 0; i < numEvents; i++) {
            final ProvenanceEventRecord event = createEvent();
            store.addEvents(Collections.singleton(event));
            assertEquals(i, store.getMaxEventId());

            events.add(event);
        }

        for (int i = 0; i < numEvents; i++) {
            final ProvenanceEventRecord read = store.getEvent(i).get();
            assertEquals(events.get(i), read);
        }
    }


    @Test()
    public void testMultipleWritesThenGetAllInSingleRead() throws IOException {
        final PartitionedWriteAheadEventStore store = new PartitionedWriteAheadEventStore(createConfig(), writerFactory, readerFactory, EventReporter.NO_OP, new EventFileManager());
        store.initialize();
        assertEquals(-1, store.getMaxEventId());

        final int numEvents = 20;
        final List<ProvenanceEventRecord> events = new ArrayList<>(numEvents);
        for (int i = 0; i < numEvents; i++) {
            final ProvenanceEventRecord event = createEvent();
            store.addEvents(Collections.singleton(event));
            assertEquals(i, store.getMaxEventId());

            events.add(event);
        }

        List<ProvenanceEventRecord> eventsRead = store.getEvents(0L, numEvents, null, EventTransformer.EMPTY_TRANSFORMER);
        assertNotNull(eventsRead);

        assertEquals(numEvents, eventsRead.size());
        for (int i = 0; i < numEvents; i++) {
            final ProvenanceEventRecord read = eventsRead.get(i);
            assertEquals(events.get(i), read);
        }

        eventsRead = store.getEvents(-1000, 1000, null, EventTransformer.EMPTY_TRANSFORMER);
        assertNotNull(eventsRead);
        assertTrue(eventsRead.isEmpty());

        eventsRead = store.getEvents(10, 0, null, EventTransformer.EMPTY_TRANSFORMER);
        assertNotNull(eventsRead);
        assertTrue(eventsRead.isEmpty());

        eventsRead = store.getEvents(10, 1, null, EventTransformer.EMPTY_TRANSFORMER);
        assertNotNull(eventsRead);
        assertFalse(eventsRead.isEmpty());
        assertEquals(1, eventsRead.size());
        assertEquals(events.get(10), eventsRead.get(0));

        eventsRead = store.getEvents(20, 1000, null, EventTransformer.EMPTY_TRANSFORMER);
        assertNotNull(eventsRead);
        assertTrue(eventsRead.isEmpty());
    }

    @Test
    public void testGetSize() throws IOException {
        final PartitionedWriteAheadEventStore store = new PartitionedWriteAheadEventStore(createConfig(), writerFactory, readerFactory, EventReporter.NO_OP, new EventFileManager());
        store.initialize();

        long storeSize = 0L;
        final int numEvents = 20;
        for (int i = 0; i < numEvents; i++) {
            final ProvenanceEventRecord event = createEvent();
            store.addEvents(Collections.singleton(event));
            final long newSize = store.getSize();
            assertTrue(newSize > storeSize);
            storeSize = newSize;
        }
    }

    @Test
    public void testMaxEventIdRestored() throws IOException {
        final RepositoryConfiguration config = createConfig();
        final PartitionedWriteAheadEventStore store = new PartitionedWriteAheadEventStore(config, writerFactory, readerFactory, EventReporter.NO_OP, new EventFileManager());
        store.initialize();

        final int numEvents = 20;
        for (int i = 0; i < numEvents; i++) {
            final ProvenanceEventRecord event = createEvent();
            store.addEvents(Collections.singleton(event));
        }

        assertEquals(19, store.getMaxEventId());
        store.close();

        final PartitionedWriteAheadEventStore recoveredStore = new PartitionedWriteAheadEventStore(config, writerFactory, readerFactory, EventReporter.NO_OP, new EventFileManager());
        recoveredStore.initialize();
        assertEquals(19, recoveredStore.getMaxEventId());
    }

    @Test
    public void testGetEvent() throws IOException {
        final RepositoryConfiguration config = createConfig();
        final PartitionedWriteAheadEventStore store = new PartitionedWriteAheadEventStore(config, writerFactory, readerFactory, EventReporter.NO_OP, new EventFileManager());
        store.initialize();

        final int numEvents = 20;
        final List<ProvenanceEventRecord> events = new ArrayList<>(numEvents);
        for (int i = 0; i < numEvents; i++) {
            final ProvenanceEventRecord event = createEvent();
            store.addEvents(Collections.singleton(event));
            events.add(event);
        }

        // Ensure that each event is retrieved successfully.
        for (int i = 0; i < numEvents; i++) {
            final ProvenanceEventRecord event = store.getEvent(i).get();
            assertEquals(events.get(i), event);
        }

        assertFalse(store.getEvent(-1L).isPresent());
        assertFalse(store.getEvent(20L).isPresent());
    }

    @Test
    public void testGetEventsWithMinIdAndCount() throws IOException {
        final RepositoryConfiguration config = createConfig();
        config.setMaxEventFileCount(100);
        final PartitionedWriteAheadEventStore store = new PartitionedWriteAheadEventStore(config, writerFactory, readerFactory, EventReporter.NO_OP, new EventFileManager());
        store.initialize();

        final int numEvents = 50_000;
        final List<ProvenanceEventRecord> events = new ArrayList<>(numEvents);
        for (int i = 0; i < numEvents; i++) {
            final ProvenanceEventRecord event = createEvent();
            store.addEvents(Collections.singleton(event));
            if (i < 1000) {
                events.add(event);
            }
        }

        assertTrue(store.getEvents(-1000L, 1000).isEmpty());
        assertEquals(events, store.getEvents(0, events.size()));
        assertEquals(events, store.getEvents(-30, events.size()));
        assertEquals(events.subList(10, events.size()), store.getEvents(10L, events.size() - 10));
        assertTrue(store.getEvents(numEvents, 100).isEmpty());
    }

    @Test
    public void testGetEventsWithMinIdAndCountWithAuthorizer() throws IOException {
        final RepositoryConfiguration config = createConfig();
        final PartitionedWriteAheadEventStore store = new PartitionedWriteAheadEventStore(config, writerFactory, readerFactory, EventReporter.NO_OP, new EventFileManager());
        store.initialize();

        final int numEvents = 20;
        final List<ProvenanceEventRecord> events = new ArrayList<>(numEvents);
        for (int i = 0; i < numEvents; i++) {
            final ProvenanceEventRecord event = createEvent();
            store.addEvents(Collections.singleton(event));
            events.add(event);
        }

        final EventAuthorizer allowEventNumberedEventIds = new EventAuthorizer() {
            @Override
            public boolean isAuthorized(final ProvenanceEventRecord event) {
                return event.getEventId() % 2 == 0L;
            }

            @Override
            public void authorize(ProvenanceEventRecord event) throws AccessDeniedException {
                if (!isAuthorized(event)) {
                    throw new AccessDeniedException();
                }
            }
        };

        final List<ProvenanceEventRecord> storedEvents = store.getEvents(0, 20, allowEventNumberedEventIds, EventTransformer.EMPTY_TRANSFORMER);
        assertEquals(numEvents / 2, storedEvents.size());
        for (int i = 0; i < storedEvents.size(); i++) {
            assertEquals(events.get(i * 2), storedEvents.get(i));
        }
    }


    @Test
    public void testGetEventsWithStartOffsetAndCountWithNothingAuthorized() throws IOException {
        final RepositoryConfiguration config = createConfig();
        final PartitionedWriteAheadEventStore store = new PartitionedWriteAheadEventStore(config, writerFactory, readerFactory, EventReporter.NO_OP, new EventFileManager());
        store.initialize();

        final int numEvents = 20;
        final List<ProvenanceEventRecord> events = new ArrayList<>(numEvents);
        for (int i = 0; i < numEvents; i++) {
            final ProvenanceEventRecord event = createEvent();
            store.addEvents(Collections.singleton(event));
            events.add(event);
        }

        final EventAuthorizer allowEventNumberedEventIds = EventAuthorizer.DENY_ALL;
        final List<ProvenanceEventRecord> storedEvents = store.getEvents(0, 20, allowEventNumberedEventIds, EventTransformer.EMPTY_TRANSFORMER);
        assertTrue(storedEvents.isEmpty());
    }

    @Test
    public void testGetSpecificEventIds() throws IOException {
        final RepositoryConfiguration config = createConfig();
        final PartitionedWriteAheadEventStore store = new PartitionedWriteAheadEventStore(config, writerFactory, readerFactory, EventReporter.NO_OP, new EventFileManager());
        store.initialize();

        final int numEvents = 20;
        final List<ProvenanceEventRecord> events = new ArrayList<>(numEvents);
        for (int i = 0; i < numEvents; i++) {
            final ProvenanceEventRecord event = createEvent();
            store.addEvents(Collections.singleton(event));
            events.add(event);
        }

        final EventAuthorizer allowEvenNumberedEventIds = new EventAuthorizer() {
            @Override
            public boolean isAuthorized(final ProvenanceEventRecord event) {
                return event.getEventId() % 2 == 0L;
            }

            @Override
            public void authorize(ProvenanceEventRecord event) throws AccessDeniedException {
                if (!isAuthorized(event)) {
                    throw new AccessDeniedException();
                }
            }
        };

        final List<Long> evenEventIds = new ArrayList<>();
        final List<Long> oddEventIds = new ArrayList<>();
        final List<Long> allEventIds = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            final Long id = Long.valueOf(i);
            allEventIds.add(id);

            if (i % 2 == 0) {
                evenEventIds.add(id);
            } else {
                oddEventIds.add(id);
            }
        }

        final List<ProvenanceEventRecord> storedEvents = store.getEvents(evenEventIds, allowEvenNumberedEventIds, EventTransformer.EMPTY_TRANSFORMER);
        assertEquals(numEvents / 2, storedEvents.size());
        for (int i = 0; i < storedEvents.size(); i++) {
            assertEquals(events.get(i * 2), storedEvents.get(i));
        }

        assertTrue(store.getEvents(oddEventIds, allowEvenNumberedEventIds, EventTransformer.EMPTY_TRANSFORMER).isEmpty());

        final List<ProvenanceEventRecord> allStoredEvents = store.getEvents(allEventIds, EventAuthorizer.GRANT_ALL, EventTransformer.EMPTY_TRANSFORMER);
        assertEquals(events, allStoredEvents);
    }


    @Test
    public void testWriteAfterRecoveringRepo() throws IOException {
        final RepositoryConfiguration config = createConfig();
        final PartitionedWriteAheadEventStore store = new PartitionedWriteAheadEventStore(config, writerFactory, readerFactory, EventReporter.NO_OP, new EventFileManager());
        store.initialize();

        for (int i = 0; i < 4; i++) {
            store.addEvents(Collections.singleton(createEvent()));
        }

        store.close();

        final PartitionedWriteAheadEventStore recoveredStore = new PartitionedWriteAheadEventStore(config, writerFactory, readerFactory, EventReporter.NO_OP, new EventFileManager());
        recoveredStore.initialize();

        List<ProvenanceEventRecord> recoveredEvents = recoveredStore.getEvents(0, 10);
        assertEquals(4, recoveredEvents.size());

        // ensure that we can still write to the store
        for (int i = 0; i < 4; i++) {
            recoveredStore.addEvents(Collections.singleton(createEvent()));
        }

        recoveredEvents = recoveredStore.getEvents(0, 10);
        assertEquals(8, recoveredEvents.size());

        for (int i = 0; i < 8; i++) {
            assertEquals(i, recoveredEvents.get(i).getEventId());
        }
    }


    @Test
    public void testGetEventsByTimestamp() throws IOException {
        final RepositoryConfiguration config = createConfig();
        config.setMaxEventFileCount(300);
        config.setCompressOnRollover(false);

        final PartitionedWriteAheadEventStore store = new PartitionedWriteAheadEventStore(config, writerFactory, readerFactory, EventReporter.NO_OP, new EventFileManager());
        store.initialize();

        for (int i = 0; i < 1_000; i++) {
            final ProvenanceEventRecord event = createEvent();
            final ProvenanceEventRecord withTimestamp = new StandardProvenanceEventRecord.Builder()
                .fromEvent(event)
                .setEventTime(i)
                .build();

            store.addEvents(Collections.singleton(withTimestamp));
        }

        final EventIterator iterator = store.getEventsByTimestamp(200, 799);

        int count = 0;
        Optional<ProvenanceEventRecord> optionalRecord;
        while ((optionalRecord = iterator.nextEvent()).isPresent()) {
            final ProvenanceEventRecord event = optionalRecord.get();
            final long timestamp = event.getEventTime();
            assertTrue(timestamp >= 200);
            assertTrue(timestamp <= 799);
            count++;
        }

        assertEquals(600, count);
    }


    private RepositoryConfiguration createConfig() {
        return createConfig(2);
    }

    private RepositoryConfiguration createConfig(final int numStorageDirs) {
        final RepositoryConfiguration config = new RepositoryConfiguration();
        final String unitTestName = testName.getMethodName();
        final File storageDir = new File("target/storage/" + unitTestName + "/" + UUID.randomUUID().toString());

        for (int i = 1; i <= numStorageDirs; i++) {
            config.addStorageDirectory(String.valueOf(i), new File(storageDir, String.valueOf(i)));
        }

        return config;
    }

    private ProvenanceEventRecord addId(final ProvenanceEventRecord event, final long eventId) {
        return new StandardProvenanceEventRecord.Builder()
            .fromEvent(event)
            .setEventId(eventId)
            .build();
    }


    private ProvenanceEventRecord createEvent() {
        final String uuid = UUID.randomUUID().toString();
        final Map<String, String> previousAttributes = new HashMap<>();
        previousAttributes.put("uuid", uuid);
        final Map<String, String> updatedAttributes = new HashMap<>();
        updatedAttributes.put("updated", "true");

        return new StandardProvenanceEventRecord.Builder()
            .setEventType(ProvenanceEventType.CONTENT_MODIFIED)
            .setAttributes(previousAttributes, updatedAttributes)
            .setComponentId("component-1")
            .setComponentType("unit test")
            .setEventTime(System.currentTimeMillis())
            .setFlowFileEntryDate(System.currentTimeMillis())
            .setFlowFileUUID(uuid)
            .setLineageStartDate(System.currentTimeMillis())
            .setCurrentContentClaim("container", "section", "unit-test-id", 0L, 1024L)
            .build();
    }
}
