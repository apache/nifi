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
package org.apache.nifi.provenance;

import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.provenance.MiNiFiPersistentProvenanceRepository.MethodNotSupportedException;
import org.apache.nifi.provenance.serialization.RecordReader;
import org.apache.nifi.provenance.serialization.RecordReaders;
import org.apache.nifi.provenance.serialization.RecordWriter;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.stream.io.DataOutputStream;
import org.apache.nifi.util.file.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPOutputStream;

import static org.apache.nifi.provenance.TestUtil.createFlowFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class TestMiNiFiPersistentProvenanceRepository {

    @Rule
    public TestName name = new TestName();

    private MiNiFiPersistentProvenanceRepository repo;
    private RepositoryConfiguration config;

    public static final int DEFAULT_ROLLOVER_MILLIS = 2000;
    private EventReporter eventReporter;
    private List<ReportedEvent> reportedEvents = Collections.synchronizedList(new ArrayList<ReportedEvent>());

    private RepositoryConfiguration createConfiguration() {
        config = new RepositoryConfiguration();
        config.addStorageDirectory(new File("target/storage/" + UUID.randomUUID().toString()));
        config.setCompressOnRollover(true);
        config.setMaxEventFileLife(2000L, TimeUnit.SECONDS);
        config.setCompressionBlockBytes(100);
        return config;
    }

    @BeforeClass
    public static void setLogLevel() {
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.provenance", "DEBUG");
    }

    @Before
    public void printTestName() {
        reportedEvents.clear();
        eventReporter = new EventReporter() {
            private static final long serialVersionUID = 1L;

            @Override
            public void reportEvent(Severity severity, String category, String message) {
                reportedEvents.add(new ReportedEvent(severity, category, message));
                System.out.println(severity + " : " + category + " : " + message);
            }
        };
    }

    @After
    public void closeRepo() throws IOException {
        if (repo != null) {
            try {
                repo.close();
            } catch (final IOException ioe) {
            }
        }

        // Delete all of the storage files. We do this in order to clean up the tons of files that
        // we create but also to ensure that we have closed all of the file handles. If we leave any
        // streams open, for instance, this will throw an IOException, causing our unit test to fail.
        for (final File storageDir : config.getStorageDirectories()) {
            int i;
            for (i = 0; i < 3; i++) {
                try {
                    FileUtils.deleteFile(storageDir, true);
                    break;
                } catch (final IOException ioe) {
                    // if there is a virus scanner, etc. running in the background we may not be able to
                    // delete the file. Wait a sec and try again.
                    if (i == 2) {
                        throw ioe;
                    } else {
                        try {
                            Thread.sleep(1000L);
                        } catch (final InterruptedException ie) {
                        }
                    }
                }
            }
        }
    }



    private EventReporter getEventReporter() {
        return eventReporter;
    }


    @Test
    public void testAddAndRecover() throws IOException, InterruptedException {
        final RepositoryConfiguration config = createConfiguration();
        config.setMaxEventFileCapacity(1L);
        config.setMaxEventFileLife(1, TimeUnit.SECONDS);
        repo = new MiNiFiPersistentProvenanceRepository(config, DEFAULT_ROLLOVER_MILLIS);
        repo.initialize(getEventReporter(), null, null);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "xyz");
        attributes.put("xyz", "abc");
        attributes.put("uuid", UUID.randomUUID().toString());

        final ProvenanceEventBuilder builder = new StandardProvenanceEventRecord.Builder();
        builder.setEventTime(System.currentTimeMillis());
        builder.setEventType(ProvenanceEventType.RECEIVE);
        builder.setTransitUri("nifi://unit-test");
        builder.fromFlowFile(createFlowFile(3L, 3000L, attributes));
        builder.setComponentId("1234");
        builder.setComponentType("dummy processor");
        final ProvenanceEventRecord record = builder.build();

        for (int i = 0; i < 10; i++) {
            repo.registerEvent(record);
        }

        Assert.assertEquals("Did not establish the correct, Max Event Id", 9, repo.getMaxEventId().intValue());

        Thread.sleep(1000L);

        repo.close();
        Thread.sleep(500L); // Give the repo time to shutdown (i.e., close all file handles, etc.)

        repo = new MiNiFiPersistentProvenanceRepository(config, DEFAULT_ROLLOVER_MILLIS);
        repo.initialize(getEventReporter(), null, null);
        final List<ProvenanceEventRecord> recoveredRecords = repo.getEvents(0L, 12);

        Assert.assertEquals("Did not establish the correct, Max Event Id through recovery after reloading", 9, repo.getMaxEventId().intValue());

        assertEquals(10, recoveredRecords.size());
        for (int i = 0; i < 10; i++) {
            final ProvenanceEventRecord recovered = recoveredRecords.get(i);
            assertEquals(i, recovered.getEventId());
            assertEquals("nifi://unit-test", recovered.getTransitUri());
            assertEquals(ProvenanceEventType.RECEIVE, recovered.getEventType());
            assertEquals(attributes, recovered.getAttributes());
        }
    }


    @Test
    public void testCompressOnRollover() throws IOException, InterruptedException, ParseException {
        final RepositoryConfiguration config = createConfiguration();
        config.setMaxEventFileLife(500, TimeUnit.MILLISECONDS);
        config.setCompressOnRollover(true);
        repo = new MiNiFiPersistentProvenanceRepository(config, DEFAULT_ROLLOVER_MILLIS);
        repo.initialize(getEventReporter(), null, null);

        final String uuid = "00000000-0000-0000-0000-000000000000";
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "xyz");
        attributes.put("xyz", "abc");
        attributes.put("filename", "file-" + uuid);

        final ProvenanceEventBuilder builder = new StandardProvenanceEventRecord.Builder();
        builder.setEventTime(System.currentTimeMillis());
        builder.setEventType(ProvenanceEventType.RECEIVE);
        builder.setTransitUri("nifi://unit-test");
        attributes.put("uuid", uuid);
        builder.fromFlowFile(createFlowFile(3L, 3000L, attributes));
        builder.setComponentId("1234");
        builder.setComponentType("dummy processor");

        for (int i = 0; i < 10; i++) {
            builder.fromFlowFile(createFlowFile(i, 3000L, attributes));
            repo.registerEvent(builder.build());
        }

        repo.waitForRollover();
        final File storageDir = config.getStorageDirectories().get(0);
        final File compressedLogFile = new File(storageDir, "0.prov.gz");
        assertTrue(compressedLogFile.exists());
    }

    @Test(expected = MethodNotSupportedException.class)
    public void testLineageRequestNotSupported() throws IOException, InterruptedException, ParseException {
        final RepositoryConfiguration config = createConfiguration();
        config.setMaxRecordLife(3, TimeUnit.SECONDS);
        config.setMaxStorageCapacity(1024L * 1024L);
        config.setMaxEventFileLife(500, TimeUnit.MILLISECONDS);
        config.setMaxEventFileCapacity(1024L * 1024L);
        config.setSearchableFields(new ArrayList<>(SearchableFields.getStandardFields()));

        repo = new MiNiFiPersistentProvenanceRepository(config, DEFAULT_ROLLOVER_MILLIS);
        repo.initialize(getEventReporter(), null, null);

        final String uuid = "00000000-0000-0000-0000-000000000001";
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "xyz");
        attributes.put("uuid", uuid);
        attributes.put("filename", "file-" + uuid);

        repo.submitLineageComputation(uuid, null);
    }


    @Test
    public void testCorrectProvenanceEventIdOnRestore() throws IOException {
        final RepositoryConfiguration config = createConfiguration();
        config.setMaxEventFileLife(1, TimeUnit.SECONDS);
        repo = new MiNiFiPersistentProvenanceRepository(config, DEFAULT_ROLLOVER_MILLIS);
        repo.initialize(getEventReporter(), null, null);

        final String uuid = "00000000-0000-0000-0000-000000000000";
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "xyz");
        attributes.put("xyz", "abc");
        attributes.put("filename", "file-" + uuid);

        final ProvenanceEventBuilder builder = new StandardProvenanceEventRecord.Builder();
        builder.setEventTime(System.currentTimeMillis());
        builder.setEventType(ProvenanceEventType.RECEIVE);
        builder.setTransitUri("nifi://unit-test");
        attributes.put("uuid", uuid);
        builder.fromFlowFile(createFlowFile(3L, 3000L, attributes));
        builder.setComponentId("1234");
        builder.setComponentType("dummy processor");

        for (int i = 0; i < 10; i++) {
            builder.fromFlowFile(createFlowFile(i, 3000L, attributes));
            attributes.put("uuid", "00000000-0000-0000-0000-00000000000" + i);
            repo.registerEvent(builder.build());
        }

        repo.close();

        final MiNiFiPersistentProvenanceRepository secondRepo = new MiNiFiPersistentProvenanceRepository(config, DEFAULT_ROLLOVER_MILLIS);
        secondRepo.initialize(getEventReporter(), null, null);

        try {
            final ProvenanceEventRecord event11 = builder.build();
            secondRepo.registerEvent(event11);
            secondRepo.waitForRollover();
            final ProvenanceEventRecord event11Retrieved = secondRepo.getEvent(10L);
            assertNotNull(event11Retrieved);
            assertEquals(10, event11Retrieved.getEventId());
            Assert.assertEquals(10, secondRepo.getMaxEventId().intValue());
        } finally {
            secondRepo.close();
        }
    }

    /**
     * Here the event file is simply corrupted by virtue of not having any event
     * records while having correct headers
     */
    @Test
    public void testWithWithEventFileMissingRecord() throws Exception {
        File eventFile = this.prepCorruptedEventFileTests();

        DataOutputStream in = new DataOutputStream(new GZIPOutputStream(new FileOutputStream(eventFile)));
        in.writeUTF("BlahBlah");
        in.writeInt(4);
        in.close();
        assertTrue(eventFile.exists());

        final List<ProvenanceEventRecord> events = repo.getEvents(0, 100);
        assertEquals(10, events.size());
    }

    /**
     * Here the event file is simply corrupted by virtue of being empty (0
     * bytes)
     */
    @Test
    public void testWithWithEventFileCorrupted() throws Exception {
        File eventFile = this.prepCorruptedEventFileTests();

        DataOutputStream in = new DataOutputStream(new GZIPOutputStream(new FileOutputStream(eventFile)));
        in.close();

        final List<ProvenanceEventRecord> events = repo.getEvents(0, 100);
        assertEquals(10, events.size());
    }

    private File prepCorruptedEventFileTests() throws Exception {
        RepositoryConfiguration config = createConfiguration();
        config.setMaxStorageCapacity(1024L * 1024L);
        config.setMaxEventFileLife(500, TimeUnit.MILLISECONDS);
        config.setMaxEventFileCapacity(1024L * 1024L);
        config.setSearchableFields(new ArrayList<>(SearchableFields.getStandardFields()));
        config.setDesiredIndexSize(10);

        repo = new MiNiFiPersistentProvenanceRepository(config, DEFAULT_ROLLOVER_MILLIS);
        repo.initialize(getEventReporter(), null, null);

        String uuid = UUID.randomUUID().toString();
        for (int i = 0; i < 20; i++) {
            ProvenanceEventRecord record = repo.eventBuilder().fromFlowFile(mock(FlowFile.class))
                .setEventType(ProvenanceEventType.CREATE).setComponentId("foo-" + i).setComponentType("myComponent")
                .setFlowFileUUID(uuid).build();
            repo.registerEvent(record);
            if (i == 9) {
                repo.waitForRollover();
                Thread.sleep(2000L);
            }
        }
        repo.waitForRollover();
        File eventFile = new File(config.getStorageDirectories().get(0), "10.prov.gz");
        assertTrue(eventFile.delete());
        return eventFile;
    }

    @Test
    public void testBackPressure() throws IOException, InterruptedException {
        final RepositoryConfiguration config = createConfiguration();
        config.setMaxEventFileCapacity(1L); // force rollover on each record.
        config.setJournalCount(1);

        final AtomicInteger journalCountRef = new AtomicInteger(0);

        repo = new MiNiFiPersistentProvenanceRepository(config, DEFAULT_ROLLOVER_MILLIS) {
            @Override
            protected int getJournalCount() {
                return journalCountRef.get();
            }
        };
        repo.initialize(getEventReporter(), null, null);

        final Map<String, String> attributes = new HashMap<>();
        final ProvenanceEventBuilder builder = new StandardProvenanceEventRecord.Builder();
        builder.setEventTime(System.currentTimeMillis());
        builder.setEventType(ProvenanceEventType.RECEIVE);
        builder.setTransitUri("nifi://unit-test");
        attributes.put("uuid", UUID.randomUUID().toString());
        builder.fromFlowFile(createFlowFile(3L, 3000L, attributes));
        builder.setComponentId("1234");
        builder.setComponentType("dummy processor");

        // ensure that we can register the events.
        for (int i = 0; i < 10; i++) {
            builder.fromFlowFile(createFlowFile(i, 3000L, attributes));
            attributes.put("uuid", "00000000-0000-0000-0000-00000000000" + i);
            repo.registerEvent(builder.build());
        }

        // set number of journals to 6 so that we will block.
        journalCountRef.set(6);

        final AtomicLong threadNanos = new AtomicLong(0L);
        final Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                final long start = System.nanoTime();
                builder.fromFlowFile(createFlowFile(13, 3000L, attributes));
                attributes.put("uuid", "00000000-0000-0000-0000-00000000000" + 13);
                repo.registerEvent(builder.build());
                threadNanos.set(System.nanoTime() - start);
            }
        });
        t.start();

        Thread.sleep(1500L);

        journalCountRef.set(1);
        t.join();

        final int threadMillis = (int) TimeUnit.NANOSECONDS.toMillis(threadNanos.get());
        assertTrue(threadMillis > 1200); // use 1200 to account for the fact that the timing is not exact

        builder.fromFlowFile(createFlowFile(15, 3000L, attributes));
        attributes.put("uuid", "00000000-0000-0000-0000-00000000000" + 15);
        repo.registerEvent(builder.build());
    }

    @Test
    public void testMergeJournals() throws IOException, InterruptedException {
        final RepositoryConfiguration config = createConfiguration();
        config.setMaxEventFileLife(3, TimeUnit.SECONDS);
        repo = new MiNiFiPersistentProvenanceRepository(config, DEFAULT_ROLLOVER_MILLIS);
        repo.initialize(getEventReporter(), null, null);

        final Map<String, String> attributes = new HashMap<>();

        final ProvenanceEventBuilder builder = new StandardProvenanceEventRecord.Builder();
        builder.setEventTime(System.currentTimeMillis());
        builder.setEventType(ProvenanceEventType.RECEIVE);
        builder.setTransitUri("nifi://unit-test");
        attributes.put("uuid", "12345678-0000-0000-0000-012345678912");
        builder.fromFlowFile(createFlowFile(3L, 3000L, attributes));
        builder.setComponentId("1234");
        builder.setComponentType("dummy processor");

        final ProvenanceEventRecord record = builder.build();

        final ExecutorService exec = Executors.newFixedThreadPool(10);
        for (int i = 0; i < 10000; i++) {
            exec.submit(new Runnable() {
                @Override
                public void run() {
                    repo.registerEvent(record);
                }
            });
        }

        repo.waitForRollover();

        final File storageDir = config.getStorageDirectories().get(0);
        long counter = 0;
        for (final File file : storageDir.listFiles()) {
            if (file.isFile()) {

                try (RecordReader reader = RecordReaders.newRecordReader(file, null, 2048)) {
                    ProvenanceEventRecord r = null;

                    while ((r = reader.nextRecord()) != null) {
                        assertEquals(counter++, r.getEventId());
                    }
                }
            }
        }

        assertEquals(10000, counter);
    }

    @Test
    public void testTruncateAttributes() throws IOException, InterruptedException {
        final RepositoryConfiguration config = createConfiguration();
        config.setMaxAttributeChars(50);
        config.setMaxEventFileLife(3, TimeUnit.SECONDS);
        repo = new MiNiFiPersistentProvenanceRepository(config, DEFAULT_ROLLOVER_MILLIS);
        repo.initialize(getEventReporter(), null, null);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("75chars", "123456789012345678901234567890123456789012345678901234567890123456789012345");

        final ProvenanceEventBuilder builder = new StandardProvenanceEventRecord.Builder();
        builder.setEventTime(System.currentTimeMillis());
        builder.setEventType(ProvenanceEventType.RECEIVE);
        builder.setTransitUri("nifi://unit-test");
        attributes.put("uuid", "12345678-0000-0000-0000-012345678912");
        builder.fromFlowFile(createFlowFile(3L, 3000L, attributes));
        builder.setComponentId("1234");
        builder.setComponentType("dummy processor");

        final ProvenanceEventRecord record = builder.build();
        repo.registerEvent(record);
        repo.waitForRollover();

        final ProvenanceEventRecord retrieved = repo.getEvent(0L);
        assertNotNull(retrieved);
        assertEquals("12345678-0000-0000-0000-012345678912", retrieved.getAttributes().get("uuid"));
        assertEquals("12345678901234567890123456789012345678901234567890", retrieved.getAttributes().get("75chars"));
    }


    @Test
    public void testFailureToCreateWriterDoesNotPreventSubsequentRollover() throws IOException, InterruptedException {
        final RepositoryConfiguration config = createConfiguration();
        config.setMaxAttributeChars(50);
        config.setMaxEventFileLife(3, TimeUnit.SECONDS);

        // Create a repo that will allow only a single writer to be created.
        final IOException failure = new IOException("Already created writers once. Unit test causing failure.");
        repo = new MiNiFiPersistentProvenanceRepository(config, DEFAULT_ROLLOVER_MILLIS) {
            int iterations = 0;

            @Override
            protected RecordWriter[] createWriters(RepositoryConfiguration config, long initialRecordId) throws IOException {
                if (iterations++ == 1) {
                    throw failure;
                } else {
                    return super.createWriters(config, initialRecordId);
                }
            }
        };

        // initialize with our event reporter
        repo.initialize(getEventReporter(), null, null);

        // create some events in the journal files.
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("75chars", "123456789012345678901234567890123456789012345678901234567890123456789012345");

        final ProvenanceEventBuilder builder = new StandardProvenanceEventRecord.Builder();
        builder.setEventTime(System.currentTimeMillis());
        builder.setEventType(ProvenanceEventType.RECEIVE);
        builder.setTransitUri("nifi://unit-test");
        attributes.put("uuid", "12345678-0000-0000-0000-012345678912");
        builder.fromFlowFile(createFlowFile(3L, 3000L, attributes));
        builder.setComponentId("1234");
        builder.setComponentType("dummy processor");

        for (int i = 0; i < 50; i++) {
            final ProvenanceEventRecord event = builder.build();
            repo.registerEvent(event);
        }

        // Attempt to rollover but fail to create new writers.
        try {
            repo.rolloverWithLock(true);
            Assert.fail("Expected to get IOException when calling rolloverWithLock");
        } catch (final IOException ioe) {
            assertTrue(ioe == failure);
        }

        // Wait for the first rollover to succeed.
        repo.waitForRollover();

        // This time when we rollover, we should not have a problem rolling over.
        repo.rolloverWithLock(true);

        // Ensure that no errors were reported.
        assertEquals(0, reportedEvents.size());
    }

    private static class ReportedEvent {
        private final Severity severity;
        private final String category;
        private final String message;

        public ReportedEvent(final Severity severity, final String category, final String message) {
            this.severity = severity;
            this.category = category;
            this.message = message;
        }

        public String getCategory() {
            return category;
        }

        public String getMessage() {
            return message;
        }

        public Severity getSeverity() {
            return severity;
        }
    }
}
