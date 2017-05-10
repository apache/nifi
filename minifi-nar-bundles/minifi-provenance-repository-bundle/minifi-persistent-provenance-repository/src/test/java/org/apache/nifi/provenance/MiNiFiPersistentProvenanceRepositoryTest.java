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

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.provenance.lucene.IndexingAction;
import org.apache.nifi.provenance.serialization.RecordReader;
import org.apache.nifi.provenance.serialization.RecordReaders;
import org.apache.nifi.provenance.serialization.RecordWriter;
import org.apache.nifi.provenance.serialization.RecordWriters;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.file.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.nifi.provenance.TestUtil.createFlowFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class MiNiFiPersistentProvenanceRepositoryTest {

    @Rule
    public TestName name = new TestName();

    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();

    private MiNiFiPersistentProvenanceRepository repo;
    private static RepositoryConfiguration config;

    public static final int DEFAULT_ROLLOVER_MILLIS = 2000;
    private EventReporter eventReporter;
    private List<ReportedEvent> reportedEvents = Collections.synchronizedList(new ArrayList<ReportedEvent>());

    private static int headerSize;
    private static int recordSize;
    private static int recordSize2;

    private RepositoryConfiguration createConfiguration() {
        config = new RepositoryConfiguration();
        config.addStorageDirectory("1", new File("target/storage/" + UUID.randomUUID().toString()));
        config.setCompressOnRollover(true);
        config.setMaxEventFileLife(2000L, TimeUnit.SECONDS);
        config.setCompressionBlockBytes(100);
        return config;
    }

    @BeforeClass
    public static void setLogLevel() {
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.provenance", "DEBUG");
    }

    @BeforeClass
    public static void findJournalSizes() throws IOException {
        // determine header and record size

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
        builder.setComponentId("2345");
        final ProvenanceEventRecord record2 = builder.build();

        final File tempRecordFile = tempFolder.newFile("record.tmp");
        System.out.println("findJournalSizes position 0 = " + tempRecordFile.length());

        final AtomicLong idGenerator = new AtomicLong(0L);
        final RecordWriter writer = RecordWriters.newSchemaRecordWriter(tempRecordFile, idGenerator, false, false);
        writer.writeHeader(12345L);
        writer.flush();
        headerSize = Long.valueOf(tempRecordFile.length()).intValue();
        writer.writeRecord(record);
        writer.flush();
        recordSize = Long.valueOf(tempRecordFile.length()).intValue() - headerSize;
        writer.writeRecord(record2);
        writer.flush();
        recordSize2 = Long.valueOf(tempRecordFile.length()).intValue() - headerSize - recordSize;
        writer.close();

        System.out.println("headerSize =" + headerSize);
        System.out.println("recordSize =" + recordSize);
        System.out.println("recordSize2=" + recordSize2);
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
        if (repo == null) {
            return;
        }

        try {
            repo.close();
        } catch (final IOException ioe) {
        }

        // Delete all of the storage files. We do this in order to clean up the tons of files that
        // we create but also to ensure that we have closed all of the file handles. If we leave any
        // streams open, for instance, this will throw an IOException, causing our unit test to fail.
        if (config != null) {
            for (final File storageDir : config.getStorageDirectories().values()) {
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
                                System.out.println("file: " + storageDir.toString() + " exists=" + storageDir.exists());
                                FileUtils.deleteFile(storageDir, true);
                                break;
                            } catch (final IOException ioe2) {
                                // if there is a virus scanner, etc. running in the background we may not be able to
                                // delete the file. Wait a sec and try again.
                                if (i == 2) {
                                    throw ioe2;
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
            }
        }
    }



    private EventReporter getEventReporter() {
        return eventReporter;
    }

    private NiFiProperties properties = new NiFiProperties() {
        @Override
        public String getProperty(String key) {
            if (key.equals(NiFiProperties.PROVENANCE_COMPRESS_ON_ROLLOVER)) {
                return "true";
            } else if (key.equals(NiFiProperties.PROVENANCE_ROLLOVER_TIME)) {
                return "2000 millis";
            } else if (key.equals(NiFiProperties.PROVENANCE_REPO_DIRECTORY_PREFIX + ".default")) {
                createConfiguration();
                return config.getStorageDirectories().values().iterator().next().getAbsolutePath();
            } else {
                return null;
            }
        }

        @Override
        public Set<String> getPropertyKeys() {
            return new HashSet<>(Arrays.asList(
                    NiFiProperties.PROVENANCE_COMPRESS_ON_ROLLOVER,
                    NiFiProperties.PROVENANCE_ROLLOVER_TIME,
                    NiFiProperties.PROVENANCE_REPO_DIRECTORY_PREFIX + ".default"));
        }
    };

    @Test
    public void constructorNoArgs() {
        TestableMiNiFiPersistentProvenanceRepository tppr = new TestableMiNiFiPersistentProvenanceRepository();
        assertEquals(0, tppr.getRolloverCheckMillis());
    }

    @Test
    public void constructorNiFiProperties() throws IOException {
        TestableMiNiFiPersistentProvenanceRepository tppr = new TestableMiNiFiPersistentProvenanceRepository(properties);
        assertEquals(10000, tppr.getRolloverCheckMillis());
    }

    @Test
    public void constructorConfig() throws IOException {
        RepositoryConfiguration configuration = RepositoryConfiguration.create(properties);
        new TestableMiNiFiPersistentProvenanceRepository(configuration, 20000);
    }

    @Test
    public void testAddAndRecover() throws IOException, InterruptedException {
        final RepositoryConfiguration config = createConfiguration();
        config.setMaxEventFileCapacity(1L);
        config.setMaxEventFileLife(1, TimeUnit.SECONDS);
        repo = new MiNiFiPersistentProvenanceRepository(config, DEFAULT_ROLLOVER_MILLIS);
        repo.initialize(getEventReporter(), null, null, IdentifierLookup.EMPTY);

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

        Thread.sleep(1000L);

        repo.close();
        Thread.sleep(500L); // Give the repo time to shutdown (i.e., close all file handles, etc.)

        repo = new MiNiFiPersistentProvenanceRepository(config, DEFAULT_ROLLOVER_MILLIS);
        repo.initialize(getEventReporter(), null, null, IdentifierLookup.EMPTY);
        final List<ProvenanceEventRecord> recoveredRecords = repo.getEvents(0L, 12);

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
        repo.initialize(getEventReporter(), null, null, IdentifierLookup.EMPTY);

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
        final File storageDir = config.getStorageDirectories().values().iterator().next();
        final File compressedLogFile = new File(storageDir, "0.prov.gz");
        assertTrue(compressedLogFile.exists());
    }

    @Test
    public void testCorrectProvenanceEventIdOnRestore() throws IOException {
        final RepositoryConfiguration config = createConfiguration();
        config.setMaxEventFileLife(1, TimeUnit.SECONDS);
        repo = new MiNiFiPersistentProvenanceRepository(config, DEFAULT_ROLLOVER_MILLIS);
        repo.initialize(getEventReporter(), null, null, IdentifierLookup.EMPTY);

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
        secondRepo.initialize(getEventReporter(), null, null, IdentifierLookup.EMPTY);

        try {
            final ProvenanceEventRecord event11 = builder.build();
            secondRepo.registerEvent(event11);
            secondRepo.waitForRollover();
            final ProvenanceEventRecord event11Retrieved = secondRepo.getEvent(10L);
            assertNotNull(event11Retrieved);
            assertEquals(10, event11Retrieved.getEventId());
        } finally {
            secondRepo.close();
        }
    }

    private File prepCorruptedEventFileTests() throws Exception {
        RepositoryConfiguration config = createConfiguration();
        config.setMaxStorageCapacity(1024L * 1024L);
        config.setMaxEventFileLife(500, TimeUnit.MILLISECONDS);
        config.setMaxEventFileCapacity(1024L * 1024L);
        config.setSearchableFields(new ArrayList<>(SearchableFields.getStandardFields()));
        config.setDesiredIndexSize(10);

        repo = new MiNiFiPersistentProvenanceRepository(config, DEFAULT_ROLLOVER_MILLIS);
        repo.initialize(getEventReporter(), null, null, IdentifierLookup.EMPTY);

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
        File eventFile = new File(config.getStorageDirectories().values().iterator().next(), "10.prov.gz");
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
        repo.initialize(getEventReporter(), null, null, IdentifierLookup.EMPTY);

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

        Thread.sleep(3000L);
    }

    private long checkJournalRecords(final File storageDir, final Boolean exact) throws IOException {
        File[] storagefiles = storageDir.listFiles();
        long counter = 0;
        assertNotNull(storagefiles);
        for (final File file : storagefiles) {
            if (file.isFile()) {
                try (RecordReader reader = RecordReaders.newRecordReader(file, null, 2048)) {
                    ProvenanceEventRecord r;
                    ProvenanceEventRecord last = null;
                    while ((r = reader.nextRecord()) != null) {
                        if (exact) {
                            assertTrue(counter++ == r.getEventId());
                        } else {
                            assertTrue(counter++ <= r.getEventId());
                        }
                    }
                }
            }
        }
        return counter;
    }

    @Test
    public void testMergeJournals() throws IOException, InterruptedException {
        final RepositoryConfiguration config = createConfiguration();
        config.setMaxEventFileLife(3, TimeUnit.SECONDS);
        repo = new MiNiFiPersistentProvenanceRepository(config, DEFAULT_ROLLOVER_MILLIS);
        repo.initialize(getEventReporter(), null, null, IdentifierLookup.EMPTY);

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

        final File storageDir = config.getStorageDirectories().values().iterator().next();
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

    private void corruptJournalFile(final File journalFile, final int position,
                                    final String original, final String replacement) throws IOException {
        final int journalLength = Long.valueOf(journalFile.length()).intValue();
        final byte[] origBytes = original.getBytes();
        final byte[] replBytes = replacement.getBytes();
        FileInputStream journalIn = new FileInputStream(journalFile);
        byte[] content = new byte[journalLength];
        assertEquals(journalLength, journalIn.read(content, 0, journalLength));
        journalIn.close();
        assertEquals(original, new String(Arrays.copyOfRange(content, position, position + origBytes.length)));
        System.arraycopy(replBytes, 0, content, position, replBytes.length);
        FileOutputStream journalOut = new FileOutputStream(journalFile);
        journalOut.write(content, 0, journalLength);
        journalOut.flush();
        journalOut.close();
    }

    @Test
    public void testMergeJournalsBadFirstRecord() throws IOException, InterruptedException {
        final RepositoryConfiguration config = createConfiguration();
        config.setMaxEventFileLife(3, TimeUnit.SECONDS);
        TestableMiNiFiPersistentProvenanceRepository testRepo = new TestableMiNiFiPersistentProvenanceRepository(config, DEFAULT_ROLLOVER_MILLIS);
        testRepo.initialize(getEventReporter(), null, null, null);

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
        final List<Future> futures = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            futures.add(exec.submit(new Runnable() {
                @Override
                public void run() {
                    testRepo.registerEvent(record);
                }
            }));
        }

        // wait for writers to finish and then corrupt the first record of the first journal file
        for (Future future : futures) {
            while (!future.isDone()) {
                Thread.sleep(10);
            }
        }
        RecordWriter firstWriter = testRepo.getWriters()[0];
        corruptJournalFile(firstWriter.getFile(), headerSize + 15,"RECEIVE", "BADTYPE");

        testRepo.recoverJournalFiles();

        final File storageDir = config.getStorageDirectories().values().iterator().next();
        assertTrue(checkJournalRecords(storageDir, false) < 10000);
    }

    @Test
    public void testMergeJournalsBadRecordAfterFirst() throws IOException, InterruptedException {
        final RepositoryConfiguration config = createConfiguration();
        config.setMaxEventFileLife(3, TimeUnit.SECONDS);
        TestableMiNiFiPersistentProvenanceRepository testRepo = new TestableMiNiFiPersistentProvenanceRepository(config, DEFAULT_ROLLOVER_MILLIS);
        testRepo.initialize(getEventReporter(), null, null, null);

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
        final List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            futures.add(exec.submit(new Runnable() {
                @Override
                public void run() {
                    testRepo.registerEvent(record);
                }
            }));
        }

        // corrupt the first record of the first journal file
        for (Future<?> future : futures) {
            while (!future.isDone()) {
                Thread.sleep(10);
            }
        }
        RecordWriter firstWriter = testRepo.getWriters()[0];
        corruptJournalFile(firstWriter.getFile(), headerSize + 15 + recordSize, "RECEIVE", "BADTYPE");

        testRepo.recoverJournalFiles();

        final File storageDir = config.getStorageDirectories().values().iterator().next();
        assertTrue(checkJournalRecords(storageDir, false) < 10000);
    }

    @Test
    public void testMergeJournalsEmptyJournal() throws IOException, InterruptedException {
        final RepositoryConfiguration config = createConfiguration();
        config.setMaxEventFileLife(3, TimeUnit.SECONDS);
        TestableMiNiFiPersistentProvenanceRepository testRepo = new TestableMiNiFiPersistentProvenanceRepository(config, DEFAULT_ROLLOVER_MILLIS);
        testRepo.initialize(getEventReporter(), null, null, null);

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
        final List<Future> futures = new ArrayList<>();
        for (int i = 0; i < config.getJournalCount() - 1; i++) {
            futures.add(exec.submit(new Runnable() {
                @Override
                public void run() {
                    testRepo.registerEvent(record);
                }
            }));
        }

        // wait for writers to finish and then corrupt the first record of the first journal file
        for (Future future : futures) {
            while (!future.isDone()) {
                Thread.sleep(10);
            }
        }

        testRepo.recoverJournalFiles();

        assertEquals("mergeJournals() should not error on empty journal", 0, reportedEvents.size());

        final File storageDir = config.getStorageDirectories().values().iterator().next();
        assertEquals(config.getJournalCount() - 1, checkJournalRecords(storageDir, true));
    }

    @Test
    public void testRolloverRetry() throws IOException, InterruptedException {
        final AtomicInteger retryAmount = new AtomicInteger(0);
        final RepositoryConfiguration config = createConfiguration();
        config.setMaxEventFileLife(3, TimeUnit.SECONDS);

        repo = new MiNiFiPersistentProvenanceRepository(config, DEFAULT_ROLLOVER_MILLIS){
            @Override
            File mergeJournals(List<File> journalFiles, File suggestedMergeFile, EventReporter eventReporter) throws IOException {
                retryAmount.incrementAndGet();
                return super.mergeJournals(journalFiles, suggestedMergeFile, eventReporter);
            }

            // Indicate that there are no files available.
            @Override
            protected List<File> filterUnavailableFiles(List<File> journalFiles) {
                return Collections.emptyList();
            }

            @Override
            protected long getRolloverRetryMillis() {
                return 10L; // retry quickly.
            }
        };
        repo.initialize(getEventReporter(), null, null, IdentifierLookup.EMPTY);

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
        exec.shutdown();
        exec.awaitTermination(10, TimeUnit.SECONDS);

        repo.waitForRollover();
        assertEquals(5,retryAmount.get());
    }

    @Test
    public void testTruncateAttributes() throws IOException, InterruptedException {
        final RepositoryConfiguration config = createConfiguration();
        config.setMaxAttributeChars(50);
        config.setMaxEventFileLife(3, TimeUnit.SECONDS);
        repo = new MiNiFiPersistentProvenanceRepository(config, DEFAULT_ROLLOVER_MILLIS);
        repo.initialize(getEventReporter(), null, null, IdentifierLookup.EMPTY);

        final String maxLengthChars = "12345678901234567890123456789012345678901234567890";
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("75chars", "123456789012345678901234567890123456789012345678901234567890123456789012345");
        attributes.put("51chars", "123456789012345678901234567890123456789012345678901");
        attributes.put("50chars", "12345678901234567890123456789012345678901234567890");
        attributes.put("49chars", "1234567890123456789012345678901234567890123456789");
        attributes.put("nullChar", null);

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
        assertEquals(maxLengthChars, retrieved.getAttributes().get("75chars"));
        assertEquals(maxLengthChars, retrieved.getAttributes().get("51chars"));
        assertEquals(maxLengthChars, retrieved.getAttributes().get("50chars"));
        assertEquals(maxLengthChars.substring(0, 49), retrieved.getAttributes().get("49chars"));
    }


    @Test(timeout = 15000)
    public void testExceptionOnIndex() throws IOException {
        final RepositoryConfiguration config = createConfiguration();
        config.setMaxAttributeChars(50);
        config.setMaxEventFileLife(3, TimeUnit.SECONDS);
        config.setIndexThreadPoolSize(1);

        final int numEventsToIndex = 10;

        final AtomicInteger indexedEventCount = new AtomicInteger(0);
        repo = new MiNiFiPersistentProvenanceRepository(config, DEFAULT_ROLLOVER_MILLIS) {
            @Override
            protected synchronized IndexingAction createIndexingAction() {
                return new IndexingAction(config.getSearchableFields(), config.getSearchableAttributes()) {
                    @Override
                    public void index(StandardProvenanceEventRecord record, IndexWriter indexWriter, Integer blockIndex) throws IOException {
                        final int count = indexedEventCount.incrementAndGet();
                        if (count <= numEventsToIndex) {
                            return;
                        }

                        throw new IOException("Unit Test - Intentional Exception");
                    }
                };
            }
        };
        repo.initialize(getEventReporter(), null, null, IdentifierLookup.EMPTY);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("uuid", "12345678-0000-0000-0000-012345678912");

        final ProvenanceEventBuilder builder = new StandardProvenanceEventRecord.Builder();
        builder.setEventTime(System.currentTimeMillis());
        builder.setEventType(ProvenanceEventType.RECEIVE);
        builder.setTransitUri("nifi://unit-test");
        builder.fromFlowFile(createFlowFile(3L, 3000L, attributes));
        builder.setComponentId("1234");
        builder.setComponentType("dummy processor");

        for (int i=0; i < 1000; i++) {
            final ProvenanceEventRecord record = builder.build();
            repo.registerEvent(record);
        }

        repo.waitForRollover();

        assertEquals(numEventsToIndex + MiNiFiPersistentProvenanceRepository.MAX_INDEXING_FAILURE_COUNT, indexedEventCount.get());
        assertEquals(1, reportedEvents.size());
        final ReportedEvent event = reportedEvents.get(0);
        assertEquals(Severity.WARNING, event.getSeverity());
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
        repo.initialize(getEventReporter(), null, null, IdentifierLookup.EMPTY);

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

        @SuppressWarnings("unused")
        public String getCategory() {
            return category;
        }

        @SuppressWarnings("unused")
        public String getMessage() {
            return message;
        }

        public Severity getSeverity() {
            return severity;
        }
    }

    private NiFiUser createUser() {
        return new NiFiUser() {
            @Override
            public String getIdentity() {
                return "unit-test";
            }

            @Override
            public NiFiUser getChain() {
                return null;
            }

            @Override
            public boolean isAnonymous() {
                return false;
            }

            @Override
            public String getClientAddress() {
                return null;
            }

        };
    }

    private static class TestableMiNiFiPersistentProvenanceRepository extends MiNiFiPersistentProvenanceRepository {

        TestableMiNiFiPersistentProvenanceRepository() {
            super();
        }

        TestableMiNiFiPersistentProvenanceRepository(final NiFiProperties nifiProperties) throws IOException {
            super(nifiProperties);
        }

        TestableMiNiFiPersistentProvenanceRepository(final RepositoryConfiguration configuration, final int rolloverCheckMillis) throws IOException {
            super(configuration, rolloverCheckMillis);
        }

        RecordWriter[] getWriters() {
            Class klass = MiNiFiPersistentProvenanceRepository.class;
            Field writersField;
            RecordWriter[] writers = null;
            try {
                writersField = klass.getDeclaredField("writers");
                writersField.setAccessible(true);
                writers = (RecordWriter[]) writersField.get(this);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                e.printStackTrace();
            }
            return writers;
        }

        int getRolloverCheckMillis() {
            Class klass = MiNiFiPersistentProvenanceRepository.class;
            java.lang.reflect.Field rolloverCheckMillisField;
            int rolloverCheckMillis = -1;
            try {
                rolloverCheckMillisField = klass.getDeclaredField("rolloverCheckMillis");
                rolloverCheckMillisField.setAccessible(true);
                rolloverCheckMillis = (int) rolloverCheckMillisField.get(this);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                e.printStackTrace();
            }
            return rolloverCheckMillis;
        }

    }

    private RepositoryConfiguration createTestableRepositoryConfiguration(final NiFiProperties properties) {
        Class klass = MiNiFiPersistentProvenanceRepository.class;
        Method createRepositoryConfigurationMethod;
        RepositoryConfiguration configuration = null;
        try {
            createRepositoryConfigurationMethod = klass.getDeclaredMethod("createRepositoryConfiguration", NiFiProperties.class);
            createRepositoryConfigurationMethod.setAccessible(true);
            configuration = (RepositoryConfiguration)createRepositoryConfigurationMethod.invoke(null, properties);
        } catch (IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            e.printStackTrace();
        }
        return configuration;
    }

}
