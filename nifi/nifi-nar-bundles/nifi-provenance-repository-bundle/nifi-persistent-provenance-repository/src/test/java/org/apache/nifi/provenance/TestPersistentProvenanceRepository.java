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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.provenance.lineage.EventNode;
import org.apache.nifi.provenance.lineage.Lineage;
import org.apache.nifi.provenance.lineage.LineageEdge;
import org.apache.nifi.provenance.lineage.LineageNode;
import org.apache.nifi.provenance.lineage.LineageNodeType;
import org.apache.nifi.provenance.search.Query;
import org.apache.nifi.provenance.search.QueryResult;
import org.apache.nifi.provenance.search.QuerySubmission;
import org.apache.nifi.provenance.search.SearchTerms;
import org.apache.nifi.provenance.search.SearchableField;
import org.apache.nifi.provenance.serialization.RecordReader;
import org.apache.nifi.provenance.serialization.RecordReaders;
import org.apache.nifi.reporting.Severity;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class TestPersistentProvenanceRepository {

    @Rule
    public TestName name = new TestName();

    private PersistentProvenanceRepository repo;
    
    public static final int DEFAULT_ROLLOVER_MILLIS = 2000;

    private RepositoryConfiguration createConfiguration() {
        final RepositoryConfiguration config = new RepositoryConfiguration();
        config.addStorageDirectory(new File("target/storage/" + UUID.randomUUID().toString()));
        config.setCompressOnRollover(false);
        config.setMaxEventFileLife(2000L, TimeUnit.SECONDS);
        return config;
    }

    @Before
    public void printTestName() {
        System.out.println("\n\n\n***********************  " + name.getMethodName() + "  *****************************");
    }

    @After
    public void closeRepo() {
        if (repo != null) {
            try {
                repo.close();
            } catch (final IOException ioe) {
            }
        }
    }

    private FlowFile createFlowFile(final long id, final long fileSize, final Map<String, String> attributes) {
        final Map<String, String> attrCopy = new HashMap<>(attributes);

        return new FlowFile() {
            @Override
            public long getId() {
                return id;
            }

            @Override
            public long getEntryDate() {
                return System.currentTimeMillis();
            }

            @Override
            public Set<String> getLineageIdentifiers() {
                return new HashSet<String>();
            }

            @Override
            public long getLineageStartDate() {
                return System.currentTimeMillis();
            }

            @Override
            public Long getLastQueueDate() {
                return System.currentTimeMillis();
            }

            @Override
            public boolean isPenalized() {
                return false;
            }

            @Override
            public String getAttribute(final String s) {
                return attrCopy.get(s);
            }

            @Override
            public long getSize() {
                return fileSize;
            }

            @Override
            public Map<String, String> getAttributes() {
                return attrCopy;
            }

            @Override
            public int compareTo(final FlowFile o) {
                return 0;
            }
        };
    }

    private EventReporter getEventReporter() {
        return new EventReporter() {
            @Override
            public void reportEvent(Severity severity, String category, String message) {
                System.out.println(severity + " : " + category + " : " + message);
            }
        };
    }

    @Test
    @Ignore("For local testing of performance only")
    public void testPerformance() throws IOException, InterruptedException {
        final RepositoryConfiguration config = createConfiguration();
        config.setMaxEventFileCapacity(1024 * 1024 * 1024L);
        config.setMaxEventFileLife(20, TimeUnit.SECONDS);
        config.setCompressOnRollover(false);
        config.setJournalCount(10);
        config.setQueryThreadPoolSize(10);
        repo = new PersistentProvenanceRepository(config, DEFAULT_ROLLOVER_MILLIS);
        repo.initialize(getEventReporter());

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

        final Runnable r = new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 100000; i++) {
                    repo.registerEvent(record);
                }
            }
        };

        final Thread[] threads = new Thread[10];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(r);
        }

        final long start = System.nanoTime();
        for (final Thread t : threads) {
            t.start();
        }

        for (final Thread t : threads) {
            t.join();
        }
        final long nanos = System.nanoTime() - start;

        final long millis = TimeUnit.NANOSECONDS.toMillis(nanos);
        final long recsPerMilli = 1000000 / millis;
        final long recsPerSec = recsPerMilli * 1000;
        System.out.println(millis + " millis to insert 1M records (" + recsPerSec + " recs/sec)");

        System.out.println("Closing and re-initializing");
        repo.close();
        repo = new PersistentProvenanceRepository(config, DEFAULT_ROLLOVER_MILLIS);
        repo.initialize(getEventReporter());
        System.out.println("Re-initialized");

        final long fetchStart = System.nanoTime();
        final List<ProvenanceEventRecord> records = repo.getEvents(0L, 1000000);
        final long fetchMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - fetchStart);
        assertEquals(1000000, records.size());
        final long fetchRecsPerMilli = 1000000 / fetchMillis;
        final long fetchRecsPerSec = fetchRecsPerMilli * 1000L;
        System.out.println(fetchMillis + " millis to fetch 1M records (" + fetchRecsPerSec + " recs/sec)");

        repo.close();
    }

    @Test
    public void testAddAndRecover() throws IOException, InterruptedException {
        final RepositoryConfiguration config = createConfiguration();
        config.setMaxEventFileCapacity(1L);
        config.setMaxEventFileLife(1, TimeUnit.SECONDS);
        repo = new PersistentProvenanceRepository(config, DEFAULT_ROLLOVER_MILLIS);
        repo.initialize(getEventReporter());

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

        repo.close();
        Thread.sleep(500L); // Give the repo time to shutdown (i.e., close all file handles, etc.)

        repo = new PersistentProvenanceRepository(config, DEFAULT_ROLLOVER_MILLIS);
        repo.initialize(getEventReporter());
        final List<ProvenanceEventRecord> recoveredRecords = repo.getEvents(0L, 12);

        assertEquals(10, recoveredRecords.size());
        for (int i = 0; i < 10; i++) {
            final ProvenanceEventRecord recovered = recoveredRecords.get(i);
            assertEquals((long) i, recovered.getEventId());
            assertEquals("nifi://unit-test", recovered.getTransitUri());
            assertEquals(ProvenanceEventType.RECEIVE, recovered.getEventType());
            assertEquals(attributes, recovered.getAttributes());
        }
    }

    @Test
    public void testAddToMultipleLogsAndRecover() throws IOException, InterruptedException {
        final List<SearchableField> searchableFields = new ArrayList<>();
        searchableFields.add(SearchableFields.ComponentID);

        final RepositoryConfiguration config = createConfiguration();
        config.setMaxEventFileCapacity(1024L * 1024L);
        config.setMaxEventFileLife(2, TimeUnit.SECONDS);
        config.setSearchableFields(searchableFields);
        repo = new PersistentProvenanceRepository(config, DEFAULT_ROLLOVER_MILLIS);
        repo.initialize(getEventReporter());

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
        ProvenanceEventRecord record = builder.build();

        for (int i = 0; i < 10; i++) {
            repo.registerEvent(record);
        }

        builder.setComponentId("XXXX"); // create a different component id so that we can make sure we query this record.

        attributes.put("uuid", "11111111-1111-1111-1111-111111111111");

        builder.fromFlowFile(createFlowFile(11L, 11L, attributes));
        repo.registerEvent(builder.build());

        repo.waitForRollover();
        Thread.sleep(500L); // Give the repo time to shutdown (i.e., close all file handles, etc.)

        // Create a new repo and add another record with component id XXXX so that we can ensure that it's added to a different
        // log file than the previous one.
        attributes.put("uuid", "22222222-2222-2222-2222-222222222222");
        builder.fromFlowFile(createFlowFile(11L, 11L, attributes));
        repo.registerEvent(builder.build());
        repo.waitForRollover();

        final Query query = new Query(UUID.randomUUID().toString());
        query.addSearchTerm(SearchTerms.newSearchTerm(SearchableFields.ComponentID, "XXXX"));
        query.setMaxResults(100);

        final QueryResult result = repo.queryEvents(query);
        assertEquals(2, result.getMatchingEvents().size());
        for (final ProvenanceEventRecord match : result.getMatchingEvents()) {
            System.out.println(match);
        }
    }

    @Test
    public void testIndexOnRolloverAndSubsequentSearch() throws IOException, InterruptedException, ParseException {
        final RepositoryConfiguration config = createConfiguration();
        config.setMaxEventFileLife(500, TimeUnit.MILLISECONDS);
        config.setSearchableFields(new ArrayList<>(SearchableFields.getStandardFields()));
        repo = new PersistentProvenanceRepository(config, DEFAULT_ROLLOVER_MILLIS);
        repo.initialize(getEventReporter());

        final String uuid = "00000000-0000-0000-0000-000000000000";
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "xyz");
        attributes.put("xyz", "abc");
        attributes.put("filename", "file-" + uuid);

        final ProvenanceEventBuilder builder = new StandardProvenanceEventRecord.Builder();
        builder.setEventTime(System.currentTimeMillis());
        builder.setEventType(ProvenanceEventType.RECEIVE);
        builder.setTransitUri("nifi://unit-test");
        builder.fromFlowFile(createFlowFile(3L, 3000L, attributes));
        builder.setComponentId("1234");
        builder.setComponentType("dummy processor");

        for (int i = 0; i < 10; i++) {
            attributes.put("uuid", "00000000-0000-0000-0000-00000000000" + i);
            builder.fromFlowFile(createFlowFile(i, 3000L, attributes));
            repo.registerEvent(builder.build());
        }

        repo.waitForRollover();

        final Query query = new Query(UUID.randomUUID().toString());
        query.addSearchTerm(SearchTerms.newSearchTerm(SearchableFields.FlowFileUUID, "000000*"));
        query.addSearchTerm(SearchTerms.newSearchTerm(SearchableFields.Filename, "file-*"));
        query.addSearchTerm(SearchTerms.newSearchTerm(SearchableFields.ComponentID, "12?4"));
        query.addSearchTerm(SearchTerms.newSearchTerm(SearchableFields.TransitURI, "nifi://*"));
        query.setMaxResults(100);

        final QueryResult result = repo.queryEvents(query);
        assertEquals(10, result.getMatchingEvents().size());
        for (final ProvenanceEventRecord match : result.getMatchingEvents()) {
            System.out.println(match);
        }
    }

    @Test
    public void testCompressOnRollover() throws IOException, InterruptedException, ParseException {
        final RepositoryConfiguration config = createConfiguration();
        config.setMaxEventFileLife(500, TimeUnit.MILLISECONDS);
        config.setCompressOnRollover(true);
        repo = new PersistentProvenanceRepository(config, DEFAULT_ROLLOVER_MILLIS);
        repo.initialize(getEventReporter());

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

    @Test
    public void testIndexAndCompressOnRolloverAndSubsequentSearch() throws IOException, InterruptedException, ParseException {
        final RepositoryConfiguration config = createConfiguration();
        config.setMaxRecordLife(3, TimeUnit.SECONDS);
        config.setMaxStorageCapacity(1024L * 1024L);
        config.setMaxEventFileLife(500, TimeUnit.MILLISECONDS);
        config.setMaxEventFileCapacity(1024L * 1024L);
        config.setSearchableFields(new ArrayList<>(SearchableFields.getStandardFields()));

        repo = new PersistentProvenanceRepository(config, DEFAULT_ROLLOVER_MILLIS);
        repo.initialize(getEventReporter());

        final String uuid = "10000000-0000-0000-0000-000000000000";
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

        repo.waitForRollover();

        final Query query = new Query(UUID.randomUUID().toString());
//        query.addSearchTerm(SearchTerms.newSearchTerm(SearchableFields.FlowFileUUID, "00000000-0000-0000-0000*"));
        query.addSearchTerm(SearchTerms.newSearchTerm(SearchableFields.Filename, "file-*"));
        query.addSearchTerm(SearchTerms.newSearchTerm(SearchableFields.ComponentID, "12?4"));
        query.addSearchTerm(SearchTerms.newSearchTerm(SearchableFields.TransitURI, "nifi://*"));
        query.setMaxResults(100);

        final QueryResult result = repo.queryEvents(query);
        assertEquals(10, result.getMatchingEvents().size());
        for (final ProvenanceEventRecord match : result.getMatchingEvents()) {
            System.out.println(match);
        }

        Thread.sleep(2000L);

        config.setMaxStorageCapacity(100L);
        config.setMaxRecordLife(500, TimeUnit.MILLISECONDS);
        repo.purgeOldEvents();
        Thread.sleep(2000L);

        final QueryResult newRecordSet = repo.queryEvents(query);
        assertTrue(newRecordSet.getMatchingEvents().isEmpty());
    }

    @Test
    public void testIndexAndCompressOnRolloverAndSubsequentSearchAsync() throws IOException, InterruptedException, ParseException {
        final RepositoryConfiguration config = createConfiguration();
        config.setMaxRecordLife(3, TimeUnit.SECONDS);
        config.setMaxStorageCapacity(1024L * 1024L);
        config.setMaxEventFileLife(500, TimeUnit.MILLISECONDS);
        config.setMaxEventFileCapacity(1024L * 1024L);
        config.setSearchableFields(new ArrayList<>(SearchableFields.getStandardFields()));

        repo = new PersistentProvenanceRepository(config, DEFAULT_ROLLOVER_MILLIS);
        repo.initialize(getEventReporter());

        final String uuid = "00000000-0000-0000-0000-000000000000";
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "xyz");
        attributes.put("xyz", "abc");
        attributes.put("filename", "file-" + uuid);

        final ProvenanceEventBuilder builder = new StandardProvenanceEventRecord.Builder();
        builder.setEventTime(System.currentTimeMillis());
        builder.setEventType(ProvenanceEventType.RECEIVE);
        builder.setTransitUri("nifi://unit-test");
        builder.fromFlowFile(createFlowFile(3L, 3000L, attributes));
        builder.setComponentId("1234");
        builder.setComponentType("dummy processor");

        for (int i = 0; i < 10; i++) {
            attributes.put("uuid", "00000000-0000-0000-0000-00000000000" + i);
            builder.fromFlowFile(createFlowFile(i, 3000L, attributes));
            repo.registerEvent(builder.build());
        }

        repo.waitForRollover();

        final Query query = new Query(UUID.randomUUID().toString());
        query.addSearchTerm(SearchTerms.newSearchTerm(SearchableFields.FlowFileUUID, "00000*"));
        query.addSearchTerm(SearchTerms.newSearchTerm(SearchableFields.Filename, "file-*"));
        query.addSearchTerm(SearchTerms.newSearchTerm(SearchableFields.ComponentID, "12?4"));
        query.addSearchTerm(SearchTerms.newSearchTerm(SearchableFields.TransitURI, "nifi://*"));
        query.setMaxResults(100);

        final QuerySubmission submission = repo.submitQuery(query);
        while (!submission.getResult().isFinished()) {
            Thread.sleep(100L);
        }

        assertEquals(10, submission.getResult().getMatchingEvents().size());
        for (final ProvenanceEventRecord match : submission.getResult().getMatchingEvents()) {
            System.out.println(match);
        }

        Thread.sleep(2000L);

        config.setMaxStorageCapacity(100L);
        config.setMaxRecordLife(500, TimeUnit.MILLISECONDS);
        repo.purgeOldEvents();
        Thread.sleep(2000L);

        final QueryResult newRecordSet = repo.queryEvents(query);
        assertTrue(newRecordSet.getMatchingEvents().isEmpty());
    }

    @Test
    public void testIndexAndCompressOnRolloverAndSubsequentSearchMultipleStorageDirs() throws IOException, InterruptedException, ParseException {
        final RepositoryConfiguration config = createConfiguration();
        config.addStorageDirectory(new File("target/storage/" + UUID.randomUUID().toString()));
        config.setMaxRecordLife(30, TimeUnit.SECONDS);
        config.setMaxStorageCapacity(1024L * 1024L);
        config.setMaxEventFileLife(1, TimeUnit.SECONDS);
        config.setMaxEventFileCapacity(1024L * 1024L);
        config.setSearchableFields(new ArrayList<>(SearchableFields.getStandardFields()));

        repo = new PersistentProvenanceRepository(config, DEFAULT_ROLLOVER_MILLIS);
        repo.initialize(getEventReporter());

        final String uuid = "00000000-0000-0000-0000-000000000000";
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "xyz");
        attributes.put("xyz", "abc");
        attributes.put("filename", "file-" + uuid);

        final ProvenanceEventBuilder builder = new StandardProvenanceEventRecord.Builder();
        for (int j = 0; j < 3; j++) {
            attributes.put("iteration", String.valueOf(j));

            builder.setEventTime(System.currentTimeMillis());
            builder.setEventType(ProvenanceEventType.RECEIVE);
            builder.setTransitUri("nifi://unit-test");
            builder.setComponentId("1234");
            builder.setComponentType("dummy processor");
            builder.fromFlowFile(createFlowFile(3L, 3000L, attributes));

            for (int i = 0; i < 10; i++) {
                attributes.put("uuid", "00000000-0000-0000-0000-00000000000" + String.valueOf(i + j * 10));
                builder.fromFlowFile(createFlowFile(i + j * 10, 3000L, attributes));
                repo.registerEvent(builder.build());
            }

            repo.waitForRollover();
        }

        final Query query = new Query(UUID.randomUUID().toString());
        query.addSearchTerm(SearchTerms.newSearchTerm(SearchableFields.Filename, "file-*"));
        query.addSearchTerm(SearchTerms.newSearchTerm(SearchableFields.ComponentID, "12?4"));
        query.addSearchTerm(SearchTerms.newSearchTerm(SearchableFields.TransitURI, "nifi://*"));
        query.setMaxResults(100);

        final QuerySubmission submission = repo.submitQuery(query);
        while (!submission.getResult().isFinished()) {
            Thread.sleep(100L);
        }

        assertEquals(30, submission.getResult().getMatchingEvents().size());
        final Map<String, Integer> counts = new HashMap<>();
        for (final ProvenanceEventRecord match : submission.getResult().getMatchingEvents()) {
            System.out.println(match);

            final String index = match.getAttributes().get("iteration");
            Integer count = counts.get(index);
            if (count == null) {
                count = 0;
            }
            counts.put(index, count + 1);
        }

        assertEquals(3, counts.size());
        assertEquals(10, counts.get("0").intValue());
        assertEquals(10, counts.get("1").intValue());
        assertEquals(10, counts.get("2").intValue());

        config.setMaxRecordLife(1, TimeUnit.MILLISECONDS);

        repo.purgeOldEvents();

        Thread.sleep(2000L);    // purge is async. Give it time to do its job.

        query.setMaxResults(100);
        final QuerySubmission noResultSubmission = repo.submitQuery(query);
        while (!noResultSubmission.getResult().isFinished()) {
            Thread.sleep(10L);
        }

        assertEquals(0, noResultSubmission.getResult().getTotalHitCount());
    }

    @Test
    public void testIndexAndCompressOnRolloverAndSubsequentEmptySearch() throws IOException, InterruptedException, ParseException {
        final RepositoryConfiguration config = createConfiguration();
        config.setMaxRecordLife(30, TimeUnit.SECONDS);
        config.setMaxStorageCapacity(1024L * 1024L);
        config.setMaxEventFileLife(500, TimeUnit.MILLISECONDS);
        config.setMaxEventFileCapacity(1024L * 1024L);
        config.setSearchableFields(new ArrayList<>(SearchableFields.getStandardFields()));

        repo = new PersistentProvenanceRepository(config, DEFAULT_ROLLOVER_MILLIS);
        repo.initialize(getEventReporter());

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

        // Give time for rollover to happen
        repo.waitForRollover();

        final Query query = new Query(UUID.randomUUID().toString());
        query.setMaxResults(100);

        final QueryResult result = repo.queryEvents(query);
        assertEquals(10, result.getMatchingEvents().size());
        for (final ProvenanceEventRecord match : result.getMatchingEvents()) {
            System.out.println(match);
        }

        Thread.sleep(2000L);

        config.setMaxStorageCapacity(100L);
        config.setMaxRecordLife(500, TimeUnit.MILLISECONDS);
        repo.purgeOldEvents();

        Thread.sleep(1000L);

        final QueryResult newRecordSet = repo.queryEvents(query);
        assertTrue(newRecordSet.getMatchingEvents().isEmpty());
    }

    @Test
    public void testLineageReceiveDrop() throws IOException, InterruptedException, ParseException {
        final RepositoryConfiguration config = createConfiguration();
        config.setMaxRecordLife(3, TimeUnit.SECONDS);
        config.setMaxStorageCapacity(1024L * 1024L);
        config.setMaxEventFileLife(500, TimeUnit.MILLISECONDS);
        config.setMaxEventFileCapacity(1024L * 1024L);
        config.setSearchableFields(new ArrayList<>(SearchableFields.getStandardFields()));

        repo = new PersistentProvenanceRepository(config, DEFAULT_ROLLOVER_MILLIS);
        repo.initialize(getEventReporter());

        final String uuid = "00000000-0000-0000-0000-000000000001";
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "xyz");
        attributes.put("uuid", uuid);
        attributes.put("filename", "file-" + uuid);

        final ProvenanceEventBuilder builder = new StandardProvenanceEventRecord.Builder();
        builder.setEventTime(System.currentTimeMillis());
        builder.setEventType(ProvenanceEventType.RECEIVE);
        builder.setTransitUri("nifi://unit-test");
        attributes.put("uuid", uuid);
        builder.fromFlowFile(createFlowFile(3L, 3000L, attributes));
        builder.setComponentId("1234");
        builder.setComponentType("dummy processor");

        repo.registerEvent(builder.build());

        builder.setEventTime(System.currentTimeMillis() + 1);
        builder.setEventType(ProvenanceEventType.DROP);
        builder.setTransitUri(null);
        repo.registerEvent(builder.build());

        repo.waitForRollover();

        final Lineage lineage = repo.computeLineage(uuid);
        assertNotNull(lineage);

        // Nodes should consist of a RECEIVE followed by FlowFileNode, followed by a DROP
        final List<LineageNode> nodes = lineage.getNodes();
        final List<LineageEdge> edges = lineage.getEdges();
        assertEquals(3, nodes.size());

        for (final LineageEdge edge : edges) {
            if (edge.getSource().getNodeType() == LineageNodeType.FLOWFILE_NODE) {
                assertTrue(edge.getDestination().getNodeType() == LineageNodeType.PROVENANCE_EVENT_NODE);
                assertTrue(((EventNode) edge.getDestination()).getEventType() == ProvenanceEventType.DROP);
            } else {
                assertTrue(((EventNode) edge.getSource()).getEventType() == ProvenanceEventType.RECEIVE);
                assertTrue(edge.getDestination().getNodeType() == LineageNodeType.FLOWFILE_NODE);
            }
        }
    }

    @Test
    public void testLineageReceiveDropAsync() throws IOException, InterruptedException, ParseException {
        final RepositoryConfiguration config = createConfiguration();
        config.setMaxRecordLife(3, TimeUnit.SECONDS);
        config.setMaxStorageCapacity(1024L * 1024L);
        config.setMaxEventFileLife(500, TimeUnit.MILLISECONDS);
        config.setMaxEventFileCapacity(1024L * 1024L);
        config.setSearchableFields(new ArrayList<>(SearchableFields.getStandardFields()));

        repo = new PersistentProvenanceRepository(config, DEFAULT_ROLLOVER_MILLIS);
        repo.initialize(getEventReporter());

        final String uuid = "00000000-0000-0000-0000-000000000001";
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "xyz");
        attributes.put("uuid", uuid);
        attributes.put("filename", "file-" + uuid);

        final ProvenanceEventBuilder builder = new StandardProvenanceEventRecord.Builder();
        builder.setEventTime(System.currentTimeMillis());
        builder.setEventType(ProvenanceEventType.RECEIVE);
        builder.setTransitUri("nifi://unit-test");
        attributes.put("uuid", uuid);
        builder.fromFlowFile(createFlowFile(3L, 3000L, attributes));
        builder.setComponentId("1234");
        builder.setComponentType("dummy processor");

        repo.registerEvent(builder.build());

        builder.setEventTime(System.currentTimeMillis() + 1);
        builder.setEventType(ProvenanceEventType.DROP);
        builder.setTransitUri(null);
        repo.registerEvent(builder.build());

        repo.waitForRollover();

        final AsyncLineageSubmission submission = repo.submitLineageComputation(uuid);
        while (!submission.getResult().isFinished()) {
            Thread.sleep(100L);
        }

        assertNotNull(submission);

        // Nodes should consist of a RECEIVE followed by FlowFileNode, followed by a DROP
        final List<LineageNode> nodes = submission.getResult().getNodes();
        final List<LineageEdge> edges = submission.getResult().getEdges();
        assertEquals(3, nodes.size());

        for (final LineageEdge edge : edges) {
            if (edge.getSource().getNodeType() == LineageNodeType.FLOWFILE_NODE) {
                assertTrue(edge.getDestination().getNodeType() == LineageNodeType.PROVENANCE_EVENT_NODE);
                assertTrue(((EventNode) edge.getDestination()).getEventType() == ProvenanceEventType.DROP);
            } else {
                assertTrue(((EventNode) edge.getSource()).getEventType() == ProvenanceEventType.RECEIVE);
                assertTrue(edge.getDestination().getNodeType() == LineageNodeType.FLOWFILE_NODE);
            }
        }
    }

    @Test
    public void testLineageManyToOneSpawn() throws IOException, InterruptedException, ParseException {
        final RepositoryConfiguration config = createConfiguration();
        config.setMaxRecordLife(3, TimeUnit.SECONDS);
        config.setMaxStorageCapacity(1024L * 1024L);
        config.setMaxEventFileLife(500, TimeUnit.MILLISECONDS);
        config.setMaxEventFileCapacity(1024L * 1024L);
        config.setSearchableFields(new ArrayList<>(SearchableFields.getStandardFields()));

        repo = new PersistentProvenanceRepository(config, DEFAULT_ROLLOVER_MILLIS);
        repo.initialize(getEventReporter());

        final String childId = "00000000-0000-0000-0000-000000000000";

        final String parentId1 = "00000000-0000-0000-0001-000000000001";
        final String parentId2 = "00000000-0000-0000-0001-000000000002";
        final String parentId3 = "00000000-0000-0000-0001-000000000003";

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "xyz");
        attributes.put("uuid", childId);
        attributes.put("filename", "file-" + childId);

        final StandardProvenanceEventRecord.Builder builder = new StandardProvenanceEventRecord.Builder();
        builder.setEventTime(System.currentTimeMillis());
        builder.setEventType(ProvenanceEventType.FORK);
        attributes.put("uuid", childId);
        builder.fromFlowFile(createFlowFile(3L, 3000L, attributes));
        builder.setComponentId("1234");
        builder.setComponentType("dummy processor");

        builder.addChildUuid(childId);
        builder.addParentUuid(parentId1);
        builder.addParentUuid(parentId2);
        builder.addParentUuid(parentId3);

        repo.registerEvent(builder.build());

        repo.waitForRollover();

        final Lineage lineage = repo.computeLineage(childId);
        assertNotNull(lineage);

        // these are not necessarily accurate asserts....
        final List<LineageNode> nodes = lineage.getNodes();
        final List<LineageEdge> edges = lineage.getEdges();
        assertEquals(2, nodes.size());
        assertEquals(1, edges.size());
    }

    @Test
    public void testLineageManyToOneSpawnAsync() throws IOException, InterruptedException, ParseException {
        final RepositoryConfiguration config = createConfiguration();
        config.setMaxRecordLife(3, TimeUnit.SECONDS);
        config.setMaxStorageCapacity(1024L * 1024L);
        config.setMaxEventFileLife(500, TimeUnit.MILLISECONDS);
        config.setMaxEventFileCapacity(1024L * 1024L);
        config.setSearchableFields(new ArrayList<>(SearchableFields.getStandardFields()));

        repo = new PersistentProvenanceRepository(config, DEFAULT_ROLLOVER_MILLIS);
        repo.initialize(getEventReporter());

        final String childId = "00000000-0000-0000-0000-000000000000";

        final String parentId1 = "00000000-0000-0000-0001-000000000001";
        final String parentId2 = "00000000-0000-0000-0001-000000000002";
        final String parentId3 = "00000000-0000-0000-0001-000000000003";

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "xyz");
        attributes.put("uuid", childId);
        attributes.put("filename", "file-" + childId);

        final StandardProvenanceEventRecord.Builder builder = new StandardProvenanceEventRecord.Builder();
        builder.setEventTime(System.currentTimeMillis());
        builder.setEventType(ProvenanceEventType.FORK);
        attributes.put("uuid", childId);
        builder.fromFlowFile(createFlowFile(3L, 3000L, attributes));
        builder.setComponentId("1234");
        builder.setComponentType("dummy processor");

        builder.addChildUuid(childId);
        builder.addParentUuid(parentId1);
        builder.addParentUuid(parentId2);
        builder.addParentUuid(parentId3);

        repo.registerEvent(builder.build());

        repo.waitForRollover();

        final AsyncLineageSubmission submission = repo.submitLineageComputation(childId);
        while (!submission.getResult().isFinished()) {
            Thread.sleep(100L);
        }

        // these are not accurate asserts....
        final List<LineageNode> nodes = submission.getResult().getNodes();
        final List<LineageEdge> edges = submission.getResult().getEdges();
        assertEquals(2, nodes.size());
        assertEquals(1, edges.size());
    }

    @Test
    public void testCorrectProvenanceEventIdOnRestore() throws IOException {
        final RepositoryConfiguration config = createConfiguration();
        config.setMaxEventFileLife(1, TimeUnit.SECONDS);
        repo = new PersistentProvenanceRepository(config, DEFAULT_ROLLOVER_MILLIS);
        repo.initialize(getEventReporter());

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

        final PersistentProvenanceRepository secondRepo = new PersistentProvenanceRepository(config, DEFAULT_ROLLOVER_MILLIS);
        secondRepo.initialize(getEventReporter());

        final ProvenanceEventRecord event11 = builder.build();
        secondRepo.registerEvent(event11);
        secondRepo.waitForRollover();
        final ProvenanceEventRecord event11Retrieved = secondRepo.getEvent(10L);
        assertNotNull(event11Retrieved);
        assertEquals(10, event11Retrieved.getEventId());
    }

    @Test
    public void testIndexDirectoryRemoved() throws InterruptedException, IOException, ParseException {
        final RepositoryConfiguration config = createConfiguration();
        config.setMaxRecordLife(3, TimeUnit.SECONDS);
        config.setMaxStorageCapacity(1024L * 1024L);
        config.setMaxEventFileLife(500, TimeUnit.MILLISECONDS);
        config.setMaxEventFileCapacity(1024L * 1024L);
        config.setSearchableFields(new ArrayList<>(SearchableFields.getStandardFields()));

        repo = new PersistentProvenanceRepository(config, DEFAULT_ROLLOVER_MILLIS);
        repo.initialize(getEventReporter());

        final String uuid = "00000000-0000-0000-0000-000000000000";
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "xyz");
        attributes.put("xyz", "abc");
        attributes.put("filename", "file-" + uuid);

        final ProvenanceEventBuilder builder = new StandardProvenanceEventRecord.Builder();
        builder.setEventTime(System.currentTimeMillis());
        builder.setEventType(ProvenanceEventType.RECEIVE);
        builder.setTransitUri("nifi://unit-test");
        builder.fromFlowFile(createFlowFile(3L, 3000L, attributes));
        builder.setComponentId("1234");
        builder.setComponentType("dummy processor");

        for (int i = 0; i < 10; i++) {
            attributes.put("uuid", "00000000-0000-0000-0000-00000000000" + i);
            builder.fromFlowFile(createFlowFile(i, 3000L, attributes));
            repo.registerEvent(builder.build());
        }

        repo.waitForRollover();

        final Query query = new Query(UUID.randomUUID().toString());
        query.addSearchTerm(SearchTerms.newSearchTerm(SearchableFields.Filename, "file-*"));
        query.addSearchTerm(SearchTerms.newSearchTerm(SearchableFields.ComponentID, "12?4"));
        query.addSearchTerm(SearchTerms.newSearchTerm(SearchableFields.TransitURI, "nifi://*"));
        query.setMaxResults(100);

        final QueryResult result = repo.queryEvents(query);
        assertEquals(10, result.getMatchingEvents().size());

        Thread.sleep(2000L);

        // Ensure index directory exists
        final FileFilter indexFileFilter = new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.getName().startsWith("index");
            }
        };
        File[] storageDirFiles = config.getStorageDirectories().get(0).listFiles(indexFileFilter);
        assertEquals(1, storageDirFiles.length);

        config.setMaxStorageCapacity(100L);
        config.setMaxRecordLife(500, TimeUnit.MILLISECONDS);
        repo.purgeOldEvents();
        Thread.sleep(2000L);

        final QueryResult newRecordSet = repo.queryEvents(query);
        assertTrue(newRecordSet.getMatchingEvents().isEmpty());

        // Ensure index directory is gone
        storageDirFiles = config.getStorageDirectories().get(0).listFiles(indexFileFilter);
        assertEquals(0, storageDirFiles.length);
    }

    @Test
    public void testTextualQuery() throws InterruptedException, IOException, ParseException {
        final RepositoryConfiguration config = createConfiguration();
        config.setMaxEventFileLife(500, TimeUnit.MILLISECONDS);
        config.setSearchableFields(new ArrayList<>(SearchableFields.getStandardFields()));
        repo = new PersistentProvenanceRepository(config, DEFAULT_ROLLOVER_MILLIS);
        repo.initialize(getEventReporter());

        final String uuid = "00000000-0000-0000-0000-000000000000";
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("abc", "xyz");
        attributes.put("xyz", "abc");
        attributes.put("filename", "file-unnamed");

        final long now = System.currentTimeMillis();

        final ProvenanceEventBuilder builder = new StandardProvenanceEventRecord.Builder();
        builder.setEventTime(now - TimeUnit.MILLISECONDS.convert(30, TimeUnit.SECONDS));
        builder.setEventType(ProvenanceEventType.RECEIVE);
        builder.setTransitUri("nifi://unit-test");
        attributes.put("uuid", uuid);
        builder.fromFlowFile(createFlowFile(3L, 3000L, attributes));
        builder.setComponentId("1234");
        builder.setComponentType("dummy processor");

        for (int i = 0; i < 10; i++) {
            if (i > 5) {
                attributes.put("filename", "file-" + i);
                builder.setEventTime(System.currentTimeMillis());
            }
            builder.fromFlowFile(createFlowFile(i, 3000L, attributes));
            attributes.put("uuid", "00000000-0000-0000-0000-00000000000" + i);
            repo.registerEvent(builder.build());
        }

        repo.waitForRollover();

        final IndexConfiguration indexConfig = new IndexConfiguration(config);
        final List<File> indexDirs = indexConfig.getIndexDirectories();

        final String query = "uuid:00000000-0000-0000-0000-0000000000* AND NOT filename:file-?";
        final List<Document> results = runQuery(indexDirs.get(0), config.getStorageDirectories(), query);

        assertEquals(6, results.size());
    }

    private List<Document> runQuery(final File indexDirectory, final List<File> storageDirs, final String query) throws IOException, ParseException {
        try (final DirectoryReader directoryReader = DirectoryReader.open(FSDirectory.open(indexDirectory))) {
            final IndexSearcher searcher = new IndexSearcher(directoryReader);

            final Analyzer analyzer = new SimpleAnalyzer();
            final org.apache.lucene.search.Query luceneQuery = new QueryParser("uuid", analyzer).parse(query);

            final Query q = new Query("");
            q.setMaxResults(1000);
            TopDocs topDocs = searcher.search(luceneQuery, 1000);

            final List<Document> docs = new ArrayList<>();
            for (int i = 0; i < topDocs.scoreDocs.length; i++) {
                final ScoreDoc scoreDoc = topDocs.scoreDocs[i];
                final int docId = scoreDoc.doc;
                final Document d = directoryReader.document(docId);
                docs.add(d);
            }

            return docs;
        }
    }

    @Test
    public void testMergeJournals() throws IOException, InterruptedException {
        final RepositoryConfiguration config = createConfiguration();
        config.setMaxEventFileLife(3, TimeUnit.SECONDS);
        repo = new PersistentProvenanceRepository(config, DEFAULT_ROLLOVER_MILLIS);
        repo.initialize(getEventReporter());

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

                try (RecordReader reader = RecordReaders.newRecordReader(file, null)) {
                    ProvenanceEventRecord r = null;

                    while ((r = reader.nextRecord()) != null) {
                        assertEquals(counter++, r.getEventId());
                    }
                }
            }
        }

        assertEquals(10000, counter);
    }
}
