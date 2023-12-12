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
package org.apache.nifi.questdb.embedded;

import org.apache.nifi.questdb.Client;
import org.apache.nifi.questdb.DatabaseException;
import org.apache.nifi.questdb.DatabaseManager;
import org.apache.nifi.questdb.mapping.RequestMapping;
import org.apache.nifi.questdb.rollover.RolloverStrategy;
import org.apache.nifi.questdb.util.Event;
import org.apache.nifi.questdb.util.QuestDbTestUtil;
import org.apache.nifi.util.file.FileUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.StreamSupport;

import static org.apache.nifi.questdb.util.QuestDbTestUtil.CREATE_EVENT2_TABLE;
import static org.apache.nifi.questdb.util.QuestDbTestUtil.CREATE_EVENT_TABLE;
import static org.apache.nifi.questdb.util.QuestDbTestUtil.EVENT2_TABLE_NAME;
import static org.apache.nifi.questdb.util.QuestDbTestUtil.EVENT_TABLE_NAME;
import static org.apache.nifi.questdb.util.QuestDbTestUtil.SELECT_QUERY;
import static org.apache.nifi.questdb.util.QuestDbTestUtil.getRandomTestData;
import static org.apache.nifi.questdb.util.QuestDbTestUtil.getTestData;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class EmbeddedDatabaseManagerTest extends EmbeddedQuestDbTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedDatabaseManagerTest.class);
    private static final int DAYS_TO_KEEP_EVENT = 1;

    @Test
    public void testAcquiringWithoutInitialization() {
        final EmbeddedDatabaseManager testSubject = new EmbeddedDatabaseManager(new SimpleEmbeddedDatabaseManagerContext());
        assertThrows(IllegalStateException.class, () -> testSubject.acquireClient());
    }

    @Test
    public void testHappyPath() throws DatabaseException {
        final List<Event> testData = getTestData();
        assertDatabaseFolderIsEmpty();

        final DatabaseManager testSubject = getTestSubject();
        assertDatabaseFolderIsNotEmpty();

        final Client client = testSubject.acquireClient();

        client.insert(EVENT_TABLE_NAME, QuestDbTestUtil.getEventTableDataSource(testData));
        final List<Event> result = client.query(SELECT_QUERY, RequestMapping.getResultProcessor(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING));

        assertIterableEquals(testData, result);

        testSubject.close();

        // Even if the client itself is not connected, manager prevents client to reach database after closing
        assertFalse(client.query(SELECT_QUERY, RequestMapping.getResultProcessor(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING)).iterator().hasNext());
    }

    @Test
    public void testRollover() throws DatabaseException, InterruptedException {
        final List<Event> testData = new ArrayList<>();
        testData.add(new Event(Instant.now().minus((DAYS_TO_KEEP_EVENT + 1), ChronoUnit.DAYS), "A", 1));
        testData.add(new Event(Instant.now(), "B", 2));
        final DatabaseManager testSubject = getTestSubject();

        final Client client = testSubject.acquireClient();
        client.insert(EVENT_TABLE_NAME, QuestDbTestUtil.getEventTableDataSource(testData));

        // The rollover runs in every 5 seconds
        Thread.sleep(TimeUnit.SECONDS.toMillis(6));

        final List<Event> result = client.query(SELECT_QUERY, RequestMapping.getResultProcessor(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING));
        testSubject.close();

        assertEquals(1, result.size());
    }

    @Test
    public void testParallelClientsOnSameThread() throws DatabaseException {
        final List<Event> testData = getTestData();
        final DatabaseManager testSubject = getTestSubject();

        final Client client1 = testSubject.acquireClient();
        client1.insert(EVENT_TABLE_NAME, QuestDbTestUtil.getEventTableDataSource(testData));
        final List<Event> result1 = client1.query(SELECT_QUERY, RequestMapping.getResultProcessor(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING));

        final Client client2 = testSubject.acquireClient();
        final List<Event> result2 = client2.query(SELECT_QUERY, RequestMapping.getResultProcessor(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING));

        testSubject.close();
        assertEquals(3, result2.size());
        assertIterableEquals(result1, result2);
    }

    @Test
    public void testParallelClientsDifferentThread() throws InterruptedException {
        final List<Event> testData = getTestData();
        final DatabaseManager testSubject = getTestSubject();
        final CountDownLatch step1 = new CountDownLatch(1);
        final CountDownLatch step2 = new CountDownLatch(1);
        final AtomicReference<List<Event>> result1 = new AtomicReference<>();
        final AtomicReference<List<Event>> result2 = new AtomicReference<>();

        new Thread(() -> {
            try {
                final Client client1 = testSubject.acquireClient();
                client1.insert(EVENT_TABLE_NAME, QuestDbTestUtil.getEventTableDataSource(testData));
                result1.set(client1.query(SELECT_QUERY, RequestMapping.getResultProcessor(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING)));
                step1.countDown();
            } catch (final DatabaseException e) {
                throw new RuntimeException(e);
            }
        }).start();

        new Thread(() -> {
            try {
                step1.await();
                final Client client2 = testSubject.acquireClient();
                result2.set(client2.query(SELECT_QUERY, RequestMapping.getResultProcessor(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING)));
                step2.countDown();
            } catch (final DatabaseException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();

        step2.await();

        testSubject.close();
        assertEquals(3, result1.get().size());
        assertIterableEquals(result1.get(), result2.get());
    }

    @Test
    public void testContactingToDatabaseWithDifferentManager() throws DatabaseException {
        final List<Event> testData = getTestData();
        final DatabaseManager testSubject1 = getTestSubject();

        final Client client1 = testSubject1.acquireClient();
        client1.insert(EVENT_TABLE_NAME, QuestDbTestUtil.getEventTableDataSource(testData));
        final List<Event> result1 = client1.query(SELECT_QUERY, RequestMapping.getResultProcessor(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING));
        client1.disconnect();
        testSubject1.close();

        assertDatabaseFolderIsNotEmpty();

        final DatabaseManager testSubject2 = getTestSubject();
        final Client client2 = testSubject2.acquireClient();
        final List<Event> result2 = client2.query(SELECT_QUERY, RequestMapping.getResultProcessor(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING));

        assertIterableEquals(result1, result2);
    }

    @Test
    public void testDatabaseRestorationAfterLostDatabase() throws DatabaseException, IOException {
        final List<Event> testData = getTestData();
        final DatabaseManager testSubject = getTestSubject();
        final Client client = testSubject.acquireClient();
        client.insert(EVENT_TABLE_NAME, QuestDbTestUtil.getEventTableDataSource(testData));

        FileUtils.deleteFilesInDir(testDbPathDirectory.toFile(), (dir, name) -> true, LOGGER, true, true);

        client.insert(EVENT_TABLE_NAME, QuestDbTestUtil.getEventTableDataSource(testData));
        final List<Event> result = client.query(SELECT_QUERY, RequestMapping.getResultProcessor(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING));

        testSubject.close();

        // Ensuring that not the fallback client answers
        assertEquals(3, result.size());
    }

    @Test
    public void testDatabaseRestorationAfterLosingTableFiles() throws DatabaseException, IOException {
        final List<Event> testData = getTestData();
        final DatabaseManager testSubject = getTestSubject();
        final Client client = testSubject.acquireClient();
        client.insert(EVENT_TABLE_NAME, QuestDbTestUtil.getEventTableDataSource(testData));

        final File eventTableDirectory = new File(testDbPathDirectory.toFile(), "event");
        FileUtils.deleteFilesInDir(eventTableDirectory, (dir, name) -> true, LOGGER, true, true);
        FileUtils.deleteFile(eventTableDirectory, LOGGER);

        client.insert(EVENT_TABLE_NAME, QuestDbTestUtil.getEventTableDataSource(testData));
        final List<Event> result = client.query(SELECT_QUERY, RequestMapping.getResultProcessor(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING));

        testDbPathDirectory.toFile().list((dir, name) -> dir.isDirectory());

        testSubject.close();

        // Ensuring that not the fallback client answers
        assertEquals(3, result.size());
    }

    @Test
    public void testDatabaseRestorationAfterCorruptedFiles() throws DatabaseException, IOException {
        final DatabaseManager testSubject1 = getTestSubject();
        final Client client1 = testSubject1.acquireClient();

        for (int i = 1; i <= 10; i++) {
            client1.insert(EVENT_TABLE_NAME, QuestDbTestUtil.getEventTableDataSource(getTestData()));
        }

        final List<Event> result1 = client1.query(SELECT_QUERY, RequestMapping.getResultProcessor(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING));
        assertEquals(30, result1.size());

        corruptDatabaseFile();

        client1.disconnect();
        testSubject1.close();

        // Corrupting the persisted files will not directly affect the database immediately. In order to enforce reading
        // information from the files, we need to have a new manager. In real cases this behaviour might be triggered
        // during normal usage.
        final DatabaseManager testSubject2 = getTestSubject();
        final Client client2 = testSubject2.acquireClient();

        final List<Event> result2 = client2.query(SELECT_QUERY, RequestMapping.getResultProcessor(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING));
        assertEquals(0, result2.size());

        client2.insert(EVENT_TABLE_NAME, QuestDbTestUtil.getEventTableDataSource(getTestData()));
        final List<Event> result3 = client2.query(SELECT_QUERY, RequestMapping.getResultProcessor(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING));
        // After successful recreation of the database, the manager does not fall back to "dummy answers" behaviour
        assertEquals(3, result3.size());

        final List<File> backup = Arrays.asList(new File(testDbPathDirectory.toFile().getParentFile(), "questDbBackup").listFiles(file -> file.isDirectory() && file.getName().startsWith("backup_")));
        assertFalse(backup.isEmpty());

        backup.forEach(f -> {
            try {
                FileUtils.deleteFile(f, true);
            } catch (final IOException e) {
                fail();
            }
        });
    }

    @Test
    public void testWhenBackupIsUnsuccessfulManagerRemovesItAndContinuesWork() throws DatabaseException, IOException {
        final DatabaseManager testSubject1 = getTestSubjectBuilder().backupLocation("/nonExistingPlace").build();
        final Client client1 = testSubject1.acquireClient();

        for (int i = 1; i <= 10; i++) {
            client1.insert(EVENT_TABLE_NAME, QuestDbTestUtil.getEventTableDataSource(getTestData()));
        }

        final List<Event> result1 = client1.query(SELECT_QUERY, RequestMapping.getResultProcessor(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING));
        assertEquals(30, result1.size());

        corruptDatabaseFile();

        client1.disconnect();
        testSubject1.close();

        final DatabaseManager testSubject2 = getTestSubjectBuilder().backupLocation("/nonExistingPlace").build();
        final Client client2 = testSubject2.acquireClient();

        client2.insert(EVENT_TABLE_NAME, QuestDbTestUtil.getEventTableDataSource(getTestData()));
        final List<Event> result2 = client2.query(SELECT_QUERY, RequestMapping.getResultProcessor(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING));
        // After corrupted database cannot be moved out from the persist folder it is simply deleted
        assertEquals(3, result2.size());
    }

    @Test
    public void testWhenRestorationIsUnsuccessfulManagerFallsBackToDummyAnswers() throws DatabaseException, IOException {
        final DatabaseManager testSubject1 = getTestSubjectBuilder().backupLocation("/nonExistingPlace").build();
        final Client client1 = testSubject1.acquireClient();

        for (int i = 1; i <= 10; i++) {
            client1.insert(EVENT_TABLE_NAME, QuestDbTestUtil.getEventTableDataSource(getTestData()));
        }

        final List<Event> result1 = client1.query(SELECT_QUERY, RequestMapping.getResultProcessor(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING));
        assertEquals(30, result1.size());

        corruptDatabaseFile();
        testDbPathDirectory.toFile().setWritable(false);

        client1.disconnect();
        testSubject1.close();

        final DatabaseManager testSubject2 = getTestSubject();
        final Client client2 = testSubject2.acquireClient();

        client2.insert(EVENT_TABLE_NAME, QuestDbTestUtil.getEventTableDataSource(getTestData()));
        final List<Event> result2 = client2.query(SELECT_QUERY, RequestMapping.getResultProcessor(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING));
        assertEquals(0, result2.size());

        testDbPathDirectory.toFile().setWritable(true);
    }

    @Test
    public void testFallsBackToDummyWhenCannotEnsureDatabaseHealth() throws DatabaseException, IOException {
        final DatabaseManager testSubject = getTestSubjectBuilder("/nonExistingPlace").build();
        final Client client = testSubject.acquireClient();

        client.insert(EVENT_TABLE_NAME, QuestDbTestUtil.getEventTableDataSource(getTestData()));
        final List<Event> result = client.query(SELECT_QUERY, RequestMapping.getResultProcessor(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING));
        assertEquals(0, result.size());

        client.disconnect();
        testSubject.close();
    }

    @Test
    @Timeout(10)
    @Disabled
    /**
     * This test case is not part of the normally running test set and needs preparation. In order to successfully
     * run this test method, a dedicated partition is necessary with relatively small space. The test intends to
     * examine, how the manager behaves when the disk used for persisting the database runs out of space. It is suggested
     * to create a "memdisk" for this particular test.
     */
    public void testDiskRunOutOfSpace() throws DatabaseException {
        final DatabaseManager testSubject = getTestSubjectBuilder("/Volumes/RAM_Disk/testDb").build();
        final Client client = testSubject.acquireClient();

        boolean reachedBreakdown = false;

        while (!reachedBreakdown) {
            client.insert(EVENT_TABLE_NAME, QuestDbTestUtil.getEventTableDataSource(getRandomTestData()));
            final List<Event> result = client.query(SELECT_QUERY, RequestMapping.getResultProcessor(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING));
            if (StreamSupport.stream(result.spliterator(), false).count() == 0) {
                reachedBreakdown = true;
            }
        }

        LOGGER.info("Memdisk is full, the manager switched to dummy answer mode");
    }

    private DatabaseManager getTestSubject() {
        return getTestSubjectBuilder().build();
    }

    private EmbeddedDatabaseManagerBuilder getTestSubjectBuilder() {
        return getTestSubjectBuilder(testDbPathDirectory.toFile().getAbsolutePath());
    }

    private static EmbeddedDatabaseManagerBuilder getTestSubjectBuilder(final String persistLocation) {
        return EmbeddedDatabaseManagerBuilder
                .builder(persistLocation)
                .lockAttemptTime(50, TimeUnit.MILLISECONDS)
                .rolloverFrequency(5, TimeUnit.SECONDS)
                .numberOfAttemptedRetries(2)
                .addTable(EVENT_TABLE_NAME, CREATE_EVENT_TABLE, RolloverStrategy.deleteOld(DAYS_TO_KEEP_EVENT))
                .addTable(EVENT2_TABLE_NAME, CREATE_EVENT2_TABLE, RolloverStrategy.keep());
    }

    private void corruptDatabaseFile() throws IOException {
        final File fileToCorrupt = new File(new File(testDbPathDirectory.toFile(), "event"), "subject.o");
        final FileWriter fileWriter = new FileWriter(fileToCorrupt);
        final BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
        bufferedWriter.write("This should corrupt the db");
        bufferedWriter.newLine();
        bufferedWriter.close();
        fileWriter.close();
    }

    private void assertDatabaseFolderIsNotEmpty() {
        assertTrue(testDbPathDirectory.toFile().list().length > 0);
    }

    private void assertDatabaseFolderIsEmpty() {
        assertEquals(0, testDbPathDirectory.toFile().list().length);
    }
}
