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

import org.apache.commons.lang3.SystemUtils;
import org.apache.nifi.questdb.Client;
import org.apache.nifi.questdb.DatabaseException;
import org.apache.nifi.questdb.DatabaseManager;
import org.apache.nifi.questdb.mapping.RequestMapping;
import org.apache.nifi.questdb.rollover.RolloverStrategy;
import org.apache.nifi.questdb.util.Event;
import org.apache.nifi.questdb.util.QuestDbTestUtil;
import org.apache.nifi.util.file.FileUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.condition.OS;
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

public class EmbeddedDatabaseManagerTest extends EmbeddedQuestDbTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedDatabaseManagerTest.class);
    private static final int DAYS_TO_KEEP_EVENT = 1;
    private static final String NON_EXISTING_PLACE = SystemUtils.IS_OS_WINDOWS ? "T:/nonExistingPlace" : "/nonExistingPlace";

    @Test
    public void testAcquiringWithoutInitialization() {
        final EmbeddedDatabaseManager testSubject = new EmbeddedDatabaseManager(new SimpleEmbeddedDatabaseManagerContext());
        assertThrows(IllegalStateException.class, testSubject::acquireClient);
    }

    @Test
    public void testHappyPath() throws DatabaseException {
        final List<Event> testData = getTestData();
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

        final long pollTimeoutDuration = 3000;
        final long pollTimeout = System.currentTimeMillis() + pollTimeoutDuration;
        final int expectedNumberOfResults = 1;
        int numberOfResults;

        do {
            Thread.sleep(100);
            final List<Event> result = client.query(SELECT_QUERY, RequestMapping.getResultProcessor(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING));
            numberOfResults = result.size();
        } while (numberOfResults != expectedNumberOfResults || pollTimeout > System.currentTimeMillis());

        testSubject.close();
        assertEquals(expectedNumberOfResults, numberOfResults);
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
        client2.disconnect();
        testSubject2.close();

        assertIterableEquals(result1, result2);
    }

    @Test
    public void testDatabaseRestorationAfterLostDatabase() throws DatabaseException {
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
    public void testDatabaseRestorationAfterLosingTableFiles() throws DatabaseException {
        final List<Event> testData = getTestData();
        final DatabaseManager testSubject = getTestSubject();
        final Client client = testSubject.acquireClient();
        client.insert(EVENT_TABLE_NAME, QuestDbTestUtil.getEventTableDataSource(testData));

        final File eventTableDirectory = new File(testDbPathDirectory.toFile(), "event");
        FileUtils.deleteFilesInDir(eventTableDirectory, (dir, name) -> true, LOGGER, true, true);
        FileUtils.deleteFile(eventTableDirectory, LOGGER);

        client.insert(EVENT_TABLE_NAME, QuestDbTestUtil.getEventTableDataSource(testData));
        final List<Event> result = client.query(SELECT_QUERY, RequestMapping.getResultProcessor(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING));

        testSubject.close();

        // Ensuring that not the fallback client answers
        assertEquals(3, result.size());
    }

    @Test
    @DisabledOnOs(value = OS.WINDOWS, disabledReason = "This testcase cannot be reproduced under Windows using Junit")
    public void testDatabaseRestorationAfterCorruptedFiles() throws DatabaseException, IOException {
        final DatabaseManager testSubject1 = getTestSubject();
        final Client client1 = testSubject1.acquireClient();

        for (int i = 1; i <= 10; i++) {
            client1.insert(EVENT_TABLE_NAME, QuestDbTestUtil.getEventTableDataSource(getTestData()));
        }

        final List<Event> result1 = client1.query(SELECT_QUERY, RequestMapping.getResultProcessor(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING));
        assertEquals(30, result1.size());

        client1.disconnect();
        testSubject1.close();

        corruptDatabaseFile();

        // Corrupting the persisted files will not directly affect the database immediately. In order to enforce reading
        // information from the files, we need to have a new manager. In real cases this behaviour might be triggered
        // during normal usage.
        final DatabaseManager testSubject2 = getTestSubject();
        final Client client2 = testSubject2.acquireClient();

        final List<Event> result2 = client2.query(SELECT_QUERY, RequestMapping.getResultProcessor(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING));
        assertEquals(0, result2.size());

        client2.insert(EVENT_TABLE_NAME, QuestDbTestUtil.getEventTableDataSource(getTestData()));
        final List<Event> result3 = client2.query(SELECT_QUERY, RequestMapping.getResultProcessor(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING));
        // After successful recreation of the database, the manager does not fall back to NoOp behaviour
        assertEquals(3, result3.size());

        final File backupDirectory = new File(testDbPathDirectory.toFile().getParentFile(), "questDbBackup");
        final List<File> backup = Arrays.asList(backupDirectory.listFiles(file -> file.isDirectory() && file.getName().startsWith("backup_")));
        assertFalse(backup.isEmpty());

        client2.disconnect();
        testSubject2.close();

        FileUtils.deleteFile(backupDirectory, true);
    }

    @Test
    @DisabledOnOs(value = OS.WINDOWS, disabledReason = "This testcase cannot be reproduced under Windows using Junit")
    public void testWhenBackupIsUnsuccessfulManagerRemovesItAndContinuesWork() throws DatabaseException, IOException {
        final DatabaseManager testSubject1 = getTestSubjectBuilder().backupLocation(NON_EXISTING_PLACE).build();
        final Client client1 = testSubject1.acquireClient();

        for (int i = 1; i <= 10; i++) {
            client1.insert(EVENT_TABLE_NAME, QuestDbTestUtil.getEventTableDataSource(getTestData()));
        }

        final List<Event> result1 = client1.query(SELECT_QUERY, RequestMapping.getResultProcessor(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING));
        assertEquals(30, result1.size());

        corruptDatabaseFile();

        client1.disconnect();
        testSubject1.close();

        final DatabaseManager testSubject2 = getTestSubjectBuilder().backupLocation(NON_EXISTING_PLACE).build();
        final Client client2 = testSubject2.acquireClient();

        client2.insert(EVENT_TABLE_NAME, QuestDbTestUtil.getEventTableDataSource(getTestData()));
        final List<Event> result2 = client2.query(SELECT_QUERY, RequestMapping.getResultProcessor(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING));
        // After corrupted database cannot be moved out from the persist folder it is simply deleted
        assertEquals(3, result2.size());

        // Close Database Manager to avoid orphaned files
        testSubject2.close();
    }

    @Test
    @DisabledOnOs(value = OS.WINDOWS, disabledReason = "This testcase cannot be reproduced under Windows using Junit")
    public void testWhenRestorationIsUnsuccessfulManagerFallsBackToNoOp() throws DatabaseException, IOException {
        final DatabaseManager testSubject1 = getTestSubjectBuilder().backupLocation(NON_EXISTING_PLACE).build();
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

        client2.disconnect();
        testSubject2.close();

        final File backupDirectory = new File(testDbPathDirectory.toFile().getParentFile(), "questDbBackup");
        FileUtils.deleteFile(backupDirectory, true);
    }

    @Test
    public void testFallsBackToNoOpWhenCannotEnsureDatabaseHealth() throws DatabaseException {
        final DatabaseManager testSubject = getTestSubjectBuilder(NON_EXISTING_PLACE).build();
        final Client client = testSubject.acquireClient();

        client.insert(EVENT_TABLE_NAME, QuestDbTestUtil.getEventTableDataSource(getTestData()));
        final List<Event> result = client.query(SELECT_QUERY, RequestMapping.getResultProcessor(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING));
        assertEquals(0, result.size());

        client.disconnect();
        testSubject.close();
    }

    /**
     * This test case is not part of the normally running test set and needs preparation. In order to successfully
     * run this test method, a dedicated partition is necessary with relatively small space. The test intends to
     * examine, how the manager behaves when the disk used for persisting the database runs out of space. It is suggested
     * to create a "memdisk" for this particular test.
     */
    @Test
    @Timeout(10)
    @EnabledIfSystemProperty(named = "testQuestDBOutOfSpace", matches = "true")
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
                .rolloverFrequency(1, TimeUnit.SECONDS)
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
        final String[] files = testDbPathDirectory.toFile().list();
        final int filesFound = files == null ? 0 : files.length;
        assertTrue(filesFound > 0);
    }
}
