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
package org.wali;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestMinimalLockingWriteAheadLog {
    private static final Logger logger = LoggerFactory.getLogger(TestMinimalLockingWriteAheadLog.class);


    @Test
    public void testRepoDoesntContinuallyGrowOnOutOfMemoryError() throws IOException, InterruptedException {
        final int numPartitions = 8;

        final Path path = Paths.get("target/minimal-locking-repo");
        deleteRecursively(path.toFile());
        assertTrue(path.toFile().mkdirs());

        final DummyRecordSerde serde = new DummyRecordSerde();
        final WriteAheadRepository<DummyRecord> repo = new MinimalLockingWriteAheadLog<>(path, numPartitions, serde, null);
        try {
            final Collection<DummyRecord> initialRecs = repo.recoverRecords();
            assertTrue(initialRecs.isEmpty());

            serde.setThrowOOMEAfterNSerializeEdits(100);
            for (int i = 0; i < 108; i++) {
                try {
                    final DummyRecord record = new DummyRecord(String.valueOf(i), UpdateType.CREATE);
                    repo.update(Collections.singleton(record), false);
                } catch (final OutOfMemoryError oome) {
                    logger.info("Received OOME on record " + i);
                }
            }

            long expectedSize = sizeOf(path.toFile());
            for (int i = 0; i < 1000; i++) {
                try {
                    final DummyRecord record = new DummyRecord(String.valueOf(i), UpdateType.CREATE);
                    repo.update(Collections.singleton(record), false);
                    Assert.fail("Expected IOE but it didn't happen");
                } catch (final IOException ioe) {
                    // will get IOException because all Partitions have been blacklisted
                }
            }

            long newSize = sizeOf(path.toFile());
            assertEquals(expectedSize, newSize);

            try {
                repo.checkpoint();
                Assert.fail("Expected OOME but it didn't happen");
            } catch (final OutOfMemoryError oome) {
            }

            expectedSize = sizeOf(path.toFile());

            for (int i = 0; i < 100000; i++) {
                try {
                    final DummyRecord record = new DummyRecord(String.valueOf(i), UpdateType.CREATE);
                    repo.update(Collections.singleton(record), false);
                    Assert.fail("Expected IOE but it didn't happen");
                } catch (final IOException ioe) {
                    // will get IOException because all Partitions have been blacklisted
                }
            }

            newSize = sizeOf(path.toFile());
            assertEquals(expectedSize, newSize);
        } finally {
            repo.shutdown();
        }
    }

    /**
     * This test is intended to continually update the Write-ahead log using many threads, then
     * stop and restore the repository to check for any corruption. There were reports of potential threading
     * issues leading to repository corruption. This was an attempt to replicate. It should not be run as a
     * unit test, really, but will be left, as it can be valuable to exercise the implementation
     *
     * @throws IOException if unable to read from/write to the write-ahead log
     * @throws InterruptedException if a thread is interrupted
     */
    @Test
    @Ignore
    public void tryToCauseThreadingIssue() throws IOException, InterruptedException {
        System.setProperty("org.slf4j.simpleLogger.log.org.wali", "INFO");

        final int numThreads = 12;
        final long iterationsPerThread = 1000000;
        final int numAttempts = 1000;

        final Path path = Paths.get("D:/dummy/minimal-locking-repo");
        path.toFile().mkdirs();

        final AtomicReference<WriteAheadRepository<DummyRecord>> writeRepoRef = new AtomicReference<>();
        final AtomicBoolean checkpointing = new AtomicBoolean(false);

        final Thread bgThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    checkpointing.set(true);

                    final WriteAheadRepository<DummyRecord> repo = writeRepoRef.get();
                    if (repo != null) {
                        try {
                            repo.checkpoint();
                        } catch (IOException e) {
                            e.printStackTrace();
                            Assert.fail();
                        }
                    }

                    checkpointing.set(false);

                    try {
                        TimeUnit.SECONDS.sleep(5);
                    } catch (InterruptedException e) {
                    }
                }
            }
        });
        bgThread.setDaemon(true);
        bgThread.start();

        for (int x = 0; x < numAttempts; x++) {
            final DummyRecordSerde serde = new DummyRecordSerde();
            final WriteAheadRepository<DummyRecord> writeRepo = new MinimalLockingWriteAheadLog<>(path, 256, serde, null);
            final Collection<DummyRecord> writeRecords = writeRepo.recoverRecords();
            for (final DummyRecord record : writeRecords) {
                assertEquals("B", record.getProperty("A"));
            }

            writeRepoRef.set(writeRepo);

            final Thread[] threads = new Thread[numThreads];
            for (int i = 0; i < numThreads; i++) {
                final Thread t = new InlineCreationInsertThread(iterationsPerThread, writeRepo);
                t.start();
                threads[i] = t;
            }

            for (final Thread t : threads) {
                t.join();
            }

            writeRepoRef.set(null);
            writeRepo.shutdown();

            boolean cp = checkpointing.get();
            while (cp) {
                Thread.sleep(100L);
                cp = checkpointing.get();
            }

            final WriteAheadRepository<DummyRecord> readRepo = new MinimalLockingWriteAheadLog<>(path, 256, serde, null);
            // ensure that we are able to recover the records properly
            final Collection<DummyRecord> readRecords = readRepo.recoverRecords();
            for (final DummyRecord record : readRecords) {
                assertEquals("B", record.getProperty("A"));
            }
            readRepo.shutdown();
        }
    }

    @Test
    public void testWrite() throws IOException, InterruptedException {
        final int numPartitions = 8;

        final Path path = Paths.get("target/minimal-locking-repo");
        deleteRecursively(path.toFile());
        assertTrue(path.toFile().mkdirs());

        final DummyRecordSerde serde = new DummyRecordSerde();
        final WriteAheadRepository<DummyRecord> repo = new MinimalLockingWriteAheadLog<>(path, numPartitions, serde, null);
        final Collection<DummyRecord> initialRecs = repo.recoverRecords();
        assertTrue(initialRecs.isEmpty());

        final List<InsertThread> threads = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            threads.add(new InsertThread(10000, 1000000 * i, repo));
        }

        final long start = System.nanoTime();
        for (final InsertThread thread : threads) {
            thread.start();
        }
        for (final InsertThread thread : threads) {
            thread.join();
        }
        final long nanos = System.nanoTime() - start;
        final long millis = TimeUnit.MILLISECONDS.convert(nanos, TimeUnit.NANOSECONDS);
        System.out.println("Took " + millis + " millis to insert 1,000,000 records each in its own transaction");
        repo.shutdown();

        final WriteAheadRepository<DummyRecord> recoverRepo = new MinimalLockingWriteAheadLog<>(path, numPartitions, serde, null);
        final Collection<DummyRecord> recoveredRecords = recoverRepo.recoverRecords();
        assertFalse(recoveredRecords.isEmpty());
        assertEquals(100000, recoveredRecords.size());
        for (final DummyRecord record : recoveredRecords) {
            final Map<String, String> recoveredProps = record.getProperties();
            assertEquals(1, recoveredProps.size());
            assertEquals("B", recoveredProps.get("A"));
        }
    }

    @Test
    public void testRecoverAfterIOException() throws IOException {
        final int numPartitions = 5;
        final Path path = Paths.get("target/minimal-locking-repo-test-recover-after-ioe");
        deleteRecursively(path.toFile());
        Files.createDirectories(path);

        final DummyRecordSerde serde = new DummyRecordSerde();
        final WriteAheadRepository<DummyRecord> repo = new MinimalLockingWriteAheadLog<>(path, numPartitions, serde, null);
        final Collection<DummyRecord> initialRecs = repo.recoverRecords();
        assertTrue(initialRecs.isEmpty());

        serde.setThrowIOEAfterNSerializeEdits(7);   // serialize the 2 transactions, then the first edit of the third transaction; then throw IOException

        final List<DummyRecord> firstTransaction = new ArrayList<>();
        firstTransaction.add(new DummyRecord("1", UpdateType.CREATE));
        firstTransaction.add(new DummyRecord("2", UpdateType.CREATE));
        firstTransaction.add(new DummyRecord("3", UpdateType.CREATE));

        final List<DummyRecord> secondTransaction = new ArrayList<>();
        secondTransaction.add(new DummyRecord("1", UpdateType.UPDATE).setProperty("abc", "123"));
        secondTransaction.add(new DummyRecord("2", UpdateType.UPDATE).setProperty("cba", "123"));
        secondTransaction.add(new DummyRecord("3", UpdateType.UPDATE).setProperty("aaa", "123"));

        final List<DummyRecord> thirdTransaction = new ArrayList<>();
        thirdTransaction.add(new DummyRecord("1", UpdateType.DELETE));
        thirdTransaction.add(new DummyRecord("2", UpdateType.DELETE));

        repo.update(firstTransaction, true);
        repo.update(secondTransaction, true);
        try {
            repo.update(thirdTransaction, true);
            Assert.fail("Did not throw IOException on third transaction");
        } catch (final IOException e) {
            // expected behavior.
        }

        repo.shutdown();

        serde.setThrowIOEAfterNSerializeEdits(-1);
        final WriteAheadRepository<DummyRecord> recoverRepo = new MinimalLockingWriteAheadLog<>(path, numPartitions, serde, null);
        final Collection<DummyRecord> recoveredRecords = recoverRepo.recoverRecords();
        assertFalse(recoveredRecords.isEmpty());
        assertEquals(3, recoveredRecords.size());

        boolean record1 = false, record2 = false, record3 = false;
        for (final DummyRecord record : recoveredRecords) {
            switch (record.getId()) {
                case "1":
                    record1 = true;
                    assertEquals("123", record.getProperty("abc"));
                    break;
                case "2":
                    record2 = true;
                    assertEquals("123", record.getProperty("cba"));
                    break;
                case "3":
                    record3 = true;
                    assertEquals("123", record.getProperty("aaa"));
                    break;
            }
        }

        assertTrue(record1);
        assertTrue(record2);
        assertTrue(record3);
    }

    @Test
    public void testCannotModifyLogAfterAllAreBlackListed() throws IOException {
        final int numPartitions = 5;
        final Path path = Paths.get("target/minimal-locking-repo-test-cannot-modify-after-all-blacklisted");
        deleteRecursively(path.toFile());
        Files.createDirectories(path);

        final DummyRecordSerde serde = new DummyRecordSerde();
        final WriteAheadRepository<DummyRecord> repo = new MinimalLockingWriteAheadLog<>(path, numPartitions, serde, null);
        final Collection<DummyRecord> initialRecs = repo.recoverRecords();
        assertTrue(initialRecs.isEmpty());

        serde.setThrowIOEAfterNSerializeEdits(3);   // serialize the first transaction, then fail on all subsequent transactions

        final List<DummyRecord> firstTransaction = new ArrayList<>();
        firstTransaction.add(new DummyRecord("1", UpdateType.CREATE));
        firstTransaction.add(new DummyRecord("2", UpdateType.CREATE));
        firstTransaction.add(new DummyRecord("3", UpdateType.CREATE));

        final List<DummyRecord> secondTransaction = new ArrayList<>();
        secondTransaction.add(new DummyRecord("1", UpdateType.UPDATE).setProperty("abc", "123"));
        secondTransaction.add(new DummyRecord("2", UpdateType.UPDATE).setProperty("cba", "123"));
        secondTransaction.add(new DummyRecord("3", UpdateType.UPDATE).setProperty("aaa", "123"));

        final List<DummyRecord> thirdTransaction = new ArrayList<>();
        thirdTransaction.add(new DummyRecord("1", UpdateType.DELETE));
        thirdTransaction.add(new DummyRecord("2", UpdateType.DELETE));

        repo.update(firstTransaction, true);

        try {
            repo.update(secondTransaction, true);
            Assert.fail("Did not throw IOException on second transaction");
        } catch (final IOException e) {
            // expected behavior.
        }

        for (int i = 0; i < 4; i++) {
            try {
                repo.update(thirdTransaction, true);
                Assert.fail("Did not throw IOException on third transaction");
            } catch (final IOException e) {
                // expected behavior.
            }
        }

        serde.setThrowIOEAfterNSerializeEdits(-1);
        final List<DummyRecord> fourthTransaction = new ArrayList<>();
        fourthTransaction.add(new DummyRecord("1", UpdateType.DELETE));

        try {
            repo.update(fourthTransaction, true);
            Assert.fail("Successfully updated repo for 4th transaction");
        } catch (final IOException e) {
            // expected behavior
            assertTrue(e.getMessage().contains("All Partitions have been blacklisted"));
        }

        repo.shutdown();
        serde.setThrowIOEAfterNSerializeEdits(-1);

        final WriteAheadRepository<DummyRecord> recoverRepo = new MinimalLockingWriteAheadLog<>(path, numPartitions, serde, null);
        final Collection<DummyRecord> recoveredRecords = recoverRepo.recoverRecords();
        assertFalse(recoveredRecords.isEmpty());
        assertEquals(3, recoveredRecords.size());
    }

    @Test
    public void testStriping() throws IOException {
        final int numPartitions = 6;
        final Path path = Paths.get("target/minimal-locking-repo-striped");
        deleteRecursively(path.toFile());
        Files.createDirectories(path);

        final SortedSet<Path> paths = new TreeSet<>();
        paths.add(path.resolve("stripe-1"));
        paths.add(path.resolve("stripe-2"));

        final DummyRecordSerde serde = new DummyRecordSerde();
        final WriteAheadRepository<DummyRecord> repo = new MinimalLockingWriteAheadLog<>(paths, numPartitions, serde, null);
        final Collection<DummyRecord> initialRecs = repo.recoverRecords();
        assertTrue(initialRecs.isEmpty());

        final InsertThread inserter = new InsertThread(100000, 0, repo);
        inserter.run();

        for (final Path partitionPath : paths) {
            final File[] files = partitionPath.toFile().listFiles(new FileFilter() {
                @Override
                public boolean accept(File pathname) {
                    return pathname.getName().startsWith("partition");
                }
            });
            assertEquals(3, files.length);

            for (final File file : files) {
                final File[] journalFiles = file.listFiles();
                assertEquals(1, journalFiles.length);
            }
        }

        repo.checkpoint();

    }

    private static class InsertThread extends Thread {

        private final List<List<DummyRecord>> records;
        private final WriteAheadRepository<DummyRecord> repo;

        public InsertThread(final int numInsertions, final int startIndex, final WriteAheadRepository<DummyRecord> repo) {
            records = new ArrayList<>();
            for (int i = 0; i < numInsertions; i++) {
                final DummyRecord record = new DummyRecord(String.valueOf(i + startIndex), UpdateType.CREATE);
                record.setProperty("A", "B");
                final List<DummyRecord> list = new ArrayList<>();
                list.add(record);
                records.add(list);
            }
            this.repo = repo;
        }

        @Override
        public void run() {
            try {
                int counter = 0;
                for (final List<DummyRecord> list : records) {
                    final boolean forceSync = (++counter == records.size());
                    repo.update(list, forceSync);
                }
            } catch (IOException e) {
                Assert.fail("Failed to update: " + e.toString());
                e.printStackTrace();
            }
        }
    }


    private static class InlineCreationInsertThread extends Thread {
        private final long iterations;
        private final WriteAheadRepository<DummyRecord> repo;

        public InlineCreationInsertThread(final long numInsertions, final WriteAheadRepository<DummyRecord> repo) {
            this.iterations = numInsertions;
            this.repo = repo;
        }

        @Override
        public void run() {
            final List<DummyRecord> list = new ArrayList<>(1);
            list.add(null);
            final UpdateType[] updateTypes = new UpdateType[] { UpdateType.CREATE, UpdateType.DELETE, UpdateType.UPDATE };
            final Random random = new Random();

            for (long i = 0; i < iterations; i++) {
                final int updateTypeIndex = random.nextInt(updateTypes.length);
                final UpdateType updateType = updateTypes[updateTypeIndex];

                final DummyRecord record = new DummyRecord(String.valueOf(i), updateType);
                record.setProperty("A", "B");
                list.set(0, record);

                try {
                    repo.update(list, false);
                } catch (final Throwable t) {
                    t.printStackTrace();
                }
            }
        }
    }

    private void deleteRecursively(final File file) {
        final File[] children = file.listFiles();
        if (children != null) {
            for (final File child : children) {
                deleteRecursively(child);
            }
        }

        file.delete();
    }

    private long sizeOf(final File file) {
        long size = 0L;
        if (file.isDirectory()) {
            final File[] children = file.listFiles();
            if (children != null) {
                for (final File child : children) {
                    size += sizeOf(child);
                }
            }
        }

        size += file.length();

        return size;
    }

}
