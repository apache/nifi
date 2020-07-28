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

package org.apache.nifi.wali;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.wali.DummyRecord;
import org.wali.DummyRecordSerde;
import org.wali.SerDeFactory;
import org.wali.SingletonSerDeFactory;
import org.wali.UpdateType;
import org.wali.WriteAheadRepository;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestSequentialAccessWriteAheadLog {
    @Rule
    public TestName testName = new TestName();


    @Test
    public void testUpdateWithExternalFile() throws IOException {
        final DummyRecordSerde serde = new DummyRecordSerde();
        final SequentialAccessWriteAheadLog<DummyRecord> repo = createWriteRepo(serde);

        final List<DummyRecord> records = new ArrayList<>();
        for (int i = 0; i < 350_000; i++) {
            final DummyRecord record = new DummyRecord(String.valueOf(i), UpdateType.CREATE);
            records.add(record);
        }

        repo.update(records, false);
        repo.shutdown();

        assertEquals(1, serde.getExternalFileReferences().size());

        final SequentialAccessWriteAheadLog<DummyRecord> recoveryRepo = createRecoveryRepo();
        final Collection<DummyRecord> recovered = recoveryRepo.recoverRecords();

        // ensure that we get the same records back, but the order may be different, so wrap both collections
        // in a HashSet so that we can compare unordered collections of the same type.
        assertEquals(new HashSet<>(records), new HashSet<>(recovered));
    }

    @Test
    public void testUpdateWithExternalFileFollowedByInlineUpdate() throws IOException {
        final DummyRecordSerde serde = new DummyRecordSerde();
        final SequentialAccessWriteAheadLog<DummyRecord> repo = createWriteRepo(serde);

        final List<DummyRecord> records = new ArrayList<>();
        for (int i = 0; i < 350_000; i++) {
            final DummyRecord record = new DummyRecord(String.valueOf(i), UpdateType.CREATE);
            records.add(record);
        }

        repo.update(records, false);

        final DummyRecord subsequentRecord = new DummyRecord("350001", UpdateType.CREATE);
        repo.update(Collections.singleton(subsequentRecord), false);
        repo.shutdown();

        assertEquals(1, serde.getExternalFileReferences().size());

        final SequentialAccessWriteAheadLog<DummyRecord> recoveryRepo = createRecoveryRepo();
        final Collection<DummyRecord> recovered = recoveryRepo.recoverRecords();

        // ensure that we get the same records back, but the order may be different, so wrap both collections
        // in a HashSet so that we can compare unordered collections of the same type.
        final Set<DummyRecord> expectedRecords = new HashSet<>(records);
        expectedRecords.add(subsequentRecord);
        assertEquals(expectedRecords, new HashSet<>(recovered));
    }

    @Test
    public void testRecoverWithNoCheckpoint() throws IOException {
        final SequentialAccessWriteAheadLog<DummyRecord> repo = createWriteRepo();

        final List<DummyRecord> records = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final DummyRecord record = new DummyRecord(String.valueOf(i), UpdateType.CREATE);
            records.add(record);
        }

        repo.update(records, false);
        repo.shutdown();

        final SequentialAccessWriteAheadLog<DummyRecord> recoveryRepo = createRecoveryRepo();
        final Collection<DummyRecord> recovered = recoveryRepo.recoverRecords();

        // ensure that we get the same records back, but the order may be different, so wrap both collections
        // in a HashSet so that we can compare unordered collections of the same type.
        assertEquals(new HashSet<>(records), new HashSet<>(recovered));
    }

    @Test
    public void testRecoverWithNoJournalUpdates() throws IOException {
        final SequentialAccessWriteAheadLog<DummyRecord> repo = createWriteRepo();

        final List<DummyRecord> records = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final DummyRecord record = new DummyRecord(String.valueOf(i), UpdateType.CREATE);
            records.add(record);
        }

        repo.update(records, false);
        repo.checkpoint();
        repo.shutdown();

        final SequentialAccessWriteAheadLog<DummyRecord> recoveryRepo = createRecoveryRepo();
        final Collection<DummyRecord> recovered = recoveryRepo.recoverRecords();

        // ensure that we get the same records back, but the order may be different, so wrap both collections
        // in a HashSet so that we can compare unordered collections of the same type.
        assertEquals(new HashSet<>(records), new HashSet<>(recovered));
    }

    @Test
    public void testRecoverWithMultipleCheckpointsBetweenJournalUpdate() throws IOException {
        final SequentialAccessWriteAheadLog<DummyRecord> repo = createWriteRepo();

        final List<DummyRecord> records = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final DummyRecord record = new DummyRecord(String.valueOf(i), UpdateType.CREATE);
            records.add(record);
        }

        repo.update(records, false);

        for (int i = 0; i < 8; i++) {
            repo.checkpoint();
        }

        final DummyRecord updateRecord = new DummyRecord("4", UpdateType.UPDATE);
        updateRecord.setProperties(Collections.singletonMap("updated", "true"));
        repo.update(Collections.singleton(updateRecord), false);

        repo.shutdown();

        final SequentialAccessWriteAheadLog<DummyRecord> recoveryRepo = createRecoveryRepo();
        final Collection<DummyRecord> recovered = recoveryRepo.recoverRecords();

        // what we expect is the same as what we updated with, except we don't want the DummyRecord for CREATE 4
        // because we will instead recover an UPDATE only for 4.
        final Set<DummyRecord> expected = new HashSet<>(records);
        expected.remove(new DummyRecord("4", UpdateType.CREATE));
        expected.add(updateRecord);

        // ensure that we get the same records back, but the order may be different, so wrap both collections
        // in a HashSet so that we can compare unordered collections of the same type.
        assertEquals(expected, new HashSet<>(recovered));
    }

    private SequentialAccessWriteAheadLog<DummyRecord> createRecoveryRepo() throws IOException {
        final File targetDir = new File("target");
        final File storageDir = new File(targetDir, testName.getMethodName());

        final DummyRecordSerde serde = new DummyRecordSerde();
        final SerDeFactory<DummyRecord> serdeFactory = new SingletonSerDeFactory<>(serde);
        final SequentialAccessWriteAheadLog<DummyRecord> repo = new SequentialAccessWriteAheadLog<>(storageDir, serdeFactory);

        return repo;
    }

    private SequentialAccessWriteAheadLog<DummyRecord> createWriteRepo() throws IOException {
        return createWriteRepo(new DummyRecordSerde());
    }

    private SequentialAccessWriteAheadLog<DummyRecord> createWriteRepo(final DummyRecordSerde serde) throws IOException {
        final File targetDir = new File("target");
        final File storageDir = new File(targetDir, testName.getMethodName());
        deleteRecursively(storageDir);
        assertTrue(storageDir.mkdirs());

        final SerDeFactory<DummyRecord> serdeFactory = new SingletonSerDeFactory<>(serde);
        final SequentialAccessWriteAheadLog<DummyRecord> repo = new SequentialAccessWriteAheadLog<>(storageDir, serdeFactory);

        final Collection<DummyRecord> recovered = repo.recoverRecords();
        assertNotNull(recovered);
        assertTrue(recovered.isEmpty());

        return repo;
    }

    /**
     * This test is designed to update the repository in several different wants, testing CREATE, UPDATE, SWAP IN, SWAP OUT, and DELETE
     * update types, as well as testing updates with single records and with multiple records in a transaction. It also verifies that we
     * are able to checkpoint, then update journals, and then recover updates to both the checkpoint and the journals.
     */
    @Test
    public void testUpdateThenRecover() throws IOException {
        final SequentialAccessWriteAheadLog<DummyRecord> repo = createWriteRepo();

        final DummyRecord firstCreate = new DummyRecord("0", UpdateType.CREATE);
        repo.update(Collections.singleton(firstCreate), false);

        final List<DummyRecord> creations = new ArrayList<>();
        for (int i = 1; i < 11; i++) {
            final DummyRecord record = new DummyRecord(String.valueOf(i), UpdateType.CREATE);
            creations.add(record);
        }
        repo.update(creations, false);

        final DummyRecord deleteRecord3 = new DummyRecord("3", UpdateType.DELETE);
        repo.update(Collections.singleton(deleteRecord3), false);

        final DummyRecord swapOutRecord4 = new DummyRecord("4", UpdateType.SWAP_OUT);
        swapOutRecord4.setSwapLocation("swap");

        final DummyRecord swapOutRecord5 = new DummyRecord("5", UpdateType.SWAP_OUT);
        swapOutRecord5.setSwapLocation("swap");

        final List<DummyRecord> swapOuts = new ArrayList<>();
        swapOuts.add(swapOutRecord4);
        swapOuts.add(swapOutRecord5);
        repo.update(swapOuts, false);

        final DummyRecord swapInRecord5 = new DummyRecord("5", UpdateType.SWAP_IN);
        swapInRecord5.setSwapLocation("swap");
        repo.update(Collections.singleton(swapInRecord5), false);

        final int recordCount = repo.checkpoint();
        assertEquals(9, recordCount);

        final DummyRecord updateRecord6 = new DummyRecord("6", UpdateType.UPDATE);
        updateRecord6.setProperties(Collections.singletonMap("greeting", "hello"));
        repo.update(Collections.singleton(updateRecord6), false);

        final List<DummyRecord> updateRecords = new ArrayList<>();
        for (int i = 7; i < 11; i++) {
            final DummyRecord updateRecord = new DummyRecord(String.valueOf(i), UpdateType.UPDATE);
            updateRecord.setProperties(Collections.singletonMap("greeting", "hi"));
            updateRecords.add(updateRecord);
        }

        final DummyRecord deleteRecord2 = new DummyRecord("2", UpdateType.DELETE);
        updateRecords.add(deleteRecord2);

        repo.update(updateRecords, false);

        repo.shutdown();

        final SequentialAccessWriteAheadLog<DummyRecord> recoveryRepo = createRecoveryRepo();
        final Collection<DummyRecord> recoveredRecords = recoveryRepo.recoverRecords();

        // We should now have records:
        // 0-10 CREATED
        // 2 & 3 deleted
        // 4 & 5 swapped out
        // 5 swapped back in
        // 6 updated with greeting = hello
        // 7-10 updated with greeting = hi

        assertEquals(8, recoveredRecords.size());
        final Map<String, DummyRecord> recordMap = recoveredRecords.stream()
            .collect(Collectors.toMap(record -> record.getId(), Function.identity()));

        assertFalse(recordMap.containsKey("2"));
        assertFalse(recordMap.containsKey("3"));
        assertFalse(recordMap.containsKey("4"));

        assertTrue(recordMap.get("1").getProperties().isEmpty());
        assertTrue(recordMap.get("5").getProperties().isEmpty());

        assertEquals("hello", recordMap.get("6").getProperties().get("greeting"));

        for (int i = 7; i < 11; i++) {
            assertEquals("hi", recordMap.get(String.valueOf(i)).getProperties().get("greeting"));
        }

        recoveryRepo.shutdown();
    }


    @Test
    @Ignore("For manual performance testing")
    public void testUpdatePerformance() throws IOException, InterruptedException {
        final Path path = Paths.get("target/sequential-access-repo");
        deleteRecursively(path.toFile());
        assertTrue(path.toFile().mkdirs());

        final DummyRecordSerde serde = new DummyRecordSerde();
        final SerDeFactory<DummyRecord> serdeFactory = new SingletonSerDeFactory<>(serde);

        final WriteAheadRepository<DummyRecord> repo = new SequentialAccessWriteAheadLog<>(path.toFile(), serdeFactory);
        final Collection<DummyRecord> initialRecs = repo.recoverRecords();
        assertTrue(initialRecs.isEmpty());

        final long updateCountPerThread = 1_000_000;
        final int numThreads = 4;

        final Thread[] threads = new Thread[numThreads];
        final int batchSize = 1;

        long previousBytes = 0L;

        for (int j = 0; j < 2; j++) {
            for (int i = 0; i < numThreads; i++) {
                final Thread t = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        final List<DummyRecord> batch = new ArrayList<>();
                        for (int i = 0; i < updateCountPerThread / batchSize; i++) {
                            batch.clear();
                            for (int j = 0; j < batchSize; j++) {
                                final DummyRecord record = new DummyRecord(String.valueOf(i), UpdateType.CREATE);
                                batch.add(record);
                            }

                            try {
                                repo.update(batch, false);
                            } catch (Throwable t) {
                                t.printStackTrace();
                                Assert.fail(t.toString());
                            }
                        }
                    }
                });

                threads[i] = t;
            }

            final long start = System.nanoTime();
            for (final Thread t : threads) {
                t.start();
            }
            for (final Thread t : threads) {
                t.join();
            }

            long bytes = 0L;
            for (final File journalFile : path.resolve("journals").toFile().listFiles()) {
                bytes += journalFile.length();
            }

            bytes -= previousBytes;
            previousBytes = bytes;

            final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            final long eventsPerSecond = (updateCountPerThread * numThreads * 1000) / millis;
            final String eps = NumberFormat.getInstance().format(eventsPerSecond);
            final long bytesPerSecond = bytes * 1000 / millis;
            final String bps = NumberFormat.getInstance().format(bytesPerSecond);

            if (j == 0) {
                System.out.println(millis + " ms to insert " + updateCountPerThread * numThreads + " updates using " + numThreads
                    + " threads, *as a warmup!*  " + eps + " events per second, " + bps + " bytes per second");
            } else {
                System.out.println(millis + " ms to insert " + updateCountPerThread * numThreads + " updates using " + numThreads
                    + " threads, " + eps + " events per second, " + bps + " bytes per second");
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

}
