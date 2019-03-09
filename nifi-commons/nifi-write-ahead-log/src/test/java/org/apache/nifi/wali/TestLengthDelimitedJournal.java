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
import org.junit.Before;
import org.junit.Test;
import org.wali.DummyRecord;
import org.wali.DummyRecordSerde;
import org.wali.SerDeFactory;
import org.wali.SingletonSerDeFactory;
import org.wali.UpdateType;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestLengthDelimitedJournal {
    private final File journalFile = new File("target/testLengthDelimitedJournal/testJournal.journal");
    private SerDeFactory<DummyRecord> serdeFactory;
    private DummyRecordSerde serde;
    private ObjectPool<ByteArrayDataOutputStream> streamPool;
    private static final int BUFFER_SIZE = 4096;

    @Before
    public void setupJournal() throws IOException {
        Files.deleteIfExists(journalFile.toPath());

        if (!journalFile.getParentFile().exists()) {
            Files.createDirectories(journalFile.getParentFile().toPath());
        }

        serde = new DummyRecordSerde();
        serdeFactory = new SingletonSerDeFactory<>(serde);
        streamPool = new BlockingQueuePool<>(1,
            () -> new ByteArrayDataOutputStream(BUFFER_SIZE),
            stream -> stream.getByteArrayOutputStream().size() < BUFFER_SIZE,
            stream -> stream.getByteArrayOutputStream().reset());
    }

    @Test
    public void testHandlingOfTrailingNulBytes() throws IOException {
        try (final LengthDelimitedJournal<DummyRecord> journal = new LengthDelimitedJournal<>(journalFile, serdeFactory, streamPool, 0L)) {
            journal.writeHeader();

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

            journal.update(firstTransaction, id -> null);
            journal.update(secondTransaction, id -> null);
            journal.update(thirdTransaction, id -> null);
        }

        // Truncate the contents of the journal file by 8 bytes. Then replace with 28 trailing NUL bytes,
        // as this is what we often see when we have a sudden power loss.
        final byte[] contents = Files.readAllBytes(journalFile.toPath());
        final byte[] truncated = Arrays.copyOfRange(contents, 0, contents.length - 8);
        final byte[] withNuls = new byte[truncated.length + 28];
        System.arraycopy(truncated, 0, withNuls, 0, truncated.length);

        try (final OutputStream fos = new FileOutputStream(journalFile)) {
            fos.write(withNuls);
        }


        try (final LengthDelimitedJournal<DummyRecord> journal = new LengthDelimitedJournal<>(journalFile, serdeFactory, streamPool, 0L)) {
            final Map<Object, DummyRecord> recordMap = new HashMap<>();
            final Set<String> swapLocations = new HashSet<>();

            journal.recoverRecords(recordMap, swapLocations);

            assertFalse(recordMap.isEmpty());
            assertEquals(3, recordMap.size());

            final DummyRecord record1 = recordMap.get("1");
            assertNotNull(record1);
            assertEquals(Collections.singletonMap("abc", "123"), record1.getProperties());

            final DummyRecord record2 = recordMap.get("2");
            assertNotNull(record2);
            assertEquals(Collections.singletonMap("cba", "123"), record2.getProperties());

            final DummyRecord record3 = recordMap.get("3");
            assertNotNull(record3);
            assertEquals(Collections.singletonMap("aaa", "123"), record3.getProperties());
        }
    }

    @Test
    public void testUpdateOnlyAppliedIfEntireTransactionApplied() throws IOException {
        try (final LengthDelimitedJournal<DummyRecord> journal = new LengthDelimitedJournal<>(journalFile, serdeFactory, streamPool, 0L)) {
            journal.writeHeader();

            for (int i = 0; i < 3; i++) {
                final DummyRecord record = new DummyRecord(String.valueOf(i), UpdateType.CREATE);
                journal.update(Collections.singleton(record), key -> null);
            }

            final DummyRecord swapOut1Record = new DummyRecord("1", UpdateType.SWAP_OUT);
            swapOut1Record.setSwapLocation("swap12");
            journal.update(Collections.singleton(swapOut1Record), id -> null);

            final DummyRecord swapOut2Record = new DummyRecord("2", UpdateType.SWAP_OUT);
            swapOut2Record.setSwapLocation("swap12");
            journal.update(Collections.singleton(swapOut2Record), id -> null);

            final List<DummyRecord> records = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                final DummyRecord record = new DummyRecord("1" + i, UpdateType.CREATE);
                records.add(record);
            }

            final DummyRecord swapIn1Record = new DummyRecord("1", UpdateType.SWAP_IN);
            swapIn1Record.setSwapLocation("swap12");
            records.add(swapIn1Record);

            final DummyRecord swapOut1AgainRecord = new DummyRecord("1", UpdateType.SWAP_OUT);
            swapOut1AgainRecord.setSwapLocation("swap12");
            records.add(swapOut1AgainRecord);

            final DummyRecord swapIn2Record = new DummyRecord("2", UpdateType.SWAP_IN);
            swapIn2Record.setSwapLocation("swap12");
            records.add(swapIn2Record);

            final DummyRecord swapOut0Record = new DummyRecord("0", UpdateType.SWAP_OUT);
            swapOut0Record.setSwapLocation("swap0");
            records.add(swapOut0Record);

            journal.update(records, id -> null);
        }

        // Truncate the last 8 bytes so that we will get an EOFException when reading the last transaction.
        try (final FileOutputStream fos = new FileOutputStream(journalFile, true)) {
            fos.getChannel().truncate(journalFile.length() - 8);
        }


        try (final LengthDelimitedJournal<DummyRecord> journal = new LengthDelimitedJournal<>(journalFile, serdeFactory, streamPool, 0L)) {
            final Map<Object, DummyRecord> recordMap = new HashMap<>();
            final Set<String> swapLocations = new HashSet<>();

            final JournalRecovery recovery = journal.recoverRecords(recordMap, swapLocations);
            assertEquals(5L, recovery.getMaxTransactionId());
            assertEquals(5, recovery.getUpdateCount());

            final Set<String> expectedSwap = Collections.singleton("swap12");
            assertEquals(expectedSwap, swapLocations);

            final Map<Object, DummyRecord> expectedRecordMap = new HashMap<>();
            expectedRecordMap.put("0", new DummyRecord("0", UpdateType.CREATE));
            assertEquals(expectedRecordMap, recordMap);
        }
    }

    @Test
    public void testPoisonedJournalNotWritableAfterIOE() throws IOException {
        try (final LengthDelimitedJournal<DummyRecord> journal = new LengthDelimitedJournal<>(journalFile, serdeFactory, streamPool, 0L)) {
            journal.writeHeader();

            serde.setThrowIOEAfterNSerializeEdits(2);

            final DummyRecord firstRecord = new DummyRecord("1", UpdateType.CREATE);
            journal.update(Collections.singleton(firstRecord), key -> null);

            final DummyRecord secondRecord = new DummyRecord("1", UpdateType.UPDATE);
            journal.update(Collections.singleton(secondRecord), key -> firstRecord);

            final DummyRecord thirdRecord = new DummyRecord("1", UpdateType.UPDATE);
            final RecordLookup<DummyRecord> lookup = key -> secondRecord;
            try {
                journal.update(Collections.singleton(thirdRecord), lookup);
                Assert.fail("Expected IOException");
            } catch (final IOException ioe) {
                // expected
            }

            serde.setThrowIOEAfterNSerializeEdits(-1);

            final Collection<DummyRecord> records = Collections.singleton(thirdRecord);
            for (int i = 0; i < 10; i++) {
                try {
                    journal.update(records, lookup);
                    Assert.fail("Expected IOException");
                } catch (final IOException expected) {
                }

                try {
                    journal.fsync();
                    Assert.fail("Expected IOException");
                } catch (final IOException expected) {
                }
            }
        }
    }

    @Test
    public void testPoisonedJournalNotWritableAfterOOME() throws IOException {
        try (final LengthDelimitedJournal<DummyRecord> journal = new LengthDelimitedJournal<>(journalFile, serdeFactory, streamPool, 0L)) {
            journal.writeHeader();

            serde.setThrowOOMEAfterNSerializeEdits(2);

            final DummyRecord firstRecord = new DummyRecord("1", UpdateType.CREATE);
            journal.update(Collections.singleton(firstRecord), key -> null);

            final DummyRecord secondRecord = new DummyRecord("1", UpdateType.UPDATE);
            journal.update(Collections.singleton(secondRecord), key -> firstRecord);

            final DummyRecord thirdRecord = new DummyRecord("1", UpdateType.UPDATE);
            final RecordLookup<DummyRecord> lookup = key -> secondRecord;
            try {
                journal.update(Collections.singleton(thirdRecord), lookup);
                Assert.fail("Expected OOME");
            } catch (final OutOfMemoryError oome) {
                // expected
            }

            serde.setThrowOOMEAfterNSerializeEdits(-1);

            final Collection<DummyRecord> records = Collections.singleton(thirdRecord);
            for (int i = 0; i < 10; i++) {
                try {
                    journal.update(records, lookup);
                    Assert.fail("Expected IOException");
                } catch (final IOException expected) {
                }

                try {
                    journal.fsync();
                    Assert.fail("Expected IOException");
                } catch (final IOException expected) {
                }
            }
        }
    }

    @Test
    public void testSuccessfulRoundTrip() throws IOException {
        try (final LengthDelimitedJournal<DummyRecord> journal = new LengthDelimitedJournal<>(journalFile, serdeFactory, streamPool, 0L)) {
            journal.writeHeader();

            final DummyRecord firstRecord = new DummyRecord("1", UpdateType.CREATE);
            journal.update(Collections.singleton(firstRecord), key -> null);

            final DummyRecord secondRecord = new DummyRecord("1", UpdateType.UPDATE);
            journal.update(Collections.singleton(secondRecord), key -> firstRecord);

            final DummyRecord thirdRecord = new DummyRecord("1", UpdateType.UPDATE);
            journal.update(Collections.singleton(thirdRecord), key -> secondRecord);

            final Map<Object, DummyRecord> recordMap = new HashMap<>();
            final Set<String> swapLocations = new HashSet<>();
            final JournalRecovery recovery = journal.recoverRecords(recordMap, swapLocations);
            assertFalse(recovery.isEOFExceptionEncountered());

            assertEquals(2L, recovery.getMaxTransactionId());
            assertEquals(3, recovery.getUpdateCount());

            assertTrue(swapLocations.isEmpty());
            assertEquals(1, recordMap.size());

            final DummyRecord retrieved = recordMap.get("1");
            assertNotNull(retrieved);
            assertEquals(thirdRecord, retrieved);
        }
    }

    @Test
    public void testMultipleThreadsCreatingOverflowDirectory() throws IOException, InterruptedException {
        final LengthDelimitedJournal<DummyRecord> journal = new LengthDelimitedJournal<DummyRecord>(journalFile, serdeFactory, streamPool, 3820L, 100) {
            @Override
            protected void createOverflowDirectory(final Path path) throws IOException {
                // Create the overflow directory.
                super.createOverflowDirectory(path);

                // Ensure that a second call to create the overflow directory will not cause an issue.
                super.createOverflowDirectory(path);
            }
        };

        // Ensure that the overflow directory does not exist.
        journal.dispose();

        try {
            journal.writeHeader();

            final List<DummyRecord> largeCollection1 = new ArrayList<>();
            for (int i=0; i < 1_000; i++) {
                largeCollection1.add(new DummyRecord(String.valueOf(i), UpdateType.CREATE));
            }
            final Map<String, DummyRecord> recordMap = largeCollection1.stream()
                .collect(Collectors.toMap(DummyRecord::getId, rec -> rec));

            final List<DummyRecord> largeCollection2 = new ArrayList<>();
            for (int i=0; i < 1_000; i++) {
                largeCollection2.add(new DummyRecord(String.valueOf(5_000_000 + i), UpdateType.CREATE));
            }
            final Map<String, DummyRecord> recordMap2 = largeCollection2.stream()
                .collect(Collectors.toMap(DummyRecord::getId, rec -> rec));

            final AtomicReference<Exception> thread1Failure = new AtomicReference<>();
            final Thread t1 = new Thread(() -> {
                try {
                    journal.update(largeCollection1, recordMap::get);
                } catch (final Exception e) {
                    e.printStackTrace();
                    thread1Failure.set(e);
                }
            });
            t1.start();

            final AtomicReference<Exception> thread2Failure = new AtomicReference<>();
            final Thread t2 = new Thread(() -> {
                try {
                    journal.update(largeCollection2, recordMap2::get);
                } catch (final Exception e) {
                    e.printStackTrace();
                    thread2Failure.set(e);
                }
            });
            t2.start();

            t1.join();
            t2.join();

            assertNull(thread1Failure.get());
            assertNull(thread2Failure.get());
        } finally {
            journal.close();
        }
    }

    @Test
    public void testTruncatedJournalFile() throws IOException {
        final DummyRecord firstRecord, secondRecord;
        try (final LengthDelimitedJournal<DummyRecord> journal = new LengthDelimitedJournal<>(journalFile, serdeFactory, streamPool, 0L)) {
            journal.writeHeader();

            firstRecord = new DummyRecord("1", UpdateType.CREATE);
            journal.update(Collections.singleton(firstRecord), key -> null);

            secondRecord = new DummyRecord("2", UpdateType.CREATE);
            journal.update(Collections.singleton(secondRecord), key -> firstRecord);

            final DummyRecord thirdRecord = new DummyRecord("1", UpdateType.UPDATE);
            journal.update(Collections.singleton(thirdRecord), key -> secondRecord);
        }

        // Truncate the file
        try (final FileOutputStream fos = new FileOutputStream(journalFile, true)) {
            fos.getChannel().truncate(journalFile.length() - 8);
        }

        // Ensure that we are able to recover the first two records without an issue but the third is lost.
        try (final LengthDelimitedJournal<DummyRecord> journal = new LengthDelimitedJournal<>(journalFile, serdeFactory, streamPool, 0L)) {
            final Map<Object, DummyRecord> recordMap = new HashMap<>();
            final Set<String> swapLocations = new HashSet<>();
            final JournalRecovery recovery = journal.recoverRecords(recordMap, swapLocations);
            assertTrue(recovery.isEOFExceptionEncountered());

            assertEquals(2L, recovery.getMaxTransactionId()); // transaction ID is still 2 because that's what was written to the journal
            assertEquals(2, recovery.getUpdateCount()); // only 2 updates because the last update will incur an EOFException and be skipped

            assertTrue(swapLocations.isEmpty());
            assertEquals(2, recordMap.size());

            final DummyRecord retrieved1 = recordMap.get("1");
            assertNotNull(retrieved1);
            assertEquals(firstRecord, retrieved1);

            final DummyRecord retrieved2 = recordMap.get("2");
            assertNotNull(retrieved2);
            assertEquals(secondRecord, retrieved2);
        }
    }
}
