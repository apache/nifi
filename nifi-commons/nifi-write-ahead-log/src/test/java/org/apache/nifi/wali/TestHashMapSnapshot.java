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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.wali.DummyRecord;
import org.wali.DummyRecordSerde;
import org.wali.SerDeFactory;
import org.wali.SingletonSerDeFactory;
import org.wali.UpdateType;

public class TestHashMapSnapshot {

    private final File storageDirectory = new File("target/test-hashmap-snapshot");
    private DummyRecordSerde serde;
    private SerDeFactory<DummyRecord> serdeFactory;

    @Before
    public void setup() throws IOException {
        if (!storageDirectory.exists()) {
            Files.createDirectories(storageDirectory.toPath());
        }

        final File[] childFiles = storageDirectory.listFiles();
        for (final File childFile : childFiles) {
            if (childFile.isFile()) {
                Files.delete(childFile.toPath());
            }
        }

        serde = new DummyRecordSerde();
        serdeFactory = new SingletonSerDeFactory<>(serde);

    }

    @Test
    public void testSuccessfulRoundTrip() throws IOException {
        final HashMapSnapshot<DummyRecord> snapshot = new HashMapSnapshot<>(storageDirectory, serdeFactory);
        final Map<String, String> props = new HashMap<>();

        for (int i = 0; i < 10; i++) {
            final DummyRecord record = new DummyRecord(String.valueOf(i), UpdateType.CREATE);
            props.put("key", String.valueOf(i));
            record.setProperties(props);
            snapshot.update(Collections.singleton(record));
        }

        for (int i = 2; i < 10; i += 2) {
            final DummyRecord record = new DummyRecord(String.valueOf(i), UpdateType.DELETE);
            snapshot.update(Collections.singleton(record));
        }

        for (int i = 1; i < 10; i += 2) {
            final DummyRecord record = new DummyRecord(String.valueOf(i), UpdateType.SWAP_OUT);
            record.setSwapLocation("swapFile-" + i);
            snapshot.update(Collections.singleton(record));
        }

        final DummyRecord swapIn7 = new DummyRecord("7", UpdateType.SWAP_IN);
        swapIn7.setSwapLocation("swapFile-7");
        snapshot.update(Collections.singleton(swapIn7));

        final Set<String> swappedOutLocations = new HashSet<>();
        swappedOutLocations.add("swapFile-1");
        swappedOutLocations.add("swapFile-3");
        swappedOutLocations.add("swapFile-5");
        swappedOutLocations.add("swapFile-9");

        final SnapshotCapture<DummyRecord> capture = snapshot.prepareSnapshot(180L);
        assertEquals(180L, capture.getMaxTransactionId());
        assertEquals(swappedOutLocations, capture.getSwapLocations());

        final Map<Object, DummyRecord> records = capture.getRecords();
        assertEquals(2, records.size());
        assertTrue(records.containsKey("0"));
        assertTrue(records.containsKey("7"));

        snapshot.writeSnapshot(capture);

        final SnapshotRecovery<DummyRecord> recovery = snapshot.recover();
        assertEquals(180L, recovery.getMaxTransactionId());
        assertEquals(swappedOutLocations, recovery.getRecoveredSwapLocations());

        final Map<Object, DummyRecord> recoveredRecords = recovery.getRecords();
        assertEquals(records, recoveredRecords);
    }

    @Test
    public void testOOMEWhenWritingResultsInPreviousSnapshotStillRecoverable() throws IOException {
        final HashMapSnapshot<DummyRecord> snapshot = new HashMapSnapshot<>(storageDirectory, serdeFactory);
        final Map<String, String> props = new HashMap<>();

        for (int i = 0; i < 11; i++) {
            final DummyRecord record = new DummyRecord(String.valueOf(i), UpdateType.CREATE);
            props.put("key", String.valueOf(i));
            record.setProperties(props);
            snapshot.update(Collections.singleton(record));
        }

        final DummyRecord swapOutRecord = new DummyRecord("10", UpdateType.SWAP_OUT);
        swapOutRecord.setSwapLocation("SwapLocation-1");
        snapshot.update(Collections.singleton(swapOutRecord));

        snapshot.writeSnapshot(snapshot.prepareSnapshot(25L));

        serde.setThrowOOMEAfterNSerializeEdits(3);

        try {
            snapshot.writeSnapshot(snapshot.prepareSnapshot(150L));
            Assert.fail("Expected OOME");
        } catch (final OutOfMemoryError oome) {
            // expected
        }

        final SnapshotRecovery<DummyRecord> recovery = snapshot.recover();
        assertEquals(25L, recovery.getMaxTransactionId());

        final Map<Object, DummyRecord> recordMap = recovery.getRecords();
        assertEquals(10, recordMap.size());
        for (int i = 0; i < 10; i++) {
            assertTrue(recordMap.containsKey(String.valueOf(i)));
        }
        for (final Map.Entry<Object, DummyRecord> entry : recordMap.entrySet()) {
            final DummyRecord record = entry.getValue();
            final Map<String, String> properties = record.getProperties();
            assertNotNull(properties);
            assertEquals(1, properties.size());
            assertEquals(entry.getKey(), properties.get("key"));
        }

        final Set<String> swapLocations = recovery.getRecoveredSwapLocations();
        assertEquals(1, swapLocations.size());
        assertTrue(swapLocations.contains("SwapLocation-1"));
    }

    @Test
    public void testIOExceptionWhenWritingResultsInPreviousSnapshotStillRecoverable() throws IOException {
        final HashMapSnapshot<DummyRecord> snapshot = new HashMapSnapshot<>(storageDirectory, serdeFactory);
        final Map<String, String> props = new HashMap<>();

        for (int i = 0; i < 11; i++) {
            final DummyRecord record = new DummyRecord(String.valueOf(i), UpdateType.CREATE);
            props.put("key", String.valueOf(i));
            record.setProperties(props);
            snapshot.update(Collections.singleton(record));
        }

        final DummyRecord swapOutRecord = new DummyRecord("10", UpdateType.SWAP_OUT);
        swapOutRecord.setSwapLocation("SwapLocation-1");
        snapshot.update(Collections.singleton(swapOutRecord));

        snapshot.writeSnapshot(snapshot.prepareSnapshot(25L));

        serde.setThrowIOEAfterNSerializeEdits(3);

        for (int i = 0; i < 5; i++) {
            try {
                snapshot.writeSnapshot(snapshot.prepareSnapshot(150L));
                Assert.fail("Expected IOE");
            } catch (final IOException ioe) {
                // expected
            }
        }

        final SnapshotRecovery<DummyRecord> recovery = snapshot.recover();
        assertEquals(25L, recovery.getMaxTransactionId());

        final Map<Object, DummyRecord> recordMap = recovery.getRecords();
        assertEquals(10, recordMap.size());
        for (int i = 0; i < 10; i++) {
            assertTrue(recordMap.containsKey(String.valueOf(i)));
        }
        for (final Map.Entry<Object, DummyRecord> entry : recordMap.entrySet()) {
            final DummyRecord record = entry.getValue();
            final Map<String, String> properties = record.getProperties();
            assertNotNull(properties);
            assertEquals(1, properties.size());
            assertEquals(entry.getKey(), properties.get("key"));
        }

        final Set<String> swapLocations = recovery.getRecoveredSwapLocations();
        assertEquals(1, swapLocations.size());
        assertTrue(swapLocations.contains("SwapLocation-1"));
    }

}
