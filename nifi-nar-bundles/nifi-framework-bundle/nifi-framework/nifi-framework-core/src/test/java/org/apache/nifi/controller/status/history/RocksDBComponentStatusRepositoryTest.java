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
package org.apache.nifi.controller.status.history;

import org.apache.nifi.properties.StandardNiFiProperties;
import org.apache.nifi.rocksdb.RocksDBMetronome;
import org.apache.nifi.util.NiFiProperties;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.RocksDBException;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;

import static org.apache.nifi.controller.status.history.RocksDBComponentStatusRepository.NUM_DATA_POINTS_PROPERTY;
import static org.apache.nifi.controller.status.history.RocksDBComponentStatusRepository.STORAGE_LOCATION_PROPERTY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RocksDBComponentStatusRepositoryTest {
    private RocksDBComponentStatusRepository rocksDBComponentStatusRepository;
    private RocksDBMetronome db;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void before() throws IOException {
        Properties properties = new Properties();
        File tempFolder = temporaryFolder.newFolder();
        properties.put(STORAGE_LOCATION_PROPERTY, tempFolder.getAbsolutePath());
        properties.put(NUM_DATA_POINTS_PROPERTY, "2");
        NiFiProperties niFiProperties = new StandardNiFiProperties(properties);
        rocksDBComponentStatusRepository = new RocksDBComponentStatusRepository(niFiProperties);
        db = rocksDBComponentStatusRepository.db;
    }

    @Test
    public void testRemoveComponent() throws RocksDBException {
        String componentIdString = "id";
        byte[] componentIdBytes = componentIdString.getBytes();
        // These history ids are expected to be 8 bytes long
        byte[] historyIds = "1234567823456789".getBytes();
        byte[] historyId1 = "12345678".getBytes();
        byte[] historyId2 = "23456789".getBytes();
        byte[] value = "value".getBytes();

        // Load values into the db that we expect to be removed.
        db.put(rocksDBComponentStatusRepository.componentsHandle, componentIdBytes, historyIds, true);
        db.put(historyId1, value, true);
        db.put(historyId2, value, true);
        db.put(rocksDBComponentStatusRepository.componentDetailsHandle, componentIdBytes, value, true);

        assertNotNull(db.get(rocksDBComponentStatusRepository.componentsHandle, componentIdBytes));
        assertNotNull(db.get(rocksDBComponentStatusRepository.componentDetailsHandle, componentIdBytes));
        assertNotNull(db.get(historyId1));
        assertNotNull(db.get(historyId2));

        rocksDBComponentStatusRepository.removeComponent(componentIdString);

        assertNull(db.get(rocksDBComponentStatusRepository.componentsHandle, componentIdBytes));
        assertNull(db.get(rocksDBComponentStatusRepository.componentDetailsHandle, componentIdBytes));
        assertNull(db.get(historyId1));
        assertNull(db.get(historyId2));
    }

    @Test
    public void testPrepareAndPersistRecords() throws IOException, RocksDBException {
        Map<String, byte[]> statusMap = new HashMap<>();
        statusMap.put("key1", "value1".getBytes());
        statusMap.put("key2", "value2".getBytes());
        Date timestamp = Date.from(Instant.now());
        List<GarbageCollectionStatus> gcStatus = new ArrayList<>();
        gcStatus.add(new StandardGarbageCollectionStatus("bob", timestamp, 1, 1L));
        gcStatus.add(new StandardGarbageCollectionStatus("joe", timestamp, 1, 1L));

        Map<Long, byte[]> recordMap = new HashMap<>();
        Map<String, Long> componentUpdateMap = new HashMap<>();

        rocksDBComponentStatusRepository.prepareAndPersistRecords(statusMap, gcStatus, recordMap, componentUpdateMap);

        byte[] expectedCompositeKey = concatLongsToBytes(4L, 5L);

        assertEquals(5 ,recordMap.size());
        assertArrayEquals("value1".getBytes(), recordMap.get(1L));
        assertArrayEquals("value2".getBytes(), recordMap.get(2L));
        assertArrayEquals(expectedCompositeKey, recordMap.get(3L));
        // Verify that the gc statuses were stored as we expected
        assertArrayEquals(rocksDBComponentStatusRepository.marshall(gcStatus.get(0)), recordMap.get(4L));
        assertArrayEquals(rocksDBComponentStatusRepository.marshall(gcStatus.get(1)), recordMap.get(5L));

        recordMap.forEach((key, value) -> {
            try {
                assertArrayEquals(value, db.get(RocksDBMetronome.getBytes(key)));
            } catch (RocksDBException e) {
                fail(e.getMessage());
            }
        });

        // Garbage Collection Status records don't end up in this map
        assertEquals(2, componentUpdateMap.size());
    }

    @Test
    public void testAssociatedRecordsWithTimestamp() throws IOException, RocksDBException {
        Map<Long, byte[]> recordMap = new HashMap<>();
        recordMap.put(1L, "value".getBytes());
        recordMap.put(2L, "value".getBytes());
        recordMap.put(3L, "value".getBytes());
        Date timestamp = new Date();

        rocksDBComponentStatusRepository.associateRecordsWithTimestamp(recordMap, timestamp);
        byte[] expectedValue = concatLongsToBytes(1L, 2L, 3L);

        // All ids in the map will be aggravated into a single composite value, mapped to the provided timestamp
        assertArrayEquals(expectedValue, db.get(rocksDBComponentStatusRepository.timestampsHandle, RocksDBMetronome.getBytes(timestamp.getTime())));
    }

    @Test
    public void testReserveIds() throws RocksDBException, IOException {
        db.putConfiguration(RocksDBComponentStatusRepository.ID_KEY, RocksDBMetronome.getBytes(5));

        // Previous value is returned
        assertEquals(5, rocksDBComponentStatusRepository.reserveIds(10));

        // New reserved value is stored
        assertArrayEquals(RocksDBMetronome.getBytes(15), db.getConfiguration(RocksDBComponentStatusRepository.ID_KEY));
    }

    @Test
    public void testAddTimestamp() throws RocksDBException {
        byte[] timestampKeyToEvict = RocksDBMetronome.getBytes(0L);
        // Keys are expected to be 8 bytes
        byte[] timestampRecordsToEvict = "1234567823456789".getBytes();
        byte[] recordKey1 = "12345678".getBytes();
        byte[] recordKey2 = "23456789".getBytes();
        byte[] value = "value".getBytes();
        rocksDBComponentStatusRepository.addTimestamp(0L);
        db.put(rocksDBComponentStatusRepository.timestampsHandle, timestampKeyToEvict, timestampRecordsToEvict, true);
        db.put(recordKey1, value, true);
        db.put(recordKey2, value, true);
        assertNotNull(db.get(recordKey1));
        assertNotNull(db.get(recordKey2));

        // Verify that when a timestamp is evicted, status snapshots associated with that timestamp are deleted. In the current
        // configuration we only keep 2 data points at any time, so the original one (above) will be evicted.
        rocksDBComponentStatusRepository.addTimestamp(System.currentTimeMillis());
        rocksDBComponentStatusRepository.addTimestamp(System.currentTimeMillis());
        assertNull(db.get(recordKey1));
        assertNull(db.get(recordKey2));
    }

    @Test
    public void testAppendLong() throws IOException {
        RocksDBComponentStatusRepository repo = new RocksDBComponentStatusRepository();
        byte[] longs = null;
        Random random = new Random();
        LinkedList<Long> listOfLongs = new LinkedList<>();

        for(int i = 0; i < RocksDBComponentStatusRepository.DEFAULT_NUM_DATA_POINTS * 2; i++) {
            long randomLong = random.nextLong();

            listOfLongs.add(randomLong);
            if(listOfLongs.size() > RocksDBComponentStatusRepository.DEFAULT_NUM_DATA_POINTS) {
                listOfLongs.pop();
            }

            longs = repo.appendLong(randomLong, longs);

            // compare "longs" and "listOfLongs"
            int index = 0;
            try(ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(longs)) {
                byte[] key = new byte[8];
                while(byteArrayInputStream.read(key, 0, 8) == 8) {
                    assertEquals((long)listOfLongs.get(index++), RocksDBMetronome.readLong(key));
                }
            }

            assertEquals(listOfLongs.size(), index);
        }
    }

    @Test
    public void testAssociateRecordsWithColumnFamilies() throws RocksDBException {
        Map<String, Long> componentUpdateMap = new HashMap<>();
        componentUpdateMap.put("key1", 1L);
        componentUpdateMap.put("key2", 2L);
        Map<String, byte[]> detailMap = new HashMap<>();
        detailMap.put("key1", "value1".getBytes());
        detailMap.put("key2", "value2".getBytes());

        rocksDBComponentStatusRepository.associateRecordsWithColumnFamilies(componentUpdateMap, detailMap);

        assertArrayEquals("value1".getBytes(), db.get(rocksDBComponentStatusRepository.componentDetailsHandle, "key1".getBytes()));
        assertArrayEquals("value2".getBytes(), db.get(rocksDBComponentStatusRepository.componentDetailsHandle, "key2".getBytes()));

        detailMap.clear();
        componentUpdateMap.put("key1", 3L);
        componentUpdateMap.put("key2", 4L);

        // Each time you call this, if the same keys exist in the componentUpdateMap then the values are concatenated with what is
        // in the underlying repository
        rocksDBComponentStatusRepository.associateRecordsWithColumnFamilies(componentUpdateMap, detailMap);

        byte[] expectedValue1 = concatLongsToBytes(1L, 3L);
        byte[] expectedValue2 = concatLongsToBytes(2L, 4L);
        assertArrayEquals(expectedValue1, db.get(rocksDBComponentStatusRepository.componentsHandle, "key1".getBytes()));
        assertArrayEquals(expectedValue2, db.get(rocksDBComponentStatusRepository.componentsHandle, "key2".getBytes()));
    }

    @Test
    public void testGetStatusHistory() throws IOException, RocksDBException {
        Instant now = Instant.now();
        Date beginTimestamp = Date.from(now.minus(5, ChronoUnit.SECONDS));
        Date timestamp = Date.from(now);
        Date endTimestamp = Date.from(now.plus(5, ChronoUnit.SECONDS));
        Map<String, byte[]> statusMap = new HashMap<>();
        StandardStatusSnapshot standardStatusSnapshot = new StandardStatusSnapshot(Collections.emptySet());
        statusMap.put("key", rocksDBComponentStatusRepository.marshall(standardStatusSnapshot));
        Map<String, byte[]> detailMap = new HashMap<>();
        ComponentDetails componentDetails = new ComponentDetails("id", "groupId", "componentName", "componentType",
                "sourceName", "destinationName", "remoteUri");
        detailMap.put("key", rocksDBComponentStatusRepository.marshall(componentDetails));

        rocksDBComponentStatusRepository.persistUpdates(statusMap, detailMap, Collections.emptyList(), timestamp);
        StatusHistory statusHistory = rocksDBComponentStatusRepository.getConnectionStatusHistory("key", beginTimestamp, endTimestamp, 2);
        assertEquals(1, statusHistory.getStatusSnapshots().size());
    }

    @Test
    public void testGetGarbageCollectionHistory() throws IOException, RocksDBException {
        Instant now = Instant.now();
        Date beginTimestamp = Date.from(now.minus(5, ChronoUnit.SECONDS));
        Date timestamp = Date.from(now);
        Date endTimestamp = Date.from(now.plus(5, ChronoUnit.SECONDS));
        Date outOfBoundsTimestamp = Date.from(now.plus(6, ChronoUnit.SECONDS));
        List<GarbageCollectionStatus> garbageCollectionStatusList = new ArrayList<>();
        garbageCollectionStatusList.add(new StandardGarbageCollectionStatus("joe", outOfBoundsTimestamp, 1, 1L));
        garbageCollectionStatusList.add(new StandardGarbageCollectionStatus("bob", timestamp, 2, 2L));

        rocksDBComponentStatusRepository.persistUpdates(Collections.emptyMap(), Collections.emptyMap(), garbageCollectionStatusList, timestamp);
        GarbageCollectionHistory garbageCollectionHistory = rocksDBComponentStatusRepository.getGarbageCollectionHistory(beginTimestamp, endTimestamp);

        // Only the gc status associated with "bob" should be returned as "joe" was not within the specified time range
        assertEquals(1, garbageCollectionHistory.getMemoryManagerNames().size());
        assertTrue(garbageCollectionHistory.getMemoryManagerNames().contains("bob"));
        assertEquals("bob", garbageCollectionHistory.getGarbageCollectionStatuses("bob").get(0).getMemoryManagerName());
    }

    private byte[] concatLongsToBytes(long... x) {
        // Within these unit tests, we are only dealing with single digit longs
        byte[] returnValue = new byte[x.length * 8];
        AtomicInteger counter = new AtomicInteger(0);
        LongStream.of(x).forEach(value -> System.arraycopy(RocksDBMetronome.getBytes(value), 0, returnValue, counter.getAndIncrement() * 8, 8));

        return returnValue;
    }
}