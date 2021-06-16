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
package org.apache.nifi.rocksdb;

import org.apache.commons.lang3.SystemUtils;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksIterator;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestRocksDBMetronome {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static final byte[] KEY = "key".getBytes(StandardCharsets.UTF_8);
    private static final byte[] VALUE = "value".getBytes(StandardCharsets.UTF_8);
    private static final byte[] KEY_2 = "key 2".getBytes(StandardCharsets.UTF_8);
    private static final byte[] VALUE_2 = "value 2".getBytes(StandardCharsets.UTF_8);

    private ExecutorService executor;

    @BeforeClass
    public static void setupClass() {
        Assume.assumeTrue("Test only runs on *nix", !SystemUtils.IS_OS_WINDOWS);
    }

    @Before
    public void before() {
        executor = Executors.newSingleThreadExecutor();
    }

    @After
    public void after() {
        executor.shutdownNow();
    }

    @Test
    public void testReadWriteLong() throws Exception {
        Random random = new Random();
        byte[] key = new byte[8];
        for (long i = 0; i < 10; i++) {
            {
                RocksDBMetronome.writeLong(i, key);
                assertEquals(i, RocksDBMetronome.readLong(key));
            }
            {
                long testValue = Long.MIN_VALUE + i;
                RocksDBMetronome.writeLong(testValue, key);
                assertEquals(testValue, RocksDBMetronome.readLong(key));
            }
            {
                long testValue = Long.MAX_VALUE - i;
                RocksDBMetronome.writeLong(testValue, key);
                assertEquals(testValue, RocksDBMetronome.readLong(key));
            }
            {
                long testValue = random.nextLong();
                RocksDBMetronome.writeLong(testValue, key);
                assertEquals(testValue, RocksDBMetronome.readLong(key));
            }
        }
    }

    @Test
    public void testPutGetDelete() throws Exception {
        try (RocksDBMetronome db = new RocksDBMetronome.Builder()
                .setStoragePath(temporaryFolder.newFolder().toPath())
                .build()) {
            db.initialize();

            assertNull(db.get(KEY));

            // test default (no sync)
            db.put(KEY, VALUE);
            assertArrayEquals(VALUE, db.get(KEY));
            db.delete(KEY);
            assertNull(db.get(KEY));

            // test with "force sync"
            db.put(KEY, VALUE, true);
            assertArrayEquals(VALUE, db.get(KEY));
            db.delete(KEY, true);
            assertNull(db.get(KEY));
        }
    }

    @Test
    public void testPutGetConfiguration() throws Exception {
        try (RocksDBMetronome db = new RocksDBMetronome.Builder()
                .setStoragePath(temporaryFolder.newFolder().toPath())
                .build()) {
            db.initialize();

            assertNull(db.getConfiguration(KEY));
            db.putConfiguration(KEY, VALUE);
            assertArrayEquals(VALUE, db.getConfiguration(KEY));
            db.delete(db.getColumnFamilyHandle(RocksDBMetronome.CONFIGURATION_FAMILY), KEY);
            assertNull(db.getConfiguration(KEY));
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testPutBeforeInit() throws Exception {
        try (RocksDBMetronome db = new RocksDBMetronome.Builder()
                .setStoragePath(temporaryFolder.newFolder().toPath())
                .build()) {
            db.put(KEY, VALUE);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testPutClosed() throws Exception {
        try (RocksDBMetronome db = new RocksDBMetronome.Builder()
                .setStoragePath(temporaryFolder.newFolder().toPath())
                .build()) {
            db.initialize();

            db.close();
            db.put(KEY_2, VALUE_2);
        }
    }

    @Test
    public void testColumnFamilies() throws Exception {
        String secondFamilyName = "second family";
        try (RocksDBMetronome db = new RocksDBMetronome.Builder()
                .setStoragePath(temporaryFolder.newFolder().toPath())
                .addColumnFamily(secondFamilyName)
                .build()) {
            db.initialize();
            ColumnFamilyHandle secondFamily = db.getColumnFamilyHandle(secondFamilyName);

            // assert nothing present
            assertNull(db.get(KEY));
            assertNull(db.get(KEY_2));

            assertNull(db.get(secondFamily, KEY));
            assertNull(db.get(secondFamily, KEY_2));

            // add values
            db.put(KEY, VALUE);
            db.put(secondFamily, KEY_2, VALUE_2);

            // assert values present in correct family
            assertArrayEquals(VALUE, db.get(KEY));
            assertNull(db.get(KEY_2));

            assertArrayEquals(VALUE_2, db.get(secondFamily, KEY_2));
            assertNull(db.get(secondFamily, KEY));

            // delete from the "wrong" family
            db.delete(KEY_2);
            db.delete(secondFamily, KEY);

            // assert values *still* present in correct family
            assertArrayEquals(VALUE, db.get(KEY));
            assertNull(db.get(KEY_2));

            assertArrayEquals(VALUE_2, db.get(secondFamily, KEY_2));
            assertNull(db.get(secondFamily, KEY));

            // delete from the "right" family
            db.delete(KEY);
            db.delete(secondFamily, KEY_2);

            // assert values removed
            assertNull(db.get(KEY));
            assertNull(db.get(KEY_2));

            assertNull(db.get(secondFamily, KEY));
            assertNull(db.get(secondFamily, KEY_2));
        }
    }

    @Test
    public void testIterator() throws Exception {
        try (RocksDBMetronome db = new RocksDBMetronome.Builder()
                .setStoragePath(temporaryFolder.newFolder().toPath())
                .build()) {
            db.initialize();

            db.put(KEY, VALUE);
            db.put(KEY_2, VALUE_2);

            RocksIterator iterator = db.getIterator();
            iterator.seekToFirst();

            Map<String, byte[]> recovered = new HashMap<>();

            while (iterator.isValid()) {
                recovered.put(new String(iterator.key(), StandardCharsets.UTF_8), iterator.value());
                iterator.next();
            }

            assertEquals(2, recovered.size());
            assertArrayEquals(VALUE, recovered.get(new String(KEY, StandardCharsets.UTF_8)));
            assertArrayEquals(VALUE_2, recovered.get(new String(KEY_2, StandardCharsets.UTF_8)));
        }
    }

    @Test
    public void testCounterIncrement() throws Exception {
        try (RocksDBMetronome db = new RocksDBMetronome.Builder()
                .setStoragePath(temporaryFolder.newFolder().toPath())
                .setSyncMillis(Long.MAX_VALUE) // effectively disable the auto-sync
                .build()) {
            db.initialize();

            // get initial counter value
            int counterValue = db.getSyncCounterValue();

            // do the sync (which would normally happen via the db's internal executor)
            db.doSync();

            // assert counter value incremented
            assertEquals(counterValue + 1, db.getSyncCounterValue());
        }
    }

    @Test(timeout = 10_000)
    public void testWaitForSync() throws Exception {
        try (RocksDBMetronome db = new RocksDBMetronome.Builder()
                .setStoragePath(temporaryFolder.newFolder().toPath())
                .setSyncMillis(Long.MAX_VALUE) // effectively disable the auto-sync
                .build()) {
            db.initialize();

            Future<Boolean> future = executor.submit(() -> {
                db.waitForSync();
                return true;
            });

            // the future should still be blocked waiting for sync to happen
            assertFalse(future.isDone());

            // give the future time to wake up and complete
            while (!future.isDone()) {
                // TESTING NOTE: this is inside a loop to address a minor *testing* race condition where our first doSync() could happen before the future runs,
                // meaning waitForSync() would be left waiting on another doSync() that never comes...

                // do the sync (which would normally happen via the db's internal executor)
                db.doSync();
                Thread.sleep(25);
            }

            // the future should no longer be blocked
            assertTrue(future.isDone());
        }
    }

    @Test(timeout = 10_000)
    public void testWaitForSyncWithValue() throws Exception {
        try (RocksDBMetronome db = new RocksDBMetronome.Builder()
                .setStoragePath(temporaryFolder.newFolder().toPath())
                .setSyncMillis(Long.MAX_VALUE) // effectively disable the auto-sync
                .build()) {
            db.initialize();

            int syncCounterValue = db.getSyncCounterValue();

            // "wait" for one before current counter value... should not block
            db.waitForSync(syncCounterValue - 1);

            // wait for current value... should block (because auto-sync isn't happening)
            assertBlocks(db, syncCounterValue);

            // do the sync (which would normally happen via the db's internal executor)
            db.doSync();

            // "wait" for initial value... should now not block
            db.waitForSync(syncCounterValue);

            // wait for current value again... should block (because auto-sync isn't happening)
            assertBlocks(db, db.getSyncCounterValue());
        }
    }

    private void assertBlocks(RocksDBMetronome db, int counterValue) throws InterruptedException, java.util.concurrent.ExecutionException {
        Future<Boolean> future = getWaitForSyncFuture(db, counterValue);

        try {
            future.get(1, TimeUnit.SECONDS);
            fail();
        } catch (TimeoutException expected) {
            assertFalse(future.isDone());
        }
        future.cancel(true);
    }

    private Future<Boolean> getWaitForSyncFuture(RocksDBMetronome db, int counterValue) {
        return executor.submit(() -> {
            db.waitForSync(counterValue);
            return true;
        });
    }
}