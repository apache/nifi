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

package org.apache.nifi.web.concurrent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.junit.Before;
import org.junit.Test;

public class TestReentrantDistributedLock {
    private ReadWriteLockSync sync;

    @Before
    public void setup() {
        sync = new ReadWriteLockSync();
    }

    @Test(timeout = 5000)
    public void testMultipleReadLocks() throws LockExpiredException {
        final ReentrantDistributedLock lock = createReadLock();
        final String id1 = lock.lock();
        final String id2 = lock.lock();
        assertEquals(id1, id2);

        assertEquals(2, lock.getClaimCount());
        lock.unlock(id1);
        assertEquals(1, lock.getClaimCount());
        lock.unlock(id2);
        assertEquals(0, lock.getClaimCount());
    }

    @Test(timeout = 10000)
    public void testMultipleWriteLocksBlock() throws LockExpiredException {
        final ReentrantDistributedLock lock = createWriteLock();
        final String id1 = lock.lock();
        assertNotNull(id1);

        final long startTime = System.nanoTime();
        final String id2 = lock.tryLock(500, TimeUnit.MILLISECONDS);
        assertNull(id2);

        // We don't know exactly how long it will take to timeout because the time periods
        // won't be exact, but it should take more than 350 milliseconds.
        final long nanos = System.nanoTime() - startTime;
        assertTrue(nanos > TimeUnit.MILLISECONDS.toNanos(350L));

        lock.unlock(id1);
        final String id3 = lock.tryLock(500, TimeUnit.MILLISECONDS);
        assertNotNull(id3);
        assertNotSame(id1, id3);
        lock.unlock(id3);
    }

    @Test(timeout = 10000)
    public void testReadLockBlocksWriteLock() throws LockExpiredException {
        final ReentrantDistributedLock readLock = createReadLock();
        final ReentrantDistributedLock writeLock = createWriteLock();

        final String id1 = readLock.lock();
        assertNotNull(id1);

        final long startTime = System.nanoTime();
        final String id2 = writeLock.tryLock(500, TimeUnit.MILLISECONDS);
        assertNull(id2);

        // We don't know exactly how long it will take to timeout because the time periods
        // won't be exact, but it should take more than 350 milliseconds.
        final long nanos = System.nanoTime() - startTime;
        assertTrue(nanos > TimeUnit.MILLISECONDS.toNanos(350L));

        readLock.unlock(id1);

        final String id3 = writeLock.lock();
        assertNotNull(id3);
        assertNotSame(id1, id3);

        writeLock.unlock(id3);
    }

    @Test(timeout = 10000)
    public void testWriteLockBlocksReadLock() throws LockExpiredException {
        final ReentrantDistributedLock readLock = createReadLock();
        final ReentrantDistributedLock writeLock = createWriteLock();

        final String id1 = writeLock.lock();
        assertNotNull(id1);

        final long startTime = System.nanoTime();
        final String id2 = readLock.tryLock(500, TimeUnit.MILLISECONDS);
        assertNull(id2);

        // We don't know exactly how long it will take to timeout because the time periods
        // won't be exact, but it should take more than 350 milliseconds.
        final long nanos = System.nanoTime() - startTime;
        assertTrue(nanos > TimeUnit.MILLISECONDS.toNanos(350L));

        writeLock.unlock(id1);

        final String id3 = readLock.lock();
        assertNotNull(id3);
        assertNotSame(id1, id3);

        readLock.unlock(id3);
    }

    @Test(timeout = 10000)
    public void testMultipleReadLocksBlockingOnWriteLock() throws InterruptedException, LockExpiredException {
        final ReentrantDistributedLock readLock = createReadLock();
        final ReentrantDistributedLock writeLock = createWriteLock();

        final String id1 = writeLock.lock();
        assertNotNull(id1);

        final ExecutorService executor = Executors.newFixedThreadPool(3);
        final AtomicReferenceArray<String> array = new AtomicReferenceArray<>(3);
        for (int i = 0; i < 3; i++) {
            final int index = i;
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    final String id = readLock.lock();
                    assertNotNull(id);
                    array.set(index, id);
                }
            });
        }

        // wait a bit and then make sure that no values have been set
        Thread.sleep(250L);
        for (int i = 0; i < 3; i++) {
            assertNull(array.get(i));
        }

        // unlock so that the readers can lock.
        writeLock.unlock(id1);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.MINUTES);

        final String id = array.get(0);
        assertNotNull(id);
        for (int i = 0; i < 3; i++) {
            assertEquals(id, array.get(i));
        }

        for (int i = 0; i < 3; i++) {
            assertEquals(3 - i, readLock.getClaimCount());
            readLock.unlock(id);
        }

        assertEquals(0, readLock.getClaimCount());
    }

    @Test(timeout = 10000)
    public void testLockExpires() {
        final ReentrantDistributedLock lock = new ReentrantDistributedLock(LockMode.MUTUALLY_EXCLUSIVE, sync, 25, TimeUnit.MILLISECONDS);
        final String id1 = lock.lock();
        assertNotNull(id1);

        final long start = System.nanoTime();
        final String id2 = lock.lock();
        final long nanos = System.nanoTime() - start;

        assertNotNull(id2);
        assertNotSame(id1, id2);

        // The timeout may not entirely elapse but will be close. Give 5 milliseconds buffer
        assertTrue(nanos > TimeUnit.MILLISECONDS.toNanos(20));
    }

    @Test(timeout = 10000)
    public void testWithLock() throws LockExpiredException, Exception {
        final ReentrantDistributedLock lock = createWriteLock();
        final String id = lock.lock();
        assertEquals(1, lock.getClaimCount());

        final Object obj = new Object();
        final Object returned = lock.withLock(id, () -> obj);
        assertTrue(returned == obj);
        assertEquals(1, lock.getClaimCount());
        lock.unlock(id);
        assertEquals(0, lock.getClaimCount());
    }

    private ReentrantDistributedLock createReadLock() {
        return new ReentrantDistributedLock(LockMode.SHARED, sync, 30, TimeUnit.SECONDS);
    }

    private ReentrantDistributedLock createWriteLock() {
        return new ReentrantDistributedLock(LockMode.MUTUALLY_EXCLUSIVE, sync, 30, TimeUnit.SECONDS);
    }
}
