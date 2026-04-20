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
package org.apache.nifi.controller.scheduling;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DynamicSemaphoreTest {

    @Test
    void testConstructorRejectsZeroPermits() {
        assertThrows(IllegalArgumentException.class, () -> new DynamicSemaphore(0));
    }

    @Test
    void testConstructorRejectsNegativePermits() {
        assertThrows(IllegalArgumentException.class, () -> new DynamicSemaphore(-1));
    }

    @Test
    void testInitialPermitCount() {
        final DynamicSemaphore semaphore = new DynamicSemaphore(5);
        assertEquals(5, semaphore.getMaxPermits());
        assertEquals(5, semaphore.availablePermits());
    }

    @Test
    void testAcquireAndRelease() throws InterruptedException {
        final DynamicSemaphore semaphore = new DynamicSemaphore(3);
        semaphore.acquire();
        assertEquals(2, semaphore.availablePermits());
        semaphore.acquire();
        assertEquals(1, semaphore.availablePermits());
        semaphore.release();
        assertEquals(2, semaphore.availablePermits());
        semaphore.release();
        assertEquals(3, semaphore.availablePermits());
    }

    @Test
    void testConcurrencyBoundedByPermits() throws InterruptedException {
        final int permits = 2;
        final int threadCount = 5;
        final DynamicSemaphore semaphore = new DynamicSemaphore(permits);
        final AtomicInteger concurrentCount = new AtomicInteger(0);
        final AtomicInteger maxObservedConcurrency = new AtomicInteger(0);
        final CountDownLatch allStarted = new CountDownLatch(threadCount);
        final CountDownLatch allDone = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            Thread.ofVirtual().start(() -> {
                try {
                    allStarted.countDown();
                    semaphore.acquire();
                    try {
                        final int current = concurrentCount.incrementAndGet();
                        maxObservedConcurrency.accumulateAndGet(current, Math::max);
                        Thread.sleep(50);
                    } finally {
                        concurrentCount.decrementAndGet();
                        semaphore.release();
                    }
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    allDone.countDown();
                }
            });
        }

        assertTrue(allDone.await(5, TimeUnit.SECONDS));
        assertTrue(maxObservedConcurrency.get() <= permits,
                "Max concurrency " + maxObservedConcurrency.get() + " exceeded permit count " + permits);
    }

    @Test
    void testSetMaxPermitsIncrease() throws InterruptedException {
        final DynamicSemaphore semaphore = new DynamicSemaphore(2);
        semaphore.acquire();
        semaphore.acquire();
        assertEquals(0, semaphore.availablePermits());

        semaphore.setMaxPermits(5);
        assertEquals(5, semaphore.getMaxPermits());
        assertEquals(3, semaphore.availablePermits());
    }

    @Test
    void testSetMaxPermitsDecrease() {
        final DynamicSemaphore semaphore = new DynamicSemaphore(5);
        assertEquals(5, semaphore.availablePermits());

        semaphore.setMaxPermits(2);
        assertEquals(2, semaphore.getMaxPermits());
        assertEquals(2, semaphore.availablePermits());
    }

    @Test
    void testSetMaxPermitsDecreaseWhileHeld() throws InterruptedException {
        final DynamicSemaphore semaphore = new DynamicSemaphore(5);
        semaphore.acquire();
        semaphore.acquire();
        semaphore.acquire();
        assertEquals(2, semaphore.availablePermits());

        semaphore.setMaxPermits(2);
        assertEquals(2, semaphore.getMaxPermits());
        assertTrue(semaphore.availablePermits() <= 0,
                "Available permits should be non-positive when more permits are held than the new max");

        semaphore.release();
        semaphore.release();
        semaphore.release();
        assertEquals(2, semaphore.availablePermits());
    }

    @Test
    void testSetMaxPermitsRejectsZero() {
        final DynamicSemaphore semaphore = new DynamicSemaphore(5);
        assertThrows(IllegalArgumentException.class, () -> semaphore.setMaxPermits(0));
    }

    @Test
    void testSetMaxPermitsRejectsNegative() {
        final DynamicSemaphore semaphore = new DynamicSemaphore(5);
        assertThrows(IllegalArgumentException.class, () -> semaphore.setMaxPermits(-1));
    }

    @Test
    void testResizeUnblocksWaitingThreads() throws InterruptedException {
        final DynamicSemaphore semaphore = new DynamicSemaphore(1);
        semaphore.acquire();

        final CountDownLatch threadBlocked = new CountDownLatch(1);
        final CountDownLatch threadAcquired = new CountDownLatch(1);

        Thread.ofVirtual().start(() -> {
            try {
                threadBlocked.countDown();
                semaphore.acquire();
                threadAcquired.countDown();
                semaphore.release();
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        assertTrue(threadBlocked.await(1, TimeUnit.SECONDS));
        Thread.sleep(100);

        semaphore.setMaxPermits(2);
        assertTrue(threadAcquired.await(2, TimeUnit.SECONDS), "Thread should have been unblocked by resize");

        semaphore.release();
    }

    @Test
    void testSetMaxPermitsNoChangeIsNoOp() {
        final DynamicSemaphore semaphore = new DynamicSemaphore(5);
        semaphore.setMaxPermits(5);
        assertEquals(5, semaphore.getMaxPermits());
        assertEquals(5, semaphore.availablePermits());
    }
}
