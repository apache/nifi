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
package org.apache.nifi.diagnostics;

import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ThreadDumpTaskTest {

    @Test
    void testCaptureDumpIncludesPlatformAndVirtualThreads() throws InterruptedException {
        final String platformThreadName = "thread-dump-platform-thread";
        final String virtualThreadName = "thread-dump-virtual-thread";
        final CountDownLatch threadsStarted = new CountDownLatch(2);
        final CountDownLatch releaseThreads = new CountDownLatch(1);
        final Runnable task = () -> {
            threadsStarted.countDown();
            try {
                releaseThreads.await();
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };

        final Thread platformThread = Thread.ofPlatform().name(platformThreadName).start(task);
        final Thread virtualThread = Thread.ofVirtual().name(virtualThreadName).start(task);

        try {
            assertTrue(threadsStarted.await(2, TimeUnit.SECONDS));

            final DiagnosticsDumpElement dumpElement = new ThreadDumpTask().captureDump(false);
            final String threadDump = dumpElement.getDetails().getFirst();

            assertTrue(threadDump.contains(platformThreadName));
            assertTrue(threadDump.contains(virtualThreadName));
        } finally {
            releaseThreads.countDown();
            platformThread.join(2_000L);
            virtualThread.join(2_000L);
        }
    }

    @Test
    void testPlatformFallbackIncludesBlockedMonitor() throws InterruptedException {
        final Object monitor = new Object();
        final CountDownLatch monitorAcquired = new CountDownLatch(1);
        final CountDownLatch releaseMonitor = new CountDownLatch(1);
        final Thread lockOwner = Thread.ofPlatform().name("thread-dump-lock-owner").start(() -> {
            synchronized (monitor) {
                monitorAcquired.countDown();
                try {
                    releaseMonitor.await();
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });

        try {
            assertTrue(monitorAcquired.await(2, TimeUnit.SECONDS));
            final Thread blockedThread = Thread.ofPlatform().name("thread-dump-blocked-thread").start(() -> {
                synchronized (monitor) {
                }
            });

            try {
                final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2L);
                while (blockedThread.getState() != Thread.State.BLOCKED && System.nanoTime() < deadline) {
                    Thread.onSpinWait();
                }
                assertEquals(Thread.State.BLOCKED, blockedThread.getState());

                final String threadDump = new ThreadDumpTask().capturePlatformThreadDump(ManagementFactory.getThreadMXBean());
                final String blockedThreadHeader = "\"thread-dump-blocked-thread\" Id=" + blockedThread.threadId() + " BLOCKED on ";
                assertTrue(threadDump.contains(blockedThreadHeader));
            } finally {
                releaseMonitor.countDown();
                blockedThread.join(2_000L);
            }
        } finally {
            releaseMonitor.countDown();
            lockOwner.join(2_000L);
        }
    }
}
