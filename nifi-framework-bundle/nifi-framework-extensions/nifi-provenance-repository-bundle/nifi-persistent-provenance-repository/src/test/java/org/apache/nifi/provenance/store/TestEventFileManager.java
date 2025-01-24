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

package org.apache.nifi.provenance.store;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 5)
public class TestEventFileManager {

    @Test
    public void testTwoWriteLocks() throws InterruptedException {
        final EventFileManager fileManager = new EventFileManager();
        final File f1 = new File("1.prov");
        final File gz = new File("1.prov.gz");

        final AtomicBoolean obtained = new AtomicBoolean(false);

        final Thread t1 = new Thread(() -> {
            fileManager.obtainWriteLock(f1);

            synchronized (obtained) {
                obtained.set(true);
                obtained.notify();
            }

            try {
                Thread.sleep(500L);
            } catch (InterruptedException ignored) {
            }
            fileManager.releaseWriteLock(f1);
        });

        t1.start();

        final Thread t2 = new Thread(() -> {
            synchronized (obtained) {
                while (!obtained.get()) {
                    try {
                        obtained.wait();
                    } catch (InterruptedException ignored) {
                    }
                }
            }

            fileManager.obtainWriteLock(gz);
            fileManager.releaseWriteLock(gz);
        });

        final long start = System.nanoTime();
        t2.start();
        t2.join();
        final long nanos = System.nanoTime() - start;
        assertTrue(nanos > TimeUnit.MILLISECONDS.toNanos(300L));
    }


    @Test
    public void testTwoReadLocks() throws InterruptedException {
        final EventFileManager fileManager = new EventFileManager();
        final File f1 = new File("1.prov");
        final File gz = new File("1.prov.gz");

        final AtomicBoolean obtained = new AtomicBoolean(false);

        final Thread t1 = new Thread(() -> {
            fileManager.obtainReadLock(f1);

            synchronized (obtained) {
                obtained.set(true);
                obtained.notify();
            }

            try {
                Thread.sleep(100000L);
            } catch (InterruptedException ignored) {
            }
            fileManager.releaseReadLock(f1);
        });

        t1.start();

        final Thread t2 = new Thread(() -> {
            synchronized (obtained) {
                while (!obtained.get()) {
                    try {
                        obtained.wait();
                    } catch (InterruptedException ignored) {
                    }
                }
            }

            fileManager.obtainReadLock(gz);
            fileManager.releaseReadLock(gz);
        });

        final long start = System.nanoTime();
        t2.start();
        t2.join();
        final long nanos = System.nanoTime() - start;
        assertTrue(nanos < TimeUnit.MILLISECONDS.toNanos(500L));
    }


    @Test
    public void testWriteThenRead() throws InterruptedException {
        final EventFileManager fileManager = new EventFileManager();
        final File f1 = new File("1.prov");
        final File gz = new File("1.prov.gz");

        final AtomicBoolean obtained = new AtomicBoolean(false);

        final Thread t1 = new Thread(() -> {
            fileManager.obtainWriteLock(f1);

            synchronized (obtained) {
                obtained.set(true);
                obtained.notify();
            }

            try {
                Thread.sleep(500L);
            } catch (InterruptedException ignored) {
            }
            fileManager.releaseWriteLock(f1);
        });

        t1.start();

        final Thread t2 = new Thread(() -> {
            synchronized (obtained) {
                while (!obtained.get()) {
                    try {
                        obtained.wait();
                    } catch (InterruptedException ignored) {
                    }
                }
            }

            fileManager.obtainReadLock(gz);
            fileManager.releaseReadLock(gz);
        });

        final long start = System.nanoTime();
        t2.start();
        t2.join();
        final long nanos = System.nanoTime() - start;
        assertTrue(nanos > TimeUnit.MILLISECONDS.toNanos(300L));
    }


    @Test
    public void testReadThenWrite() throws InterruptedException {
        final EventFileManager fileManager = new EventFileManager();
        final File f1 = new File("1.prov");
        final File gz = new File("1.prov.gz");

        final AtomicBoolean obtained = new AtomicBoolean(false);

        final Thread t1 = new Thread(() -> {
            fileManager.obtainReadLock(f1);

            synchronized (obtained) {
                obtained.set(true);
                obtained.notify();
            }

            try {
                Thread.sleep(500L);
            } catch (InterruptedException ignored) {
            }
            fileManager.releaseReadLock(f1);
        });

        t1.start();

        final Thread t2 = new Thread(() -> {
            synchronized (obtained) {
                while (!obtained.get()) {
                    try {
                        obtained.wait();
                    } catch (InterruptedException ignored) {
                    }
                }
            }

            fileManager.obtainWriteLock(gz);
            fileManager.releaseWriteLock(gz);
        });

        final long start = System.nanoTime();
        t2.start();
        t2.join();
        final long nanos = System.nanoTime() - start;
        assertTrue(nanos > TimeUnit.MILLISECONDS.toNanos(300L));
    }
}
