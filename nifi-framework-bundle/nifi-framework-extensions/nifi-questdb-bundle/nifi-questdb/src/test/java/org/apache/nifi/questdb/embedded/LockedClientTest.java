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
package org.apache.nifi.questdb.embedded;

import org.apache.nifi.questdb.Client;
import org.apache.nifi.questdb.DatabaseException;
import org.apache.nifi.questdb.InsertRowDataSource;
import org.apache.nifi.questdb.QueryResultProcessor;
import org.apache.nifi.questdb.mapping.RequestMapping;
import org.apache.nifi.questdb.util.QuestDbTestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.nifi.questdb.util.QuestDbTestUtil.EVENT_TABLE_NAME;
import static org.apache.nifi.questdb.util.QuestDbTestUtil.SELECT_QUERY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

public class LockedClientTest {
    private static final Duration LOCK_ATTEMPT_DURATION = Duration.of(20, TimeUnit.MILLISECONDS.toChronoUnit());

    // The LockedQuestDbClient supports different types of locks as well, but the intention here is to prove expected behaviour with read-write lock
    private ReentrantReadWriteLock lock;
    private CountDownLatch latchStep1;
    private CountDownLatch latchStep2;
    private CountDownLatch latchStep3;

    @BeforeEach
    public void setUp() {
        lock = new ReentrantReadWriteLock();
        latchStep1 = new CountDownLatch(1);
        latchStep2 = new CountDownLatch(1);
        latchStep3 = new CountDownLatch(1);
    }

    @Test
    public void testCompile() throws InterruptedException {
        final Client testSubject = new Step3SustainerClient(
            latchStep3,
            new LockedClient(lock.readLock(), LOCK_ATTEMPT_DURATION, new Step1And2SustainerClient(latchStep1, latchStep2))
        );

        executeCompileOnDifferentThread(testSubject);
        assertLockedOnlyDuringExecution();
    }

    @Test
    public void testCompileWhenException() throws InterruptedException {
        final Client testSubject = new Step3SustainerClient(
            latchStep3,
            new LockedClient(lock.readLock(), LOCK_ATTEMPT_DURATION, new Step1And2SustainerClient(latchStep1, latchStep2, true))
        );

        executeCompileOnDifferentThread(testSubject);
        assertLockedOnlyDuringExecution();
    }

    @Test
    public void testInsertEntries() throws InterruptedException {
        final Client testSubject = new Step3SustainerClient(
            latchStep3,
            new LockedClient(lock.readLock(), LOCK_ATTEMPT_DURATION, new Step1And2SustainerClient(latchStep1, latchStep2))
        );

        executeInsertOnDifferentThread(testSubject);
        assertLockedOnlyDuringExecution();
    }

    @Test
    public void testInsertEntriesWhenException() throws InterruptedException {
        final Client testSubject = new Step3SustainerClient(
            latchStep3,
            new LockedClient(lock.readLock(), LOCK_ATTEMPT_DURATION, new Step1And2SustainerClient(latchStep1, latchStep2, true))
        );

        executeInsertOnDifferentThread(testSubject);
        assertLockedOnlyDuringExecution();
    }

    @Test
    public void testQuery() throws InterruptedException {
        final Client testSubject = new Step3SustainerClient(
            latchStep3,
            new LockedClient(lock.readLock(), LOCK_ATTEMPT_DURATION, new Step1And2SustainerClient(latchStep1, latchStep2))
        );

        executeQueryOnDifferentThread(testSubject);
        assertLockedOnlyDuringExecution();
    }

    @Test
    public void testQueryWhenException() throws InterruptedException {
        final Client testSubject = new Step3SustainerClient(
            latchStep3,
            new LockedClient(lock.readLock(), LOCK_ATTEMPT_DURATION, new Step1And2SustainerClient(latchStep1, latchStep2, true))
        );

        executeQueryOnDifferentThread(testSubject);
        assertLockedOnlyDuringExecution();
    }

    @Test
    public void testLockIsReleasedAfterExceptionWhenCompile() {
        final Client testSubject = new Step3SustainerClient(
            new CountDownLatch(0),
            new LockedClient(lock.readLock(), LOCK_ATTEMPT_DURATION, new Step1And2SustainerClient(new CountDownLatch(0), new CountDownLatch(0), true))
        );
        assertThrows(ConditionFailedException.class, () -> testSubject.execute(SELECT_QUERY));
        assertEquals(0, lock.getReadLockCount());
    }

    @Test
    public void testLockIsReleasedAfterExceptionWhenInsertEntries() {
        final Client testSubject = new Step3SustainerClient(
            new CountDownLatch(0),
            new LockedClient(lock.readLock(), LOCK_ATTEMPT_DURATION, new Step1And2SustainerClient(new CountDownLatch(0), new CountDownLatch(0), true))
        );
        assertThrows(ConditionFailedException.class, () -> testSubject.insert(EVENT_TABLE_NAME, QuestDbTestUtil.getEventTableDataSource(Collections.emptyList())));
        assertEquals(0, lock.getReadLockCount());
    }

    @Test
    public void testLockIsReleasedAfterExceptionWhenQueryEntries() {
        final Client testSubject = new Step3SustainerClient(
            new CountDownLatch(0),
            new LockedClient(lock.readLock(), LOCK_ATTEMPT_DURATION, new Step1And2SustainerClient(new CountDownLatch(0), new CountDownLatch(0), true))
        );
        assertThrows(ConditionFailedException.class, () -> testSubject.query(SELECT_QUERY, RequestMapping.getResultProcessor(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING)));
        assertEquals(0, lock.getReadLockCount());
    }

    @Test
    public void testCompileWhenUnsuccessfulLockingBecauseOfReadLock() throws InterruptedException {
        final Client client = Mockito.mock(Client.class);
        final Client testSubject = new LockedClient(lock.readLock(), LOCK_ATTEMPT_DURATION, client);
        final CountDownLatch lockLatch = new CountDownLatch(1);

        new Thread(() -> {
            lock.writeLock().lock();
            lockLatch.countDown();
        }).start();

        lockLatch.await();
        assertThrows(LockUnsuccessfulException.class, () -> testSubject.execute(SELECT_QUERY));
    }

    @Test
    public void testInsertWhenUnsuccessfulLockingBecauseOfReadLock() throws InterruptedException {
        final Client client = Mockito.mock(Client.class);
        final Client testSubject = new LockedClient(lock.readLock(), LOCK_ATTEMPT_DURATION, client);
        final CountDownLatch lockLatch = new CountDownLatch(1);

        new Thread(() -> {
            lock.writeLock().lock();
            lockLatch.countDown();
        }).start();

        lockLatch.await();
        assertThrows(LockUnsuccessfulException.class, () -> testSubject.insert(EVENT_TABLE_NAME, QuestDbTestUtil.getEventTableDataSource(Collections.emptyList())));
    }

    @Test
    public void testQueryWhenUnsuccessfulLockingBecauseOfReadLock() throws InterruptedException {
        final Client client = Mockito.mock(Client.class);
        final Client testSubject = new LockedClient(lock.readLock(), LOCK_ATTEMPT_DURATION, client);
        final CountDownLatch lockLatch = new CountDownLatch(1);

        new Thread(() -> {
            lock.writeLock().lock();
            lockLatch.countDown();
        }).start();

        lockLatch.await();
        assertThrows(LockUnsuccessfulException.class, () -> testSubject.query(SELECT_QUERY, RequestMapping.getResultProcessor(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING)));
    }

    private static void executeQueryOnDifferentThread(final Client testSubject) {
        new Thread(() -> {
            try {
                testSubject.query(SELECT_QUERY, RequestMapping.getResultProcessor(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING));
            } catch (DatabaseException e) {
                fail();
            }
        }).start();
    }

    private void executeCompileOnDifferentThread(final Client testSubject) {
        new Thread(() -> {
            try {
                testSubject.execute(SELECT_QUERY);
            } catch (DatabaseException e) {
                fail();
            }
        }).start();
    }

    private void executeInsertOnDifferentThread(final Client testSubject) {
        new Thread(() -> {
            try {
                testSubject.insert(EVENT_TABLE_NAME, QuestDbTestUtil.getEventTableDataSource(Collections.emptyList()));
            } catch (DatabaseException e) {
                fail();
            }
        }).start();
    }

    private void assertLockedOnlyDuringExecution() throws InterruptedException {
        latchStep1.await();
        assertEquals(1, lock.getReadLockCount());
        latchStep2.countDown();
        latchStep3.await();
        assertEquals(0, lock.getReadLockCount());
    }

    private static class Step1And2SustainerClient implements Client {
        private final CountDownLatch step1countDownLatch;
        private final CountDownLatch step2countDownLatch;
        private final boolean doThrowException;

        private Step1And2SustainerClient(final CountDownLatch step1countDownLatch, final CountDownLatch step2countDownLatch) {
            this(step1countDownLatch, step2countDownLatch, false);
        }

        private Step1And2SustainerClient(final CountDownLatch step1countDownLatch, final CountDownLatch step2countDownLatch, final boolean doThrowException) {
            this.step1countDownLatch = step1countDownLatch;
            this.step2countDownLatch = step2countDownLatch;
            this.doThrowException = doThrowException;
        }

        @Override
        public void execute(final String query) throws DatabaseException {
            try {
                if (doThrowException) {
                    throw new ConditionFailedException("Test error");
                }
            } finally {
                step1countDownLatch.countDown();
                try {
                    step2countDownLatch.await();
                } catch (final InterruptedException e) {
                    fail();
                    throw new RuntimeException(e);
                }
            }
        }

        @Override
        public void insert(final String tableName, final InsertRowDataSource rowSource) throws DatabaseException {
            try {
                if (doThrowException) {
                    throw new ConditionFailedException("Test error");
                }
            } finally {
                step1countDownLatch.countDown();
                try {
                    step2countDownLatch.await();
                } catch (final InterruptedException e) {
                    fail();
                    throw new RuntimeException(e);
                }
            }
        }

        @Override
        public <T> T query(final String query, final QueryResultProcessor<T> rowProcessor) throws DatabaseException {
            try {
                if (doThrowException) {
                    throw new ConditionFailedException("Test error");
                }

                return rowProcessor.getResult();
            } finally {
                step1countDownLatch.countDown();
                try {
                    step2countDownLatch.await();
                } catch (final InterruptedException e) {
                    fail();
                    throw new RuntimeException(e);
                }
            }
        }

        @Override
        public void disconnect() { }
    }

    private static class Step3SustainerClient implements Client {
        private final CountDownLatch step3countDownLatch;
        private final Client client;

        private Step3SustainerClient(final CountDownLatch step3countDownLatch, final Client client) {
            this.step3countDownLatch = step3countDownLatch;
            this.client = client;
        }

        @Override
        public void execute(final String query) throws DatabaseException {
            try {
                client.execute(query);
            } finally {
                step3countDownLatch.countDown();
            }
        }

        @Override
        public void insert(final String tableName, final InsertRowDataSource rowSource) throws DatabaseException {
            try {
                client.insert(tableName, rowSource);
            } finally {
                step3countDownLatch.countDown();
            }
        }

        @Override
        public <T> T query(final String query, final QueryResultProcessor<T> rowProcessor) throws DatabaseException {
            try {
                return client.query(query, rowProcessor);
            } finally {
                step3countDownLatch.countDown();
            }
        }

        @Override
        public void disconnect() { }
    }
}
