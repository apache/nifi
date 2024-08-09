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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;

import static org.apache.nifi.questdb.util.QuestDbTestUtil.SELECT_QUERY;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

// This test uses the {@code compile} method to cover different scenarios. It is expected that other methods behave the same
// but the coverage of the individual behaviour of the individual methods is provided by different tests.
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class CompositeClientTest {
    private static final int NUMBER_OF_RETRIES = 3;
    private static final int NUMBER_OF_ATTEMPTS = NUMBER_OF_RETRIES + 1;
    private static final Duration LOCK_ATTEMPT_DURATION = Duration.of(20, TimeUnit.MILLISECONDS.toChronoUnit());

    @Mock
    Client client;

    @Mock
    Client fallback;

    @Mock
    BiConsumer<Integer, Throwable> errorAction;

    @Mock
    ConditionAwareClient.Condition condition;

    private ReadWriteLock databaseStructureLock;

    private LockedClient lockedClientSpy;

    private Client testSubject;

    @BeforeEach
    public void setUp() {
        databaseStructureLock = new ReentrantReadWriteLock();
        testSubject = getTestSubject();

        when(condition.check()).thenReturn(true);
    }

    @Test
    public void testHappyPath() throws DatabaseException {
        testSubject.execute(SELECT_QUERY);

        assertWrappedClientIsCalled(1);
        assertFallbackIsNotCalled();
        assertErrorActionIsNotCalled();
    }

    @Test
    public void testSingleErrorDuringExecution() throws DatabaseException {
        doThrow(new DatabaseException("Test")).doNothing().when(client).execute(SELECT_QUERY);

        testSubject.execute(SELECT_QUERY);

        assertWrappedClientIsCalled(2);
        assertFallbackIsNotCalled();
        assertErrorActionIsCalled(1);
    }

    @Test
    public void testConstantErrorDuringExecution() throws DatabaseException {
        doThrow(new DatabaseException("Test")).when(client).execute(SELECT_QUERY);

        testSubject.execute(SELECT_QUERY);

        assertWrappedClientIsCalled(NUMBER_OF_ATTEMPTS);
        assertFallbackIsCalled();
        assertErrorActionIsCalled(NUMBER_OF_ATTEMPTS);
    }

    @Test
    public void testWhenTheWriteLockIsOngoing() throws DatabaseException, InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);

        new Thread(() -> {
            databaseStructureLock.writeLock().lock();
            countDownLatch.countDown();
        }).start();

        countDownLatch.await();
        testSubject.execute(SELECT_QUERY);

        assertWrappedClientIsNotCalled();
        assertFallbackIsCalled();
        assertErrorActionIsCalled(NUMBER_OF_ATTEMPTS);
    }

    @Test
    public void testWhenTheWriteLockIsOngoingButReleased() throws DatabaseException, InterruptedException {
        final CountDownLatch lockLatch = new CountDownLatch(1);
        final CountDownLatch unlockLatch = new CountDownLatch(1);
        final AtomicInteger numberOfCallsExecuted = new AtomicInteger(0);

        // In order to be able to release lock from the same thread, we stall the execution until after the first attempt
        new Thread(() -> {
            databaseStructureLock.writeLock().lock();
            lockLatch.countDown();
            try {
                unlockLatch.await();
            } catch (final InterruptedException e) {
                throw new RuntimeException(e);
            }
            databaseStructureLock.writeLock().unlock();
        }).start();

        // The actual LockedQuestDbClient is surrounded with a spy, which allows the worker thread above to run at the second attempt
        doAnswer(invocationOnMock -> {
            if (numberOfCallsExecuted.get() == 1) {
                unlockLatch.countDown();
            }

            numberOfCallsExecuted.incrementAndGet();
            return invocationOnMock.callRealMethod();
        }).when(lockedClientSpy).execute(SELECT_QUERY);

        lockLatch.await();
        testSubject.execute(SELECT_QUERY);

        Assertions.assertEquals(2, numberOfCallsExecuted.get());
        assertWrappedClientIsCalled(1);
        assertFallbackIsNotCalled();
        assertErrorActionIsCalled(1);
    }

    @Test
    public void testWhenConditionDisallowsRun() throws DatabaseException {
        when(condition.check()).thenReturn(false);

        testSubject.execute(SELECT_QUERY);

        assertWrappedClientIsNotCalled();
        assertFallbackIsCalled();
        assertErrorActionIsCalled(NUMBER_OF_ATTEMPTS);
    }

    @Test
    public void testWhenConditionDisallowsRunForTheFirstAttempt() throws DatabaseException {
        when(condition.check()).thenReturn(false).thenReturn(true);

        testSubject.execute(SELECT_QUERY);

        assertWrappedClientIsCalled(1);
        assertFallbackIsNotCalled();
        assertErrorActionIsCalled(1);
    }

    private Client getTestSubject() {
        final LockedClient lockedClient = new LockedClient(
                databaseStructureLock.readLock(),
                LOCK_ATTEMPT_DURATION,
                new ConditionAwareClient(condition, client)
        );

        lockedClientSpy = spy(lockedClient);

        return RetryingClient.getInstance(NUMBER_OF_RETRIES, errorAction, lockedClientSpy, fallback);
    }

    private void assertWrappedClientIsNotCalled() throws DatabaseException {
        verify(client, never()).execute(anyString());
    }

    private void assertWrappedClientIsCalled(final int times) throws DatabaseException {
        verify(client, times(times)).execute(SELECT_QUERY);
    }

    private void assertFallbackIsNotCalled() throws DatabaseException {
        verify(fallback, never()).execute(anyString());
    }

    private void assertFallbackIsCalled() throws DatabaseException {
        verify(fallback, times(1)).execute(SELECT_QUERY);
    }

    private void assertErrorActionIsCalled(final int times) {
        verify(errorAction, times(times)).accept(any(Integer.class), any(Exception.class));
    }

    private void assertErrorActionIsNotCalled() {
        verify(errorAction, never()).accept(any(Integer.class), any(Exception.class));
    }
}
