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

import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.GarbageCollectionLog;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.repository.RepositoryContext;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarThreadContextClassLoader;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.ReportingTask;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.util.NiFiProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class VirtualThreadSchedulingAgentTest {

    private static final int MAX_THREADS = 10;
    private static final String COMPONENT_ID = UUID.randomUUID().toString();

    @Mock
    private FlowController flowController;

    @Mock
    private RepositoryContextFactory contextFactory;

    @Mock
    private NiFiProperties nifiProperties;

    @Mock
    private StateManagerProvider stateManagerProvider;

    @Mock
    private StateManager stateManager;

    @Mock
    private GarbageCollectionLog garbageCollectionLog;

    @Mock
    private ExtensionManager extensionManager;

    private VirtualThreadSchedulingAgent agent;

    @BeforeEach
    void setUp() {
        when(nifiProperties.getBoredYieldDuration()).thenReturn("10 millis");
        agent = new VirtualThreadSchedulingAgent(flowController, contextFactory, nifiProperties, MAX_THREADS);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        agent.shutdown();
        assertTrue(agent.awaitTermination(5, TimeUnit.SECONDS));
    }

    @Test
    void testIncrementMaxThreadCountAdjustsSemaphore() {
        final int originalPermits = agent.getGlobalSemaphore().getMaxPermits();

        agent.incrementMaxThreadCount(0);
        assertEquals(originalPermits, agent.getGlobalSemaphore().getMaxPermits());

        agent.incrementMaxThreadCount(5);
        assertEquals(originalPermits + 5, agent.getGlobalSemaphore().getMaxPermits());

        agent.incrementMaxThreadCount(-3);
        assertEquals(originalPermits + 2, agent.getGlobalSemaphore().getMaxPermits());

        assertThrows(IllegalStateException.class, () -> agent.incrementMaxThreadCount(-1000));
    }

    @Test
    void testScheduleSpawnsThreadsThatInvoke() throws InterruptedException {
        final int concurrentTasks = 3;
        final AtomicInteger invocationCount = new AtomicInteger(0);
        final CountDownLatch allTasksInvoked = new CountDownLatch(concurrentTasks);
        final AtomicBoolean virtualThreadsUsed = new AtomicBoolean(true);

        final Connectable connectable = createMockedConnectable(concurrentTasks, SchedulingStrategy.TIMER_DRIVEN, invocationCount, allTasksInvoked);
        doAnswer(invocation -> {
            invocationCount.incrementAndGet();
            allTasksInvoked.countDown();
            if (!Thread.currentThread().isVirtual()) {
                virtualThreadsUsed.set(false);
            }
            return null;
        }).when(connectable).onTrigger(any(), any());
        final LifecycleState lifecycleState = new LifecycleState(COMPONENT_ID);

        scheduleConnectable(connectable, lifecycleState);

        assertTrue(allTasksInvoked.await(5, TimeUnit.SECONDS),
                "Expected " + concurrentTasks + " threads to invoke, but only " + (concurrentTasks - allTasksInvoked.getCount()) + " did");
        assertTrue(invocationCount.get() >= concurrentTasks,
                "Expected at least " + concurrentTasks + " invocations but got " + invocationCount.get());
        assertTrue(virtualThreadsUsed.get());

        unscheduleConnectable(connectable, lifecycleState);
        waitForRunningThreadCount(0, 2, TimeUnit.SECONDS);
    }

    @Test
    void testProcessorContinuesAfterInterruptStatusSet() throws InterruptedException {
        final AtomicInteger invocationCount = new AtomicInteger();
        final CountDownLatch secondInvocation = new CountDownLatch(1);
        final Connectable connectable = createMockedConnectable(1, SchedulingStrategy.TIMER_DRIVEN, new AtomicInteger(), new CountDownLatch(0));
        doAnswer(invocation -> {
            if (invocationCount.incrementAndGet() == 1) {
                Thread.currentThread().interrupt();
            } else {
                secondInvocation.countDown();
            }

            return null;
        }).when(connectable).onTrigger(any(), any());

        final LifecycleState lifecycleState = new LifecycleState(COMPONENT_ID);
        scheduleConnectable(connectable, lifecycleState);

        try {
            assertTrue(secondInvocation.await(2, TimeUnit.SECONDS));
        } finally {
            unscheduleConnectable(connectable, lifecycleState);
        }
    }

    @Test
    void testSchedulingThreadUsesFrameworkClassLoaderWithoutInheritedThreadLocals() throws InterruptedException {
        final InheritableThreadLocal<String> inheritedValue = new InheritableThreadLocal<>();
        final ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        final ClassLoader lifecycleClassLoader = new ClassLoader(originalClassLoader) {
        };
        final AtomicReference<ClassLoader> observedClassLoader = new AtomicReference<>();
        final AtomicReference<String> observedInheritedValue = new AtomicReference<>();
        final CountDownLatch schedulingThreadObserved = new CountDownLatch(1);
        final Connectable connectable = createMockedConnectable(1, SchedulingStrategy.TIMER_DRIVEN, new AtomicInteger(), new CountDownLatch(0));
        when(connectable.getYieldExpiration()).thenAnswer(invocation -> {
            observedClassLoader.compareAndSet(null, Thread.currentThread().getContextClassLoader());
            observedInheritedValue.compareAndSet(null, inheritedValue.get());
            schedulingThreadObserved.countDown();
            return 0L;
        });

        inheritedValue.set("lifecycle-thread-value");
        Thread.currentThread().setContextClassLoader(lifecycleClassLoader);
        final LifecycleState lifecycleState = new LifecycleState(COMPONENT_ID);

        try {
            scheduleConnectable(connectable, lifecycleState);
            assertTrue(schedulingThreadObserved.await(2, TimeUnit.SECONDS));
            assertEquals(NarThreadContextClassLoader.getInstance(), observedClassLoader.get());
            assertNull(observedInheritedValue.get());
        } finally {
            inheritedValue.remove();
            Thread.currentThread().setContextClassLoader(originalClassLoader);
            unscheduleConnectable(connectable, lifecycleState);
        }
    }

    @Test
    void testDuplicateScheduleIsRejected() throws InterruptedException {
        final CountDownLatch invocationStarted = new CountDownLatch(1);
        final CountDownLatch releaseInvocation = new CountDownLatch(1);
        final Connectable connectable = createMockedConnectable(1, SchedulingStrategy.TIMER_DRIVEN, new AtomicInteger(), new CountDownLatch(0));
        doAnswer(invocation -> {
            invocationStarted.countDown();
            releaseInvocation.await();
            return null;
        }).when(connectable).onTrigger(any(), any());

        final LifecycleState lifecycleState = new LifecycleState(COMPONENT_ID);
        scheduleConnectable(connectable, lifecycleState);

        try {
            assertTrue(invocationStarted.await(2, TimeUnit.SECONDS));
            assertThrows(IllegalStateException.class, () -> agent.schedule(connectable, lifecycleState));
        } finally {
            unscheduleConnectable(connectable, lifecycleState);
            releaseInvocation.countDown();
        }
    }

    @Test
    void testComponentYieldDoesNotShortenSchedulingPeriod() throws InterruptedException {
        final long schedulingPeriodMillis = 500L;
        final AtomicLong yieldExpiration = new AtomicLong();
        final AtomicInteger invocationCount = new AtomicInteger();
        final CountDownLatch firstInvocation = new CountDownLatch(1);
        final CountDownLatch secondInvocation = new CountDownLatch(1);
        final Connectable connectable = createMockedConnectable(1, SchedulingStrategy.TIMER_DRIVEN, new AtomicInteger(), new CountDownLatch(0));
        when(connectable.getSchedulingPeriod(TimeUnit.MILLISECONDS)).thenReturn(schedulingPeriodMillis);
        when(connectable.getSchedulingPeriod(TimeUnit.NANOSECONDS)).thenReturn(TimeUnit.MILLISECONDS.toNanos(schedulingPeriodMillis));
        when(connectable.getYieldExpiration()).thenAnswer(invocation -> yieldExpiration.get());
        doAnswer(invocation -> {
            final int currentInvocation = invocationCount.incrementAndGet();
            if (currentInvocation == 1) {
                yieldExpiration.set(System.currentTimeMillis() + 50L);
                firstInvocation.countDown();
            } else if (currentInvocation == 2) {
                secondInvocation.countDown();
            }
            return null;
        }).when(connectable).onTrigger(any(), any());

        final LifecycleState lifecycleState = new LifecycleState(COMPONENT_ID);
        scheduleConnectable(connectable, lifecycleState);

        try {
            assertTrue(firstInvocation.await(2, TimeUnit.SECONDS));
            assertFalse(secondInvocation.await(250, TimeUnit.MILLISECONDS));
            assertTrue(secondInvocation.await(2, TimeUnit.SECONDS));
        } finally {
            unscheduleConnectable(connectable, lifecycleState);
        }
    }

    @Test
    void testZeroBoredYieldUsesSchedulingPeriod() throws InterruptedException {
        agent.shutdown();
        when(nifiProperties.getBoredYieldDuration()).thenReturn("0 millis");
        agent = new VirtualThreadSchedulingAgent(flowController, contextFactory, nifiProperties, MAX_THREADS);

        final long schedulingPeriodMillis = 500L;
        final AtomicInteger schedulingAttempts = new AtomicInteger();
        final CountDownLatch firstAttempt = new CountDownLatch(1);
        final CountDownLatch secondAttempt = new CountDownLatch(1);
        final Connectable connectable = createMockedConnectable(1, SchedulingStrategy.TIMER_DRIVEN, new AtomicInteger(), new CountDownLatch(0));
        when(connectable.getSchedulingPeriod(TimeUnit.MILLISECONDS)).thenReturn(schedulingPeriodMillis);
        when(connectable.getSchedulingPeriod(TimeUnit.NANOSECONDS)).thenReturn(TimeUnit.MILLISECONDS.toNanos(schedulingPeriodMillis));
        when(connectable.isIsolated()).thenAnswer(invocation -> {
            final int attempt = schedulingAttempts.incrementAndGet();
            if (attempt == 1) {
                firstAttempt.countDown();
            } else if (attempt == 2) {
                secondAttempt.countDown();
            }
            return true;
        });
        when(flowController.isConfiguredForClustering()).thenReturn(true);
        when(flowController.isPrimary()).thenReturn(false);

        final LifecycleState lifecycleState = new LifecycleState(COMPONENT_ID);
        scheduleConnectable(connectable, lifecycleState);

        try {
            assertTrue(firstAttempt.await(2, TimeUnit.SECONDS));
            assertFalse(secondAttempt.await(250, TimeUnit.MILLISECONDS));
            assertTrue(secondAttempt.await(2, TimeUnit.SECONDS));
        } finally {
            unscheduleConnectable(connectable, lifecycleState);
        }
    }

    @Test
    void testUnscheduleWakesLongSchedulingDelay() throws InterruptedException {
        final CountDownLatch invocationCompleted = new CountDownLatch(1);
        final Connectable connectable = createMockedConnectable(1, SchedulingStrategy.TIMER_DRIVEN, new AtomicInteger(), invocationCompleted);
        when(connectable.getSchedulingPeriod(TimeUnit.MILLISECONDS)).thenReturn(TimeUnit.DAYS.toMillis(1L));
        when(connectable.getSchedulingPeriod(TimeUnit.NANOSECONDS)).thenReturn(TimeUnit.DAYS.toNanos(1L));
        final LifecycleState lifecycleState = new LifecycleState(COMPONENT_ID);

        scheduleConnectable(connectable, lifecycleState);
        assertTrue(invocationCompleted.await(2, TimeUnit.SECONDS));

        unscheduleConnectable(connectable, lifecycleState);
        waitForRunningThreadCount(0, 2, TimeUnit.SECONDS);
        assertEquals(0, lifecycleState.getActiveThreadCount());
    }

    @Test
    void testScheduleOnceInvokesAndStops() throws InterruptedException {
        final Connectable connectable = createMockedConnectable(1, SchedulingStrategy.TIMER_DRIVEN, new AtomicInteger(), new CountDownLatch(0));
        final LifecycleState lifecycleState = new LifecycleState(COMPONENT_ID);
        final CountDownLatch stopCallbackInvoked = new CountDownLatch(1);

        lifecycleState.setScheduled(true);
        agent.scheduleOnce(connectable, lifecycleState, () -> {
            stopCallbackInvoked.countDown();
            return null;
        });

        assertTrue(stopCallbackInvoked.await(5, TimeUnit.SECONDS),
                "Stop callback should have been invoked after scheduleOnce");
    }

    @Test
    void testUnscheduleExitsWhenSemaphoreFullyContended() throws InterruptedException {
        agent.setMaxThreadCount(1);

        final CountDownLatch releaseHeldPermit = new CountDownLatch(1);
        final CountDownLatch permitAcquired = new CountDownLatch(1);
        final Thread permitHolder = Thread.ofVirtual().start(() -> {
            try {
                agent.getGlobalSemaphore().acquire();
                permitAcquired.countDown();
                releaseHeldPermit.await();
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                agent.getGlobalSemaphore().release();
            }
        });
        assertTrue(permitAcquired.await(2, TimeUnit.SECONDS), "Failed to acquire permit for test setup");

        final AtomicInteger invocationCount = new AtomicInteger(0);
        final Connectable connectable = createMockedConnectable(1, SchedulingStrategy.TIMER_DRIVEN, invocationCount, new CountDownLatch(0));
        final LifecycleState lifecycleState = new LifecycleState(COMPONENT_ID);
        scheduleConnectable(connectable, lifecycleState);

        waitForRunningThreadCount(1, 2, TimeUnit.SECONDS);
        assertEquals(0, invocationCount.get());
        unscheduleConnectable(connectable, lifecycleState);
        waitForRunningThreadCount(0, 2, TimeUnit.SECONDS);

        releaseHeldPermit.countDown();
        permitHolder.join(1_000L);

        assertEquals(0, invocationCount.get());
    }

    @Test
    void testConcurrentIncrementMaxThreadCountIsThreadSafe() throws InterruptedException {
        agent.setMaxThreadCount(100);

        final int threadCount = 20;
        final int incrementsPerThread = 50;
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            Thread.ofVirtual().start(() -> {
                try {
                    start.await();
                    for (int j = 0; j < incrementsPerThread; j++) {
                        agent.incrementMaxThreadCount(1);
                    }
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        assertTrue(done.await(5, TimeUnit.SECONDS));

        assertEquals(100 + threadCount * incrementsPerThread, agent.getGlobalSemaphore().getMaxPermits(),
                "Lost increments imply a race condition in incrementMaxThreadCount");
    }

    @Test
    void testSchedulingPeriodReadOnceWhenScheduled() throws InterruptedException {
        final CountDownLatch invocationsCompleted = new CountDownLatch(5);
        final Connectable connectable = createMockedConnectable(1, SchedulingStrategy.TIMER_DRIVEN, new AtomicInteger(), invocationsCompleted);
        final AtomicInteger schedulingPeriodCalls = new AtomicInteger();
        when(connectable.getSchedulingPeriod(TimeUnit.NANOSECONDS)).thenAnswer(invocation -> {
            schedulingPeriodCalls.incrementAndGet();
            return TimeUnit.MILLISECONDS.toNanos(10L);
        });

        final LifecycleState lifecycleState = new LifecycleState(COMPONENT_ID);
        scheduleConnectable(connectable, lifecycleState);

        try {
            assertTrue(invocationsCompleted.await(5, TimeUnit.SECONDS));
        } finally {
            unscheduleConnectable(connectable, lifecycleState);
            waitForRunningThreadCount(0, 2, TimeUnit.SECONDS);
        }

        assertEquals(1, schedulingPeriodCalls.get());
    }

    @Test
    void testSchedulingLoopContinuesAfterUnexpectedError() throws InterruptedException {
        agent.setAdministrativeYieldDuration("1 millis");
        final CountDownLatch invocationCompleted = new CountDownLatch(1);
        final Connectable connectable = createMockedConnectable(1, SchedulingStrategy.TIMER_DRIVEN, new AtomicInteger(), invocationCompleted);
        when(connectable.isIsolated()).thenThrow(new AssertionError("Simulated scheduling error")).thenReturn(false);

        final LifecycleState lifecycleState = new LifecycleState(COMPONENT_ID);
        scheduleConnectable(connectable, lifecycleState);

        try {
            assertTrue(invocationCompleted.await(2, TimeUnit.SECONDS));
        } finally {
            unscheduleConnectable(connectable, lifecycleState);
            waitForRunningThreadCount(0, 2, TimeUnit.SECONDS);
        }

        assertEquals(MAX_THREADS, agent.getGlobalSemaphore().availablePermits());
    }

    @Test
    void testInvocationExceptionStillReleasesPermit() throws InterruptedException {
        agent.setMaxThreadCount(2);
        final Connectable connectable = createMockedConnectable(1, SchedulingStrategy.TIMER_DRIVEN, new AtomicInteger(), new CountDownLatch(0));
        final AtomicInteger invocationCount = new AtomicInteger(0);
        final CountDownLatch successfulInvocation = new CountDownLatch(1);
        doAnswer(invocation -> {
            final int count = invocationCount.incrementAndGet();
            if (count <= 3) {
                throw new IllegalStateException("Simulated failure " + count);
            }
            successfulInvocation.countDown();
            return null;
        }).when(connectable).onTrigger(any(), any());

        final LifecycleState lifecycleState = new LifecycleState(COMPONENT_ID);
        scheduleConnectable(connectable, lifecycleState);

        assertTrue(successfulInvocation.await(2, TimeUnit.SECONDS));
        unscheduleConnectable(connectable, lifecycleState);
        waitForRunningThreadCount(0, 2, TimeUnit.SECONDS);
        assertEquals(2, agent.getGlobalSemaphore().availablePermits());
    }

    @Test
    void testCronScheduleSpawnsThreadsAndInvokes() throws InterruptedException {
        final AtomicInteger invocationCount = new AtomicInteger(0);
        final CountDownLatch atLeastOneInvocation = new CountDownLatch(1);
        final Connectable connectable = createMockedConnectable(1, SchedulingStrategy.CRON_DRIVEN, invocationCount, atLeastOneInvocation);
        when(connectable.getSchedulingPeriod()).thenReturn("* * * * * ?");
        when(connectable.evaluateParameters(eq("* * * * * ?"))).thenReturn("* * * * * ?");

        final LifecycleState lifecycleState = new LifecycleState(COMPONENT_ID);
        scheduleConnectable(connectable, lifecycleState);

        assertTrue(atLeastOneInvocation.await(3, TimeUnit.SECONDS),
                "CRON-scheduled connectable should have invoked at least once within 3 seconds");

        unscheduleConnectable(connectable, lifecycleState);
        waitForRunningThreadCount(0, 2, TimeUnit.SECONDS);
    }

    @Test
    void testCronDrivenReportingTaskIsScheduled() throws InterruptedException {
        final CountDownLatch invocationCompleted = new CountDownLatch(1);
        final ReportingTask reportingTask = mock(ReportingTask.class);
        doAnswer(invocation -> {
            invocationCompleted.countDown();
            return null;
        }).when(reportingTask).onTrigger(any());

        final ReportingTaskNode taskNode = mock(ReportingTaskNode.class);
        when(taskNode.getSchedulingStrategy()).thenReturn(SchedulingStrategy.CRON_DRIVEN);
        when(taskNode.getSchedulingPeriod()).thenReturn("* * * * * ?");
        when(taskNode.getSchedulingPeriod(TimeUnit.NANOSECONDS))
                .thenThrow(new IllegalArgumentException("CRON expression cannot be parsed as a time duration"));
        when(taskNode.getReportingTask()).thenReturn(reportingTask);
        when(taskNode.getReportingContext()).thenReturn(mock(ReportingContext.class));
        when(taskNode.getIdentifier()).thenReturn(COMPONENT_ID);
        when(taskNode.getName()).thenReturn("TestReporter");
        when(flowController.getExtensionManager()).thenReturn(extensionManager);

        final LifecycleState lifecycleState = new LifecycleState(COMPONENT_ID);
        lifecycleState.setScheduled(true);
        agent.schedule(taskNode, lifecycleState);

        try {
            assertTrue(invocationCompleted.await(3, TimeUnit.SECONDS));
        } finally {
            lifecycleState.setScheduled(false);
            agent.unschedule(taskNode, lifecycleState);
            waitForRunningThreadCount(0, 2, TimeUnit.SECONDS);
        }
    }

    @Test
    void testReportingTaskContinuesAfterInterruptStatusSet() throws InterruptedException {
        final AtomicInteger invocationCount = new AtomicInteger();
        final CountDownLatch secondInvocation = new CountDownLatch(1);
        final ReportingTask reportingTask = mock(ReportingTask.class);
        doAnswer(invocation -> {
            if (invocationCount.incrementAndGet() == 1) {
                Thread.currentThread().interrupt();
            } else {
                secondInvocation.countDown();
            }

            return null;
        }).when(reportingTask).onTrigger(any());

        final ReportingTaskNode taskNode = mock(ReportingTaskNode.class);
        when(taskNode.getSchedulingStrategy()).thenReturn(SchedulingStrategy.TIMER_DRIVEN);
        when(taskNode.getSchedulingPeriod(TimeUnit.NANOSECONDS)).thenReturn(TimeUnit.MILLISECONDS.toNanos(100L));
        when(taskNode.getReportingTask()).thenReturn(reportingTask);
        when(taskNode.getReportingContext()).thenReturn(mock(ReportingContext.class));
        when(taskNode.getIdentifier()).thenReturn(COMPONENT_ID);
        when(taskNode.getName()).thenReturn("TestReporter");
        when(flowController.getExtensionManager()).thenReturn(extensionManager);

        final LifecycleState lifecycleState = new LifecycleState(COMPONENT_ID);
        lifecycleState.setScheduled(true);
        agent.schedule(taskNode, lifecycleState);

        try {
            assertTrue(secondInvocation.await(2, TimeUnit.SECONDS));
        } finally {
            lifecycleState.setScheduled(false);
            agent.unschedule(taskNode, lifecycleState);
        }
    }

    @Test
    void testCronConnectableExitsCleanlyWhenNoFutureFirings() throws InterruptedException {
        final String unreachableCron = "0 0 0 30 2 ?";
        final AtomicInteger invocationCount = new AtomicInteger(0);
        final Connectable connectable = createMockedConnectable(1, SchedulingStrategy.CRON_DRIVEN, invocationCount, new CountDownLatch(0));
        when(connectable.getSchedulingPeriod()).thenReturn(unreachableCron);
        when(connectable.evaluateParameters(eq(unreachableCron))).thenReturn(unreachableCron);

        final LifecycleState lifecycleState = new LifecycleState(COMPONENT_ID);
        scheduleConnectable(connectable, lifecycleState);

        Thread.sleep(500L);

        assertEquals(0, invocationCount.get());
        assertEquals(MAX_THREADS, agent.getGlobalSemaphore().availablePermits());

        unscheduleConnectable(connectable, lifecycleState);
    }

    @Test
    void testRapidStopStartDoesNotLeakSchedulingThreads() throws InterruptedException {
        final AtomicReference<Thread> firstSchedulingThread = new AtomicReference<>();
        final AtomicInteger invocationCount = new AtomicInteger();
        final CountDownLatch firstInvocation = new CountDownLatch(1);
        final CountDownLatch secondInvocation = new CountDownLatch(1);
        final Connectable connectable = createMockedConnectable(1, SchedulingStrategy.TIMER_DRIVEN, new AtomicInteger(), new CountDownLatch(0));
        when(connectable.getSchedulingPeriod(TimeUnit.NANOSECONDS)).thenReturn(TimeUnit.MILLISECONDS.toNanos(500L));
        doAnswer(invocation -> {
            final int currentInvocation = invocationCount.incrementAndGet();
            if (currentInvocation == 1) {
                firstSchedulingThread.set(Thread.currentThread());
                firstInvocation.countDown();
            } else if (currentInvocation == 2) {
                secondInvocation.countDown();
            }
            return null;
        }).when(connectable).onTrigger(any(), any());

        final LifecycleState lifecycleState = new LifecycleState(COMPONENT_ID);
        scheduleConnectable(connectable, lifecycleState);

        try {
            assertTrue(firstInvocation.await(2, TimeUnit.SECONDS));

            unscheduleConnectable(connectable, lifecycleState);
            scheduleConnectable(connectable, lifecycleState);
            assertTrue(secondInvocation.await(2, TimeUnit.SECONDS));

            firstSchedulingThread.get().join(2_000L);
            assertFalse(firstSchedulingThread.get().isAlive());
        } finally {
            unscheduleConnectable(connectable, lifecycleState);
            waitForRunningThreadCount(0, 2, TimeUnit.SECONDS);
        }
    }

    private void scheduleConnectable(final Connectable connectable, final LifecycleState lifecycleState) {
        lifecycleState.setScheduled(true);
        agent.schedule(connectable, lifecycleState);
    }

    private void unscheduleConnectable(final Connectable connectable, final LifecycleState lifecycleState) {
        lifecycleState.setScheduled(false);
        agent.unschedule(connectable, lifecycleState);
    }

    private void waitForRunningThreadCount(final int expectedCount, final long timeout, final TimeUnit timeUnit) throws InterruptedException {
        final long deadline = System.nanoTime() + timeUnit.toNanos(timeout);
        while (agent.getRunningThreadCount() != expectedCount && System.nanoTime() < deadline) {
            Thread.sleep(10L);
        }
        assertEquals(expectedCount, agent.getRunningThreadCount());
    }

    private Connectable createMockedConnectable(final int maxConcurrentTasks, final SchedulingStrategy schedulingStrategy,
                                                final AtomicInteger invocationCount, final CountDownLatch invocationLatch) {
        final Connectable connectable = mock(Connectable.class);
        when(connectable.getIdentifier()).thenReturn(COMPONENT_ID);
        when(connectable.getName()).thenReturn("TestProcessor");
        when(connectable.getMaxConcurrentTasks()).thenReturn(maxConcurrentTasks);
        when(connectable.getIncomingConnections()).thenReturn(Collections.emptyList());
        when(connectable.getRelationships()).thenReturn(Collections.emptySet());
        when(connectable.getSchedulingPeriod(TimeUnit.MILLISECONDS)).thenReturn(100L);
        when(connectable.getSchedulingPeriod(TimeUnit.NANOSECONDS)).thenReturn(TimeUnit.MILLISECONDS.toNanos(100L));
        when(connectable.getYieldExpiration()).thenReturn(0L);
        when(connectable.getSchedulingStrategy()).thenReturn(schedulingStrategy);
        when(connectable.isTriggerWhenEmpty()).thenReturn(true);
        when(connectable.isIsolated()).thenReturn(false);
        when(connectable.getRunDuration(TimeUnit.NANOSECONDS)).thenReturn(0L);
        when(connectable.isSessionBatchingSupported()).thenReturn(false);
        when(connectable.getScheduledState()).thenReturn(ScheduledState.RUNNING);

        final Processor runnableComponent = mock(Processor.class);
        when(connectable.getRunnableComponent()).thenReturn(runnableComponent);

        doAnswer(invocation -> {
            invocationCount.incrementAndGet();
            invocationLatch.countDown();
            return null;
        }).when(connectable).onTrigger(any(), any());

        final ProcessGroup processGroup = mock(ProcessGroup.class);
        when(processGroup.getName()).thenReturn("RootGroup");
        when(processGroup.getParent()).thenReturn(null);
        when(connectable.getProcessGroup()).thenReturn(processGroup);

        when(flowController.getStateManagerProvider()).thenReturn(stateManagerProvider);
        when(stateManagerProvider.getStateManager(eq(COMPONENT_ID))).thenReturn(stateManager);
        when(flowController.getGarbageCollectionLog()).thenReturn(garbageCollectionLog);
        when(flowController.getPerformanceTrackingPercentage()).thenReturn(0);
        when(flowController.getExtensionManager()).thenReturn(extensionManager);

        final RepositoryContext repositoryContext = mock(RepositoryContext.class);
        when(repositoryContext.isRelationshipAvailabilitySatisfied(0)).thenReturn(true);
        final FlowFileEventRepository flowFileEventRepository = mock(FlowFileEventRepository.class);
        when(repositoryContext.getFlowFileEventRepository()).thenReturn(flowFileEventRepository);
        when(contextFactory.newProcessContext(eq(connectable), any(AtomicLong.class))).thenReturn(repositoryContext);

        return connectable;
    }

    @Test
    void testScheduleRollsBackScheduledFlagOnFailure() {
        final Connectable connectable = createMockedConnectable(2, SchedulingStrategy.TIMER_DRIVEN, new AtomicInteger(), new CountDownLatch(0));
        when(connectable.getMaxConcurrentTasks()).thenThrow(new IllegalStateException("Simulated failure during schedule"));

        final LifecycleState lifecycleState = new LifecycleState(COMPONENT_ID);
        final IllegalStateException thrown = assertThrows(IllegalStateException.class, () -> agent.schedule(connectable, lifecycleState));
        assertEquals("Simulated failure during schedule", thrown.getMessage());
        assertFalse(lifecycleState.isScheduled());
        assertEquals(0, agent.getRunningThreadCount());
    }

    @Test
    void testScheduleOnceRollsBackScheduledFlagOnFailure() {
        final Connectable connectable = createMockedConnectable(1, SchedulingStrategy.TIMER_DRIVEN, new AtomicInteger(), new CountDownLatch(0));
        when(connectable.getProcessGroup()).thenThrow(new IllegalStateException("Simulated failure building thread name"));

        final LifecycleState lifecycleState = new LifecycleState(COMPONENT_ID);
        assertThrows(IllegalStateException.class, () -> agent.scheduleOnce(connectable, lifecycleState, () -> null));
        assertFalse(lifecycleState.isScheduled());
    }

    @Test
    void testScheduleReportingTaskRollsBackScheduledFlagOnFailure() {
        final ReportingTaskNode taskNode = mock(ReportingTaskNode.class);
        when(taskNode.getSchedulingStrategy()).thenReturn(SchedulingStrategy.TIMER_DRIVEN);
        when(taskNode.getSchedulingPeriod(TimeUnit.NANOSECONDS)).thenReturn(TimeUnit.MILLISECONDS.toNanos(50L));
        when(taskNode.getIdentifier()).thenReturn(COMPONENT_ID);
        when(taskNode.getName()).thenThrow(new IllegalStateException("Simulated failure building thread name"));
        when(flowController.getExtensionManager()).thenReturn(extensionManager);

        final LifecycleState lifecycleState = new LifecycleState(COMPONENT_ID);
        assertThrows(IllegalStateException.class, () -> agent.schedule(taskNode, lifecycleState));
        assertFalse(lifecycleState.isScheduled());
    }

    @Test
    void testShutdownInterruptsRunningVirtualThreads() throws InterruptedException {
        final CountDownLatch invocationStarted = new CountDownLatch(1);
        final CountDownLatch releaseInvocation = new CountDownLatch(1);
        final Connectable connectable = createMockedConnectable(1, SchedulingStrategy.TIMER_DRIVEN, new AtomicInteger(), new CountDownLatch(0));
        doAnswer(invocation -> {
            invocationStarted.countDown();
            try {
                releaseInvocation.await();
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            return null;
        }).when(connectable).onTrigger(any(), any());

        final LifecycleState lifecycleState = new LifecycleState(COMPONENT_ID);
        scheduleConnectable(connectable, lifecycleState);

        assertTrue(invocationStarted.await(5, TimeUnit.SECONDS));
        assertTrue(agent.getRunningThreadCount() >= 1);

        agent.shutdown();

        assertTrue(agent.awaitTermination(5, TimeUnit.SECONDS));
        assertEquals(0, agent.getRunningThreadCount());
        assertTrue(agent.isShutdown());
    }

    @Test
    void testShutdownPreventsInvocationAfterInterruptedProcessorReturns() throws InterruptedException {
        final AtomicInteger invocationCount = new AtomicInteger();
        final CountDownLatch firstInvocationStarted = new CountDownLatch(1);
        final CountDownLatch releaseFirstInvocation = new CountDownLatch(1);
        final CountDownLatch secondInvocationStarted = new CountDownLatch(1);
        final Connectable connectable = createMockedConnectable(1, SchedulingStrategy.TIMER_DRIVEN, new AtomicInteger(), new CountDownLatch(0));
        doAnswer(invocation -> {
            final int currentInvocation = invocationCount.incrementAndGet();
            if (currentInvocation == 1) {
                firstInvocationStarted.countDown();
                try {
                    releaseFirstInvocation.await();
                } catch (final InterruptedException ignored) {
                }
            } else if (currentInvocation == 2) {
                secondInvocationStarted.countDown();
            }
            return null;
        }).when(connectable).onTrigger(any(), any());

        final LifecycleState lifecycleState = new LifecycleState(COMPONENT_ID);
        scheduleConnectable(connectable, lifecycleState);
        assertTrue(firstInvocationStarted.await(2, TimeUnit.SECONDS));

        agent.shutdown();

        assertFalse(secondInvocationStarted.await(500, TimeUnit.MILLISECONDS));
        assertTrue(agent.awaitTermination(5, TimeUnit.SECONDS));
    }

    @Test
    void testGracefulShutdownWaitsForRunningInvocation() throws InterruptedException {
        final CountDownLatch invocationStarted = new CountDownLatch(1);
        final CountDownLatch releaseInvocation = new CountDownLatch(1);
        final CountDownLatch secondInvocationStarted = new CountDownLatch(1);
        final AtomicInteger invocationCount = new AtomicInteger();
        final Connectable connectable = createMockedConnectable(1, SchedulingStrategy.TIMER_DRIVEN, new AtomicInteger(), new CountDownLatch(0));
        doAnswer(invocation -> {
            if (invocationCount.incrementAndGet() == 1) {
                invocationStarted.countDown();
                releaseInvocation.await();
            } else {
                secondInvocationStarted.countDown();
            }
            return null;
        }).when(connectable).onTrigger(any(), any());

        final LifecycleState lifecycleState = new LifecycleState(COMPONENT_ID);
        scheduleConnectable(connectable, lifecycleState);
        assertTrue(invocationStarted.await(2, TimeUnit.SECONDS));

        agent.shutdownGracefully();

        assertFalse(agent.awaitTermination(100, TimeUnit.MILLISECONDS));
        releaseInvocation.countDown();
        assertTrue(agent.awaitTermination(2, TimeUnit.SECONDS));
        assertFalse(secondInvocationStarted.await(100, TimeUnit.MILLISECONDS));
    }

    @Test
    void testScheduleAfterShutdownFailsFast() {
        agent.shutdown();

        final Connectable connectable = createMockedConnectable(1, SchedulingStrategy.TIMER_DRIVEN, new AtomicInteger(), new CountDownLatch(0));
        final LifecycleState lifecycleState = new LifecycleState(COMPONENT_ID);

        assertThrows(IllegalStateException.class, () -> agent.schedule(connectable, lifecycleState));
        assertFalse(lifecycleState.isScheduled());
    }

    @Test
    void testShutdownIsIdempotent() {
        agent.shutdown();
        agent.shutdown();
        assertTrue(agent.isShutdown());
        assertEquals(0, agent.getRunningThreadCount());
    }
}
