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
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
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
    void tearDown() {
        agent.shutdown();
    }

    @Test
    void testSetMaxThreadCountAdjustsSemaphore() {
        assertEquals(MAX_THREADS, agent.getGlobalSemaphore().getMaxPermits());
        agent.setMaxThreadCount(20);
        assertEquals(20, agent.getGlobalSemaphore().getMaxPermits());
        agent.setMaxThreadCount(5);
        assertEquals(5, agent.getGlobalSemaphore().getMaxPermits());
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

        assertThrows(IllegalStateException.class, () -> agent.incrementMaxThreadCount(-1000),
                "Removing more permits than exist must fail");
    }

    @Test
    void testAdministrativeYieldDuration() {
        agent.setAdministrativeYieldDuration("5 sec");
        assertEquals("5 sec", agent.getAdministrativeYieldDuration());
        assertEquals(5000L, agent.getAdministrativeYieldDuration(TimeUnit.MILLISECONDS));
    }

    @Test
    void testScheduleSpawnsThreadsThatInvoke() throws InterruptedException {
        final int concurrentTasks = 3;
        final AtomicInteger invocationCount = new AtomicInteger(0);
        final CountDownLatch allTasksInvoked = new CountDownLatch(concurrentTasks);

        final Connectable connectable = createMockedConnectable(concurrentTasks, SchedulingStrategy.TIMER_DRIVEN, invocationCount, allTasksInvoked);
        final LifecycleState lifecycleState = new LifecycleState(COMPONENT_ID);

        scheduleConnectable(connectable, lifecycleState);

        assertTrue(allTasksInvoked.await(5, TimeUnit.SECONDS),
                "Expected " + concurrentTasks + " threads to invoke, but only " + (concurrentTasks - allTasksInvoked.getCount()) + " did");
        assertTrue(invocationCount.get() >= concurrentTasks,
                "Expected at least " + concurrentTasks + " invocations but got " + invocationCount.get());

        unscheduleConnectable(connectable, lifecycleState);
        Thread.sleep(100);
    }

    @Test
    void testUnscheduleExitsQuickly() throws InterruptedException {
        final Connectable connectable = createMockedConnectable(2, SchedulingStrategy.TIMER_DRIVEN, new AtomicInteger(), new CountDownLatch(0));
        final LifecycleState lifecycleState = new LifecycleState(COMPONENT_ID);

        scheduleConnectable(connectable, lifecycleState);
        Thread.sleep(300);

        unscheduleConnectable(connectable, lifecycleState);
        Thread.sleep(250);

        assertEquals(0, lifecycleState.getActiveThreadCount(),
                "Active threads should be 0 after unschedule settles");
    }

    @Test
    void testSemaphoreLimitsConcurrentInvocations() throws InterruptedException {
        final int semaphorePermits = 2;
        final int totalThreads = 5;
        agent.setMaxThreadCount(semaphorePermits);

        final AtomicInteger concurrentCount = new AtomicInteger(0);
        final AtomicInteger maxObservedConcurrency = new AtomicInteger(0);
        final CountDownLatch allDone = new CountDownLatch(totalThreads);

        for (int i = 0; i < totalThreads; i++) {
            Thread.ofVirtual().start(() -> {
                try {
                    agent.getGlobalSemaphore().acquire();
                    try {
                        final int current = concurrentCount.incrementAndGet();
                        maxObservedConcurrency.accumulateAndGet(current, Math::max);
                        Thread.sleep(50);
                    } finally {
                        concurrentCount.decrementAndGet();
                        agent.getGlobalSemaphore().release();
                    }
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    allDone.countDown();
                }
            });
        }

        assertTrue(allDone.await(5, TimeUnit.SECONDS));
        assertTrue(maxObservedConcurrency.get() <= semaphorePermits,
                "Max concurrency " + maxObservedConcurrency.get() + " exceeded semaphore permits " + semaphorePermits);
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
    void testActiveThreadCountReflectsSemaphoreUsage() throws InterruptedException {
        agent.setMaxThreadCount(4);
        assertEquals(0, agent.getActiveThreadCount());

        final CountDownLatch acquired = new CountDownLatch(2);
        final CountDownLatch release = new CountDownLatch(1);
        for (int i = 0; i < 2; i++) {
            Thread.ofVirtual().start(() -> {
                try {
                    agent.getGlobalSemaphore().acquire();
                    acquired.countDown();
                    release.await();
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    agent.getGlobalSemaphore().release();
                }
            });
        }

        assertTrue(acquired.await(5, TimeUnit.SECONDS));
        assertEquals(2, agent.getActiveThreadCount());
        release.countDown();
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

        Thread.sleep(100);
        assertEquals(0, invocationCount.get(), "No invocation should have run while the permit was held elsewhere");

        final long unscheduleStart = System.nanoTime();
        unscheduleConnectable(connectable, lifecycleState);
        Thread.sleep(150);
        final long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - unscheduleStart);

        releaseHeldPermit.countDown();
        permitHolder.join(1_000L);
        Thread.sleep(100);

        assertEquals(0, invocationCount.get(),
                "Scheduling loop should have exited after unschedule even though the permit was held; subsequent release must not trigger an invocation");
        assertTrue(elapsedMillis < 1_000L, "Unschedule did not propagate within 1s; measured " + elapsedMillis + "ms");
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
    void testSchedulingLoopSurvivesThrowableEscapingInvoke() throws InterruptedException {
        final AtomicInteger invocationCount = new AtomicInteger(0);
        final Connectable connectable = createMockedConnectable(1, SchedulingStrategy.TIMER_DRIVEN, invocationCount, new CountDownLatch(0));

        final AtomicInteger schedulingPeriodCalls = new AtomicInteger(0);
        when(connectable.getSchedulingPeriod(TimeUnit.NANOSECONDS)).thenAnswer(invocation -> {
            final int call = schedulingPeriodCalls.incrementAndGet();
            if (call <= 2) {
                throw new OutOfMemoryError("Simulated Error " + call);
            }
            return TimeUnit.MILLISECONDS.toNanos(10L);
        });

        final LifecycleState lifecycleState = new LifecycleState(COMPONENT_ID);
        scheduleConnectable(connectable, lifecycleState);

        final long deadline = System.currentTimeMillis() + 5_000L;
        while (invocationCount.get() < 5 && System.currentTimeMillis() < deadline) {
            Thread.sleep(50L);
        }

        unscheduleConnectable(connectable, lifecycleState);
        Thread.sleep(100L);

        assertTrue(invocationCount.get() >= 5,
                "Scheduling loop should have continued past the Errors thrown during scheduling; observed only " + invocationCount.get() + " invocations");
        assertEquals(MAX_THREADS, agent.getGlobalSemaphore().availablePermits(),
                "All permits should be returned to the semaphore even after Throwables escape the loop body");
    }

    @Test
    void testInvocationExceptionStillReleasesPermit() throws InterruptedException {
        agent.setMaxThreadCount(2);
        final Connectable connectable = createMockedConnectable(1, SchedulingStrategy.TIMER_DRIVEN, new AtomicInteger(), new CountDownLatch(0));
        final AtomicInteger invocationCount = new AtomicInteger(0);
        doAnswer(invocation -> {
            final int count = invocationCount.incrementAndGet();
            if (count <= 3) {
                throw new RuntimeException("Simulated failure " + count);
            }
            return null;
        }).when(connectable).onTrigger(any(), any());

        final LifecycleState lifecycleState = new LifecycleState(COMPONENT_ID);
        scheduleConnectable(connectable, lifecycleState);

        Thread.sleep(500);
        assertEquals(2, agent.getGlobalSemaphore().availablePermits(),
                "All permits should be returned to the semaphore after invocation exceptions");

        unscheduleConnectable(connectable, lifecycleState);
        Thread.sleep(100);
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
        Thread.sleep(100);
    }

    @Test
    void testCronDrivenReportingTaskIsScheduled() throws InterruptedException {
        final AtomicInteger invocationCount = new AtomicInteger(0);
        final ReportingTask reportingTask = mock(ReportingTask.class);
        doAnswer(invocation -> {
            invocationCount.incrementAndGet();
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
        // Mirror the real framework flow: the scheduler transitions the lifecycle state to scheduled before the
        // agent is invoked. A prior version of the agent threw IllegalStateException here, which silently broke all
        // reporting-task scheduling in production because StandardProcessScheduler.schedule(ReportingTaskNode, ...)
        // calls setScheduled(true) synchronously before handing control to the agent.
        lifecycleState.setScheduled(true);
        agent.schedule(taskNode, lifecycleState);

        try {
            final long deadline = System.currentTimeMillis() + 3_000L;
            while (invocationCount.get() == 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(50L);
            }

            assertTrue(invocationCount.get() >= 1,
                    "CRON-driven reporting task should have been invoked at least once; got " + invocationCount.get());
        } finally {
            lifecycleState.setScheduled(false);
            agent.unschedule(taskNode, lifecycleState);
            Thread.sleep(100L);
        }
    }

    @Test
    void testAgentAcceptsAlreadyScheduledLifecycleStateForReportingTask() throws InterruptedException {
        final AtomicInteger invocationCount = new AtomicInteger(0);
        final ReportingTask reportingTask = mock(ReportingTask.class);
        doAnswer(invocation -> {
            invocationCount.incrementAndGet();
            return null;
        }).when(reportingTask).onTrigger(any());

        final ReportingTaskNode taskNode = mock(ReportingTaskNode.class);
        when(taskNode.getSchedulingStrategy()).thenReturn(SchedulingStrategy.TIMER_DRIVEN);
        when(taskNode.getSchedulingPeriod(TimeUnit.NANOSECONDS)).thenReturn(TimeUnit.MILLISECONDS.toNanos(50L));
        when(taskNode.getReportingTask()).thenReturn(reportingTask);
        when(taskNode.getReportingContext()).thenReturn(mock(ReportingContext.class));
        when(taskNode.getIdentifier()).thenReturn(COMPONENT_ID);
        when(taskNode.getName()).thenReturn("TestReporter");
        when(flowController.getExtensionManager()).thenReturn(extensionManager);

        final LifecycleState lifecycleState = new LifecycleState(COMPONENT_ID);

        lifecycleState.setScheduled(true);
        agent.schedule(taskNode, lifecycleState);

        try {
            final long deadline = System.currentTimeMillis() + 3_000L;
            while (invocationCount.get() == 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(25L);
            }
            assertTrue(invocationCount.get() >= 1,
                    "Agent must accept a LifecycleState that is already marked scheduled (the real scheduler does that before "
                            + "handing control to the agent); observed " + invocationCount.get() + " invocations");
        } finally {
            lifecycleState.setScheduled(false);
            agent.unschedule(taskNode, lifecycleState);
            Thread.sleep(100L);
        }
    }

    @Test
    void testLastStopTimeAdvancesOnEveryStop() {
        final LifecycleState lifecycleState = new LifecycleState(COMPONENT_ID);
        lifecycleState.setScheduled(true);
        final long initialStopTime = lifecycleState.getLastStopTime();

        lifecycleState.setScheduled(false);
        final long firstStopTime = lifecycleState.getLastStopTime();
        assertTrue(firstStopTime > initialStopTime, "lastStopTime must advance on every stop; was " + initialStopTime + ", now " + firstStopTime);

        lifecycleState.setScheduled(true);
        assertEquals(firstStopTime, lifecycleState.getLastStopTime(), "lastStopTime must not change when (re)starting");

        lifecycleState.setScheduled(false);
        final long secondStopTime = lifecycleState.getLastStopTime();
        assertTrue(secondStopTime > firstStopTime,
                "lastStopTime must strictly advance even for rapid stop/start/stop; was " + firstStopTime + ", now " + secondStopTime);
    }

    @Test
    void testRapidStopStartDoesNotLeakSchedulingThreads() throws InterruptedException {
        final Set<Thread> observedThreads = ConcurrentHashMap.newKeySet();
        final AtomicInteger invocationCount = new AtomicInteger(0);
        final Connectable connectable = createMockedConnectable(1, SchedulingStrategy.TIMER_DRIVEN, invocationCount, new CountDownLatch(0));
        when(connectable.getSchedulingPeriod(TimeUnit.NANOSECONDS)).thenReturn(TimeUnit.MILLISECONDS.toNanos(500L));
        doAnswer(invocation -> {
            observedThreads.add(Thread.currentThread());
            invocationCount.incrementAndGet();
            return null;
        }).when(connectable).onTrigger(any(), any());

        final LifecycleState lifecycleState = new LifecycleState(COMPONENT_ID);
        scheduleConnectable(connectable, lifecycleState);

        try {
            final long deadline = System.currentTimeMillis() + 2_000L;
            while (observedThreads.isEmpty() && System.currentTimeMillis() < deadline) {
                Thread.sleep(25L);
            }
            assertFalse(observedThreads.isEmpty(), "First schedule should have produced at least one invocation");
            final Set<Thread> firstCycleThreads = new HashSet<>(observedThreads);

            unscheduleConnectable(connectable, lifecycleState);
            scheduleConnectable(connectable, lifecycleState);

            final long leakDeadline = System.currentTimeMillis() + 2_000L;
            while (System.currentTimeMillis() < leakDeadline) {
                boolean allExited = true;
                for (final Thread original : firstCycleThreads) {
                    if (original.isAlive()) {
                        allExited = false;
                        break;
                    }
                }
                if (allExited) {
                    return;
                }
                Thread.sleep(50L);
            }

            for (final Thread original : firstCycleThreads) {
                if (original.isAlive()) {
                    fail("Scheduling thread " + original + " from the first schedule should have exited after unschedule, "
                            + "but remained alive after a subsequent schedule. This indicates a leaked scheduling loop.");
                }
            }
        } finally {
            unscheduleConnectable(connectable, lifecycleState);
            Thread.sleep(200L);
        }
    }

    /**
     * Drives the agent through the same lifecycle contract enforced by {@link StandardProcessScheduler}: the framework
     * transitions the {@link LifecycleState} to scheduled before invoking the agent, and the agent is a pure consumer
     * of that state. Tests must therefore follow the same contract rather than relying on the agent to mutate
     * {@code isScheduled} itself.
     */
    private void scheduleConnectable(final Connectable connectable, final LifecycleState lifecycleState) {
        lifecycleState.setScheduled(true);
        agent.schedule(connectable, lifecycleState);
    }

    private void unscheduleConnectable(final Connectable connectable, final LifecycleState lifecycleState) {
        lifecycleState.setScheduled(false);
        agent.unschedule(connectable, lifecycleState);
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
}
