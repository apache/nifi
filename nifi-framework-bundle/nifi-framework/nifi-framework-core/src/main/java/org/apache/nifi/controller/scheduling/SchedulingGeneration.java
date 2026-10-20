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

import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.queue.QueueSchedulingRegistration;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Tracks one period during which a component is scheduled to run, from the time it is scheduled until it is unscheduled. Threads started
 * for the component belong to the generation, and stopping the generation tells them to stop. For a Processor that uses automatic scheduling,
 * the generation also wakes waiting workers when the Processor's queues or concurrent task slots change.
 */
final class SchedulingGeneration {
    private final CountDownLatch stopSignal = new CountDownLatch(1);
    private final Set<Thread> threads = ConcurrentHashMap.newKeySet();
    private final Set<WaitingProcessorTask> waiters = ConcurrentHashMap.newKeySet();
    private final Set<WaitingProcessorTask> taskSlotWaiters = ConcurrentHashMap.newKeySet();
    private final Set<FlowFileQueue> queues = ConcurrentHashMap.newKeySet();
    private final List<QueueSchedulingRegistration> queueRegistrations = new ArrayList<>();
    private final AtomicBoolean interruptRequested = new AtomicBoolean();
    private final AtomicLong changeSequence = new AtomicLong();
    private final AtomicLong taskSlotUpdateCount = new AtomicLong();
    private volatile ProcessorAutoSchedulingState autoSchedulingState;

    void setAutoSchedulingState(final ProcessorAutoSchedulingState autoSchedulingState) {
        this.autoSchedulingState = autoSchedulingState;
    }

    ProcessorAutoSchedulingState getAutoSchedulingState() {
        return autoSchedulingState;
    }

    void addQueue(final FlowFileQueue queue) {
        queues.add(queue);
    }

    synchronized void addQueueRegistration(final QueueSchedulingRegistration registration) {
        if (isStopped()) {
            registration.close();
        } else {
            queueRegistrations.add(registration);
        }
    }

    synchronized void clearQueueRegistrations() {
        for (final QueueSchedulingRegistration registration : queueRegistrations) {
            registration.close();
        }

        queueRegistrations.clear();
        queues.clear();
    }

    /**
     * @return the earliest time at which the FlowFile at the front of one of the Processor's queues becomes available, or
     * {@link Instant#EPOCH} if none of the queues knows of such a time
     */
    Instant getNextQueueDeadline() {
        Instant earliestDeadline = Instant.EPOCH;
        for (final FlowFileQueue queue : queues) {
            final Instant deadline = queue.getNextFlowFileAvailabilityTime();
            if (!deadline.isAfter(Instant.EPOCH)) {
                continue;
            }

            if (earliestDeadline.equals(Instant.EPOCH) || deadline.isBefore(earliestDeadline)) {
                earliestDeadline = deadline;
            }
        }

        return earliestDeadline;
    }

    long getChangeSequence() {
        return changeSequence.get();
    }

    int getWaiterCount() {
        return waiters.size();
    }

    /**
     * @return a count that increases each time a concurrent task slot may have become available: a slot is released, a worker stops
     * or is retired, the scheduling settings change, or scheduling stops
     */
    long getTaskSlotUpdateCount() {
        return taskSlotUpdateCount.get();
    }

    int getTaskSlotWaiterCount() {
        return taskSlotWaiters.size();
    }

    void signalChange() {
        changeSequence.incrementAndGet();
        final ProcessorAutoSchedulingState state = autoSchedulingState;
        final int wakeLimit = state == null ? waiters.size() : state.getConcurrentTasks();
        int awakened = 0;
        for (final WaitingProcessorTask waiter : waiters) {
            if (waiter.worker().isRetired()) {
                waiters.remove(waiter);
                continue;
            }

            LockSupport.unpark(waiter.thread());
            if (++awakened >= wakeLimit) {
                break;
            }
        }
    }

    void awaitChange(final long expectedSequence, final long delayNanos, final ProcessorTaskWorker worker) {
        final Thread currentThread = Thread.currentThread();
        final WaitingProcessorTask waiter = new WaitingProcessorTask(currentThread, worker);
        waiters.add(waiter);
        try {
            if (!isStopped() && !worker.isRetired() && changeSequence.get() == expectedSequence) {
                LockSupport.parkNanos(Math.max(1L, delayNanos));
            }
        } finally {
            waiters.remove(waiter);
        }
    }

    void signalTaskSlotUpdate() {
        taskSlotUpdateCount.incrementAndGet();
        // Retired waiters are woken as well so that they observe retirement and exit.
        for (final WaitingProcessorTask waiter : taskSlotWaiters) {
            LockSupport.unpark(waiter.thread());
        }
    }

    /**
     * Waits until the task slot update count differs from the given count, the worker is retired, or scheduling stops.
     */
    void waitForTaskSlotUpdate(final long previousUpdateCount, final ProcessorTaskWorker worker) {
        final Thread currentThread = Thread.currentThread();
        final WaitingProcessorTask waiter = new WaitingProcessorTask(currentThread, worker);
        taskSlotWaiters.add(waiter);
        try {
            while (!isStopped() && !worker.isRetired() && taskSlotUpdateCount.get() == previousUpdateCount) {
                LockSupport.park();
            }
        } finally {
            taskSlotWaiters.remove(waiter);
        }
    }

    void addThread(final Thread thread) {
        threads.add(thread);

        if (interruptRequested.get()) {
            thread.interrupt();
        }
    }

    void removeThread(final Thread thread) {
        threads.remove(thread);
    }

    void stop(final boolean interrupt) {
        if (interrupt) {
            interruptRequested.set(true);
        }

        stopSignal.countDown();
        signalChange();
        signalTaskSlotUpdate();
        clearQueueRegistrations();

        if (interruptRequested.get()) {
            for (final Thread thread : threads) {
                thread.interrupt();
            }
        }
    }

    boolean isStopped() {
        return stopSignal.getCount() == 0L;
    }

    boolean isRunning() {
        return !isStopped();
    }

    void awaitStop(final long timeout, final TimeUnit timeUnit) throws InterruptedException {
        stopSignal.await(timeout, timeUnit);
    }
}
