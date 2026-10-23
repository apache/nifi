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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.IntSupplier;

/**
 * Lets the workers of a Processor that uses automatic scheduling wait while there is nothing for them to do, and wakes them when that
 * may have changed. A worker waits for one of two things: a change that may make the Processor ready to run, such as a FlowFile arriving
 * in one of its queues or its scheduling settings changing, or one of the Processor's concurrent task slots becoming available.
 */
final class WaitingWorkers {
    private final SchedulingGeneration generation;
    private final IntSupplier wakeLimit;
    private final Set<WaitingProcessorTask> changeWaiters = ConcurrentHashMap.newKeySet();
    private final Set<WaitingProcessorTask> taskSlotWaiters = ConcurrentHashMap.newKeySet();
    private final Set<FlowFileQueue> queues = ConcurrentHashMap.newKeySet();
    private final List<QueueSchedulingRegistration> queueRegistrations = new ArrayList<>();
    private final AtomicLong changeSequence = new AtomicLong();
    private final AtomicLong taskSlotUpdateCount = new AtomicLong();

    /**
     * @param generation the scheduling generation that the workers belong to; workers stop waiting once it is stopped
     * @param wakeLimit returns the most workers to wake for a single change
     */
    WaitingWorkers(final SchedulingGeneration generation, final IntSupplier wakeLimit) {
        this.generation = generation;
        this.wakeLimit = wakeLimit;
    }

    /**
     * Wakes waiting workers whenever the given queue reports a change, until the listeners are removed or scheduling stops.
     */
    synchronized void addQueueListener(final FlowFileQueue queue) {
        final QueueSchedulingRegistration registration = queue.addSchedulingListener(this::signalChange);
        if (generation.isStopped()) {
            registration.close();
            return;
        }

        queues.add(queue);
        queueRegistrations.add(registration);
    }

    synchronized void removeQueueListeners() {
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

    int getChangeWaiterCount() {
        return changeWaiters.size();
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

    /**
     * Wakes workers that are waiting for a change, up to the wake limit.
     */
    void signalChange() {
        changeSequence.incrementAndGet();
        final int limit = wakeLimit.getAsInt();
        int awakened = 0;
        for (final WaitingProcessorTask waiter : changeWaiters) {
            if (waiter.worker().isRetired()) {
                changeWaiters.remove(waiter);
                continue;
            }

            LockSupport.unpark(waiter.thread());
            if (++awakened >= limit) {
                break;
            }
        }
    }

    /**
     * Waits up to the given delay unless a change has been signaled since the given change sequence was read, the worker is retired,
     * or scheduling stops.
     */
    void waitForChange(final long expectedSequence, final long delayNanos, final ProcessorTaskWorker worker) {
        final WaitingProcessorTask waiter = new WaitingProcessorTask(Thread.currentThread(), worker);
        changeWaiters.add(waiter);
        try {
            if (!generation.isStopped() && !worker.isRetired() && changeSequence.get() == expectedSequence) {
                LockSupport.parkNanos(Math.max(1L, delayNanos));
            }
        } finally {
            changeWaiters.remove(waiter);
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
        final WaitingProcessorTask waiter = new WaitingProcessorTask(Thread.currentThread(), worker);
        taskSlotWaiters.add(waiter);
        try {
            while (!generation.isStopped() && !worker.isRetired() && taskSlotUpdateCount.get() == previousUpdateCount) {
                LockSupport.park();
            }
        } finally {
            taskSlotWaiters.remove(waiter);
        }
    }

    /**
     * Wakes every waiting worker and stops listening to the Processor's queues. Called after the scheduling generation is stopped.
     */
    void stop() {
        signalChange();
        signalTaskSlotUpdate();
        removeQueueListeners();
    }
}
