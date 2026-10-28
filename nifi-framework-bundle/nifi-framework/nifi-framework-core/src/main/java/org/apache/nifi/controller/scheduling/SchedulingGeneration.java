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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tracks one period during which a component is scheduled to run, from the time it is scheduled until it is unscheduled. Threads started
 * for the component belong to the generation, and stopping the generation tells them to stop. For a Processor that uses automatic
 * scheduling, the generation also holds the Processor's automatic scheduling state, which lasts only as long as the generation.
 */
final class SchedulingGeneration {
    private final CountDownLatch stopSignal = new CountDownLatch(1);
    private final Set<Thread> threads = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean interruptRequested = new AtomicBoolean();
    private volatile ProcessorAutoSchedulingState autoSchedulingState;

    void setAutoSchedulingState(final ProcessorAutoSchedulingState autoSchedulingState) {
        this.autoSchedulingState = autoSchedulingState;
    }

    ProcessorAutoSchedulingState getAutoSchedulingState() {
        return autoSchedulingState;
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
        final ProcessorAutoSchedulingState state = autoSchedulingState;
        if (state != null) {
            state.getWaitingWorkers().stop();
        }

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
