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

import org.apache.nifi.controller.tasks.ConnectableTask;
import org.apache.nifi.controller.tasks.InvocationOutcome;
import org.apache.nifi.controller.tasks.InvocationResult;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Limits how often a source Processor that uses automatic scheduling is checked for new data after it finds none. A source Processor
 * has no queued FlowFiles to tell the framework that it has work, so the only way to find out is to invoke it. Each time an invocation
 * finds no new data, the wait before the next check doubles, up to a maximum, and only one concurrent task at a time may perform the
 * check. Once an invocation finds new data, or FlowFiles are queued for the Processor, it may be invoked freely again.
 * <p>
 * Each invocation of the Processor must be wrapped as follows, so that a check that is started is always finished:
 * <pre>
 * if (sourceWorkCheck.tryStart()) {
 *     try {
 *         // invoke the Processor
 *     } finally {
 *         sourceWorkCheck.finish();
 *     }
 * }
 * </pre>
 * For a Processor that is not a source Processor, or a source Processor that recently found new data, {@link #tryStart()} always returns
 * <code>true</code> and {@link #finish()} does nothing.
 */
final class SourceWorkCheck {
    private static final long RECENT_DATA_NANOS = TimeUnit.SECONDS.toNanos(2L);

    private final ConnectableTask connectableTask;
    private final boolean sourceProcessor;
    private final long baseDelayNanos;
    private final long maximumDelayNanos;
    private final Runnable readinessListener;
    private final AtomicReference<Thread> checkingThread = new AtomicReference<>();

    private volatile long delayNanos;
    private volatile long nextCheckNanos;
    private volatile long lastDataFoundNanos;

    /**
     * @param noWorkYieldNanos how long a Processor waits after an invocation that found no work, used as the shortest wait between checks
     * @param readinessListener called when the Processor may have become ready to be invoked again, so that waiting workers can be woken
     */
    SourceWorkCheck(final ConnectableTask connectableTask, final boolean sourceProcessor, final long noWorkYieldNanos, final Runnable readinessListener) {
        this.connectableTask = connectableTask;
        this.sourceProcessor = sourceProcessor;
        this.baseDelayNanos = Math.max(noWorkYieldNanos, TimeUnit.MILLISECONDS.toNanos(1L));
        this.maximumDelayNanos = Math.max(baseDelayNanos, TimeUnit.MILLISECONDS.toNanos(100L));
        this.readinessListener = readinessListener;
    }

    /**
     * Returns whether {@link #tryStart()} would currently succeed, without starting a check. Workers use this to decide whether to wait
     * instead of taking a global permit.
     *
     * @return <code>true</code> unless a check is required and either it is not yet time for the next check or another concurrent task is
     * already performing it
     */
    boolean isInvocationAllowed() {
        if (!isCheckRequired()) {
            return true;
        }

        if (checkingThread.get() != null) {
            return false;
        }

        return System.nanoTime() >= nextCheckNanos;
    }

    /**
     * Starts a check for new data if one is required. If this returns <code>true</code>, the caller must invoke the Processor and then
     * call {@link #finish()} from the same thread.
     *
     * @return <code>true</code> if the Processor may be invoked now, <code>false</code> if a check is required and either it is not yet time
     * for the next check or another concurrent task is already performing it
     */
    boolean tryStart() {
        if (!isCheckRequired()) {
            return true;
        }

        if (System.nanoTime() < nextCheckNanos) {
            return false;
        }

        return checkingThread.compareAndSet(null, Thread.currentThread());
    }

    /**
     * Finishes the check started by the current thread, if any, so that another concurrent task may perform the next one.
     */
    void finish() {
        if (checkingThread.compareAndSet(Thread.currentThread(), null)) {
            readinessListener.run();
        }
    }

    /**
     * @return how long until the next check is allowed, or zero if it is already allowed
     */
    long getDelayUntilNextCheckNanos() {
        return Math.max(0L, nextCheckNanos - System.nanoTime());
    }

    /**
     * Records the result of an invocation. An invocation that finds new data ends the waiting between checks. An invocation that finds
     * no new data doubles the wait before the next check, but only if no other concurrent task is invoking the Processor and no
     * FlowFiles are queued for it.
     *
     * @param activeInvocations the number of concurrent tasks currently invoking the Processor, including the one that produced the result
     */
    void recordInvocationResult(final InvocationResult result, final int activeInvocations) {
        if (!sourceProcessor) {
            return;
        }

        if (result.getOutcome() == InvocationOutcome.INVOKED_WITH_ACTIVITY) {
            delayNanos = 0L;
            nextCheckNanos = 0L;
            lastDataFoundNanos = System.nanoTime();
            readinessListener.run();
            return;
        }

        if (result.getOutcome() != InvocationOutcome.INVOKED_WITHOUT_ACTIVITY) {
            return;
        }

        // Another concurrent task may still find new data, and queued FlowFiles are work to do, so neither counts as coming up empty.
        if (activeInvocations > 1 || connectableTask.hasLocallyConsumableInput()) {
            return;
        }

        delayNanos = delayNanos == 0L ? baseDelayNanos : Math.min(maximumDelayNanos, delayNanos * 2L);
        nextCheckNanos = System.nanoTime() + delayNanos;
    }

    /**
     * @param activeInvocations the number of concurrent tasks currently invoking the Processor
     * @return <code>true</code> if this is a source Processor that has not come up empty since it last found new data, and it is either
     * being invoked now or found new data within the last two seconds
     */
    boolean hasRecentlyFoundData(final long nowNanos, final int activeInvocations) {
        if (!sourceProcessor || delayNanos > 0L) {
            return false;
        }

        if (activeInvocations > 0) {
            return true;
        }

        if (lastDataFoundNanos == 0L) {
            return false;
        }

        return nowNanos - lastDataFoundNanos < RECENT_DATA_NANOS;
    }

    /**
     * @return <code>true</code> if this is a source Processor that recently found no new data and has no FlowFiles queued for it
     */
    private boolean isCheckRequired() {
        if (!sourceProcessor || delayNanos == 0L) {
            return false;
        }

        return !connectableTask.hasLocallyConsumableInput();
    }
}
