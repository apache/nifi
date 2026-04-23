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

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * A semaphore wrapper that supports dynamically adjusting the maximum number of permits.
 * Uses fair ordering so that any virtual thread that has been waiting the longest is the
 * next to acquire a permit; this prevents any particular processor's scheduling loop from
 * being starved under heavy contention.
 */
public class DynamicSemaphore {

    private final ResizableSemaphore semaphore;
    private volatile int maxPermits;

    public DynamicSemaphore(final int permits) {
        if (permits < 1) {
            throw new IllegalArgumentException("Permits must be at least 1");
        }
        this.maxPermits = permits;
        this.semaphore = new ResizableSemaphore(permits);
    }

    public void acquire() throws InterruptedException {
        semaphore.acquire();
    }

    public boolean tryAcquire(final long timeout, final TimeUnit timeUnit) throws InterruptedException {
        return semaphore.tryAcquire(timeout, timeUnit);
    }

    public void release() {
        semaphore.release();
    }

    /**
     * Adjusts the maximum number of permits to the specified count. If the new maximum is
     * greater than the current maximum, additional permits are released. If it is less than
     * the current maximum, permits are reduced: threads currently holding permits are not
     * interrupted, and subsequent releases will not top up available permits until the
     * number of permits in circulation falls below the new cap.
     *
     * @param newMaxPermits the desired maximum number of permits (must be at least 1)
     */
    public synchronized void setMaxPermits(final int newMaxPermits) {
        if (newMaxPermits < 1) {
            throw new IllegalArgumentException("Max permits must be at least 1");
        }

        final int delta = newMaxPermits - this.maxPermits;
        this.maxPermits = newMaxPermits;
        if (delta > 0) {
            semaphore.release(delta);
        } else if (delta < 0) {
            semaphore.reducePermits(-delta);
        }
    }

    public int getMaxPermits() {
        return maxPermits;
    }

    public int availablePermits() {
        return semaphore.availablePermits();
    }

    /**
     * Returns the number of permits currently in use, computed atomically against any concurrent
     * call to {@link #setMaxPermits(int)}. A non-atomic {@code getMaxPermits() - availablePermits()}
     * outside this class can observe a transient inconsistency if a resize is in progress between
     * the two reads, which is undesirable for metrics that feed cluster heartbeats.
     *
     * @return the number of permits that have been acquired but not yet released. The returned value
     *         is a best-effort snapshot because permits may be acquired or released by other threads
     *         before the caller can act on it, but it is consistent with a single point in time.
     */
    public synchronized int getInUsePermits() {
        return maxPermits - semaphore.availablePermits();
    }

    /**
     * Extends {@link Semaphore} in order to expose the protected {@link #reducePermits(int)}
     * method, which is needed in order to dynamically shrink the pool of available permits.
     */
    private static class ResizableSemaphore extends Semaphore {

        ResizableSemaphore(final int permits) {
            super(permits, true);
        }

        @Override
        protected void reducePermits(final int reduction) {
            super.reducePermits(reduction);
        }
    }
}
