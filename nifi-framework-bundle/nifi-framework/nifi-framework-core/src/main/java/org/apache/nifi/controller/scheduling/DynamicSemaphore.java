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
 * Semaphore with a configurable maximum permit count and fair waiter ordering.
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
     * Adjusts the maximum permit count without interrupting current permit holders.
     *
     * @param newMaxPermits maximum permits, at least one
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
     * Returns the number of acquired permits. The result can exceed the configured maximum
     * while a reduced permit limit waits for current holders to release permits.
     *
     * @return acquired permit count
     */
    public synchronized int getInUsePermits() {
        return maxPermits - semaphore.availablePermits();
    }

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
