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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.nifi.annotation.lifecycle.OnStopped;

public class ScheduleState {

    private final AtomicInteger activeThreadCount = new AtomicInteger(0);
    private final AtomicBoolean scheduled = new AtomicBoolean(false);
    private final Set<ScheduledFuture<?>> futures = new HashSet<>();
    private final AtomicBoolean mustCallOnStoppedMethods = new AtomicBoolean(false);
    private volatile long lastStopTime = -1;

    public int incrementActiveThreadCount() {
        return activeThreadCount.incrementAndGet();
    }

    public int decrementActiveThreadCount() {
        return activeThreadCount.decrementAndGet();
    }

    public int getActiveThreadCount() {
        return activeThreadCount.get();
    }

    public boolean isScheduled() {
        return scheduled.get();
    }

    void setScheduled(final boolean scheduled) {
        this.scheduled.set(scheduled);
        mustCallOnStoppedMethods.set(true);

        if (!scheduled) {
            lastStopTime = System.currentTimeMillis();
        }
    }

    public long getLastStopTime() {
        return lastStopTime;
    }

    @Override
    public String toString() {
        return new StringBuilder().append("activeThreads:").append(activeThreadCount.get()).append("; ")
                .append("scheduled:").append(scheduled.get()).append("; ").toString();
    }

    /**
     * Maintains an AtomicBoolean so that the first thread to call this method after a Processor is no longer
     * scheduled to run will receive a <code>true</code> and MUST call the methods annotated with
     * {@link OnStopped @OnStopped}
     *
     * @return <code>true</code> if the caller is required to call Processor methods annotated with
     *         @OnStopped, <code>false</code> otherwise
     */
    public boolean mustCallOnStoppedMethods() {
        return mustCallOnStoppedMethods.getAndSet(false);
    }

    /**
     * Establishes the list of relevant futures for this processor. Replaces any previously held futures.
     *
     * @param newFutures futures
     */
    public synchronized void setFutures(final Collection<ScheduledFuture<?>> newFutures) {
        futures.clear();
        futures.addAll(newFutures);
    }

    public synchronized void replaceFuture(final ScheduledFuture<?> oldFuture, final ScheduledFuture<?> newFuture) {
        futures.remove(oldFuture);
        futures.add(newFuture);
    }

    public synchronized Set<ScheduledFuture<?>> getFutures() {
        return Collections.unmodifiableSet(futures);
    }
}
