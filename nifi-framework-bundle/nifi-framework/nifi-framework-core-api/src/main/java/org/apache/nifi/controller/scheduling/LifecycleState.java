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

import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.controller.repository.ActiveProcessSessionFactory;
import org.apache.nifi.processor.exception.TerminatedTaskException;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class LifecycleState {

    private final Object componentId;
    private final AtomicInteger activeThreadCount = new AtomicInteger(0);
    private final AtomicBoolean scheduled = new AtomicBoolean(false);
    private final Set<ScheduledFuture<?>> futures = new HashSet<>();
    private final AtomicBoolean mustCallOnStoppedMethods = new AtomicBoolean(false);
    private volatile long lastStopTime = -1;
    private volatile boolean terminated = false;
    private final Map<ActiveProcessSessionFactory, Object> activeProcessSessionFactories = new WeakHashMap<>();

    public LifecycleState(final String componentId) {
        this.componentId = componentId;
    }

    public synchronized boolean tryIncrementActiveThreadCount(final ActiveProcessSessionFactory sessionFactory) {
        if ((terminated) || (!scheduled.get())) {
            return false;
        }
        incrementActiveThreadCount(sessionFactory);
        return true;
    }

    public synchronized int incrementActiveThreadCount(final ActiveProcessSessionFactory sessionFactory) {
        if (terminated) {
            throw new TerminatedTaskException();
        }

        if (sessionFactory != null) {
            // If a session factory is provided, add it to our WeakHashMap. The value that we use is not relevant,
            // as this just serves, essentially, as a WeakHashSet, but there is no WeakHashSet implementation.
            // We need to keep track of any ActiveProcessSessionFactory that has been created for this component,
            // as long as the session factory has not been garbage collected. This is important because when we offload
            // a node, we will terminate all active processors and we need the ability to terminate any active sessions
            // at that time. We cannot simply store a Set of all ActiveProcessSessionFactories and then remove them in the
            // decrementActiveThreadCount because a Processor may choose to continue using the ProcessSessionFactory even after
            // returning from its onTrigger method.
            //
            // For example, it may stash the ProcessSessionFactory away in a member variable in order to aggregate FlowFiles across
            // many onTrigger invocations. In this case, we need the ability to force the rollback of any created session upon Processor
            // termination.
            activeProcessSessionFactories.put(sessionFactory, null);
        }

        return activeThreadCount.incrementAndGet();
    }

    public synchronized int decrementActiveThreadCount() {
        if (terminated) {
            return activeThreadCount.get();
        }

        return activeThreadCount.decrementAndGet();
    }

    public int getActiveThreadCount() {
        return activeThreadCount.get();
    }

    public boolean isScheduled() {
        return scheduled.get();
    }

    public synchronized void setScheduled(final boolean scheduled) {
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
        return "LifecycleState[componentId=" + componentId + ", activeThreads= " + activeThreadCount.get() + ", scheduled=" + scheduled.get() + "]";
    }

    /**
     * Maintains an AtomicBoolean so that the first thread to call this method after a Processor is no longer
     * scheduled to run will receive a <code>true</code> and MUST call the methods annotated with
     * {@link OnStopped @OnStopped}
     *
     * @return <code>true</code> if the caller is required to call Processor methods annotated with @OnStopped
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

    public synchronized void terminate() {
        this.terminated = true;
        activeThreadCount.set(0);

        // Terminate any active sessions.
        for (final ActiveProcessSessionFactory factory : activeProcessSessionFactories.keySet()) {
            factory.terminateActiveSessions();
        }
    }

    public void clearTerminationFlag() {
        this.terminated = false;
    }

    public boolean isTerminated() {
        return this.terminated;
    }

}
