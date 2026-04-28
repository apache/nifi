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

import org.apache.nifi.controller.repository.ActiveProcessSessionFactory;
import org.apache.nifi.controller.repository.WeakHashMapProcessSessionFactory;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.junit.jupiter.api.Test;

import java.lang.ref.Reference;
import java.lang.ref.WeakReference;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LifecycleStateTest {

    private static final long GARBAGE_COLLECTION_TIMEOUT_MILLIS = 5_000L;

    /**
     * Verifies that a Session created by an ActiveProcessSessionFactory is rolled back when the
     * LifecycleState is terminated, even when the only strong reference to the factory has been released.
     *
     * This represents the scenario in which a Processor that extends AbstractSessionFactoryProcessor
     * (rather than AbstractProcessor) creates a Session and stashes it in a member field for use across
     * subsequent onTrigger invocations. ConnectableTask creates a fresh ActiveProcessSessionFactory for
     * each invocation and registers it with the LifecycleState via incrementActiveThreadCount, but it
     * does not retain a strong reference to that factory once the invocation returns. The Processor
     * retains the Session, not the factory.
     *
     * If the factory is allowed to become unreachable while the Session is still in use, terminate()
     * must still be able to reach the factory in order to roll back the Session. Otherwise, a stop
     * followed by a terminate (such as the sequence performed during node offload and decommission)
     * leaves the FlowFiles unacknowledged on their source queues, preventing the queue counts from
     * dropping to zero and preventing offload from completing.
     */
    @Test
    void testTerminateRollsBackSessionRetainedAfterFactoryReferenceReleased() throws InterruptedException {
        final ProcessSession retainedSession = mock(ProcessSession.class);
        final ProcessSessionFactory delegate = mock(ProcessSessionFactory.class);
        when(delegate.createSession()).thenReturn(retainedSession);

        final LifecycleState lifecycleState = new LifecycleState("component-id");

        final FactoryRegistration registration = registerAndReleaseFactory(lifecycleState, delegate);

        encourageGarbageCollection(registration.factoryReference());

        lifecycleState.terminate();

        // The session returned to a Processor is a wrapper that delegates to retainedSession; verify the
        // delegate mock observes the rollback rather than the wrapper itself.
        verify(retainedSession).rollback();

        // Keep the registration reachable through verification so that the wrapper Session it holds (which
        // strongly references the factory) prevents the LifecycleState WeakHashMap entry from clearing
        // before terminate() runs.
        Reference.reachabilityFence(registration);
    }

    /**
     * Performs the registration in a separate stack frame so that no strong references to the factory
     * exist on the caller's stack outside of what {@code WeakHashMapProcessSessionFactory.createSession()}
     * arranges to retain via the returned Session wrapper.
     */
    private FactoryRegistration registerAndReleaseFactory(final LifecycleState lifecycleState, final ProcessSessionFactory delegate) {
        final ActiveProcessSessionFactory factory = new WeakHashMapProcessSessionFactory(delegate);
        lifecycleState.incrementActiveThreadCount(factory);
        final ProcessSession sessionWrapper = factory.createSession();
        return new FactoryRegistration(sessionWrapper, new WeakReference<>(factory));
    }

    /**
     * Applies garbage collection pressure to encourage clearing of the WeakReference. This surfaces
     * the orphaned-session scenario when no other reference path keeps the factory reachable. A
     * correct implementation that retains the factory through the Session will simply iterate to
     * the timeout without the WeakReference being cleared, and the subsequent terminate() call will
     * still roll back the Session.
     */
    private static void encourageGarbageCollection(final WeakReference<?> reference) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + GARBAGE_COLLECTION_TIMEOUT_MILLIS;
        while (reference.get() != null && System.currentTimeMillis() < deadline) {
            System.gc();
            Thread.sleep(50L);
        }
    }

    private record FactoryRegistration(ProcessSession sessionWrapper, WeakReference<ActiveProcessSessionFactory> factoryReference) {
    }
}
