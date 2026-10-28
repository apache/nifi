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
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SourceWorkCheckTest {
    private static final long NO_WORK_YIELD_NANOS = TimeUnit.MILLISECONDS.toNanos(50L);
    private static final InvocationResult NO_DATA_FOUND = InvocationResult.completed(InvocationOutcome.INVOKED_WITHOUT_ACTIVITY);
    private static final InvocationResult DATA_FOUND = InvocationResult.completed(InvocationOutcome.INVOKED_WITH_ACTIVITY);

    @Test
    void testOnlyOneConcurrentTaskChecksSourceProcessorAfterNoDataFound() throws InterruptedException {
        final ConnectableTask connectableTask = mock(ConnectableTask.class);
        final AtomicInteger readinessNotifications = new AtomicInteger();
        final SourceWorkCheck sourceWorkCheck = new SourceWorkCheck(connectableTask, true, NO_WORK_YIELD_NANOS, readinessNotifications::incrementAndGet);

        // Until the Processor comes up empty, every invocation is allowed and finishing does nothing
        assertTrue(sourceWorkCheck.tryStart());
        sourceWorkCheck.finish();
        assertEquals(0, readinessNotifications.get());

        // An empty invocation while another concurrent task is also invoking the Processor does not start the wait between checks
        sourceWorkCheck.recordInvocationResult(NO_DATA_FOUND, 2);
        assertTrue(sourceWorkCheck.isInvocationAllowed());

        sourceWorkCheck.recordInvocationResult(NO_DATA_FOUND, 1);
        assertFalse(sourceWorkCheck.isInvocationAllowed());
        assertFalse(sourceWorkCheck.tryStart());
        assertTrue(sourceWorkCheck.getDelayUntilNextCheckNanos() > 0L);

        // Queued FlowFiles are work to do, so no check is required while they are present
        when(connectableTask.hasLocallyConsumableInput()).thenReturn(true);
        assertTrue(sourceWorkCheck.isInvocationAllowed());
        when(connectableTask.hasLocallyConsumableInput()).thenReturn(false);

        while (sourceWorkCheck.getDelayUntilNextCheckNanos() > 0L) {
            Thread.sleep(5L);
        }

        assertTrue(sourceWorkCheck.isInvocationAllowed());
        assertTrue(sourceWorkCheck.tryStart());
        assertFalse(sourceWorkCheck.isInvocationAllowed());

        // Another thread can neither start a second check nor finish the check that this thread started
        final AtomicBoolean otherThreadStarted = new AtomicBoolean(true);
        final Thread otherThread = Thread.ofVirtual().start(() -> {
            otherThreadStarted.set(sourceWorkCheck.tryStart());
            sourceWorkCheck.finish();
        });
        otherThread.join();
        assertFalse(otherThreadStarted.get());
        assertFalse(sourceWorkCheck.isInvocationAllowed());
        assertEquals(0, readinessNotifications.get());

        sourceWorkCheck.finish();
        assertEquals(1, readinessNotifications.get());

        // The check found no data again, so the Processor waits again before the next check
        sourceWorkCheck.recordInvocationResult(NO_DATA_FOUND, 1);
        assertFalse(sourceWorkCheck.isInvocationAllowed());
        assertFalse(sourceWorkCheck.hasRecentlyFoundData(System.nanoTime(), 0));

        sourceWorkCheck.recordInvocationResult(DATA_FOUND, 1);
        assertEquals(2, readinessNotifications.get());
        assertTrue(sourceWorkCheck.isInvocationAllowed());
        assertTrue(sourceWorkCheck.hasRecentlyFoundData(System.nanoTime(), 0));
        assertFalse(sourceWorkCheck.hasRecentlyFoundData(System.nanoTime() + TimeUnit.SECONDS.toNanos(3L), 0));
    }

    @Test
    void testInvocationAlwaysAllowedForProcessorThatIsNotSource() {
        final ConnectableTask connectableTask = mock(ConnectableTask.class);
        final AtomicInteger readinessNotifications = new AtomicInteger();
        final SourceWorkCheck sourceWorkCheck = new SourceWorkCheck(connectableTask, false, NO_WORK_YIELD_NANOS, readinessNotifications::incrementAndGet);

        sourceWorkCheck.recordInvocationResult(NO_DATA_FOUND, 1);
        assertTrue(sourceWorkCheck.isInvocationAllowed());
        assertTrue(sourceWorkCheck.tryStart());
        sourceWorkCheck.finish();
        assertEquals(0L, sourceWorkCheck.getDelayUntilNextCheckNanos());
        assertFalse(sourceWorkCheck.hasRecentlyFoundData(System.nanoTime(), 1));
        assertEquals(0, readinessNotifications.get());
    }
}
