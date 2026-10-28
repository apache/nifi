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

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.controller.scheduling.auto.ProcessorSchedulingSnapshot;
import org.apache.nifi.controller.scheduling.auto.StandardAutoSchedulingController;
import org.apache.nifi.controller.tasks.ConnectableTask;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ProcessorAutoSchedulingStateTest {
    private static final String COMPONENT_ID = "processor-1";

    @Test
    void testResetMeasurementsAfterEvent() throws InterruptedException {
        final Connectable connectable = mock(Connectable.class);
        when(connectable.getIdentifier()).thenReturn(COMPONENT_ID);
        when(connectable.getIncomingConnections()).thenReturn(List.of());
        final ConnectableTask connectableTask = mock(ConnectableTask.class);
        when(connectableTask.getConnectable()).thenReturn(connectable);
        final SchedulingGeneration generation = new SchedulingGeneration();
        final StandardAutoSchedulingController controller = new StandardAutoSchedulingController(4, false, false, false, 4);
        final ProcessorAutoSchedulingState state = new ProcessorAutoSchedulingState(connectableTask, new LifecycleState(COMPONENT_ID), generation, controller,
                TimeUnit.MILLISECONDS.toNanos(10L), () -> false, () -> 10, worker -> { });
        generation.setAutoSchedulingState(state);
        final WaitingWorkers waitingWorkers = state.getWaitingWorkers();

        state.getMeasurements().createInvocationObserver().onCommit(new CommittedSchedulingWork(0, 1000));
        final SchedulingSettings settingsBeforeReset = state.getSchedulingSettings();
        final long taskSlotUpdateCount = waitingWorkers.getTaskSlotUpdateCount();

        state.resetMeasurementsAfterEvent();

        // A worker that read the settings just before the reset cannot take a task slot with them, so it waits for a task slot update.
        // No invocation holds a slot, so nothing else will signal one: the reset itself must wake the worker.
        assertFalse(state.tryAcquireConcurrentTaskSlot(settingsBeforeReset));
        final ProcessorTaskWorker worker = new ProcessorTaskWorker(0L, state);
        final Thread waitingThread = Thread.ofVirtual().start(() -> waitingWorkers.waitForTaskSlotUpdate(taskSlotUpdateCount, worker));
        try {
            assertTrue(waitingThread.join(Duration.ofSeconds(2L)), "Worker kept waiting for a task slot update after the measurements were reset");
        } finally {
            generation.stop(false);
            waitingThread.join(Duration.ofSeconds(2L));
        }

        assertTrue(state.tryAcquireConcurrentTaskSlot(state.getSchedulingSettings()));
        state.releaseConcurrentTaskSlot();

        // Work committed before the reset belongs to the measurements that were discarded
        final ProcessorSchedulingSnapshot snapshot = state.captureProcessorSchedulingSnapshot(System.nanoTime(), false);
        assertEquals(0L, snapshot.committedFlowFiles());
    }
}
