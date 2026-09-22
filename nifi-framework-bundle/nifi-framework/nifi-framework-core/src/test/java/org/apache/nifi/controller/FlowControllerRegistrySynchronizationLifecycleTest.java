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
package org.apache.nifi.controller;

import org.apache.nifi.controller.flow.FlowManager;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.verify;

class FlowControllerRegistrySynchronizationLifecycleTest {

    @Test
    void testScheduleAndSubmitPostInitializationRegistrySynchronization() {
        final FlowManager flowManager = mock(FlowManager.class);
        final ScheduledExecutorService timerDrivenEngine = mock(ScheduledExecutorService.class);
        final ProcessScheduler processScheduler = mock(ProcessScheduler.class);

        final RegistryFlowSynchronizationTask registrySynchronizationTask = FlowController.scheduleRegistrySynchronizationTask(timerDrivenEngine, flowManager, 1800L, 30L);

        final Runnable postInitializationSynchronization = registrySynchronizationTask::synchronizeAllProcessGroups;
        assertDoesNotThrow(() -> FlowController.submitPostInitializationRegistrySynchronizationTask(processScheduler, postInitializationSynchronization, () -> false));

        verify(timerDrivenEngine).scheduleWithFixedDelay(same(registrySynchronizationTask), eq(300L), eq(30L), eq(TimeUnit.SECONDS));
        final ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(processScheduler).submitFrameworkTask(runnableCaptor.capture());
        assertNotSame(registrySynchronizationTask, runnableCaptor.getValue());
    }

    @Test
    void testSubmitPostInitializationRegistrySynchronizationTaskRequiresReleasedWriteLock() {
        final ProcessScheduler processScheduler = mock(ProcessScheduler.class);
        final Runnable registrySynchronizationTask = mock(Runnable.class);

        assertThrows(IllegalStateException.class,
                () -> FlowController.submitPostInitializationRegistrySynchronizationTask(processScheduler, registrySynchronizationTask, () -> true));

        verify(processScheduler, never()).submitFrameworkTask(any(Runnable.class));
    }

    @Test
    void testSubmitPostInitializationRegistrySynchronizationTaskIgnoresAbsentTask() {
        final ProcessScheduler processScheduler = mock(ProcessScheduler.class);

        assertDoesNotThrow(() -> FlowController.submitPostInitializationRegistrySynchronizationTask(processScheduler, null, () -> false));

        verify(processScheduler, never()).submitFrameworkTask(any(Runnable.class));
    }
}
