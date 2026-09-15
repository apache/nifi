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
package org.apache.nifi.controller.tasks;

import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.scheduling.LifecycleState;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.ReportingTask;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ReportingTaskWrapperTest {

    @Test
    void testRunDoesNotHoldWrapperMonitorDuringReportingTaskInvocation() {
        final ReportingTask reportingTask = mock(ReportingTask.class);
        final ReportingTaskNode taskNode = mock(ReportingTaskNode.class);
        final ReportingContext reportingContext = mock(ReportingContext.class);
        final ExtensionManager extensionManager = mock(ExtensionManager.class);
        final LifecycleState lifecycleState = new LifecycleState("reporting-task");
        final AtomicBoolean callbackInvoked = new AtomicBoolean();
        final AtomicBoolean wrapperMonitorHeld = new AtomicBoolean();
        final AtomicReference<ReportingTaskWrapper> wrapperReference = new AtomicReference<>();

        when(taskNode.getReportingTask()).thenReturn(reportingTask);
        when(taskNode.getReportingContext()).thenReturn(reportingContext);
        when(taskNode.getIdentifier()).thenReturn("reporting-task");
        doAnswer(invocation -> {
            callbackInvoked.set(true);
            wrapperMonitorHeld.set(Thread.holdsLock(wrapperReference.get()));
            return null;
        }).when(reportingTask).onTrigger(any(ReportingContext.class));

        final ReportingTaskWrapper wrapper = new ReportingTaskWrapper(taskNode, lifecycleState, extensionManager);
        wrapperReference.set(wrapper);
        lifecycleState.setScheduled(true);

        wrapper.run();

        assertTrue(callbackInvoked.get());
        assertFalse(wrapperMonitorHeld.get());
    }
}
