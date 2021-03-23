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
package org.apache.nifi.components;

import org.apache.nifi.components.monitor.LongRunningTaskMonitor;
import org.apache.nifi.controller.ActiveThreadInfo;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.TerminationAwareLogger;
import org.apache.nifi.controller.ThreadDetails;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.reporting.Severity;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class LongRunningTaskMonitorTest {

    private static final String STACKTRACE = "line1\nline2";

    @Test
    public void test() {
        // GIVEN
        ThreadDetails threadDetails = mock(ThreadDetails.class);

        ActiveThreadInfo activeThreadInfo11 = mockActiveThreadInfo("Thread-11", 60_000);
        ActiveThreadInfo activeThreadInfo12 = mockActiveThreadInfo("Thread-12", 60_001);

        TerminationAwareLogger processorLogger1 = mock(TerminationAwareLogger.class);
        ProcessorNode processorNode1 = mockProcessorNode("Processor-1-ID", "Processor-1-Name", "Processor-1-Type", processorLogger1,
                threadDetails, activeThreadInfo11, activeThreadInfo12);

        ActiveThreadInfo activeThreadInfo21 = mockActiveThreadInfo("Thread-21", 1_000_000);
        ActiveThreadInfo activeThreadInfo22 = mockActiveThreadInfo("Thread-22", 1_000);

        TerminationAwareLogger processorLogger2 = mock(TerminationAwareLogger.class);
        ProcessorNode processorNode2 = mockProcessorNode("Processor-2-ID", "Processor-2-Name", "Processor-2-Type", processorLogger2,
                threadDetails, activeThreadInfo21, activeThreadInfo22);

        ProcessGroup processGroup = mockProcessGroup(processorNode1, processorNode2);

        FlowManager flowManager = mockFlowManager(processGroup);

        EventReporter eventReporter = mock(EventReporter.class);

        Logger longRunningTaskMonitorLogger = mock(Logger.class);

        LongRunningTaskMonitor longRunningTaskMonitor = new LongRunningTaskMonitor(flowManager, eventReporter, 60_000) {
            @Override
            protected Logger getLogger() {
                return longRunningTaskMonitorLogger;
            }

            @Override
            protected ThreadDetails captureThreadDetails() {
                return threadDetails;
            }
        };

        // WHEN
        longRunningTaskMonitor.run();

        // THEN
        verify(longRunningTaskMonitorLogger).debug("Checking long running processor tasks...");

        ArgumentCaptor<String> logMessages = ArgumentCaptor.forClass(String.class);
        verify(longRunningTaskMonitorLogger, times(2)).warn(logMessages.capture());
        assertEquals("Long running task detected on processor [id=Processor-1-ID, name=Processor-1-Name, type=Processor-1-Type]. Task time: 60 seconds. Stack trace:\n" + STACKTRACE,
                logMessages.getAllValues().get(0));
        assertEquals("Long running task detected on processor [id=Processor-2-ID, name=Processor-2-Name, type=Processor-2-Type]. Task time: 1,000 seconds. Stack trace:\n" + STACKTRACE,
                logMessages.getAllValues().get(1));

        ArgumentCaptor<String> controllerBulletinMessages = ArgumentCaptor.forClass(String.class);
        verify(eventReporter, times(2)).reportEvent(eq(Severity.WARNING), eq("Long Running Task"), controllerBulletinMessages.capture());
        assertEquals("Processor with ID Processor-1-ID, Name Processor-1-Name and Type Processor-1-Type has a task that has been running for 60 seconds (thread name: Thread-12).",
                controllerBulletinMessages.getAllValues().get(0));
        assertEquals("Processor with ID Processor-2-ID, Name Processor-2-Name and Type Processor-2-Type has a task that has been running for 1,000 seconds (thread name: Thread-21).",
                controllerBulletinMessages.getAllValues().get(1));

        verify(processorLogger1).warn("The processor has a task that has been running for 60 seconds (thread name: Thread-12).");

        verify(processorLogger2).warn("The processor has a task that has been running for 1,000 seconds (thread name: Thread-21).");

        verify(longRunningTaskMonitorLogger).info("Active threads: {}; Long running threads: {}", 4, 2);

        verifyNoMoreInteractions(longRunningTaskMonitorLogger, eventReporter, processorLogger1, processorLogger2);
    }

    private ActiveThreadInfo mockActiveThreadInfo(String threadName, long activeMillis) {
        ActiveThreadInfo activeThreadInfo = mock(ActiveThreadInfo.class);

        when(activeThreadInfo.getThreadName()).thenReturn(threadName);
        when(activeThreadInfo.getStackTrace()).thenReturn(STACKTRACE);
        when(activeThreadInfo.getActiveMillis()).thenReturn(activeMillis);

        return activeThreadInfo;
    }

    private ProcessorNode mockProcessorNode(String processorId, String processorName, String processorType, TerminationAwareLogger processorLogger,
                                            ThreadDetails threadDetails, ActiveThreadInfo... activeThreadInfos) {
        ProcessorNode processorNode = mock(ProcessorNode.class);

        when(processorNode.getIdentifier()).thenReturn(processorId);
        when(processorNode.getName()).thenReturn(processorName);
        when(processorNode.getComponentType()).thenReturn(processorType);
        when(processorNode.getLogger()).thenReturn(processorLogger);
        when(processorNode.getActiveThreads(threadDetails)).thenReturn(Arrays.asList(activeThreadInfos));

        return processorNode;
    }

    private ProcessGroup mockProcessGroup(ProcessorNode... processorNodes) {
        ProcessGroup processGroup = mock(ProcessGroup.class);

        when(processGroup.findAllProcessors()).thenReturn(Arrays.asList(processorNodes));

        return processGroup;
    }

    private FlowManager mockFlowManager(ProcessGroup processGroup) {
        FlowManager flowManager = mock(FlowManager.class);

        when(flowManager.getRootGroup()).thenReturn(processGroup);

        return flowManager;
    }
}
