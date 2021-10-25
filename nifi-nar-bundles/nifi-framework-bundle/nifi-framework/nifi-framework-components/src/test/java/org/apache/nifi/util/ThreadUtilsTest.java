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
package org.apache.nifi.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.management.LockInfo;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ThreadUtilsTest {
    private static final String THREAD_NAME = "TestThread";

    private static final String DECLARING_CLASS = ThreadUtilsTest.class.getName();

    private static final String METHOD = "createStackTrace";

    private static final String FILE_NAME = ThreadUtilsTest.class.getSimpleName();

    private static final String MONITOR_CLASS = String.class.getName();

    private static final int LINE_NUMBER = 100;

    private static final int IDENTITY_HASH_CODE = 1024;

    private static final int STACK_DEPTH = 1;

    private static final int NULL_STACK_DEPTH = -1;

    @Mock
    private ThreadInfo threadInfo;

    @Test
    public void testCreateStackTrace() {
        final Thread.State threadState = Thread.State.BLOCKED;
        setThreadInfo(threadState, getStackTraceElement(MONITOR_CLASS));

        final String stackTrace = ThreadUtils.createStackTrace(threadInfo, null, null);
        assertThreadInfoFound(stackTrace, threadState);
    }

    @Test
    public void testCreateStackTraceNullLockedStackFrame() {
        final Thread.State threadState = Thread.State.RUNNABLE;
        setThreadInfo(threadState, null);

        final String stackTrace = ThreadUtils.createStackTrace(threadInfo, null, null);
        assertThreadInfoFound(stackTrace, threadState);
    }

    private void setThreadInfo(final Thread.State threadState, final StackTraceElement lockedStackFrame) {
        when(threadInfo.getThreadName()).thenReturn(THREAD_NAME);

        when(threadInfo.getThreadState()).thenReturn(threadState);

        final StackTraceElement stackTraceElement = getStackTraceElement(DECLARING_CLASS);
        when(threadInfo.getStackTrace()).thenReturn(new StackTraceElement[]{stackTraceElement});

        final int stackDepth = lockedStackFrame == null ? NULL_STACK_DEPTH : STACK_DEPTH;
        final MonitorInfo monitorInfo = new MonitorInfo(MONITOR_CLASS, IDENTITY_HASH_CODE, stackDepth, lockedStackFrame);
        when(threadInfo.getLockedMonitors()).thenReturn(new MonitorInfo[]{monitorInfo});

        when(threadInfo.getLockedSynchronizers()).thenReturn(new LockInfo[]{});
    }

    private void assertThreadInfoFound(final String stackTrace, final Thread.State threadState) {
        assertNotNull(stackTrace);
        assertTrue(stackTrace.contains(THREAD_NAME), "Thread Name not found");
        assertTrue(stackTrace.contains(threadState.toString()), "Thread State not found");
        assertTrue(stackTrace.contains(DECLARING_CLASS), "Stack Trace declaring class not found");
        assertTrue(stackTrace.contains(METHOD), "Stack Trace method not found");
    }

    private StackTraceElement getStackTraceElement(final String declaringClass) {
        return new StackTraceElement(declaringClass, METHOD, FILE_NAME, LINE_NUMBER);
    }
}
