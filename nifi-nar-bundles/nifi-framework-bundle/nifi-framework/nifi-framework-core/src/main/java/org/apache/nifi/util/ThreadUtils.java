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

import java.lang.management.LockInfo;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;

public class ThreadUtils {

    public static String createStackTrace(final Thread thread, final ThreadInfo threadInfo, final long[] deadlockedThreadIds, final long[] monitorDeadlockThreadIds, final long activeMillis) {
        final StringBuilder sb = new StringBuilder();
        sb.append("\"").append(threadInfo.getThreadName()).append("\" Id=");
        sb.append(threadInfo.getThreadId()).append(" ");
        sb.append(threadInfo.getThreadState().toString()).append(" ");

        switch (threadInfo.getThreadState()) {
            case BLOCKED:
            case TIMED_WAITING:
            case WAITING:
                sb.append(" on ");
                sb.append(threadInfo.getLockInfo());
                break;
            default:
                break;
        }

        if (threadInfo.isSuspended()) {
            sb.append(" (suspended)");
        }
        if (threadInfo.isInNative()) {
            sb.append(" (in native code)");
        }

        if (deadlockedThreadIds != null && deadlockedThreadIds.length > 0) {
            for (final long id : deadlockedThreadIds) {
                if (id == threadInfo.getThreadId()) {
                    sb.append(" ** DEADLOCKED THREAD **");
                }
            }
        }

        if (monitorDeadlockThreadIds != null && monitorDeadlockThreadIds.length > 0) {
            for (final long id : monitorDeadlockThreadIds) {
                if (id == threadInfo.getThreadId()) {
                    sb.append(" ** MONITOR-DEADLOCKED THREAD **");
                }
            }
        }

        final StackTraceElement[] stackTraces = threadInfo.getStackTrace();
        for (final StackTraceElement element : stackTraces) {
            sb.append("\n\tat ").append(element);

            final MonitorInfo[] monitors = threadInfo.getLockedMonitors();
            for (final MonitorInfo monitor : monitors) {
                if (monitor.getLockedStackFrame().equals(element)) {
                    sb.append("\n\t- waiting on ").append(monitor);
                }
            }
        }

        final LockInfo[] lockInfos = threadInfo.getLockedSynchronizers();
        if (lockInfos.length > 0) {
            sb.append("\n\t");
            sb.append("Number of Locked Synchronizers: ").append(lockInfos.length);
            for (final LockInfo lockInfo : lockInfos) {
                sb.append("\n\t- ").append(lockInfo.toString());
            }
        }

        sb.append("\n");
        return sb.toString();
    }
}
