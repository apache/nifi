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
package org.apache.nifi.diagnostics;

import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class ThreadDumpTask implements DiagnosticTask {
    @Override
    public DiagnosticsDumpElement captureDump(boolean verbose) {
        final ThreadMXBean mbean = ManagementFactory.getThreadMXBean();

        final ThreadInfo[] infos = mbean.dumpAllThreads(true, true);
        final long[] deadlockedThreadIds = mbean.findDeadlockedThreads();
        final long[] monitorDeadlockThreadIds = mbean.findMonitorDeadlockedThreads();

        final List<ThreadInfo> sortedInfos = new ArrayList<>(infos.length);
        Collections.addAll(sortedInfos, infos);
        sortedInfos.sort(new Comparator<ThreadInfo>() {
            @Override
            public int compare(ThreadInfo o1, ThreadInfo o2) {
                return o1.getThreadName().toLowerCase().compareTo(o2.getThreadName().toLowerCase());
            }
        });

        final StringBuilder sb = new StringBuilder();
        for (final ThreadInfo info : sortedInfos) {
            sb.append("\n");
            sb.append("\"").append(info.getThreadName()).append("\" Id=");
            sb.append(info.getThreadId()).append(" ");
            sb.append(info.getThreadState().toString()).append(" ");

            switch (info.getThreadState()) {
                case BLOCKED:
                case TIMED_WAITING:
                case WAITING:
                    sb.append(" on ");
                    sb.append(info.getLockInfo());
                    break;
                default:
                    break;
            }

            if (info.isSuspended()) {
                sb.append(" (suspended)");
            }
            if (info.isInNative()) {
                sb.append(" (in native code)");
            }

            if (deadlockedThreadIds != null && deadlockedThreadIds.length > 0) {
                for (final long id : deadlockedThreadIds) {
                    if (id == info.getThreadId()) {
                        sb.append(" ** DEADLOCKED THREAD **");
                    }
                }
            }

            if (monitorDeadlockThreadIds != null && monitorDeadlockThreadIds.length > 0) {
                for (final long id : monitorDeadlockThreadIds) {
                    if (id == info.getThreadId()) {
                        sb.append(" ** MONITOR-DEADLOCKED THREAD **");
                    }
                }
            }

            final StackTraceElement[] stackTraces = info.getStackTrace();
            for (final StackTraceElement element : stackTraces) {
                sb.append("\n\tat ").append(element);

                final MonitorInfo[] monitors = info.getLockedMonitors();
                for (final MonitorInfo monitor : monitors) {
                    if (monitor.getLockedStackFrame().equals(element)) {
                        sb.append("\n\t- waiting on ").append(monitor);
                    }
                }
            }

            final LockInfo[] lockInfos = info.getLockedSynchronizers();
            if (lockInfos.length > 0) {
                sb.append("\n\t");
                sb.append("Number of Locked Synchronizers: ").append(lockInfos.length);
                for (final LockInfo lockInfo : lockInfos) {
                    sb.append("\n\t- ").append(lockInfo.toString());
                }
            }

            sb.append("\n");
        }

        if (deadlockedThreadIds != null && deadlockedThreadIds.length > 0) {
            sb.append("\n\nDEADLOCK DETECTED!");
            sb.append("\nThe following thread IDs are deadlocked:");
            for (final long id : deadlockedThreadIds) {
                sb.append("\n").append(id);
            }
        }

        if (monitorDeadlockThreadIds != null && monitorDeadlockThreadIds.length > 0) {
            sb.append("\n\nMONITOR DEADLOCK DETECTED!");
            sb.append("\nThe following thread IDs are deadlocked:");
            for (final long id : monitorDeadlockThreadIds) {
                sb.append("\n").append(id);
            }
        }

        return new StandardDiagnosticsDumpElement("Thread Dump", Collections.singletonList(sb.toString()));
    }
}
