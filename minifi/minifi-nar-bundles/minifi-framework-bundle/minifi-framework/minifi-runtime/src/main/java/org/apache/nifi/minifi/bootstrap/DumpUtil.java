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

package org.apache.nifi.minifi.bootstrap;

import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class DumpUtil {

    public static String getDump() {
        ThreadMXBean mbean = ManagementFactory.getThreadMXBean();
        ThreadInfo[] infos = mbean.dumpAllThreads(true, true);
        long[] deadlockedThreadIds = mbean.findDeadlockedThreads();
        long[] monitorDeadlockThreadIds = mbean.findMonitorDeadlockedThreads();

        List<ThreadInfo> sortedInfos = new ArrayList<>(infos.length);
        sortedInfos.addAll(Arrays.asList(infos));
        sortedInfos.sort(Comparator.comparing(o -> o.getThreadName().toLowerCase()));

        StringBuilder sb = new StringBuilder();
        for (ThreadInfo info : sortedInfos) {
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
                for (long id : deadlockedThreadIds) {
                    if (id == info.getThreadId()) {
                        sb.append(" ** DEADLOCKED THREAD **");
                    }
                }
            }

            if (monitorDeadlockThreadIds != null && monitorDeadlockThreadIds.length > 0) {
                for (long id : monitorDeadlockThreadIds) {
                    if (id == info.getThreadId()) {
                        sb.append(" ** MONITOR-DEADLOCKED THREAD **");
                    }
                }
            }

            StackTraceElement[] stackTraces = info.getStackTrace();
            for (StackTraceElement element : stackTraces) {
                sb.append("\n\tat ").append(element);

                MonitorInfo[] monitors = info.getLockedMonitors();
                for (MonitorInfo monitor : monitors) {
                    if (monitor.getLockedStackFrame().equals(element)) {
                        sb.append("\n\t- waiting on ").append(monitor);
                    }
                }
            }

            LockInfo[] lockInfos = info.getLockedSynchronizers();
            if (lockInfos.length > 0) {
                sb.append("\n\t");
                sb.append("Number of Locked Synchronizes: ").append(lockInfos.length);
                for (LockInfo lockInfo : lockInfos) {
                    sb.append("\n\t- ").append(lockInfo.toString());
                }
            }

            sb.append("\n");
        }

        if (deadlockedThreadIds != null && deadlockedThreadIds.length > 0) {
            sb.append("\n\nDEADLOCK DETECTED");
            sb.append("\nThe following thread IDs are deadlocked:");
            for (long id : deadlockedThreadIds) {
                sb.append("\n").append(id);
            }
        }

        if (monitorDeadlockThreadIds != null && monitorDeadlockThreadIds.length > 0) {
            sb.append("\n\nMONITOR DEADLOCK DETECTED");
            sb.append("\nThe following thread IDs are deadlocked:");
            for (long id : monitorDeadlockThreadIds) {
                sb.append("\n").append(id);
            }
        }

        return sb.toString();
    }
}
