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

import com.sun.management.HotSpotDiagnosticMXBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Captures platform and virtual thread stack traces when supported by the Java runtime.
 */
public class ThreadDumpTask implements DiagnosticTask {

    private static final Logger logger = LoggerFactory.getLogger(ThreadDumpTask.class);

    @Override
    public DiagnosticsDumpElement captureDump(final boolean verbose) {
        final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        final String threadDump = captureThreadDump(threadMXBean);

        final StringBuilder dumpBuilder = new StringBuilder(threadDump);
        appendDeadlockedThreadIds(dumpBuilder, "DEADLOCK DETECTED!", threadMXBean.findDeadlockedThreads());
        appendDeadlockedThreadIds(dumpBuilder, "MONITOR DEADLOCK DETECTED!", threadMXBean.findMonitorDeadlockedThreads());

        return new StandardDiagnosticsDumpElement("Thread Dump", Collections.singletonList(dumpBuilder.toString()));
    }

    private String captureThreadDump(final ThreadMXBean threadMXBean) {
        try {
            return captureHotSpotThreadDump();
        } catch (final IOException | RuntimeException | LinkageError e) {
            logger.warn("Failed to capture virtual threads using the HotSpot diagnostic interface; capturing platform threads instead", e);
            return capturePlatformThreadDump(threadMXBean);
        }
    }

    private String captureHotSpotThreadDump() throws IOException {
        final HotSpotDiagnosticMXBean diagnosticMXBean = ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);
        if (diagnosticMXBean == null) {
            throw new UnsupportedOperationException("HotSpot diagnostic interface is not available");
        }

        final Path tempDirectory = Files.createTempDirectory("nifi-thread-dump-");
        final Path tempFile = tempDirectory.resolve("thread-dump.txt");
        try {
            diagnosticMXBean.dumpThreads(tempFile.toString(), HotSpotDiagnosticMXBean.ThreadDumpFormat.TEXT_PLAIN);
            return Files.readString(tempFile);
        } finally {
            try {
                Files.deleteIfExists(tempFile);
                Files.deleteIfExists(tempDirectory);
            } catch (final IOException e) {
                logger.debug("Failed to delete temporary thread-dump files in {}", tempDirectory, e);
            }
        }
    }

    String capturePlatformThreadDump(final ThreadMXBean threadMXBean) {
        final ThreadInfo[] threadInfos = threadMXBean.dumpAllThreads(true, true);
        final List<ThreadInfo> sortedThreadInfos = new ArrayList<>(threadInfos.length);
        Collections.addAll(sortedThreadInfos, threadInfos);
        sortedThreadInfos.sort(Comparator.comparing(ThreadInfo::getThreadName, String.CASE_INSENSITIVE_ORDER));

        final StringBuilder dumpBuilder = new StringBuilder();
        for (final ThreadInfo threadInfo : sortedThreadInfos) {
            dumpBuilder.append(System.lineSeparator())
                    .append('"').append(threadInfo.getThreadName()).append("\" Id=")
                    .append(threadInfo.getThreadId()).append(' ')
                    .append(threadInfo.getThreadState());

            switch (threadInfo.getThreadState()) {
                case BLOCKED:
                case TIMED_WAITING:
                case WAITING:
                    dumpBuilder.append(" on ").append(threadInfo.getLockInfo());
                    if (threadInfo.getLockOwnerName() != null) {
                        dumpBuilder.append(" owned by \"").append(threadInfo.getLockOwnerName()).append("\" Id=").append(threadInfo.getLockOwnerId());
                    }
                    break;
                default:
                    break;
            }

            if (threadInfo.isSuspended()) {
                dumpBuilder.append(" (suspended)");
            }
            if (threadInfo.isInNative()) {
                dumpBuilder.append(" (in native code)");
            }

            final MonitorInfo[] lockedMonitors = threadInfo.getLockedMonitors();
            for (final StackTraceElement stackTraceElement : threadInfo.getStackTrace()) {
                dumpBuilder.append(System.lineSeparator()).append("\tat ").append(stackTraceElement);
                for (final MonitorInfo monitorInfo : lockedMonitors) {
                    if (Objects.equals(monitorInfo.getLockedStackFrame(), stackTraceElement)) {
                        dumpBuilder.append(System.lineSeparator()).append("\t- locked ").append(monitorInfo);
                    }
                }
            }

            final LockInfo[] lockedSynchronizers = threadInfo.getLockedSynchronizers();
            if (lockedSynchronizers.length > 0) {
                dumpBuilder.append(System.lineSeparator()).append("\tNumber of Locked Synchronizers: ").append(lockedSynchronizers.length);
                for (final LockInfo lockInfo : lockedSynchronizers) {
                    dumpBuilder.append(System.lineSeparator()).append("\t- locked ").append(lockInfo);
                }
            }
            dumpBuilder.append(System.lineSeparator());
        }

        return dumpBuilder.toString();
    }

    private void appendDeadlockedThreadIds(final StringBuilder dumpBuilder, final String heading, final long[] threadIds) {
        if (threadIds == null || threadIds.length == 0) {
            return;
        }

        dumpBuilder.append(System.lineSeparator()).append(System.lineSeparator()).append(heading)
                .append(System.lineSeparator()).append("The following thread IDs are deadlocked:");

        for (final long threadId : threadIds) {
            dumpBuilder.append(System.lineSeparator()).append(threadId);
        }
    }
}
