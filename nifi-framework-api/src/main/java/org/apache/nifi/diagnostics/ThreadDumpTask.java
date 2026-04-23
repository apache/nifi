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
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

/**
 * Captures a textual dump of every thread in the JVM. Uses
 * {@link HotSpotDiagnosticMXBean#dumpThreads(String, HotSpotDiagnosticMXBean.ThreadDumpFormat)}
 * so that virtual threads are included in the dump alongside platform threads.
 */
public class ThreadDumpTask implements DiagnosticTask {

    private static final Logger logger = LoggerFactory.getLogger(ThreadDumpTask.class);

    @Override
    public DiagnosticsDumpElement captureDump(final boolean verbose) {
        String threadDump;

        Path tempDirectory = null;
        try {
            final HotSpotDiagnosticMXBean diagnosticMXBean = ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);
            // dumpThreads requires that the destination file does not already exist. Creating a private
            // temporary directory and writing to a fresh filename inside it avoids a time-of-check to
            // time-of-use race that would exist if we created a temp file and then deleted it before the
            // dumpThreads call.
            tempDirectory = Files.createTempDirectory("nifi-thread-dump-");
            final Path tempFile = tempDirectory.resolve("thread-dump.txt");
            try {
                diagnosticMXBean.dumpThreads(tempFile.toString(), HotSpotDiagnosticMXBean.ThreadDumpFormat.TEXT_PLAIN);
                threadDump = Files.readString(tempFile);
            } finally {
                Files.deleteIfExists(tempFile);
            }
        } catch (final IOException e) {
            threadDump = "Failed to capture thread dump: " + e.getMessage();
        } finally {
            if (tempDirectory != null) {
                try {
                    Files.deleteIfExists(tempDirectory);
                } catch (final IOException e) {
                    logger.debug("Failed to delete temporary thread-dump directory {}", tempDirectory, e);
                }
            }
        }

        return new StandardDiagnosticsDumpElement("Thread Dump", Collections.singletonList(threadDump));
    }
}
