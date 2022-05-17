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

package org.apache.nifi.minifi.bootstrap.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessUtils.class);

    public static boolean isProcessRunning(String pid) {
        if (pid == null) {
            LOGGER.error("Unable to get process status due to missing process id");
            return false;
        }
        try {
            // We use the "ps" command to check if the process is still running.
            ProcessBuilder builder = new ProcessBuilder();

            builder.command("ps", "-p", pid);
            Process proc = builder.start();

            // Look for the pid in the output of the 'ps' command.
            boolean running = false;
            String line;
            try (InputStream in = proc.getInputStream();
                Reader streamReader = new InputStreamReader(in);
                BufferedReader reader = new BufferedReader(streamReader)) {

                while ((line = reader.readLine()) != null) {
                    if (line.trim().startsWith(pid)) {
                        running = true;
                    }
                }
            }

            // If output of the ps command had our PID, the process is running.
            LOGGER.debug("Process with PID {} is {}running", pid, running ? "" : "not ");

            return running;
        } catch (IOException ioe) {
            LOGGER.error("Failed to determine if Process {} is running; assuming that it is not", pid);
            return false;
        }
    }

    public static void gracefulShutDownMiNiFiProcess(String pid, String s, int gracefulShutdownSeconds) {
        long startWait = System.nanoTime();
        while (ProcessUtils.isProcessRunning(pid)) {
            LOGGER.info("Waiting for Apache MiNiFi to finish shutting down...");
            long waitNanos = System.nanoTime() - startWait;
            long waitSeconds = TimeUnit.NANOSECONDS.toSeconds(waitNanos);
            if (waitSeconds >= gracefulShutdownSeconds || gracefulShutdownSeconds == 0) {
                if (ProcessUtils.isProcessRunning(pid)) {
                    LOGGER.warn(s, gracefulShutdownSeconds);
                    try {
                        ProcessUtils.killProcessTree(pid);
                    } catch (IOException ioe) {
                        LOGGER.error("Failed to kill Process with PID {}", pid);
                    }
                }
                break;
            } else {
                try {
                    Thread.sleep(2000L);
                } catch (InterruptedException ie) {
                }
            }
        }
    }

    public static void killProcessTree(String pid) throws IOException {
        LOGGER.debug("Killing Process Tree for PID {}", pid);

        List<String> children = getChildProcesses(pid);
        LOGGER.debug("Children of PID {}: {}", pid, children);

        for (String childPid : children) {
            killProcessTree(childPid);
        }

        Runtime.getRuntime().exec(new String[]{"kill", "-9", pid});
    }

    public static boolean isAlive(Process process) {
        try {
            process.exitValue();
            return false;
        } catch (IllegalStateException | IllegalThreadStateException itse) {
            return true;
        }
    }

    private static List<String> getChildProcesses(String ppid) throws IOException {
        Process proc = Runtime.getRuntime().exec(new String[]{"ps", "-o", "pid", "--no-headers", "--ppid", ppid});
        List<String> childPids = new ArrayList<>();
        try (InputStream in = proc.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {

            String line;
            while ((line = reader.readLine()) != null) {
                childPids.add(line.trim());
            }
        }

        return childPids;
    }
}
