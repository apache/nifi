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
package org.apache.nifi.authorization.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class ShellRunner {
    private final static Logger logger = LoggerFactory.getLogger(ShellRunner.class);

    static String SHELL = "sh";
    static String OPTS = "-c";

    private final int timeoutSeconds;
    private final ExecutorService executor;

    public ShellRunner(final int timeoutSeconds) {
        this.timeoutSeconds = timeoutSeconds;
        this.executor = Executors.newFixedThreadPool(1, new ThreadFactory() {
            @Override
            public Thread newThread(final Runnable r) {
                final Thread t = Executors.defaultThreadFactory().newThread(r);
                t.setName("ShellRunner");
                t.setDaemon(true);
                return t;
            }
        });
    }

    public List<String> runShell(String command) throws IOException {
        return runShell(command, "<unknown>");
    }

    public List<String> runShell(String command, String description) throws IOException {
        final ProcessBuilder builder = new ProcessBuilder(SHELL, OPTS, command);
        builder.redirectErrorStream(true);

        final List<String> builderCommand = builder.command();
        logger.debug("Run Command '{}': {}", new Object[]{description, builderCommand});

        final Process proc = builder.start();

        final List<String> lines = new ArrayList<>();
        executor.submit(() -> {
            try {
                try (final Reader stdin = new InputStreamReader(proc.getInputStream());
                     final BufferedReader reader = new BufferedReader(stdin)) {
                    logger.trace("Reading process input stream...");

                    String line;
                    int lineCount = 0;
                    while ((line = reader.readLine()) != null) {
                        if (logger.isTraceEnabled()) {
                            logger.trace((++lineCount) + " - " + line);
                        }
                        lines.add(line.trim());
                    }

                    logger.trace("Finished reading process input stream");
                }
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        });

        boolean completed;
        try {
            completed = proc.waitFor(timeoutSeconds, TimeUnit.SECONDS);
        } catch (InterruptedException irexc) {
            throw new IOException(irexc.getMessage(), irexc.getCause());
        }

        if (!completed) {
            logger.debug("Process did not complete in allotted time, attempting to forcibly destroy process...");
            try {
                proc.destroyForcibly();
            } catch (Exception e) {
                logger.debug("Process failed to destroy: " + e.getMessage(), e);
            }
            throw new IllegalStateException("Shell command '" + command + "' did not complete during the allotted time period");
        }

        if (proc.exitValue() != 0) {
            throw new IOException("Process exited with non-zero value: " + proc.exitValue());
        }

        return lines;
    }

    public void shutdown() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5000L, TimeUnit.MILLISECONDS)) {
                logger.info("Failed to stop ShellRunner executor in 5 seconds. Terminating");
                executor.shutdownNow();
            }
        } catch (InterruptedException ie) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
