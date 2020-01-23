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
import java.util.concurrent.TimeUnit;

public class ShellRunner {
    private final static Logger logger = LoggerFactory.getLogger(ShellRunner.class);

    static String SHELL = "sh";
    static String OPTS = "-c";
    static Integer TIMEOUT = 30;

    public static List<String> runShell(String command) throws IOException {
        return runShell(command, "<unknown>");
    }

    public static List<String> runShell(String command, String description) throws IOException {
        final ProcessBuilder builder = new ProcessBuilder(SHELL, OPTS, command);
        final List<String> builderCommand = builder.command();

        logger.debug("Run Command '" + description + "': " + builderCommand);
        final Process proc = builder.start();

        try {
            proc.waitFor(TIMEOUT, TimeUnit.SECONDS);
        } catch (InterruptedException irexc) {
            throw new IOException(irexc.getMessage(), irexc.getCause());
        }

        if (proc.exitValue() != 0) {
            try (final Reader stderr = new InputStreamReader(proc.getErrorStream());
                 final BufferedReader reader = new BufferedReader(stderr)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    logger.warn(line.trim());
                }
            }
            throw new IOException("Command exit non-zero: " + proc.exitValue());
        }

        final List<String> lines = new ArrayList<>();
        try (final Reader stdin = new InputStreamReader(proc.getInputStream());
             final BufferedReader reader = new BufferedReader(stdin)) {
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line.trim());
            }
        }

        return lines;
    }
}
