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
package org.apache.nifi.bootstrap.process;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RuntimeValidatorExecutorTest {
    @TempDir
    private File tempDir;

    private RuntimeValidatorExecutor runtimeValidatorExecutor;

    @Test
    public void testAllSatisfactory() throws IOException {
        final List<RuntimeValidator> configurationClasses = getAllTestConfigurationClasses();
        runtimeValidatorExecutor = new RuntimeValidatorExecutor(configurationClasses);

        final List<RuntimeValidatorResult> results = runtimeValidatorExecutor.execute();
        assertEquals(5, results.size());
        final List<RuntimeValidatorResult> failures = getFailures(results);
        assertEquals(0, failures.size());
    }

    @Test
    public void testAllFailuresEmptyFiles() throws IOException {
        final File emptyFile = getTempFile("empty_file", "");
        runtimeValidatorExecutor = new RuntimeValidatorExecutor(getConfigurationClassesWithFile(emptyFile));

        final List<RuntimeValidatorResult> results = runtimeValidatorExecutor.execute();
        assertEquals(5, results.size());
        final List<RuntimeValidatorResult> failures = getFailures(results);
        assertEquals(5, failures.size());
        for (final RuntimeValidatorResult failure : failures) {
            assertTrue(failure.getExplanation().contains("parse"));
        }
    }

    @Test
    public void testAllFailuresUnparsable() throws IOException {
        final File unparsableFile = getTempFile("unparsable", "abcdefghijklmnopqrstuvwxyz");
        runtimeValidatorExecutor = new RuntimeValidatorExecutor(getConfigurationClassesWithFile(unparsableFile));

        final List<RuntimeValidatorResult> results = runtimeValidatorExecutor.execute();
        assertEquals(5, results.size());
        final List<RuntimeValidatorResult> failures = getFailures(results);
        assertEquals(5, failures.size());
        for (final RuntimeValidatorResult failure : failures) {
            assertTrue(failure.getExplanation().contains("parse"));
        }
    }

    @Test
    public void testCannotFindFilesForConfiguration() {
        final File missingFile = new File("missing_file");
        runtimeValidatorExecutor = new RuntimeValidatorExecutor(getConfigurationClassesWithFile(missingFile));

        final List<RuntimeValidatorResult> results = runtimeValidatorExecutor.execute();
        assertEquals(5, results.size());
        final List<RuntimeValidatorResult> skipped = getSkipped(results);
        assertEquals(5, skipped.size());
        for (final RuntimeValidatorResult result : skipped) {
            assertTrue(result.getExplanation().contains("read"));
        }
    }

    @Test
    public void testNotEnoughAvailablePorts() throws IOException {
        final List<RuntimeValidator> configurationClasses = new ArrayList<>();
        configurationClasses.add(new AvailableLocalPorts(getTempFile("available_ports_not_enough", "0   1")));
        runtimeValidatorExecutor = new RuntimeValidatorExecutor(configurationClasses);

        final List<RuntimeValidatorResult> results = runtimeValidatorExecutor.execute();
        assertEquals(1, results.size());
        final List<RuntimeValidatorResult> failures = getFailures(results);
        assertEquals(1, failures.size());
        for (final RuntimeValidatorResult failure : failures) {
            assertTrue(failure.getExplanation().contains("less than"));
        }
    }

    @Test
    public void testNotEnoughFileHandlesAndForkedProcesses() {
        final List<RuntimeValidator> configurationClasses = new ArrayList<>();
        configurationClasses.add(new FileHandles(getTestFile("limits_not_enough")));
        configurationClasses.add(new ForkedProcesses(getTestFile("limits_not_enough")));
        runtimeValidatorExecutor = new RuntimeValidatorExecutor(configurationClasses);

        final List<RuntimeValidatorResult> results = runtimeValidatorExecutor.execute();
        assertEquals(4, results.size());
        final List<RuntimeValidatorResult> failures = getFailures(results);
        assertEquals(4, failures.size());
        for (final RuntimeValidatorResult failure : failures) {
            assertTrue(failure.getExplanation().contains("less than"));
        }
    }

    @Test
    public void testHighSwappiness() throws IOException {
        final List<RuntimeValidator> configurationClasses = new ArrayList<>();
        configurationClasses.add(new Swappiness(getTempFile("swappiness_high", "50")));
        runtimeValidatorExecutor = new RuntimeValidatorExecutor(configurationClasses);

        final List<RuntimeValidatorResult> results = runtimeValidatorExecutor.execute();
        assertEquals(1, results.size());
        final List<RuntimeValidatorResult> failures = getFailures(results);
        assertEquals(1, failures.size());
        for (final RuntimeValidatorResult failure : failures) {
            assertTrue(failure.getExplanation().contains("more than"));
        }
    }

    @Test
    public void testHighTimedWaitDuration() throws IOException {
        final List<RuntimeValidator> configurationClasses = new ArrayList<>();
        configurationClasses.add(new SocketTimedWaitDuration(getTempFile("tcp_tw_timeout_high", "50")));
        runtimeValidatorExecutor = new RuntimeValidatorExecutor(configurationClasses);

        final List<RuntimeValidatorResult> results = runtimeValidatorExecutor.execute();
        assertEquals(1, results.size());
        final List<RuntimeValidatorResult> failures = getFailures(results);
        assertEquals(1, failures.size());
        for (final RuntimeValidatorResult failure : failures) {
            assertTrue(failure.getExplanation().contains("more than"));
        }
    }

    private List<RuntimeValidatorResult> getFailures(final List<RuntimeValidatorResult> results) {
        return results
                .stream()
                .filter((result) -> result.getOutcome().equals(RuntimeValidatorResult.Outcome.FAILED))
                .collect(Collectors.toList());
    }

    private List<RuntimeValidatorResult> getSkipped(final List<RuntimeValidatorResult> results) {
        return results
                .stream()
                .filter((result) -> result.getOutcome().equals(RuntimeValidatorResult.Outcome.SKIPPED))
                .collect(Collectors.toList());
    }

    private File getTestFile(final String filename) {
        final ClassLoader classLoader = this.getClass().getClassLoader();
        final URL url = classLoader.getResource(filename);
        if (url == null) {
            throw new IllegalStateException(String.format("File [%s] not found", filename));
        }
        return new File(url.getFile());
    }

    private File getTempFile(final String fileName, final String text) throws IOException {
        final File tempFile = new File(tempDir, fileName);
        Files.write(tempFile.toPath(), text.getBytes());
        return tempFile;
    }

    private List<RuntimeValidator> getAllTestConfigurationClasses() throws IOException {
        final List<RuntimeValidator> configurationClasses = new ArrayList<>();
        configurationClasses.add(new AvailableLocalPorts(getTempFile("available_ports", "1 550001")));
        configurationClasses.add(new FileHandles(getTestFile("limits")));
        configurationClasses.add(new ForkedProcesses(getTestFile("limits")));
        configurationClasses.add(new Swappiness(getTempFile("swappiness", "0")));
        configurationClasses.add(new SocketTimedWaitDuration(getTempFile("tcp_tw_timeout", "1")));
        return configurationClasses;
    }

    private List<RuntimeValidator> getConfigurationClassesWithFile(final File file) {
        final List<RuntimeValidator> configurationClasses = new ArrayList<>();
        configurationClasses.add(new AvailableLocalPorts(file));
        configurationClasses.add(new FileHandles(file));
        configurationClasses.add(new ForkedProcesses(file));
        configurationClasses.add(new Swappiness(file));
        configurationClasses.add(new SocketTimedWaitDuration(file));
        return configurationClasses;
    }
}
