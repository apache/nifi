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

package org.apache.nifi.mock.connectors.tests;

import org.apache.nifi.mock.connector.StandardConnectorTestRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StandardConnectorTestRunnerIsolationIT {

    private static final String CONNECTOR_CLASS = "org.apache.nifi.mock.connectors.GenerateAndLog";

    @TempDir
    private Path temporaryDirectory;

    @Test
    @Timeout(120)
    void testConcurrentRunnersUseIndependentInstanceDirectories() throws Exception {
        final Path firstInstanceDirectory = temporaryDirectory.resolve("first");
        final Path secondInstanceDirectory = temporaryDirectory.resolve("second");
        final ExecutorService executorService = Executors.newFixedThreadPool(2);
        final Queue<StandardConnectorTestRunner> runners = new ConcurrentLinkedQueue<>();

        try {
            final List<Callable<StandardConnectorTestRunner>> builders = List.of(
                    () -> createRunner(firstInstanceDirectory, runners),
                    () -> createRunner(secondInstanceDirectory, runners)
            );
            final List<Future<StandardConnectorTestRunner>> runnerFutures = executorService.invokeAll(builders);
            final StandardConnectorTestRunner firstRunner = runnerFutures.get(0).get();
            final StandardConnectorTestRunner secondRunner = runnerFutures.get(1).get();

            final Future<?> firstRun = executorService.submit(() -> {
                startAndStop(firstRunner);
                return null;
            });
            final Future<?> secondRun = executorService.submit(() -> {
                startAndStop(secondRunner);
                return null;
            });
            firstRun.get();
            secondRun.get();

            addAsset(firstRunner, "first.txt", "first-runner");
            addAsset(secondRunner, "second.txt", "second-runner");

            assertEquals(Set.of("first-runner"), new HashSet<>(readAssetContents(firstInstanceDirectory)));
            assertEquals(Set.of("second-runner"), new HashSet<>(readAssetContents(secondInstanceDirectory)));

            firstRunner.close();
            runners.remove(firstRunner);

            assertDoesNotThrow(secondRunner::validate);
            addAsset(secondRunner, "after-close.txt", "second-runner-after-close");
            assertEquals(Set.of("second-runner", "second-runner-after-close"), new HashSet<>(readAssetContents(secondInstanceDirectory)));
        } finally {
            executorService.shutdownNow();
            runners.forEach(StandardConnectorTestRunner::close);
        }

        assertTrue(Files.isDirectory(firstInstanceDirectory));
        assertTrue(Files.isDirectory(secondInstanceDirectory));
        assertTrue(Files.isDirectory(firstInstanceDirectory.resolve("work/framework")));
        assertTrue(Files.isDirectory(firstInstanceDirectory.resolve("work/extensions")));
        assertTrue(Files.isDirectory(secondInstanceDirectory.resolve("work/framework")));
        assertTrue(Files.isDirectory(secondInstanceDirectory.resolve("work/extensions")));
    }

    @Test
    void testCallerOwnedInstanceDirectoryRetainedAndReusableAfterBootstrapFailure() throws Exception {
        final Path instanceDirectory = temporaryDirectory.resolve("failed");
        Files.createDirectories(instanceDirectory);
        final Path marker = Files.writeString(instanceDirectory.resolve("marker"), "caller-owned");

        assertThrows(RuntimeException.class, () -> new StandardConnectorTestRunner.Builder()
                .connectorClassName("org.apache.nifi.mock.connectors.DoesNotExist")
                .narLibraryDirectory(new File("target/libDir"))
                .instanceDirectory(instanceDirectory)
                .build());

        assertEquals("caller-owned", Files.readString(marker));
        assertDoesNotThrow(() -> {
            try (StandardConnectorTestRunner runner = createRunner(instanceDirectory)) {
                runner.validate();
            }
        });
    }

    private StandardConnectorTestRunner createRunner(final Path instanceDirectory) {
        return new StandardConnectorTestRunner.Builder()
                .connectorClassName(CONNECTOR_CLASS)
                .narLibraryDirectory(new File("target/libDir"))
                .instanceDirectory(instanceDirectory)
                .build();
    }

    private StandardConnectorTestRunner createRunner(final Path instanceDirectory, final Queue<StandardConnectorTestRunner> runners) {
        final StandardConnectorTestRunner runner = createRunner(instanceDirectory);
        runners.add(runner);
        return runner;
    }

    private static void startAndStop(final StandardConnectorTestRunner runner) throws TimeoutException {
        runner.startConnector();
        runner.waitForDataIngested(Duration.ofSeconds(20));
        runner.stopConnector(Duration.ofSeconds(20));
    }

    private static void addAsset(final StandardConnectorTestRunner runner, final String name, final String contents) {
        runner.addAsset(name, new ByteArrayInputStream(contents.getBytes(StandardCharsets.UTF_8)));
    }

    private static List<String> readAssetContents(final Path instanceDirectory) throws Exception {
        try (Stream<Path> paths = Files.walk(instanceDirectory.resolve("connector-assets"))) {
            return paths.filter(Files::isRegularFile)
                    .sorted()
                    .map(StandardConnectorTestRunnerIsolationIT::readString)
                    .toList();
        }
    }

    private static String readString(final Path path) {
        try {
            return Files.readString(path);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

}
