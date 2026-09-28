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
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StandardConnectorTestRunnerIsolationIT {

    private static final String CONNECTOR_CLASS = "org.apache.nifi.mock.connectors.GenerateAndLog";

    @TempDir
    private Path temporaryDirectory;

    @Test
    @Timeout(120)
    void testIndependentInstanceDirectories() throws Exception {
        final Path firstInstanceDirectory = temporaryDirectory.resolve("first");
        final Path secondInstanceDirectory = temporaryDirectory.resolve("second");
        StandardConnectorTestRunner firstRunner = createRunner(firstInstanceDirectory);
        StandardConnectorTestRunner secondRunner = null;

        try {
            secondRunner = createRunner(secondInstanceDirectory);

            startAndStop(firstRunner);
            startAndStop(secondRunner);

            addAsset(firstRunner, "first.txt", "first-runner");
            addAsset(secondRunner, "second.txt", "second-runner");

            assertEquals(Set.of("first-runner"), new HashSet<>(readAssetContents(firstInstanceDirectory)));
            assertEquals(Set.of("second-runner"), new HashSet<>(readAssetContents(secondInstanceDirectory)));

            firstRunner.close();
            firstRunner = null;

            secondRunner.validate();
            addAsset(secondRunner, "after-close.txt", "second-runner-after-close");
            assertEquals(Set.of("second-runner", "second-runner-after-close"), new HashSet<>(readAssetContents(secondInstanceDirectory)));
        } finally {
            if (firstRunner != null) {
                firstRunner.close();
            }

            if (secondRunner != null) {
                secondRunner.close();
            }
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
                .instanceDirectory(instanceDirectory.toFile())
                .build());

        assertEquals("caller-owned", Files.readString(marker));
        try (final StandardConnectorTestRunner runner = createRunner(instanceDirectory)) {
            runner.validate();
        }
    }

    private StandardConnectorTestRunner createRunner(final Path instanceDirectory) {
        return new StandardConnectorTestRunner.Builder()
                .connectorClassName(CONNECTOR_CLASS)
                .narLibraryDirectory(new File("target/libDir"))
                .instanceDirectory(instanceDirectory.toFile())
                .build();
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
        try (final Stream<Path> paths = Files.walk(instanceDirectory.resolve("connector-assets"))) {
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
