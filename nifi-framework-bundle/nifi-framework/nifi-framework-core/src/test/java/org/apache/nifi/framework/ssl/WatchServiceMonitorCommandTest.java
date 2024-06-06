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
package org.apache.nifi.framework.ssl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WatchServiceMonitorCommandTest {
    @TempDir
    private Path tempDir;

    @Test
    void testRunChangedPathsNotFound() throws IOException {
        final FileSystem fileSystem = FileSystems.getDefault();
        try (WatchService watchService = fileSystem.newWatchService()) {
            registerTempDir(watchService);

            final List<Path> changedPaths = new ArrayList<>();

            final WatchServiceMonitorCommand command = new WatchServiceMonitorCommand(watchService, changedPaths::addAll);

            command.run();

            assertTrue(changedPaths.isEmpty());
        }
    }

    @Timeout(5)
    @Test
    void testRunChangedPathsFound() throws IOException {
        final FileSystem fileSystem = FileSystems.getDefault();
        try (WatchService watchService = fileSystem.newWatchService()) {
            registerTempDir(watchService);

            final List<Path> changedPaths = new ArrayList<>();

            final WatchServiceMonitorCommand command = new WatchServiceMonitorCommand(watchService, changedPaths::addAll);

            final Path tempFile = Files.createTempFile(tempDir, WatchServiceMonitorCommandTest.class.getSimpleName(), null);
            while (changedPaths.isEmpty()) {
                command.run();
            }

            final Path firstChangedPath = changedPaths.getFirst();
            assertEquals(tempFile.getFileName(), firstChangedPath.getFileName());
        }
    }

    private void registerTempDir(final WatchService watchService) throws IOException {
        tempDir.register(watchService, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY);
    }
}
