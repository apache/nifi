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
package org.apache.nifi.processors.standard;

import org.apache.nifi.processor.util.file.transfer.FetchFileTransfer;
import org.apache.nifi.processors.standard.util.SFTPTransfer;
import org.apache.nifi.processors.standard.util.SSHTestServer;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FetchSFTPTest {

    private static final String SOURCE_FILENAME = "test.txt";
    private static final String SOURCE_CONTENTS = "source content";
    private static final String DESTINATION_DIR = "/completed";

    private SSHTestServer sshTestServer;
    private TestRunner runner;
    private Path serverRootPath;

    @BeforeEach
    void setup(@TempDir final Path rootPath) throws IOException {
        sshTestServer = new SSHTestServer(rootPath);
        sshTestServer.startServer();
        serverRootPath = rootPath;

        runner = TestRunners.newTestRunner(FetchSFTP.class);
        runner.setProperty(FetchFileTransfer.HOSTNAME, sshTestServer.getHost());
        runner.setProperty(FetchFileTransfer.UNDEFAULTED_PORT, Integer.toString(sshTestServer.getSSHPort()));
        runner.setProperty(SFTPTransfer.USERNAME, sshTestServer.getUsername());
        runner.setProperty(SFTPTransfer.PASSWORD, sshTestServer.getPassword());
        runner.setProperty(SFTPTransfer.USE_KEEPALIVE_ON_TIMEOUT, Boolean.FALSE.toString());
        runner.setProperty(SFTPTransfer.STRICT_HOST_KEY_CHECKING, Boolean.FALSE.toString());
        runner.setProperty(SFTPTransfer.DATA_TIMEOUT, "30 sec");
    }

    @AfterEach
    void stopServer() throws IOException {
        sshTestServer.stopServer();
    }

    @Test
    void testMoveCompletionStrategyCleansUpSourceFile() throws IOException {
        final Path sourceDir = Files.createDirectory(serverRootPath.resolve("source"));
        final Path sourceFile = sourceDir.resolve(SOURCE_FILENAME);
        Files.writeString(sourceFile, SOURCE_CONTENTS, StandardCharsets.UTF_8);

        final Path completedDir = Files.createDirectory(serverRootPath.resolve("completed"));

        runner.setProperty(FetchFileTransfer.REMOTE_FILENAME, "/source/" + SOURCE_FILENAME);
        runner.setProperty(FetchFileTransfer.COMPLETION_STRATEGY, FetchFileTransfer.COMPLETION_MOVE.getValue());
        runner.setProperty(FetchFileTransfer.MOVE_DESTINATION_DIR, DESTINATION_DIR);
        runner.setProperty(FetchFileTransfer.MOVE_CREATE_DIRECTORY, Boolean.FALSE.toString());

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(FetchFileTransfer.REL_SUCCESS, 1);
        assertFalse(Files.exists(sourceFile), "Source file should have been moved");
        assertTrue(Files.exists(completedDir.resolve(SOURCE_FILENAME)), "File should exist in destination directory");
    }

    @Test
    void testMoveCompletionStrategyOverwritesExistingDestinationFile() throws IOException {
        // When conflict resolution is set to Replace, FetchSFTP should proactively delete the destination
        // before renaming. This mirrors the behaviour needed for SFTP servers (e.g. OpenSSH) that return
        // SSH_FX_FAILURE on rename when the destination already exists.
        final Path sourceDir = Files.createDirectory(serverRootPath.resolve("source"));
        final Path sourceFile = sourceDir.resolve(SOURCE_FILENAME);
        Files.writeString(sourceFile, SOURCE_CONTENTS, StandardCharsets.UTF_8);

        final Path completedDir = Files.createDirectory(serverRootPath.resolve("completed"));
        final Path existingDestination = completedDir.resolve(SOURCE_FILENAME);
        Files.writeString(existingDestination, "old content", StandardCharsets.UTF_8);

        runner.setProperty(FetchFileTransfer.REMOTE_FILENAME, "/source/" + SOURCE_FILENAME);
        runner.setProperty(FetchFileTransfer.COMPLETION_STRATEGY, FetchFileTransfer.COMPLETION_MOVE.getValue());
        runner.setProperty(FetchFileTransfer.MOVE_DESTINATION_DIR, DESTINATION_DIR);
        runner.setProperty(FetchFileTransfer.MOVE_CREATE_DIRECTORY, Boolean.FALSE.toString());
        runner.setProperty(FetchFileTransfer.MOVE_CONFLICT_RESOLUTION, FetchFileTransfer.MoveConflictResolution.REPLACE.getValue());

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(FetchFileTransfer.REL_SUCCESS, 1);
        assertFalse(Files.exists(sourceFile), "Source file should have been moved");
        assertTrue(Files.exists(existingDestination), "Destination file should exist after move");
    }
}
