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

import org.apache.commons.io.FileUtils;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processors.standard.util.SFTPTransfer;
import org.apache.nifi.processors.standard.util.SSHTestServer;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestDeleteSFTP {

    private static final int BATCH_SIZE = 2;

    private final TestRunner runner = TestRunners.newTestRunner(DeleteSFTP.class);
    private final SSHTestServer sshTestServer = new SSHTestServer();
    private Path sshServerRootPath;

    @BeforeEach
    void setRunner() throws IOException {
        sshTestServer.startServer();
        sshServerRootPath = Paths.get(sshTestServer.getVirtualFileSystemPath()).toAbsolutePath();

        runner.setProperty(SFTPTransfer.HOSTNAME, sshTestServer.getHost());
        runner.setProperty(SFTPTransfer.PORT, Integer.toString(sshTestServer.getSSHPort()));
        runner.setProperty(SFTPTransfer.USERNAME, sshTestServer.getUsername());
        runner.setProperty(SFTPTransfer.PASSWORD, sshTestServer.getPassword());
        runner.setProperty(SFTPTransfer.BATCH_SIZE, Integer.toString(BATCH_SIZE));
    }

    @AfterEach
    void clearDirectory() throws IOException {
        sshTestServer.stopServer();
        FileUtils.deleteQuietly(sshServerRootPath.toFile());
    }

    @Test
    void deletesExistingFile() throws IOException {
        final Path fileToDelete = createFile("rel/path", "test.txt");
        final MockFlowFile enqueuedFlowFile = enqueue(fileToDelete);
        assertExists(fileToDelete);

        runner.run();

        assertNotExists(fileToDelete);
        runner.assertAllFlowFilesTransferred(DeleteSFTP.REL_SUCCESS, 1);
        runner.assertAllFlowFiles(
                DeleteSFTP.REL_SUCCESS,
                flowFileInRelationship -> assertEquals(enqueuedFlowFile, flowFileInRelationship)
        );
        runner.assertProvenanceEvent(ProvenanceEventType.REMOTE_INVOCATION);
    }

    @Test
    void sendsFlowFileToNotFoundWhenFileDoesNotExist() throws IOException {
        final Path directoryPath = Files.createDirectories(sshServerRootPath.resolve("rel/path"));
        final String filename = "not-exist.txt";
        final Path fileToDelete = directoryPath.resolve(filename);
        enqueue(fileToDelete);
        assertNotExists(fileToDelete);

        runner.run();

        assertNotExists(fileToDelete);
        runner.assertAllFlowFilesTransferred(DeleteSFTP.REL_NOT_FOUND);
    }

    @Test
    void sendsFlowFileToNotFoundWhenDirectoryDoesNotExist() {
        final Path directoryPath = sshServerRootPath.resolve("rel/path");
        final String filename = "not-exist.txt";
        final Path fileToDelete = directoryPath.resolve(filename);
        enqueue(fileToDelete);
        assertNotExists(fileToDelete);

        runner.run();

        assertNotExists(fileToDelete);
        runner.assertAllFlowFilesTransferred(DeleteSFTP.REL_NOT_FOUND);
    }

    @Test
    void sendsFlowFileToFailureWhenTargetIsADirectory() throws IOException {
        Path fileToDelete = Files.createDirectories(sshServerRootPath.resolve("a/directory"));
        enqueue(fileToDelete);
        assertExists(fileToDelete);

        runner.run();

        assertExists(fileToDelete);
        runner.assertAllFlowFilesTransferred(DeleteSFTP.REL_FAILURE);
        runner.assertPenalizeCount(1);
    }

    @Test
    void sendsFlowFileToFailureWhenFileIsNotADirectChildOfTheDirectory() throws IOException {
        final Path directoryPath = Files.createDirectories(sshServerRootPath.resolve("rel/path"));
        final String filename = "../sibling.txt";
        enqueue(directoryPath.toString(), filename);
        final Path fileToDelete = Files.writeString(directoryPath.resolve(filename), "sibling content");
        assertExists(fileToDelete);

        runner.run();

        assertExists(fileToDelete);
        runner.assertAllFlowFilesTransferred(DeleteSFTP.REL_FAILURE, 1);
        runner.assertPenalizeCount(1);
    }

    @Test
    void deletesUpToBatchSizeFilesWithASingleConnection() throws IOException {
        for (int fileNumber = 1; fileNumber <= 2 * BATCH_SIZE; fileNumber++) {
            final Path fileToDelete = createFile("a/directory", "file-%d".formatted(fileNumber));
            enqueue(fileToDelete);
            assertExists(fileToDelete);
        }

        runner.run();
        runner.assertTransferCount(DeleteSFTP.REL_SUCCESS, BATCH_SIZE);
        runner.clearTransferState();

        runner.run();
        runner.assertTransferCount(DeleteSFTP.REL_SUCCESS, BATCH_SIZE);
        runner.assertQueueEmpty();
    }

    private Path createFile(String directoryPath, String filename) throws IOException {
        Path directory = Files.createDirectories(sshServerRootPath.resolve(directoryPath));

        return Files.writeString(directory.resolve(filename), "some text");
    }

    private MockFlowFile enqueue(Path path) {
        final Path relativePath = sshServerRootPath.relativize(path);

        return enqueue("/%s".formatted(relativePath.getParent()), relativePath.getFileName().toString());
    }

    private MockFlowFile enqueue(String directoryPath, String filename) {
        final Map<String, String> attributes = Map.of(
                CoreAttributes.PATH.key(), directoryPath,
                CoreAttributes.FILENAME.key(), filename
        );

        return runner.enqueue("data", attributes);
    }

    private static void assertNotExists(Path filePath) {
        assertTrue(Files.notExists(filePath), () -> "File " + filePath + "still exists");
    }

    private static void assertExists(Path filePath) {
        assertTrue(Files.exists(filePath), () -> "File " + filePath + "does not exist");
    }
}