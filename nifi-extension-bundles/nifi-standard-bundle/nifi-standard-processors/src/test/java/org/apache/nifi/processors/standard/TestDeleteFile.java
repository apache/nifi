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

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestDeleteFile {

    private final TestRunner runner = TestRunners.newTestRunner(DeleteFile.class);

    @TempDir
    private Path testDirectory;

    @Test
    void deletesExistingFile() throws IOException {
        final Path directoryPath = testDirectory.toAbsolutePath();
        final String filename = "test.txt";
        final MockFlowFile enqueuedFlowFile = enqueue(directoryPath.toString(), filename);
        final Path fileToDelete = Files.writeString(testDirectory.resolve(filename), "some text");
        assertExists(fileToDelete);

        runner.run();

        assertNotExists(fileToDelete);
        runner.assertAllFlowFilesTransferred(DeleteFile.REL_SUCCESS, 1);
        runner.assertAllFlowFiles(
                DeleteFile.REL_SUCCESS,
                flowFileInRelationship -> assertEquals(enqueuedFlowFile, flowFileInRelationship)
        );
        runner.assertProvenanceEvent(ProvenanceEventType.REMOTE_INVOCATION);
    }

    @Test
    void deletesExistingEmptyDirectory() throws IOException {
        final Path directoryPath = testDirectory.toAbsolutePath();
        final String filename = "test-directory";
        enqueue(directoryPath.toString(), filename);
        final Path fileToDelete = Files.createDirectory(testDirectory.resolve(filename));
        assertExists(fileToDelete);

        runner.run();

        assertNotExists(fileToDelete);
        runner.assertAllFlowFilesTransferred(DeleteFile.REL_SUCCESS);
    }

    @Test
    void sendsFlowFileToNotFoundWhenFileDoesNotExist() {
        final Path directoryPath = testDirectory.toAbsolutePath();
        final String filename = "test.txt";
        enqueue(directoryPath.toString(), filename);
        final Path fileToDelete = testDirectory.resolve(filename);
        assertNotExists(fileToDelete);

        runner.run();

        assertNotExists(fileToDelete);
        runner.assertAllFlowFilesTransferred(DeleteFile.REL_NOT_FOUND);
    }

    @Test
    void sendsFlowFileToNotFoundWhenDirectoryDoesNotExist() {
        final Path directoryPath = testDirectory.resolve("non-existing-directory").toAbsolutePath();
        final String filename = "test.txt";
        enqueue(directoryPath.toString(), filename);
        final Path fileToDelete = testDirectory.resolve(filename);
        assertNotExists(fileToDelete);

        runner.run();

        assertNotExists(fileToDelete);
        runner.assertAllFlowFilesTransferred(DeleteFile.REL_NOT_FOUND);
    }

    @Test
    void sendsFlowFileToFailureWhenTargetIsAnNonEmptyDirectory() throws IOException {
        final Path directoryPath = testDirectory.toAbsolutePath();
        final String filename = "test-directory";
        enqueue(directoryPath.toString(), filename);
        final Path fileToDelete = Files.createDirectory(testDirectory.resolve(filename));
        Files.writeString(testDirectory.resolve(filename).resolve("disturbance"), "not empty");
        assertExists(fileToDelete);

        runner.run();

        assertExists(fileToDelete);
        runner.assertAllFlowFilesTransferred(DeleteFile.REL_FAILURE);
        runner.assertPenalizeCount(1);
    }

    @Test
    void sendsFlowFileToFailureWhenFileIsNotADirectChildOfTheDirectory() throws IOException {
        final Path directory = Files.createDirectory(testDirectory.resolve("test-directory")).toAbsolutePath();
        final String filename = "../sibling.txt";
        enqueue(directory.toString(), filename);
        final Path fileToDelete = Files.writeString(directory.resolve(filename), "sibling content");
        assertExists(fileToDelete);

        runner.run();

        assertExists(fileToDelete);
        runner.assertAllFlowFilesTransferred(DeleteFile.REL_FAILURE, 1);
        runner.assertPenalizeCount(1);
    }

    private MockFlowFile enqueue(String directoryPath, String filename) {
        final Map<String, String> attributes = Map.of(
                CoreAttributes.ABSOLUTE_PATH.key(), directoryPath,
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