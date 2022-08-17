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
import org.apache.nifi.processors.standard.util.FileTransfer;
import org.apache.nifi.processors.standard.util.SFTPTransfer;
import org.apache.nifi.processors.standard.util.SSHTestServer;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

class TestPutSFTP {
    private static final String FLOW_FILE_CONTENTS = TestPutSFTP.class.getSimpleName();

    private SSHTestServer sshTestServer;

    private TestRunner runner;

    @BeforeEach
    void setRunner() throws IOException {
        sshTestServer = new SSHTestServer();
        sshTestServer.startServer();

        runner = TestRunners.newTestRunner(PutSFTP.class);
        runner.setProperty(SFTPTransfer.HOSTNAME, "localhost");
        runner.setProperty(SFTPTransfer.PORT, Integer.toString(sshTestServer.getSSHPort()));
        runner.setProperty(SFTPTransfer.USERNAME, sshTestServer.getUsername());
        runner.setProperty(SFTPTransfer.PASSWORD, sshTestServer.getPassword());
        runner.setProperty(SFTPTransfer.STRICT_HOST_KEY_CHECKING, "false");
        runner.setProperty(SFTPTransfer.BATCH_SIZE, "2");
        runner.setProperty(SFTPTransfer.REMOTE_PATH, "nifi_test/");
        runner.setProperty(SFTPTransfer.REJECT_ZERO_BYTE, "true");
        runner.setProperty(SFTPTransfer.CONFLICT_RESOLUTION, FileTransfer.CONFLICT_RESOLUTION_REPLACE);
        runner.setProperty(SFTPTransfer.CREATE_DIRECTORY, "true");
        runner.setProperty(SFTPTransfer.DATA_TIMEOUT, "30 sec");
        runner.setValidateExpressionUsage(false);
    }

    @AfterEach
    void clearDirectory() throws IOException {
        sshTestServer.stopServer();
        final Path fileSystemPath = Paths.get(sshTestServer.getVirtualFileSystemPath());
        FileUtils.deleteQuietly(fileSystemPath.toFile());
    }

    @Test
    void testRunNewDirectory() {
        runner.enqueue(FLOW_FILE_CONTENTS, Collections.singletonMap(CoreAttributes.FILENAME.key(), "1.txt"));
        runner.run();

        runner.assertTransferCount(PutSFTP.REL_SUCCESS, 1);

        //verify directory exists
        Path newDirectory = Paths.get(sshTestServer.getVirtualFileSystemPath() + "nifi_test/");
        Path newFile = Paths.get(sshTestServer.getVirtualFileSystemPath() + "nifi_test/1.txt");
        assertTrue(newDirectory.toAbsolutePath().toFile().exists(), "New Directory not created");
        assertTrue(newFile.toAbsolutePath().toFile().exists(), "New File not created");
        runner.clearTransferState();
    }

    @Test
    void testRunZeroByteFile() {
        runner.enqueue(FLOW_FILE_CONTENTS, Collections.singletonMap(CoreAttributes.FILENAME.key(), "1.txt"));
        runner.enqueue("", Collections.singletonMap(CoreAttributes.FILENAME.key(), "2.txt"));

        runner.run();

        //Two files in batch, should have only 1 transferred to sucess, 1 to failure
        runner.assertTransferCount(PutSFTP.REL_SUCCESS, 1);
        runner.assertTransferCount(PutSFTP.REL_REJECT, 1);
        runner.clearTransferState();

        runner.enqueue("", Collections.singletonMap(CoreAttributes.FILENAME.key(), "1.txt"));

        runner.run();

        //One files in batch, should have 0 transferred to output since it's zero byte
        runner.assertTransferCount(PutSFTP.REL_REJECT, 1);
        runner.assertTransferCount(PutSFTP.REL_SUCCESS, 0);
        runner.clearTransferState();

        //allow zero byte files
        runner.setProperty(SFTPTransfer.REJECT_ZERO_BYTE, "false");

        runner.enqueue("", Collections.singletonMap(CoreAttributes.FILENAME.key(), "1.txt"));

        runner.run();

        //should have 1 transferred to sucess
        runner.assertTransferCount(PutSFTP.REL_SUCCESS, 1);

        //revert settings
        runner.setProperty(SFTPTransfer.REJECT_ZERO_BYTE, "true");
        runner.clearTransferState();
    }

    @Test
    void testRunConflictResolution() throws IOException {
        final String directoryName = "nifi_test";
        final String filename = "1";

        final Path directory = Paths.get(sshTestServer.getVirtualFileSystemPath() + directoryName );
        final Path subDirectory = Paths.get(directory.toString(), filename);
        Files.createDirectory(directory);
        Files.createFile(subDirectory);

        final Map<String, String> flowFileAttributes = Collections.singletonMap(CoreAttributes.FILENAME.key(), filename);

        // REPLACE Strategy
        runner.setProperty(SFTPTransfer.CONFLICT_RESOLUTION, FileTransfer.CONFLICT_RESOLUTION_REPLACE);

        runner.enqueue(FLOW_FILE_CONTENTS, flowFileAttributes);
        runner.run();

        runner.assertTransferCount(PutSFTP.REL_SUCCESS, 1);
        runner.assertTransferCount(PutSFTP.REL_REJECT, 0);
        runner.assertTransferCount(PutSFTP.REL_FAILURE, 0);
        runner.clearTransferState();

        // REJECT Strategy
        runner.setProperty(SFTPTransfer.CONFLICT_RESOLUTION, FileTransfer.CONFLICT_RESOLUTION_REJECT);

        runner.enqueue(FLOW_FILE_CONTENTS, flowFileAttributes);
        runner.run();

        runner.assertTransferCount(PutSFTP.REL_SUCCESS, 0);
        runner.assertTransferCount(PutSFTP.REL_REJECT, 1);
        runner.assertTransferCount(PutSFTP.REL_FAILURE, 0);
        runner.clearTransferState();

        // IGNORE Strategy
        runner.setProperty(SFTPTransfer.CONFLICT_RESOLUTION, FileTransfer.CONFLICT_RESOLUTION_IGNORE);
        runner.enqueue(FLOW_FILE_CONTENTS, flowFileAttributes);
        runner.run();

        runner.assertTransferCount(PutSFTP.REL_SUCCESS, 1);
        runner.assertTransferCount(PutSFTP.REL_REJECT, 0);
        runner.assertTransferCount(PutSFTP.REL_FAILURE, 0);

        runner.clearTransferState();

        // FAIL Strategy
        runner.setProperty(SFTPTransfer.CONFLICT_RESOLUTION, FileTransfer.CONFLICT_RESOLUTION_FAIL);
        runner.enqueue(FLOW_FILE_CONTENTS, flowFileAttributes);
        runner.run();

        runner.assertTransferCount(PutSFTP.REL_SUCCESS, 0);
        runner.assertTransferCount(PutSFTP.REL_REJECT, 0);
        runner.assertTransferCount(PutSFTP.REL_FAILURE, 1);

        runner.clearTransferState();
    }

    @Test
    void testRunBatching() {
        runner.setProperty(SFTPTransfer.BATCH_SIZE, "2");

        runner.enqueue(FLOW_FILE_CONTENTS, Collections.singletonMap(CoreAttributes.FILENAME.key(), "1.txt"));
        runner.enqueue(FLOW_FILE_CONTENTS, Collections.singletonMap(CoreAttributes.FILENAME.key(), "2.txt"));
        runner.enqueue(FLOW_FILE_CONTENTS, Collections.singletonMap(CoreAttributes.FILENAME.key(), "3.txt"));
        runner.enqueue(FLOW_FILE_CONTENTS, Collections.singletonMap(CoreAttributes.FILENAME.key(), "4.txt"));
        runner.enqueue(FLOW_FILE_CONTENTS, Collections.singletonMap(CoreAttributes.FILENAME.key(), "5.txt"));

        runner.run();
        runner.assertTransferCount(PutSFTP.REL_SUCCESS, 2);

        runner.clearTransferState();

        runner.run();
        runner.assertTransferCount(PutSFTP.REL_SUCCESS, 2);

        runner.clearTransferState();

        runner.run();
        runner.assertTransferCount(PutSFTP.REL_SUCCESS, 1);
        runner.clearTransferState();
    }

    @Test
    void testRunTransitUri() {
        runner.setProperty(SFTPTransfer.REJECT_ZERO_BYTE, "false");
        Map<String,String> attributes = new HashMap<>();
        attributes.put("filename", "testfile.txt");
        attributes.put("transfer-host","localhost");
        runner.enqueue(FLOW_FILE_CONTENTS, attributes);

        attributes = new HashMap<>();
        attributes.put("filename", "testfile1.txt");
        attributes.put("transfer-host","127.0.0.1");

        runner.enqueue(FLOW_FILE_CONTENTS, attributes);
        runner.run();

        runner.assertTransferCount(PutSFTP.REL_SUCCESS, 2);
        runner.getProvenanceEvents().forEach(k -> assertTrue(k.getTransitUri().contains("sftp://localhost")));
        //Two files in batch, should have 2 transferred to success, 0 to failure
        runner.assertTransferCount(PutSFTP.REL_SUCCESS, 2);
        runner.assertTransferCount(PutSFTP.REL_REJECT, 0);

        MockFlowFile flowFile1 = runner.getFlowFilesForRelationship(PutFileTransfer.REL_SUCCESS).get(0);
        MockFlowFile flowFile2 = runner.getFlowFilesForRelationship(PutFileTransfer.REL_SUCCESS).get(1);
        runner.clearProvenanceEvents();
        runner.clearTransferState();

        //Test different destinations on flow file attributes
        runner.setProperty(SFTPTransfer.HOSTNAME,"${transfer-host}"); //set to derive hostname

        runner.setThreadCount(1);
        runner.enqueue(flowFile1);
        runner.enqueue(flowFile2);
        runner.run();

        runner.assertTransferCount(PutSFTP.REL_SUCCESS, 2);
        assertTrue(runner.getProvenanceEvents().get(0).getTransitUri().contains("sftp://localhost"));
        assertTrue(runner.getProvenanceEvents().get(1).getTransitUri().contains("sftp://127.0.0.1"));

        runner.clearProvenanceEvents();
        runner.clearTransferState();
    }
}