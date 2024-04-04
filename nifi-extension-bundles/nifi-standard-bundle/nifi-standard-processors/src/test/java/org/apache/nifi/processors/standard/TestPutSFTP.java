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
import org.apache.nifi.processor.util.file.transfer.FileTransfer;
import org.apache.nifi.processors.standard.util.SFTPTransfer;
import org.apache.nifi.processors.standard.util.SSHTestServer;
import org.apache.nifi.provenance.ProvenanceEventRecord;
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
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

class TestPutSFTP {
    private static final String FLOW_FILE_CONTENTS = TestPutSFTP.class.getSimpleName();

    private static final String LOCALHOST = "localhost";

    private static final String REMOTE_DIRECTORY = "nifi_test/";

    private static final String FIRST_FILENAME = "1.txt";

    private static final int BATCH_SIZE = 2;

    private static final byte[] ZERO_BYTES = new byte[]{};

    private static final String TRANSIT_URI_FORMAT = "sftp://%s";

    private SSHTestServer sshTestServer;

    private TestRunner runner;

    @BeforeEach
    void setRunner() throws IOException {
        sshTestServer = new SSHTestServer();
        sshTestServer.startServer();

        runner = TestRunners.newTestRunner(PutSFTP.class);
        runner.setProperty(SFTPTransfer.HOSTNAME, LOCALHOST);
        runner.setProperty(SFTPTransfer.PORT, Integer.toString(sshTestServer.getSSHPort()));
        runner.setProperty(SFTPTransfer.USERNAME, sshTestServer.getUsername());
        runner.setProperty(SFTPTransfer.PASSWORD, sshTestServer.getPassword());

        runner.setProperty(SFTPTransfer.USE_KEEPALIVE_ON_TIMEOUT, Boolean.FALSE.toString());
        runner.setProperty(SFTPTransfer.STRICT_HOST_KEY_CHECKING, Boolean.FALSE.toString());
        runner.setProperty(SFTPTransfer.BATCH_SIZE, Integer.toString(BATCH_SIZE));
        runner.setProperty(SFTPTransfer.REMOTE_PATH, REMOTE_DIRECTORY);
        runner.setProperty(SFTPTransfer.REJECT_ZERO_BYTE, Boolean.TRUE.toString());
        runner.setProperty(SFTPTransfer.CONFLICT_RESOLUTION, FileTransfer.CONFLICT_RESOLUTION_REPLACE);
        runner.setProperty(SFTPTransfer.CREATE_DIRECTORY, Boolean.TRUE.toString());
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
        runner.enqueue(FLOW_FILE_CONTENTS, Collections.singletonMap(CoreAttributes.FILENAME.key(), FIRST_FILENAME));
        runner.run();

        runner.assertTransferCount(PutSFTP.REL_SUCCESS, 1);

        Path newDirectory = Paths.get(sshTestServer.getVirtualFileSystemPath() + REMOTE_DIRECTORY);
        Path newFile = Paths.get(sshTestServer.getVirtualFileSystemPath() + REMOTE_DIRECTORY + FIRST_FILENAME);
        assertTrue(newDirectory.toAbsolutePath().toFile().exists(), "New Directory not created");
        assertTrue(newFile.toAbsolutePath().toFile().exists(), "New File not created");
        runner.clearTransferState();
    }

    @Test
    void testRunZeroByteFileRejected() {
        runner.enqueue(ZERO_BYTES, Collections.singletonMap(CoreAttributes.FILENAME.key(), FIRST_FILENAME));
        runner.run();

        runner.assertTransferCount(PutSFTP.REL_REJECT, 1);
    }

    @Test
    void testRunZeroByteFileAllowed() {
        runner.setProperty(SFTPTransfer.REJECT_ZERO_BYTE, Boolean.FALSE.toString());
        runner.enqueue(ZERO_BYTES, Collections.singletonMap(CoreAttributes.FILENAME.key(), FIRST_FILENAME));
        runner.run();

        runner.assertTransferCount(PutSFTP.REL_SUCCESS, 1);
    }

    @Test
    void testRunConflictResolutionReplaceStrategy() throws IOException {
        createRemoteFile();

        runner.setProperty(SFTPTransfer.CONFLICT_RESOLUTION, FileTransfer.CONFLICT_RESOLUTION_REPLACE);
        runner.enqueue(FLOW_FILE_CONTENTS, Collections.singletonMap(CoreAttributes.FILENAME.key(), FIRST_FILENAME));
        runner.run();

        runner.assertTransferCount(PutSFTP.REL_SUCCESS, 1);
        runner.assertTransferCount(PutSFTP.REL_REJECT, 0);
        runner.assertTransferCount(PutSFTP.REL_FAILURE, 0);
    }

    @Test
    void testRunConflictResolutionRejectStrategy() throws IOException {
        createRemoteFile();

        runner.setProperty(SFTPTransfer.CONFLICT_RESOLUTION, FileTransfer.CONFLICT_RESOLUTION_REJECT);
        runner.enqueue(FLOW_FILE_CONTENTS, Collections.singletonMap(CoreAttributes.FILENAME.key(), FIRST_FILENAME));
        runner.run();

        runner.assertTransferCount(PutSFTP.REL_SUCCESS, 0);
        runner.assertTransferCount(PutSFTP.REL_REJECT, 1);
        runner.assertTransferCount(PutSFTP.REL_FAILURE, 0);
    }

    @Test
    void testRunConflictResolutionIgnoreStrategy() throws IOException {
        createRemoteFile();

        runner.setProperty(SFTPTransfer.CONFLICT_RESOLUTION, FileTransfer.CONFLICT_RESOLUTION_IGNORE);
        runner.enqueue(FLOW_FILE_CONTENTS, Collections.singletonMap(CoreAttributes.FILENAME.key(), FIRST_FILENAME));
        runner.run();

        runner.assertTransferCount(PutSFTP.REL_SUCCESS, 1);
        runner.assertTransferCount(PutSFTP.REL_REJECT, 0);
        runner.assertTransferCount(PutSFTP.REL_FAILURE, 0);
    }

    @Test
    void testRunConflictResolutionFailStrategy() throws IOException {
        createRemoteFile();

        runner.setProperty(SFTPTransfer.CONFLICT_RESOLUTION, FileTransfer.CONFLICT_RESOLUTION_FAIL);
        runner.enqueue(FLOW_FILE_CONTENTS, Collections.singletonMap(CoreAttributes.FILENAME.key(), FIRST_FILENAME));
        runner.run();

        runner.assertTransferCount(PutSFTP.REL_SUCCESS, 0);
        runner.assertTransferCount(PutSFTP.REL_REJECT, 0);
        runner.assertTransferCount(PutSFTP.REL_FAILURE, 1);
    }

    @Test
    void testRunBatching() {
        final int files = 4;
        for (int fileNumber = 1; fileNumber <= files; fileNumber++) {
            final String filename = Integer.toString(fileNumber);
            runner.enqueue(FLOW_FILE_CONTENTS, Collections.singletonMap(CoreAttributes.FILENAME.key(), filename));
        }

        runner.run();
        runner.assertTransferCount(PutSFTP.REL_SUCCESS, BATCH_SIZE);
        runner.clearTransferState();

        runner.run();
        runner.assertTransferCount(PutSFTP.REL_SUCCESS, BATCH_SIZE);
        runner.assertQueueEmpty();
    }

    @Test
    void testRunTransitUri() {
        runner.enqueue(FLOW_FILE_CONTENTS, Collections.singletonMap(CoreAttributes.FILENAME.key(), FIRST_FILENAME));

        runner.run();

        runner.assertTransferCount(PutSFTP.REL_SUCCESS, 1);

        final List<ProvenanceEventRecord> records = runner.getProvenanceEvents();
        assertFalse(records.isEmpty());

        final ProvenanceEventRecord record = records.iterator().next();
        final String firstTransitUri = String.format(TRANSIT_URI_FORMAT, LOCALHOST);
        assertTrue(record.getTransitUri().startsWith(firstTransitUri), "Transit URI not found");
    }

    private void createRemoteFile() throws IOException {
        final Path directory = Paths.get(sshTestServer.getVirtualFileSystemPath() + REMOTE_DIRECTORY);
        final Path subDirectory = Paths.get(directory.toString(), FIRST_FILENAME);
        Files.createDirectory(directory);
        Files.createFile(subDirectory);
    }
}
