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
import org.apache.nifi.processors.standard.util.SFTPTransfer;
import org.apache.nifi.processors.standard.util.SSHTestServer;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestGetSFTP {

    private TestRunner runner;
    private static SSHTestServer sshTestServer;

    @BeforeAll
    public static void startServer() throws IOException {
        sshTestServer = new SSHTestServer();
        sshTestServer.startServer();
    }

    @AfterAll
    public static void stopServer() throws IOException {
        sshTestServer.stopServer();
    }

    @BeforeEach
    public void setup() {
        runner = TestRunners.newTestRunner(GetSFTP.class);
        runner.setProperty(SFTPTransfer.HOSTNAME, sshTestServer.getHost());
        runner.setProperty(SFTPTransfer.PORT, Integer.toString(sshTestServer.getSSHPort()));
        runner.setProperty(SFTPTransfer.USERNAME, sshTestServer.getUsername());
        runner.setProperty(SFTPTransfer.PASSWORD, sshTestServer.getPassword());

        runner.setProperty(SFTPTransfer.USE_KEEPALIVE_ON_TIMEOUT, Boolean.FALSE.toString());
        runner.setProperty(SFTPTransfer.STRICT_HOST_KEY_CHECKING, Boolean.FALSE.toString());
        runner.setProperty(SFTPTransfer.DATA_TIMEOUT, "30 sec");
        runner.setProperty(SFTPTransfer.REMOTE_PATH, "/");
        runner.removeProperty(SFTPTransfer.FILE_FILTER_REGEX);
        runner.setProperty(SFTPTransfer.PATH_FILTER_REGEX, "");
        runner.setProperty(SFTPTransfer.POLLING_INTERVAL, "60 sec");
        runner.setProperty(SFTPTransfer.RECURSIVE_SEARCH, "false");
        runner.setProperty(SFTPTransfer.IGNORE_DOTTED_FILES, "true");
        runner.setProperty(SFTPTransfer.DELETE_ORIGINAL, "true");
        runner.setProperty(SFTPTransfer.MAX_SELECTS, "100");
        runner.setProperty(SFTPTransfer.REMOTE_POLL_BATCH_SIZE, "5000");

        runner.setValidateExpressionUsage(false);
    }

    @Test
    public void testGetSFTPFileBasicRead() throws IOException {
        emptyTestDirectory();

        touchFile(sshTestServer.getVirtualFileSystemPath() + "testFile1.txt");
        touchFile(sshTestServer.getVirtualFileSystemPath() + "testFile2.txt");
        touchFile(sshTestServer.getVirtualFileSystemPath() + "testFile3.txt");
        touchFile(sshTestServer.getVirtualFileSystemPath() + "testFile4.txt");

        runner.run();

        runner.assertTransferCount(GetSFTP.REL_SUCCESS, 4);

        //Verify files deleted
        for (int i = 1; i < 5; i++) {
            Path file1 = Paths.get(sshTestServer.getVirtualFileSystemPath() + "/testFile" + i + ".txt");
            assertFalse(file1.toAbsolutePath().toFile().exists(), "File not deleted.");
        }

        runner.clearTransferState();
    }

    @Test
    public void testGetSFTPShouldNotThrowIOExceptionIfUserHomeDirNotExixts() throws IOException {
        emptyTestDirectory();

        String userHome = System.getProperty("user.home");
        try {
            // Set 'user.home' system property value to not_existdir
            System.setProperty("user.home", "/not_existdir");
            touchFile(sshTestServer.getVirtualFileSystemPath() + "testFile1.txt");
            touchFile(sshTestServer.getVirtualFileSystemPath() + "testFile2.txt");

            runner.run();

            runner.assertTransferCount(GetSFTP.REL_SUCCESS, 2);

            // Verify files deleted
            for (int i = 1; i < 3; i++) {
                Path file1 = Paths.get(sshTestServer.getVirtualFileSystemPath() + "/testFile" + i + ".txt");
                assertFalse(file1.toAbsolutePath().toFile().exists(), "File not deleted.");
            }

            runner.clearTransferState();

        } finally {
            // set back the original value for 'user.home' system property
            System.setProperty("user.home", userHome);
        }
    }

    @Test
    public void testGetSFTPIgnoreDottedFiles() throws IOException {
        emptyTestDirectory();

        touchFile(sshTestServer.getVirtualFileSystemPath() + "testFile1.txt");
        touchFile(sshTestServer.getVirtualFileSystemPath() + ".testFile2.txt");
        touchFile(sshTestServer.getVirtualFileSystemPath() + "testFile3.txt");
        touchFile(sshTestServer.getVirtualFileSystemPath() + ".testFile4.txt");

        runner.run();

        runner.assertTransferCount(GetSFTP.REL_SUCCESS, 2);

        //Verify non-dotted files were deleted and dotted files were not deleted
        Path file1 = Paths.get(sshTestServer.getVirtualFileSystemPath() + "/testFile1.txt");
        assertFalse(file1.toAbsolutePath().toFile().exists(), "File not deleted.");

        file1 = Paths.get(sshTestServer.getVirtualFileSystemPath() + "/testFile3.txt");
        assertFalse(file1.toAbsolutePath().toFile().exists(), "File not deleted.");

        file1 = Paths.get(sshTestServer.getVirtualFileSystemPath() + "/.testFile2.txt");
        assertTrue(file1.toAbsolutePath().toFile().exists(), "File deleted.");

        file1 = Paths.get(sshTestServer.getVirtualFileSystemPath() + "/.testFile4.txt");
        assertTrue(file1.toAbsolutePath().toFile().exists(), "File deleted.");

        runner.clearTransferState();
    }

    private void touchFile(String file) throws IOException {
        FileUtils.writeStringToFile(new File(file), "", StandardCharsets.UTF_8);
    }

    private void emptyTestDirectory() throws IOException {
        //Delete Virtual File System folder
        Path dir = Paths.get(sshTestServer.getVirtualFileSystemPath());
        FileUtils.cleanDirectory(dir.toFile());
    }
}