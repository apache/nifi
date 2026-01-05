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

public class TestGetSFTP {

    private SSHTestServer sshTestServer;

    private TestRunner runner;

    @BeforeEach
    void setup(@TempDir final Path rootPath) throws IOException {
        sshTestServer = new SSHTestServer(rootPath);
        sshTestServer.startServer();

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
    }

    @AfterEach
    void stopServer() throws IOException {
        sshTestServer.stopServer();
    }

    @Test
    void testRunFilesFound() throws IOException {
        touchFile(sshTestServer.getRootPath().resolve("testFile1.txt"));
        touchFile(sshTestServer.getRootPath().resolve("testFile2.txt"));
        touchFile(sshTestServer.getRootPath().resolve("testFile3.txt"));
        touchFile(sshTestServer.getRootPath().resolve("testFile4.txt"));

        runner.run();

        runner.assertTransferCount(GetSFTP.REL_SUCCESS, 4);

        assertFileNotFound("testFile1.txt");
        assertFileNotFound("testFile2.txt");
        assertFileNotFound("testFile3.txt");
        assertFileNotFound("testFile4.txt");

        runner.clearTransferState();
    }

    @Test
    void testRunNoExceptionWhenUserHomeNotFound() throws IOException {
        final String userHome = System.getProperty("user.home");
        try {
            System.setProperty("user.home", "/directory-not-found");
            touchFile(sshTestServer.getRootPath().resolve("testFile1.txt"));
            touchFile(sshTestServer.getRootPath().resolve("testFile2.txt"));

            runner.run();

            runner.assertTransferCount(GetSFTP.REL_SUCCESS, 2);

            assertFileNotFound("testFile1.txt");
            assertFileNotFound("testFile2.txt");
        } finally {
            System.setProperty("user.home", userHome);
        }
    }

    @Test
    void testRunDottedFilesIgnored() throws IOException {
        touchFile(sshTestServer.getRootPath().resolve("testFile1.txt"));
        touchFile(sshTestServer.getRootPath().resolve(".testFile2.txt"));
        touchFile(sshTestServer.getRootPath().resolve("testFile3.txt"));
        touchFile(sshTestServer.getRootPath().resolve(".testFile4.txt"));

        runner.run();

        runner.assertTransferCount(GetSFTP.REL_SUCCESS, 2);

        assertFileNotFound("testFile1.txt");
        assertFileNotFound("testFile3.txt");

        assertFileExists(".testFile2.txt");
        assertFileExists(".testFile4.txt");
    }

    private void touchFile(final Path filePath) throws IOException {
        FileUtils.writeStringToFile(filePath.toFile(), "", StandardCharsets.UTF_8);
    }

    private void assertFileExists(final String relativePath) {
        final Path filePath = sshTestServer.getRootPath().resolve(relativePath);
        assertTrue(Files.exists(filePath), "File [%s] not found".formatted(filePath));
    }

    private void assertFileNotFound(final String relativePath) {
        final Path filePath = sshTestServer.getRootPath().resolve(relativePath);
        assertFalse(Files.exists(filePath), "File [%s] found".formatted(filePath));
    }
}
