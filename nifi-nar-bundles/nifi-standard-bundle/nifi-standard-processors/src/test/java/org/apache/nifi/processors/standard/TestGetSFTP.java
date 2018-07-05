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
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class TestGetSFTP {

    private static final Logger logger = LoggerFactory.getLogger(TestGetSFTP.class);

    private TestRunner getSFTPRunner;
    private static SSHTestServer sshTestServer;

    @BeforeClass
    public static void setupSSHD() throws IOException {
        sshTestServer = new SSHTestServer();
        sshTestServer.startServer();
    }

    @AfterClass
    public static void cleanupSSHD() throws IOException {
        sshTestServer.stopServer();
    }

    @Before
    public void setup(){
        getSFTPRunner = TestRunners.newTestRunner(GetSFTP.class);
        getSFTPRunner.setProperty(SFTPTransfer.HOSTNAME, "localhost");
        getSFTPRunner.setProperty(SFTPTransfer.PORT, Integer.toString(sshTestServer.getSSHPort()));
        getSFTPRunner.setProperty(SFTPTransfer.USERNAME, sshTestServer.getUsername());
        getSFTPRunner.setProperty(SFTPTransfer.PASSWORD, sshTestServer.getPassword());
        getSFTPRunner.setProperty(SFTPTransfer.STRICT_HOST_KEY_CHECKING, "false");
        getSFTPRunner.setProperty(SFTPTransfer.DATA_TIMEOUT, "30 sec");
        getSFTPRunner.setProperty(SFTPTransfer.REMOTE_PATH, "/");
        getSFTPRunner.removeProperty(SFTPTransfer.FILE_FILTER_REGEX);
        getSFTPRunner.setProperty(SFTPTransfer.PATH_FILTER_REGEX, "");
        getSFTPRunner.setProperty(SFTPTransfer.POLLING_INTERVAL, "60 sec");
        getSFTPRunner.setProperty(SFTPTransfer.RECURSIVE_SEARCH, "false");
        getSFTPRunner.setProperty(SFTPTransfer.IGNORE_DOTTED_FILES, "true");
        getSFTPRunner.setProperty(SFTPTransfer.DELETE_ORIGINAL, "true");
        getSFTPRunner.setProperty(SFTPTransfer.MAX_SELECTS, "100");
        getSFTPRunner.setProperty(SFTPTransfer.REMOTE_POLL_BATCH_SIZE, "5000");

        getSFTPRunner.setValidateExpressionUsage(false);
    }

    @Test
    public void testGetSFTPFileBasicRead() throws IOException {
        emptyTestDirectory();

        touchFile(sshTestServer.getVirtualFileSystemPath() + "testFile1.txt");
        touchFile(sshTestServer.getVirtualFileSystemPath() + "testFile2.txt");
        touchFile(sshTestServer.getVirtualFileSystemPath() + "testFile3.txt");
        touchFile(sshTestServer.getVirtualFileSystemPath() + "testFile4.txt");

        getSFTPRunner.run();

        getSFTPRunner.assertTransferCount(GetSFTP.REL_SUCCESS, 4);

        //Verify files deleted
        for(int i=1;i<5;i++){
            Path file1 = Paths.get(sshTestServer.getVirtualFileSystemPath() + "/testFile" + i + ".txt");
            Assert.assertTrue("File not deleted.", !file1.toAbsolutePath().toFile().exists());
        }

        getSFTPRunner.clearTransferState();
    }

    @Test
    public void testGetSFTPIgnoreDottedFiles() throws IOException {
        emptyTestDirectory();

        touchFile(sshTestServer.getVirtualFileSystemPath() + "testFile1.txt");
        touchFile(sshTestServer.getVirtualFileSystemPath() + ".testFile2.txt");
        touchFile(sshTestServer.getVirtualFileSystemPath() + "testFile3.txt");
        touchFile(sshTestServer.getVirtualFileSystemPath() + ".testFile4.txt");

        getSFTPRunner.run();

        getSFTPRunner.assertTransferCount(GetSFTP.REL_SUCCESS, 2);

        //Verify non-dotted files were deleted and dotted files were not deleted
        Path file1 = Paths.get(sshTestServer.getVirtualFileSystemPath() + "/testFile1.txt");
        Assert.assertTrue("File not deleted.", !file1.toAbsolutePath().toFile().exists());

        file1 = Paths.get(sshTestServer.getVirtualFileSystemPath() + "/testFile3.txt");
        Assert.assertTrue("File not deleted.", !file1.toAbsolutePath().toFile().exists());

        file1 = Paths.get(sshTestServer.getVirtualFileSystemPath() + "/.testFile2.txt");
        Assert.assertTrue("File deleted.", file1.toAbsolutePath().toFile().exists());

        file1 = Paths.get(sshTestServer.getVirtualFileSystemPath() + "/.testFile4.txt");
        Assert.assertTrue("File deleted.", file1.toAbsolutePath().toFile().exists());

        getSFTPRunner.clearTransferState();
    }

    private void touchFile(String file) throws IOException {
        FileUtils.writeStringToFile(new File(file), "", "UTF-8");
    }

    private void emptyTestDirectory() throws IOException {
        //Delete Virtual File System folder
        Path dir = Paths.get(sshTestServer.getVirtualFileSystemPath());
        FileUtils.cleanDirectory(dir.toFile());
    }
}