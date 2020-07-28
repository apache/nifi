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
import org.apache.nifi.processors.standard.util.FileTransfer;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class TestPutSFTP {

    private static final Logger logger = LoggerFactory.getLogger(TestPutSFTP.class);

    private TestRunner putSFTPRunner;
    private static SSHTestServer sshTestServer;

    private final String testFile = "src" + File.separator + "test" + File.separator + "resources" + File.separator + "hello.txt";

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
        putSFTPRunner = TestRunners.newTestRunner(PutSFTP.class);
        putSFTPRunner.setProperty(SFTPTransfer.HOSTNAME, "localhost");
        putSFTPRunner.setProperty(SFTPTransfer.PORT, Integer.toString(sshTestServer.getSSHPort()));
        putSFTPRunner.setProperty(SFTPTransfer.USERNAME, sshTestServer.getUsername());
        putSFTPRunner.setProperty(SFTPTransfer.PASSWORD, sshTestServer.getPassword());
        putSFTPRunner.setProperty(SFTPTransfer.STRICT_HOST_KEY_CHECKING, "false");
        putSFTPRunner.setProperty(SFTPTransfer.BATCH_SIZE, "2");
        putSFTPRunner.setProperty(SFTPTransfer.REMOTE_PATH, "nifi_test/");
        putSFTPRunner.setProperty(SFTPTransfer.REJECT_ZERO_BYTE, "true");
        putSFTPRunner.setProperty(SFTPTransfer.CONFLICT_RESOLUTION, FileTransfer.CONFLICT_RESOLUTION_REPLACE);
        putSFTPRunner.setProperty(SFTPTransfer.CREATE_DIRECTORY, "true");
        putSFTPRunner.setProperty(SFTPTransfer.DATA_TIMEOUT, "30 sec");
        putSFTPRunner.setValidateExpressionUsage(false);
    }

    @Test
    public void testPutSFTPFile() throws IOException {
        emptyTestDirectory();

        Map<String,String> attributes = new HashMap<>();
        attributes.put("filename", "testfile.txt");

        putSFTPRunner.enqueue(Paths.get(testFile), attributes);
        putSFTPRunner.run();

        putSFTPRunner.assertTransferCount(PutSFTP.REL_SUCCESS, 1);

        //verify directory exists
        Path newDirectory = Paths.get(sshTestServer.getVirtualFileSystemPath() + "nifi_test/");
        Path newFile = Paths.get(sshTestServer.getVirtualFileSystemPath() + "nifi_test/testfile.txt");
        Assert.assertTrue("New directory not created.", newDirectory.toAbsolutePath().toFile().exists());
        Assert.assertTrue("New File not created.", newFile.toAbsolutePath().toFile().exists());
        putSFTPRunner.clearTransferState();
    }

    @Test
    public void testPutSFTPFileZeroByte() throws IOException {
        emptyTestDirectory();

        Map<String,String> attributes = new HashMap<>();
        attributes.put("filename", "testfile.txt");
        putSFTPRunner.enqueue(Paths.get(testFile), attributes);

        attributes = new HashMap<>();
        attributes.put("filename", "testfile1.txt");
        putSFTPRunner.enqueue("", attributes);

        putSFTPRunner.run();

        //Two files in batch, should have only 1 transferred to sucess, 1 to failure
        putSFTPRunner.assertTransferCount(PutSFTP.REL_SUCCESS, 1);
        putSFTPRunner.assertTransferCount(PutSFTP.REL_REJECT, 1);
        putSFTPRunner.clearTransferState();

        attributes = new HashMap<>();
        attributes.put("filename", "testfile1.txt");
        putSFTPRunner.enqueue("", attributes);

        putSFTPRunner.run();

        //One files in batch, should have 0 transferred to output since it's zero byte
        putSFTPRunner.assertTransferCount(PutSFTP.REL_REJECT, 1);
        putSFTPRunner.assertTransferCount(PutSFTP.REL_SUCCESS, 0);
        putSFTPRunner.clearTransferState();

        //allow zero byte files
        putSFTPRunner.setProperty(SFTPTransfer.REJECT_ZERO_BYTE, "false");

        attributes = new HashMap<>();
        attributes.put("filename", "testfile1.txt");
        putSFTPRunner.enqueue("", attributes);

        putSFTPRunner.run();

        //should have 1 transferred to sucess
        putSFTPRunner.assertTransferCount(PutSFTP.REL_SUCCESS, 1);

        //revert settings
        putSFTPRunner.setProperty(SFTPTransfer.REJECT_ZERO_BYTE, "true");
        putSFTPRunner.clearTransferState();
    }

    @Test
    public void testPutSFTPFileConflictResolution() throws IOException {
        emptyTestDirectory();

        //Try transferring file with the same name as a directory, should fail in all cases
        // except RESOLUTION of NONE
        Path dir = Paths.get(sshTestServer.getVirtualFileSystemPath() + "nifi_test" );
        Path dir2 = Paths.get(sshTestServer.getVirtualFileSystemPath() + "nifi_test/testfile" );
        Files.createDirectory(dir);
        Files.createDirectory(dir2);

        Map<String,String> attributes = new HashMap<>();
        attributes.put("filename", "testfile");

        putSFTPRunner.enqueue(Paths.get(testFile), attributes);
        putSFTPRunner.run();

        putSFTPRunner.assertTransferCount(PutSFTP.REL_SUCCESS, 0);
        putSFTPRunner.assertTransferCount(PutSFTP.REL_FAILURE, 1);

        //Prepare by uploading test file
        attributes = new HashMap<>();
        attributes.put("filename", "testfile.txt");

        putSFTPRunner.setProperty(SFTPTransfer.CONFLICT_RESOLUTION, FileTransfer.CONFLICT_RESOLUTION_REPLACE);

        putSFTPRunner.enqueue(Paths.get(testFile), attributes);
        putSFTPRunner.run();
        putSFTPRunner.clearTransferState();

        //set conflict resolution mode to REJECT
        putSFTPRunner.setProperty(SFTPTransfer.CONFLICT_RESOLUTION, FileTransfer.CONFLICT_RESOLUTION_REJECT);

        putSFTPRunner.enqueue(Paths.get(testFile), attributes);
        putSFTPRunner.run();

        putSFTPRunner.assertTransferCount(PutSFTP.REL_SUCCESS, 0);
        putSFTPRunner.assertTransferCount(PutSFTP.REL_REJECT, 1);
        putSFTPRunner.clearTransferState();

        //set conflict resolution mode to IGNORE
        putSFTPRunner.setProperty(SFTPTransfer.CONFLICT_RESOLUTION, FileTransfer.CONFLICT_RESOLUTION_IGNORE);
        putSFTPRunner.enqueue(Paths.get(testFile), attributes);
        putSFTPRunner.run();

        putSFTPRunner.assertTransferCount(PutSFTP.REL_SUCCESS, 1);
        putSFTPRunner.assertTransferCount(PutSFTP.REL_REJECT, 0);

        putSFTPRunner.clearTransferState();

        //set conflict resolution mode to FAIL
        putSFTPRunner.setProperty(SFTPTransfer.CONFLICT_RESOLUTION, FileTransfer.CONFLICT_RESOLUTION_FAIL);
        putSFTPRunner.enqueue(Paths.get(testFile), attributes);
        putSFTPRunner.run();

        putSFTPRunner.assertTransferCount(PutSFTP.REL_SUCCESS, 0);
        putSFTPRunner.assertTransferCount(PutSFTP.REL_REJECT, 0);
        putSFTPRunner.assertTransferCount(PutSFTP.REL_FAILURE, 1);

        putSFTPRunner.clearTransferState();
    }

    @Test
    public void testPutSFTPBatching() throws IOException {
        emptyTestDirectory();

        Map<String,String> attributes = new HashMap<>();
        attributes.put("filename", "testfile.txt");
        putSFTPRunner.enqueue(Paths.get(testFile), attributes);

        attributes = new HashMap<>();
        attributes.put("filename", "testfile2.txt");
        putSFTPRunner.enqueue(Paths.get(testFile), attributes);

        attributes = new HashMap<>();
        attributes.put("filename", "testfile3.txt");
        putSFTPRunner.enqueue(Paths.get(testFile), attributes);

        attributes = new HashMap<>();
        attributes.put("filename", "testfile4.txt");
        putSFTPRunner.enqueue(Paths.get(testFile), attributes);

        attributes = new HashMap<>();
        attributes.put("filename", "testfile5.txt");
        putSFTPRunner.enqueue(Paths.get(testFile), attributes);

        putSFTPRunner.run();
        putSFTPRunner.assertTransferCount(PutSFTP.REL_SUCCESS, 2);

        putSFTPRunner.clearTransferState();

        putSFTPRunner.run();
        putSFTPRunner.assertTransferCount(PutSFTP.REL_SUCCESS, 2);

        putSFTPRunner.clearTransferState();

        putSFTPRunner.run();
        putSFTPRunner.assertTransferCount(PutSFTP.REL_SUCCESS, 1);
        putSFTPRunner.clearTransferState();
    }

    private void emptyTestDirectory() throws IOException {
        //Delete Virtual File System folder
        Path dir = Paths.get(sshTestServer.getVirtualFileSystemPath());
        FileUtils.cleanDirectory(dir.toFile());
    }
}