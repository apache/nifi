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

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.file.transfer.FetchFileTransfer;
import org.apache.nifi.processor.util.file.transfer.PutFileTransfer;
import org.apache.nifi.processor.util.list.AbstractListProcessor;
import org.apache.nifi.processors.standard.util.FTPTransfer;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.mockftpserver.fake.FakeFtpServer;
import org.mockftpserver.fake.UserAccount;
import org.mockftpserver.fake.filesystem.DirectoryEntry;
import org.mockftpserver.fake.filesystem.FileEntry;
import org.mockftpserver.fake.filesystem.FileSystem;
import org.mockftpserver.fake.filesystem.Permissions;
import org.mockftpserver.fake.filesystem.WindowsFakeFileSystem;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestFTP {

    private static final String LOCALHOST_ADDRESS = "127.0.0.1";

    final FakeFtpServer fakeFtpServer = new FakeFtpServer();
    final String username = "nifi-ftp-user";
    final String password = "Test test test chocolate";
    int ftpPort;

    @BeforeEach
    public void setUp() throws Exception {
        fakeFtpServer.setServerControlPort(0);
        fakeFtpServer.addUserAccount(new UserAccount(username, password, "c:\\data"));

        FileSystem fileSystem = new WindowsFakeFileSystem();
        fileSystem.add(new DirectoryEntry("c:\\data"));
        fakeFtpServer.setFileSystem(fileSystem);

        fakeFtpServer.start();

        ftpPort = fakeFtpServer.getServerControlPort();
    }

    @AfterEach
    public void tearDown() throws Exception {
        fakeFtpServer.stop();
    }

    @Test
    public void testValidators() {
        TestRunner runner = TestRunners.newTestRunner(PutFTP.class);
        Collection<ValidationResult> results;
        ProcessContext pc;

        /* Set the basic required values */
        runner.setProperty(FTPTransfer.USERNAME, "${el-username}");
        runner.setProperty(FTPTransfer.HOSTNAME, "static-hostname");
        runner.setProperty(FTPTransfer.PORT, "${el-portNumber}");

        results = new HashSet<>();
        runner.setProperty(FTPTransfer.REMOTE_PATH, "static-remote-target");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        assertEquals(0, results.size());


        results = new HashSet<>();
        runner.setProperty(FTPTransfer.REMOTE_PATH, "${el-remote-target}");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        assertEquals(0, results.size());

        results = new HashSet<>();
        runner.setProperty(FTPTransfer.USERNAME, "static-username");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        assertEquals(0, results.size());

        /* Try an invalid expression */
        results = new HashSet<>();
        runner.setProperty(FTPTransfer.USERNAME, "");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        assertEquals(1, results.size());

    }

    @Test
    public void testPutFtp() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(PutFTP.class);
        runner.setProperty(FTPTransfer.HOSTNAME, LOCALHOST_ADDRESS);
        runner.setProperty(FTPTransfer.USERNAME, username);
        runner.setProperty(FTPTransfer.PASSWORD, password);
        runner.setProperty(FTPTransfer.PORT, Integer.toString(ftpPort));
        try (FileInputStream fis = new FileInputStream("src/test/resources/randombytes-1")) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put(CoreAttributes.FILENAME.key(), "randombytes-1");
            runner.enqueue(fis, attributes);
            runner.run();
        }
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(PutFTP.REL_SUCCESS);
        FileSystem results = fakeFtpServer.getFileSystem();

        // Check file was uploaded
        assertTrue(results.exists("c:\\data\\randombytes-1"));
    }

    @Test
    public void testPutFtpProvenanceEvents() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(PutFTP.class);

        runner.setProperty(FTPTransfer.HOSTNAME, "localhost");
        runner.setProperty(FTPTransfer.USERNAME, username);
        runner.setProperty(FTPTransfer.PASSWORD, password);
        runner.setProperty(FTPTransfer.PORT, Integer.toString(ftpPort));

        // Get two flowfiles to test by running data
        try (FileInputStream fis = new FileInputStream("src/test/resources/randombytes-1")) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put(CoreAttributes.FILENAME.key(), "randombytes-1");
            attributes.put("transfer-host", "localhost");
            runner.enqueue(fis, attributes);
            runner.run();
        }
        try (FileInputStream fis = new FileInputStream("src/test/resources/hello.txt")) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put(CoreAttributes.FILENAME.key(), "hello.txt");
            attributes.put("transfer-host", LOCALHOST_ADDRESS);
            runner.enqueue(fis, attributes);
            runner.run();
        }
        runner.assertQueueEmpty();
        runner.assertTransferCount(PutFTP.REL_SUCCESS, 2);

        MockFlowFile flowFile1 = runner.getFlowFilesForRelationship(PutFileTransfer.REL_SUCCESS).get(0);
        MockFlowFile flowFile2 = runner.getFlowFilesForRelationship(PutFileTransfer.REL_SUCCESS).get(1);

        runner.clearProvenanceEvents();
        runner.clearTransferState();
        HashMap<String, String> map1 = new HashMap<>();
        HashMap<String, String> map2 = new HashMap<>();
        map1.put(CoreAttributes.FILENAME.key(), "randombytes-xx");
        map2.put(CoreAttributes.FILENAME.key(), "randombytes-yy");

        flowFile1.putAttributes(map1);
        flowFile2.putAttributes(map2);
        //set to derive hostname
        runner.setProperty(FTPTransfer.HOSTNAME, "${transfer-host}");
        runner.setThreadCount(1);
        runner.enqueue(flowFile1);
        runner.enqueue(flowFile2);
        runner.run();

        runner.assertTransferCount(PutFTP.REL_SUCCESS, 2);
        assert(runner.getProvenanceEvents().get(0).getTransitUri().contains("ftp://localhost"));
        assert(runner.getProvenanceEvents().get(1).getTransitUri().contains("ftp://127.0.0.1"));
    }

    @Test
    public void testGetFtp() {
        FileSystem results = fakeFtpServer.getFileSystem();

        FileEntry sampleFile = new FileEntry("c:\\data\\randombytes-2");
        sampleFile.setContents("Just some random test test test chocolate");
        results.add(sampleFile);

        // Check file exists
        assertTrue(results.exists("c:\\data\\randombytes-2"));

        TestRunner runner = TestRunners.newTestRunner(GetFTP.class);
        runner.setProperty(FTPTransfer.HOSTNAME, LOCALHOST_ADDRESS);
        runner.setProperty(FTPTransfer.USERNAME, username);
        runner.setProperty(FTPTransfer.PASSWORD, password);
        runner.setProperty(FTPTransfer.PORT, Integer.toString(ftpPort));
        runner.setProperty(FTPTransfer.REMOTE_PATH, "/");

        runner.run();

        final MockFlowFile retrievedFile = runner.getFlowFilesForRelationship(GetFTP.REL_SUCCESS).get(0);
        retrievedFile.assertContentEquals("Just some random test test test chocolate");
    }

    @Test
    public void testFetchFtp() {
        FileSystem results = fakeFtpServer.getFileSystem();

        FileEntry sampleFile = new FileEntry("c:\\data\\randombytes-2");
        sampleFile.setContents("Just some random test test test chocolate");
        results.add(sampleFile);

        // Check file exists
        assertTrue(results.exists("c:\\data\\randombytes-2"));

        TestRunner runner = TestRunners.newTestRunner(FetchFTP.class);
        runner.setProperty(FetchFTP.HOSTNAME, "${host}");
        runner.setProperty(FetchFTP.USERNAME, "${username}");
        runner.setProperty(FTPTransfer.PASSWORD, password);
        runner.setProperty(FTPTransfer.PORT, "${port}");
        runner.setProperty(FetchFTP.REMOTE_FILENAME, "c:\\data\\randombytes-2");
        runner.setProperty(FetchFTP.COMPLETION_STRATEGY, FetchFTP.COMPLETION_MOVE);
        runner.setProperty(FetchFTP.MOVE_DESTINATION_DIR, "data");


        Map<String, String> attrs = new HashMap<>();
        attrs.put("host", "localhost");
        attrs.put("username", username);
        attrs.put("port", Integer.toString(ftpPort));
        runner.enqueue("", attrs);

        runner.run();

        final MockFlowFile retrievedFile = runner.getFlowFilesForRelationship(FetchFTP.REL_SUCCESS).get(0);
        retrievedFile.assertContentEquals("Just some random test test test chocolate");
    }

    @Test
    public void testFetchFtpFileNotFound() {
        final TestRunner runner = TestRunners.newTestRunner(FetchFTP.class);
        runner.setProperty(FetchFTP.HOSTNAME, LOCALHOST_ADDRESS);
        runner.setProperty(FetchFTP.USERNAME, username);
        runner.setProperty(FTPTransfer.PASSWORD, password);
        runner.setProperty(FTPTransfer.PORT, Integer.toString(ftpPort));
        runner.setProperty(FetchFTP.REMOTE_FILENAME, "remote-file-not-found");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(FetchFTP.REL_NOT_FOUND);
        runner.assertAllFlowFilesContainAttribute(FetchFileTransfer.FAILURE_REASON_ATTRIBUTE);
    }

    @Test
    public void testFetchFtpFilePermissionDenied() {
        final FileSystem fs = fakeFtpServer.getFileSystem();

        final FileEntry restrictedFileEntry = new FileEntry("c:\\data\\restricted");
        restrictedFileEntry.setPermissions(Permissions.NONE);
        fs.add(restrictedFileEntry);

        final TestRunner runner = TestRunners.newTestRunner(FetchFTP.class);
        runner.setProperty(FetchFTP.HOSTNAME, LOCALHOST_ADDRESS);
        runner.setProperty(FetchFTP.USERNAME, username);
        runner.setProperty(FTPTransfer.PASSWORD, password);
        runner.setProperty(FTPTransfer.PORT, Integer.toString(ftpPort));
        runner.setProperty(FetchFTP.REMOTE_FILENAME, "restricted");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertAllFlowFilesTransferred(FetchFTP.REL_PERMISSION_DENIED);
        runner.assertAllFlowFilesContainAttribute(FetchFileTransfer.FAILURE_REASON_ATTRIBUTE);
    }

    @Test
    public void testListFtpHostPortVariablesFileFound() throws InterruptedException {
        final FileSystem fs = fakeFtpServer.getFileSystem();

        final FileEntry fileEntry = new FileEntry("c:\\data\\found");
        fs.add(fileEntry);

        final TestRunner runner = TestRunners.newTestRunner(ListFTP.class);
        runner.setEnvironmentVariableValue("host", LOCALHOST_ADDRESS);
        runner.setEnvironmentVariableValue("port", Integer.toString(ftpPort));

        runner.setProperty(ListFTP.HOSTNAME, "${host}");
        runner.setProperty(FTPTransfer.PORT, "${port}");
        runner.setProperty(ListFTP.USERNAME, username);
        runner.setProperty(FTPTransfer.PASSWORD, password);
        runner.setProperty(ListFile.TARGET_SYSTEM_TIMESTAMP_PRECISION, ListFile.PRECISION_MILLIS);

        runner.enqueue(new byte[0]);

        // Ensure wait for enough lag time.
        Thread.sleep(AbstractListProcessor.LISTING_LAG_MILLIS.get(TimeUnit.MILLISECONDS) * 2);

        runner.run();

        runner.assertTransferCount(ListFTP.REL_SUCCESS, 1);
    }

    @Test
    @EnabledIfSystemProperty(named = "file.encoding", matches = "UTF-8",
            disabledReason = "org.mockftpserver does not support specification of charset")
    public void testFetchFtpUnicodeFileName() {
        FileSystem fs = fakeFtpServer.getFileSystem();

        FileEntry sampleFile = new FileEntry("c:\\data\\őűőű.txt");
        sampleFile.setContents("Just some random test test test chocolate");
        fs.add(sampleFile);

        TestRunner runner = TestRunners.newTestRunner(FetchFTP.class);
        runner.setProperty(FetchFTP.HOSTNAME, LOCALHOST_ADDRESS);
        runner.setProperty(FetchFTP.USERNAME, username);
        runner.setProperty(FTPTransfer.PASSWORD, password);
        runner.setProperty(FTPTransfer.PORT, String.valueOf(ftpPort));
        runner.setProperty(FetchFTP.REMOTE_FILENAME, "c:\\data\\őűőű.txt");
        runner.setProperty(FetchFTP.COMPLETION_STRATEGY, FetchFTP.COMPLETION_MOVE);
        runner.setProperty(FetchFTP.MOVE_DESTINATION_DIR, "data");
        runner.setProperty(FTPTransfer.UTF8_ENCODING, "true");

        runner.enqueue("");

        runner.run();

        runner.assertTransferCount(FetchFTP.REL_SUCCESS, 1);
        final MockFlowFile retrievedFile = runner.getFlowFilesForRelationship(FetchFTP.REL_SUCCESS).get(0);
        retrievedFile.assertContentEquals("Just some random test test test chocolate");
    }

    @Test
    public void testListFtp() throws InterruptedException {
        FileSystem results = fakeFtpServer.getFileSystem();

        FileEntry sampleFile = new FileEntry("c:\\data\\randombytes-2");
        sampleFile.setContents("Just some random test test test chocolate");
        results.add(sampleFile);

        // Check file exists
        assertTrue(results.exists("c:\\data\\randombytes-2"));

        TestRunner runner = TestRunners.newTestRunner(ListFTP.class);
        runner.setProperty(ListFTP.HOSTNAME, LOCALHOST_ADDRESS);
        runner.setProperty(ListFTP.USERNAME, username);
        runner.setProperty(FTPTransfer.PASSWORD, password);
        runner.setProperty(FTPTransfer.PORT, Integer.toString(ftpPort));
        runner.setProperty(ListFTP.REMOTE_PATH, "/");
        // FakeFTPServer has timestamp precision in minutes.
        // Specify milliseconds precision so that test does not need to wait for minutes.
        runner.setProperty(ListFile.TARGET_SYSTEM_TIMESTAMP_PRECISION, ListFile.PRECISION_MILLIS);
        runner.assertValid();

        // Ensure wait for enough lag time.
        Thread.sleep(AbstractListProcessor.LISTING_LAG_MILLIS.get(TimeUnit.MILLISECONDS) * 2);

        runner.run();

        runner.assertTransferCount(FetchFTP.REL_SUCCESS, 1);
        final MockFlowFile retrievedFile = runner.getFlowFilesForRelationship(FetchFTP.REL_SUCCESS).get(0);
        runner.assertAllFlowFilesContainAttribute("ftp.remote.host");
        runner.assertAllFlowFilesContainAttribute("ftp.remote.port");
        runner.assertAllFlowFilesContainAttribute("ftp.listing.user");
        runner.assertAllFlowFilesContainAttribute(ListFile.FILE_OWNER_ATTRIBUTE);
        runner.assertAllFlowFilesContainAttribute(ListFile.FILE_GROUP_ATTRIBUTE);
        runner.assertAllFlowFilesContainAttribute(ListFile.FILE_PERMISSIONS_ATTRIBUTE);
        runner.assertAllFlowFilesContainAttribute(ListFile.FILE_SIZE_ATTRIBUTE);
        runner.assertAllFlowFilesContainAttribute(ListFile.FILE_LAST_MODIFY_TIME_ATTRIBUTE);
        retrievedFile.assertAttributeEquals("ftp.listing.user", username);
        retrievedFile.assertAttributeEquals("filename", "randombytes-2");
    }
}