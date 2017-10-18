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
import org.apache.nifi.processor.util.list.AbstractListProcessor;
import org.apache.nifi.processors.standard.util.FTPTransfer;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockftpserver.fake.FakeFtpServer;
import org.mockftpserver.fake.UserAccount;
import org.mockftpserver.fake.filesystem.DirectoryEntry;
import org.mockftpserver.fake.filesystem.FileEntry;
import org.mockftpserver.fake.filesystem.FileSystem;
import org.mockftpserver.fake.filesystem.WindowsFakeFileSystem;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class TestFTP {

    final FakeFtpServer fakeFtpServer = new FakeFtpServer();
    final String username = "nifi-ftp-user";
    final String password = "Test test test chocolate";
    int ftpPort;

    @Before
    public void setUp() throws Exception {
        fakeFtpServer.setServerControlPort(0);
        fakeFtpServer.addUserAccount(new UserAccount(username, password, "c:\\data"));

        FileSystem fileSystem = new WindowsFakeFileSystem();
        fileSystem.add(new DirectoryEntry("c:\\data"));
        fakeFtpServer.setFileSystem(fileSystem);

        fakeFtpServer.start();

        ftpPort = fakeFtpServer.getServerControlPort();
    }

    @After
    public void tearDown() throws Exception {
        fakeFtpServer.stop();
    }

    @Test
    public void testValidators() {
        TestRunner runner = TestRunners.newTestRunner(PutFTP.class);
        Collection<ValidationResult> results;
        ProcessContext pc;

        /* Set the basic required values */
        results = new HashSet<>();
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
    public void basicFileUpload() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(PutFTP.class);
        runner.setProperty(FTPTransfer.HOSTNAME, "localhost");
        runner.setProperty(FTPTransfer.USERNAME, username);
        runner.setProperty(FTPTransfer.PASSWORD, password);
        runner.setProperty(FTPTransfer.PORT, Integer.toString(ftpPort));
        try (FileInputStream fis = new FileInputStream("src/test/resources/randombytes-1");) {
            Map<String, String> attributes = new HashMap<String, String>();
            attributes.put(CoreAttributes.FILENAME.key(), "randombytes-1");
            runner.enqueue(fis, attributes);
            runner.run();
        }
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(PutFTP.REL_SUCCESS);
        FileSystem results = fakeFtpServer.getFileSystem();

        // Check file was uploaded
        Assert.assertTrue(results.exists("c:\\data\\randombytes-1"));
    }

    @Test
    public void basicFileGet() throws IOException {
        FileSystem results = fakeFtpServer.getFileSystem();

        FileEntry sampleFile = new FileEntry("c:\\data\\randombytes-2");
        sampleFile.setContents("Just some random test test test chocolate");
        results.add(sampleFile);

        // Check file exists
        Assert.assertTrue(results.exists("c:\\data\\randombytes-2"));

        TestRunner runner = TestRunners.newTestRunner(GetFTP.class);
        runner.setProperty(FTPTransfer.HOSTNAME, "localhost");
        runner.setProperty(FTPTransfer.USERNAME, username);
        runner.setProperty(FTPTransfer.PASSWORD, password);
        runner.setProperty(FTPTransfer.PORT, Integer.toString(ftpPort));
        runner.setProperty(FTPTransfer.REMOTE_PATH, "/");

        runner.run();

        final MockFlowFile retrievedFile = runner.getFlowFilesForRelationship(GetFTP.REL_SUCCESS).get(0);
        retrievedFile.assertContentEquals("Just some random test test test chocolate");
    }

    @Test
    public void basicFileFetch() throws IOException {
        FileSystem results = fakeFtpServer.getFileSystem();

        FileEntry sampleFile = new FileEntry("c:\\data\\randombytes-2");
        sampleFile.setContents("Just some random test test test chocolate");
        results.add(sampleFile);

        // Check file exists
        Assert.assertTrue(results.exists("c:\\data\\randombytes-2"));

        TestRunner runner = TestRunners.newTestRunner(FetchFTP.class);
        runner.setProperty(FetchFTP.HOSTNAME, "${host}");
        runner.setProperty(FetchFTP.USERNAME, "${username}");
        runner.setProperty(FTPTransfer.PASSWORD, password);
        runner.setProperty(FTPTransfer.PORT, "${port}");
        runner.setProperty(FetchFTP.REMOTE_FILENAME, "c:\\data\\randombytes-2");
        runner.setProperty(FetchFTP.COMPLETION_STRATEGY, FetchFTP.COMPLETION_MOVE);
        runner.setProperty(FetchFTP.MOVE_DESTINATION_DIR, "data");


        Map<String, String> attrs = new HashMap<String, String>();
        attrs.put("host", "localhost");
        attrs.put("username", username);
        attrs.put("port", Integer.toString(ftpPort));
        runner.enqueue("", attrs);

        runner.run();

        final MockFlowFile retrievedFile = runner.getFlowFilesForRelationship(FetchFTP.REL_SUCCESS).get(0);
        retrievedFile.assertContentEquals("Just some random test test test chocolate");
    }

    @Test
    public void basicFileList() throws IOException, InterruptedException {
        FileSystem results = fakeFtpServer.getFileSystem();

        FileEntry sampleFile = new FileEntry("c:\\data\\randombytes-2");
        sampleFile.setContents("Just some random test test test chocolate");
        results.add(sampleFile);

        // Check file exists
        Assert.assertTrue(results.exists("c:\\data\\randombytes-2"));

        TestRunner runner = TestRunners.newTestRunner(ListFTP.class);
        runner.setProperty(ListFTP.HOSTNAME, "localhost");
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