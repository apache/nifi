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

import com.github.stefanbirkner.fakesftpserver.rule.FakeSftpServerRule;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.processor.util.list.AbstractListProcessor;
import org.apache.nifi.processors.standard.util.FTPTransfer;
import org.apache.nifi.processors.standard.util.SFTPTransfer;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import java.security.SecureRandom;

public class TestListSFTP {
    @Rule
    public final FakeSftpServerRule sftpServer = new FakeSftpServerRule();
    int port;

    final String username = "nifi-sftp-user";
    final String password = "Test test test chocolate";

    @Before
    public void setUp() throws Exception {
        sftpServer.addUser(username, password);
        port = sftpServer.getPort();


        sftpServer.putFile("/directory/smallfile.txt", "byte", StandardCharsets.UTF_8);

        sftpServer.putFile("/directory/file.txt", "a bit more content in this file", StandardCharsets.UTF_8);

        byte[] bytes = new byte[120];
        SecureRandom.getInstanceStrong().nextBytes(bytes);

        sftpServer.putFile("/directory/file.bin", bytes);
    }

    @After
    public void tearDown() throws Exception {
        sftpServer.deleteAllFilesAndDirectories();
    }

    @Test
    public void basicFileList() throws InterruptedException {
        TestRunner runner = TestRunners.newTestRunner(ListSFTP.class);
        runner.setProperty(ListSFTP.HOSTNAME, "localhost");
        runner.setProperty(ListSFTP.USERNAME, username);
        runner.setProperty(SFTPTransfer.PASSWORD, password);
        runner.setProperty(FTPTransfer.PORT, Integer.toString(port));
        runner.setProperty(ListSFTP.REMOTE_PATH, "/directory/");

        runner.setProperty(ListFile.TARGET_SYSTEM_TIMESTAMP_PRECISION, ListFile.PRECISION_MILLIS);
        runner.assertValid();

        // Ensure wait for enough lag time.
        Thread.sleep(AbstractListProcessor.LISTING_LAG_MILLIS.get(TimeUnit.MILLISECONDS) * 2);

        runner.run();

        runner.assertTransferCount(ListSFTP.REL_SUCCESS, 3);

        runner.assertAllFlowFilesContainAttribute("sftp.remote.host");
        runner.assertAllFlowFilesContainAttribute("sftp.remote.port");
        runner.assertAllFlowFilesContainAttribute("sftp.listing.user");
        runner.assertAllFlowFilesContainAttribute(ListFile.FILE_OWNER_ATTRIBUTE);
        runner.assertAllFlowFilesContainAttribute(ListFile.FILE_GROUP_ATTRIBUTE);
        runner.assertAllFlowFilesContainAttribute(ListFile.FILE_PERMISSIONS_ATTRIBUTE);
        runner.assertAllFlowFilesContainAttribute(ListFile.FILE_SIZE_ATTRIBUTE);
        runner.assertAllFlowFilesContainAttribute(ListFile.FILE_LAST_MODIFY_TIME_ATTRIBUTE);
        runner.assertAllFlowFilesContainAttribute( "filename");

        final MockFlowFile retrievedFile = runner.getFlowFilesForRelationship(ListSFTP.REL_SUCCESS).get(0);
        retrievedFile.assertAttributeEquals("sftp.listing.user", username);
    }


    @Test
    public void sizeFilteredFileList() throws InterruptedException {
        TestRunner runner = TestRunners.newTestRunner(ListSFTP.class);
        runner.setProperty(ListSFTP.HOSTNAME, "localhost");
        runner.setProperty(ListSFTP.USERNAME, username);
        runner.setProperty(SFTPTransfer.PASSWORD, password);
        runner.setProperty(FTPTransfer.PORT, Integer.toString(port));
        runner.setProperty(ListSFTP.REMOTE_PATH, "/directory/");
        runner.setProperty(ListFile.MIN_SIZE, "8B");
        runner.setProperty(ListFile.MAX_SIZE, "100B");


        runner.setProperty(ListFile.TARGET_SYSTEM_TIMESTAMP_PRECISION, ListFile.PRECISION_MILLIS);
        runner.assertValid();

        // Ensure wait for enough lag time.
        Thread.sleep(AbstractListProcessor.LISTING_LAG_MILLIS.get(TimeUnit.MILLISECONDS) * 2);

        runner.run();

        runner.assertTransferCount(ListSFTP.REL_SUCCESS, 1);

        final MockFlowFile retrievedFile = runner.getFlowFilesForRelationship(ListSFTP.REL_SUCCESS).get(0);
        //the only file between the limits
        retrievedFile.assertAttributeEquals("filename", "file.txt");
    }
}
