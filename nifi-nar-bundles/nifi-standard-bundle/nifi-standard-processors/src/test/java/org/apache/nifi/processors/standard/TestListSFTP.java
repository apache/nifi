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

import java.io.File;
import java.io.IOException;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.processor.util.list.AbstractListProcessor;
import org.apache.nifi.processors.standard.util.SFTPTransfer;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;
import java.security.SecureRandom;
import org.apache.commons.io.FileUtils;
import org.apache.nifi.processors.standard.util.SSHTestServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestListSFTP {
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

    @Test
    public void basicFileList() throws InterruptedException, IOException, NoSuchAlgorithmException {
        TestRunner runner = TestRunners.newTestRunner(ListSFTP.class);
        runner.setProperty(ListSFTP.HOSTNAME, "localhost");
        runner.setProperty(SFTPTransfer.PORT, Integer.toString(sshTestServer.getSSHPort()));
        runner.setProperty(ListSFTP.USERNAME, sshTestServer.getUsername());
        runner.setProperty(SFTPTransfer.PASSWORD, sshTestServer.getPassword());
        runner.setProperty(ListSFTP.REMOTE_PATH, "/directory/");
        runner.setProperty(SFTPTransfer.STRICT_HOST_KEY_CHECKING, "false");

        runner.setProperty(ListFile.TARGET_SYSTEM_TIMESTAMP_PRECISION, ListFile.PRECISION_MILLIS);
        runner.assertValid();



        FileUtils.writeStringToFile(new File(sshTestServer.getVirtualFileSystemPath() + "/directory/smallfile.txt"), "byte", StandardCharsets.UTF_8);
        FileUtils.writeStringToFile(new File(sshTestServer.getVirtualFileSystemPath() + "/directory/file.txt"), "a bit more content in this file", StandardCharsets.UTF_8);
        byte[] bytes = new byte[120];
        SecureRandom.getInstanceStrong().nextBytes(bytes);

        FileUtils.writeByteArrayToFile(new File(sshTestServer.getVirtualFileSystemPath() + "/directory/file.bin"), bytes);

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
        retrievedFile.assertAttributeEquals("sftp.listing.user", sshTestServer.getUsername());
    }

    @Test
    public void sizeFilteredFileList() throws InterruptedException, IOException, NoSuchAlgorithmException {
        TestRunner runner = TestRunners.newTestRunner(ListSFTP.class);
        runner.setProperty(ListSFTP.HOSTNAME, "localhost");
        runner.setProperty(SFTPTransfer.PORT, Integer.toString(sshTestServer.getSSHPort()));
        runner.setProperty(ListSFTP.USERNAME, sshTestServer.getUsername());
        runner.setProperty(SFTPTransfer.PASSWORD, sshTestServer.getPassword());
        runner.setProperty(SFTPTransfer.STRICT_HOST_KEY_CHECKING, "false");
        runner.setProperty(ListSFTP.REMOTE_PATH, "/directory/");
        runner.setProperty(ListFile.MIN_SIZE, "8B");
        runner.setProperty(ListFile.MAX_SIZE, "100B");
        runner.setProperty(ListFile.TARGET_SYSTEM_TIMESTAMP_PRECISION, ListFile.PRECISION_MILLIS);

        runner.assertValid();

        FileUtils.writeStringToFile(new File(sshTestServer.getVirtualFileSystemPath() + "/directory/smallfile.txt"), "byte", StandardCharsets.UTF_8);
        FileUtils.writeStringToFile(new File(sshTestServer.getVirtualFileSystemPath() + "/directory/file.txt"), "a bit more content in this file", StandardCharsets.UTF_8);
        byte[] bytes = new byte[120];
        SecureRandom.getInstanceStrong().nextBytes(bytes);

        FileUtils.writeByteArrayToFile(new File(sshTestServer.getVirtualFileSystemPath() + "/directory/file.bin"), bytes);

        // Ensure wait for enough lag time.
        Thread.sleep(AbstractListProcessor.LISTING_LAG_MILLIS.get(TimeUnit.MILLISECONDS) * 2);

        runner.run();

        runner.assertTransferCount(ListSFTP.REL_SUCCESS, 1);

        final MockFlowFile retrievedFile = runner.getFlowFilesForRelationship(ListSFTP.REL_SUCCESS).get(0);
        //the only file between the limits
        retrievedFile.assertAttributeEquals("filename", "file.txt");
    }

    @Test
    public void testListSymFile() throws IOException, InterruptedException {
        emptyTestDirectory();
        TestRunner runner = TestRunners.newTestRunner(ListSFTP.class);
        runner.setProperty(ListSFTP.HOSTNAME, "localhost");
        runner.setProperty(SFTPTransfer.PORT, Integer.toString(sshTestServer.getSSHPort()));
        runner.setProperty(ListSFTP.USERNAME, sshTestServer.getUsername());
        runner.setProperty(SFTPTransfer.PASSWORD, sshTestServer.getPassword());
        runner.setProperty(SFTPTransfer.STRICT_HOST_KEY_CHECKING, "false");
        runner.setProperty(SFTPTransfer.FOLLOW_SYMLINK, "true");
        runner.setProperty(ListSFTP.REMOTE_PATH, "/");
        runner.setProperty(ListFile.TARGET_SYSTEM_TIMESTAMP_PRECISION, ListFile.PRECISION_MILLIS);


        String fileContent = "this is definitely not the sym";

        FileUtils.writeStringToFile(new File(sshTestServer.getVirtualFileSystemPath() + "/bar/testFile2.txt"), fileContent, "UTF-8");
        FileUtils.forceMkdir(new File(sshTestServer.getVirtualFileSystemPath() + "/foo/"));
        Files.createSymbolicLink(Paths.get(sshTestServer.getVirtualFileSystemPath() + "/foo/testFile1.txt").toAbsolutePath(),
                Paths.get(sshTestServer.getVirtualFileSystemPath() + "/bar/testFile2.txt").toAbsolutePath());
        Files.createSymbolicLink(Paths.get(sshTestServer.getVirtualFileSystemPath() + "/testFile.txt").toAbsolutePath(),
                Paths.get(sshTestServer.getVirtualFileSystemPath() + "/foo/testFile1.txt").toAbsolutePath());

        // Ensure wait for enough lag time.
        Thread.sleep(AbstractListProcessor.LISTING_LAG_MILLIS.get(TimeUnit.MILLISECONDS) * 2);

        runner.run();

        runner.assertTransferCount(ListSFTP.REL_SUCCESS, 1);

        final MockFlowFile retrievedFileListing = runner.getFlowFilesForRelationship(ListSFTP.REL_SUCCESS).get(0);

        //path isn't the sym target path
        retrievedFileListing.assertAttributeEquals("path", "");
        //filename isn't the sym target name
        retrievedFileListing.assertAttributeEquals("filename", "testFile.txt");
        //size is the size of the sym target
        retrievedFileListing.assertAttributeEquals(ListFile.FILE_SIZE_ATTRIBUTE, String.valueOf(fileContent.length()));

        runner.clearTransferState();
    }

    @Test
    public void testRecurseSymDirWhenSpecified() throws IOException, InterruptedException {
        emptyTestDirectory();

        TestRunner runner = TestRunners.newTestRunner(ListSFTP.class);
        runner.setProperty(SFTPTransfer.HOSTNAME, "localhost");
        runner.setProperty(SFTPTransfer.PORT, Integer.toString(sshTestServer.getSSHPort()));
        runner.setProperty(SFTPTransfer.USERNAME, sshTestServer.getUsername());
        runner.setProperty(SFTPTransfer.PASSWORD, sshTestServer.getPassword());
        runner.setProperty(SFTPTransfer.STRICT_HOST_KEY_CHECKING, "false");
        runner.setProperty(SFTPTransfer.FOLLOW_SYMLINK, "true");
        runner.setProperty(SFTPTransfer.RECURSIVE_SEARCH, "true");
        runner.setProperty(SFTPTransfer.REMOTE_PATH, "foo/");
        runner.setProperty(ListFile.TARGET_SYSTEM_TIMESTAMP_PRECISION, ListFile.PRECISION_MILLIS);

        touchFile(sshTestServer.getVirtualFileSystemPath() + "foo/testFile1.txt");
        touchFile(sshTestServer.getVirtualFileSystemPath() + "stuff/testFile2.txt");

        Files.createSymbolicLink(Paths.get(sshTestServer.getVirtualFileSystemPath() + "/foo/bar").toAbsolutePath(),
                Paths.get(sshTestServer.getVirtualFileSystemPath() + "bar").toAbsolutePath());

        Files.createSymbolicLink(Paths.get(sshTestServer.getVirtualFileSystemPath() + "/bar").toAbsolutePath(),
                Paths.get(sshTestServer.getVirtualFileSystemPath() + "stuff").toAbsolutePath());

        // Ensure wait for enough lag time.
        Thread.sleep(AbstractListProcessor.LISTING_LAG_MILLIS.get(TimeUnit.MILLISECONDS) * 2);

        runner.run();

        runner.assertTransferCount(ListSFTP.REL_SUCCESS, 2);

        //non sym dir file
        final MockFlowFile retrievedFileListing1 = runner.getFlowFilesForRelationship(ListSFTP.REL_SUCCESS).get(0);
        retrievedFileListing1.assertAttributeEquals("path", "foo");
        retrievedFileListing1.assertAttributeEquals("filename", "testFile1.txt");

        //sym dir file
        final MockFlowFile retrievedFileListing2 = runner.getFlowFilesForRelationship(ListSFTP.REL_SUCCESS).get(1);
        //path is the sym target path
        retrievedFileListing2.assertAttributeEquals("path", "foo/bar");
        retrievedFileListing2.assertAttributeEquals("filename", "testFile2.txt");

        runner.clearTransferState();
    }

    @Test
    public void testDoesntRecurseSymDirWhenNotSpecified() throws IOException, InterruptedException {
        emptyTestDirectory();

        TestRunner runner = TestRunners.newTestRunner(ListSFTP.class);
        runner.setProperty(SFTPTransfer.HOSTNAME, "localhost");
        runner.setProperty(SFTPTransfer.PORT, Integer.toString(sshTestServer.getSSHPort()));
        runner.setProperty(SFTPTransfer.USERNAME, sshTestServer.getUsername());
        runner.setProperty(SFTPTransfer.PASSWORD, sshTestServer.getPassword());
        runner.setProperty(SFTPTransfer.STRICT_HOST_KEY_CHECKING, "false");
        runner.setProperty(SFTPTransfer.FOLLOW_SYMLINK, "true");
        runner.setProperty(SFTPTransfer.RECURSIVE_SEARCH, "false");
        runner.setProperty(SFTPTransfer.REMOTE_PATH, "foo/");
        runner.setProperty(ListFile.TARGET_SYSTEM_TIMESTAMP_PRECISION, ListFile.PRECISION_MILLIS);

        touchFile(sshTestServer.getVirtualFileSystemPath() + "foo/testFile1.txt");
        touchFile(sshTestServer.getVirtualFileSystemPath() + "stuff/testFile2.txt");

        Files.createSymbolicLink(Paths.get(sshTestServer.getVirtualFileSystemPath() + "/foo/bar").toAbsolutePath(),
                Paths.get(sshTestServer.getVirtualFileSystemPath() + "bar").toAbsolutePath());

        Files.createSymbolicLink(Paths.get(sshTestServer.getVirtualFileSystemPath() + "/bar").toAbsolutePath(),
                Paths.get(sshTestServer.getVirtualFileSystemPath() + "stuff").toAbsolutePath());

        // Ensure wait for enough lag time.
        Thread.sleep(AbstractListProcessor.LISTING_LAG_MILLIS.get(TimeUnit.MILLISECONDS) * 2);

        runner.run();

        runner.assertTransferCount(ListSFTP.REL_SUCCESS, 1);

        final MockFlowFile retrievedFileListing = runner.getFlowFilesForRelationship(ListSFTP.REL_SUCCESS).get(0);
        retrievedFileListing.assertAttributeEquals("path", "foo");
        retrievedFileListing.assertAttributeEquals("filename", "testFile1.txt");


        runner.clearTransferState();
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
