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
import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.ftplet.FileSystemFactory;
import org.apache.ftpserver.ftplet.FileSystemView;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.FtpFile;
import org.apache.ftpserver.ftplet.UserManager;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.usermanager.ClearTextPasswordEncryptor;
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory;
import org.apache.ftpserver.usermanager.impl.BaseUser;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.util.list.AbstractListProcessor;
import org.apache.nifi.processors.standard.util.FTPTransfer;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Seed a UTF-7 folder in FTP server with files using i18n known filenames.  Iterate through a set of i18n folder
 * names, copying the known content into each new target.  Afterwards, verify that the last folder contains all
 * files with the expected filenames.
 * <p>
 * To test against a live FTP server:
 * <ol>
 *     <li>replace EMBED_FTP_SERVER value with <code>false</code></li>
 *     <li>replace HOSTNAME, PORT, USER, PASSWORD as needed</li>
 * </ol>
 */
@TestMethodOrder(MethodOrderer.MethodName.class)
public class TestFTPCharset {
    private static final boolean EMBED_FTP_SERVER = true;
    private static FtpServer FTP_SERVER;

    private static final String USE_UTF8 = Boolean.TRUE.toString();
    private static final String HOSTNAME = "localhost";
    private static final int PORT = 2121;
    private static final String USER = "ftpuser";
    private static final String PASSWORD = "admin";
    private static final File FOLDER = new File("target", TestFTPCharset.class.getSimpleName());

    public static Stream<Arguments> folderNamesProvider() {
        return Stream.of(
                arguments("folder1", "folder2"),
                arguments("folder2", "æøå"),
                arguments("æøå", "folder3"),
                arguments("folder3", "اختبار"),
                arguments("اختبار", "folder4"),
                arguments("folder4", "Госагїzатїой"),
                arguments("Госагїzатїой", "folder5"),
                arguments("folder5", "し回亡丹し工z丹卞工回几"),
                arguments("し回亡丹し工z丹卞工回几", "folder6")
        );
    }

    public static Stream<String> filenamesProvider() {
        return Stream.of(
                "1.txt",
                "æøå.txt",
                "اختبار.txt",
                "Госагїzатїой.txt",
                "し回亡丹し工z丹卞工回几.txt");
    }

    /**
     * Start test FTP server (if using embedded).
     *
     * @throws FtpException on server startup failure
     */
    @BeforeAll
    static void beforeAll() throws FtpException {
        if (EMBED_FTP_SERVER) {
            FOLDER.mkdirs();
            // setup ftp user
            final PropertiesUserManagerFactory userManagerFactory = new PropertiesUserManagerFactory();
            userManagerFactory.setUrl(TestFTPCharset.class.getClassLoader()
                    .getResource("TestFTPCharset/users.properties"));
            userManagerFactory.setPasswordEncryptor(new ClearTextPasswordEncryptor());
            final UserManager userManager = userManagerFactory.createUserManager();
            final BaseUser ftpuser = (BaseUser) userManager.getUserByName(USER);
            ftpuser.setHomeDirectory(FOLDER.getAbsolutePath());
            userManager.save(ftpuser);
            // setup embedded ftp server
            final FtpServerFactory serverFactory = new FtpServerFactory();
            serverFactory.setUserManager(userManager);
            final FileSystemFactory fileSystemFactory = serverFactory.getFileSystem();
            final FileSystemView view = fileSystemFactory.createFileSystemView(ftpuser);
            final FtpFile workingDirectory = view.getWorkingDirectory();
            final Object physicalFile = workingDirectory.getPhysicalFile();
            assertTrue(physicalFile instanceof File);
            assertEquals(FOLDER.getAbsolutePath(), ((File) physicalFile).getAbsolutePath());
            final ListenerFactory factory = new ListenerFactory();
            factory.setPort(PORT);
            serverFactory.addListener("default", factory.createListener());
            FTP_SERVER = serverFactory.createServer();
            FTP_SERVER.start();
        }
    }

    /**
     * Stop test FTP server (if using embedded).
     *
     * @throws IOException on failure to clean up test data
     */
    @AfterAll
    static void afterAll() throws IOException {
        if (EMBED_FTP_SERVER) {
            FTP_SERVER.stop();
            while (!FTP_SERVER.isStopped()) {
                try {
                    Thread.sleep(100L);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            FileUtils.deleteDirectory(FOLDER);
        }
    }


    /**
     * Test connectivity to FTP server.
     */
    @Test
    public void test0SeedFTPPut1() {
        final TestRunner runnerPut = provisionTestRunner(PutFTP.class);
        final String folderName = "folder0";
        final MockFlowFile flowFile = new MockFlowFile(1L);
        flowFile.putAttributes(Collections.singletonMap("filename", "0.txt"));
        flowFile.setData(new Date().toString().getBytes(UTF_8));
        runnerPut.enqueue(flowFile);
        runnerPut.setValidateExpressionUsage(false);
        runnerPut.setProperty(FTPTransfer.REMOTE_PATH, folderName);
        runnerPut.setProperty(FTPTransfer.CREATE_DIRECTORY, Boolean.TRUE.toString());
        runnerPut.setProperty(FTPTransfer.DOT_RENAME, Boolean.FALSE.toString());
        runnerPut.run();
        runnerPut.assertTransferCount(PutFileTransfer.REL_FAILURE, 0);
        runnerPut.assertTransferCount(PutFileTransfer.REL_REJECT, 0);
        runnerPut.assertTransferCount(PutFileTransfer.REL_SUCCESS, 1);
    }

    /**
     * Seed FTP server with data to be propagated.
     */
    @Test
    public void test0SeedFTPPutAll() {
        int id = 0;
        final Object[] argumentsFirstFolder = folderNamesProvider().iterator().next().get();
        final String folderName = Arrays.stream(argumentsFirstFolder).iterator().next().toString();
        final TestRunner runnerPut = provisionTestRunner(PutFTP.class);
        runnerPut.setValidateExpressionUsage(false);
        runnerPut.setProperty(FTPTransfer.REMOTE_PATH, folderName);
        runnerPut.setProperty(FTPTransfer.CREATE_DIRECTORY, Boolean.TRUE.toString());
        runnerPut.setProperty(FTPTransfer.DOT_RENAME, Boolean.FALSE.toString());

        final Iterator<String> iteratorFilenames = filenamesProvider().iterator();
        while (iteratorFilenames.hasNext()) {
            final String filename = iteratorFilenames.next();
            final MockFlowFile flowFile = new MockFlowFile(++id);
            flowFile.putAttributes(Collections.singletonMap("filename", filename));
            flowFile.setData(new Date().toString().getBytes(UTF_8));
            runnerPut.enqueue(flowFile);
        }
        final int fileCount = (int) filenamesProvider().count();
        runnerPut.run();
        runnerPut.assertTransferCount(PutFileTransfer.REL_FAILURE, 0);
        runnerPut.assertTransferCount(PutFileTransfer.REL_REJECT, 0);
        runnerPut.assertTransferCount(PutFileTransfer.REL_SUCCESS, fileCount);
    }

    /**
     * Verify that data has been successfully propagated.
     */
    @Test
    public void test9FTPVerifyAll() {
        final Set<String> filenamesExpected = filenamesProvider().collect(Collectors.toSet());

        final Object[] argumentsLastFolder = folderNamesProvider()
                .reduce((prev, next) -> next).orElseThrow(IllegalStateException::new).get();
        final String folderName = Arrays.stream(argumentsLastFolder)
                .reduce((prev, next) -> next).orElseThrow(IllegalStateException::new).toString();
        final TestRunner runnerList = provisionTestRunner(ListFTP.class);
        runnerList.setProperty(ListFileTransfer.FILE_TRANSFER_LISTING_STRATEGY, AbstractListProcessor.NO_TRACKING);
        runnerList.setProperty(FTPTransfer.REMOTE_PATH, folderName);
        runnerList.clearTransferState();
        runnerList.run(1);
        runnerList.assertTransferCount("success", filenamesExpected.size());
        final List<MockFlowFile> flowFilesList = runnerList.getFlowFilesForRelationship("success");
        for (MockFlowFile flowFile : flowFilesList) {
            filenamesExpected.remove(flowFile.getAttribute("filename"));
        }
        assertTrue(filenamesExpected.isEmpty());
    }

    /**
     * For each parameterized invocation, copy files using FTP protocol from source folder to target folder.  This
     * implicitly verifies charset handling of ListFTP, FetchFTP, and PutFTP processors.
     *
     * @param source the folder name from which to retrieve data
     * @param target the folder name to which data should be copied
     */
    @ParameterizedTest
    @MethodSource("folderNamesProvider")
    public void test1FTP(final String source, final String target) {
        final int fileCount = (int) filenamesProvider().count();
        // ListFTP
        final TestRunner runnerList = provisionTestRunner(ListFTP.class);
        runnerList.setProperty(ListFileTransfer.FILE_TRANSFER_LISTING_STRATEGY, AbstractListProcessor.NO_TRACKING);
        runnerList.setProperty(FTPTransfer.REMOTE_PATH, source);
        runnerList.clearTransferState();
        runnerList.run(1);
        runnerList.assertTransferCount("success", fileCount);
        final List<MockFlowFile> flowFilesList = runnerList.getFlowFilesForRelationship("success");
        // FetchFTP
        final TestRunner runnerFetch = provisionTestRunner(FetchFTP.class);
        for (MockFlowFile flowFile : flowFilesList) {
            runnerFetch.enqueue(flowFile);
        }
        runnerFetch.setProperty(FetchFTP.REMOTE_FILENAME, "${path}/${filename}");
        runnerFetch.run(flowFilesList.size());
        runnerFetch.assertTransferCount("comms.failure", 0);
        runnerFetch.assertTransferCount("not.found", 0);
        runnerFetch.assertTransferCount("permission.denied", 0);
        runnerFetch.assertTransferCount("success", fileCount);
        final List<MockFlowFile> flowFilesFetch = runnerFetch.getFlowFilesForRelationship("success");
        // PutFTP
        final TestRunner runnerPut = provisionTestRunner(PutFTP.class);
        for (MockFlowFile flowFile : flowFilesFetch) {
            runnerPut.enqueue(flowFile);
        }
        runnerPut.setValidateExpressionUsage(false);
        runnerPut.setProperty(FTPTransfer.REMOTE_PATH, target);
        runnerPut.setProperty(FTPTransfer.CREATE_DIRECTORY, Boolean.TRUE.toString());
        runnerPut.setProperty(FTPTransfer.DOT_RENAME, Boolean.FALSE.toString());
        runnerPut.run(flowFilesList.size());
        runnerPut.assertTransferCount(PutFileTransfer.REL_FAILURE, 0);
        runnerPut.assertTransferCount(PutFileTransfer.REL_REJECT, 0);
        runnerPut.assertTransferCount(PutFileTransfer.REL_SUCCESS, fileCount);
    }

    private static TestRunner provisionTestRunner(final Class<? extends Processor> processorClass) {
        final TestRunner runner = TestRunners.newTestRunner(processorClass);
        runner.setProperty(FTPTransfer.HOSTNAME, HOSTNAME);
        runner.setProperty(FTPTransfer.PORT, Integer.toString(PORT));
        runner.setProperty(FTPTransfer.USERNAME, USER);
        runner.setProperty(FTPTransfer.PASSWORD, PASSWORD);
        runner.setProperty(FTPTransfer.UTF8_ENCODING, USE_UTF8);
        runner.setProperty(FTPTransfer.CONNECTION_TIMEOUT, "3 secs");
        runner.setProperty(FTPTransfer.DATA_TIMEOUT, "3 secs");
        return runner;
    }
}
