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

import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.ftplet.FileSystemFactory;
import org.apache.ftpserver.ftplet.FileSystemView;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.FtpFile;
import org.apache.ftpserver.ftplet.UserManager;
import org.apache.ftpserver.listener.Listener;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.usermanager.ClearTextPasswordEncryptor;
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory;
import org.apache.ftpserver.usermanager.impl.BaseUser;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.util.file.transfer.FetchFileTransfer;
import org.apache.nifi.processor.util.file.transfer.ListFileTransfer;
import org.apache.nifi.processor.util.file.transfer.PutFileTransfer;
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
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Seed an ASCII charset folder in FTP server with files using i18n known filenames.  Iterate through a set of i18n folder
 * names, copying the known content into each new target.  Afterwards, verify that the last folder contains all
 * files with the expected filenames.
 * <p>
 * To test against a live FTP server, run test with system property set
 * <code>TestFTPCharset=hostname,port,user,password</code>, like
 * <code>TestFTPCharset=localhost,21,ftpuser,ftppassword</code>.
 */
@TestMethodOrder(MethodOrderer.MethodName.class)
public class FTPCharsetIT {
    private static final String SERVER_OVERRIDE = System.getProperty(FTPCharsetIT.class.getSimpleName());
    private static final boolean EMBED_FTP_SERVER = (SERVER_OVERRIDE == null);
    private static FtpServer FTP_SERVER;

    private static final String USE_UTF8 = Boolean.TRUE.toString();
    private static final String USER = "ftpuser";
    private static final String PASSWORD = "admin";
    private static final String TIMEOUT = "3 secs";

    @TempDir
    private static File FOLDER_FTP;
    @TempDir
    private static File FOLDER_USER_PROPERTIES;

    private static int listeningPort;


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

    @BeforeAll
    static void startEmbeddedServer() throws IOException, FtpException {
        if (!EMBED_FTP_SERVER) {
            return;
        }

        // setup ftp user
        final Properties userProperties = new Properties();
        userProperties.setProperty("ftpserver.user.ftpuser.idletime", "0");
        userProperties.setProperty("ftpserver.user.ftpuser.enableflag", Boolean.TRUE.toString());
        userProperties.setProperty("ftpserver.user.ftpuser.userpassword", PASSWORD);
        userProperties.setProperty("ftpserver.user.ftpuser.writepermission", Boolean.TRUE.toString());
        userProperties.setProperty("ftpserver.user.ftpuser.homedirectory", FOLDER_FTP.getAbsolutePath());
        final File userPropertiesFile = new File(FOLDER_USER_PROPERTIES, "user.properties");
        try (final FileOutputStream fos = new FileOutputStream(userPropertiesFile)) {
            userProperties.store(fos, "ftp-user-properties");
        }
        final PropertiesUserManagerFactory userManagerFactory = new PropertiesUserManagerFactory();
        userManagerFactory.setUrl(userPropertiesFile.toURI().toURL());
        userManagerFactory.setPasswordEncryptor(new ClearTextPasswordEncryptor());
        final UserManager userManager = userManagerFactory.createUserManager();
        final BaseUser ftpuser = (BaseUser) userManager.getUserByName(USER);

        // setup embedded ftp server
        final FtpServerFactory serverFactory = new FtpServerFactory();
        serverFactory.setUserManager(userManager);
        final FileSystemFactory fileSystemFactory = serverFactory.getFileSystem();
        final FileSystemView view = fileSystemFactory.createFileSystemView(ftpuser);
        final FtpFile workingDirectory = view.getWorkingDirectory();
        final Object physicalFile = workingDirectory.getPhysicalFile();
        assertInstanceOf(File.class, physicalFile);
        assertEquals(FOLDER_FTP.getAbsolutePath(), ((File) physicalFile).getAbsolutePath());

        final ListenerFactory factory = new ListenerFactory();
        factory.setPort(0);
        serverFactory.addListener("default", factory.createListener());
        FTP_SERVER = serverFactory.createServer();
        FTP_SERVER.start();

        final Collection<Listener> listeners = serverFactory.getListeners().values();
        listeningPort = listeners.isEmpty() ? 0 : listeners.iterator().next().getPort();
    }

    @AfterAll
    static void stopEmbeddedServer() {
        if (EMBED_FTP_SERVER) {
            FTP_SERVER.stop();
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
        flowFile.putAttributes(Collections.singletonMap(CoreAttributes.FILENAME.key(), "0.txt"));
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
            flowFile.putAttributes(Collections.singletonMap(CoreAttributes.FILENAME.key(), filename));
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
        runnerList.assertTransferCount(AbstractListProcessor.REL_SUCCESS, filenamesExpected.size());
        final List<MockFlowFile> flowFilesList = runnerList.getFlowFilesForRelationship(AbstractListProcessor.REL_SUCCESS);
        for (MockFlowFile flowFile : flowFilesList) {
            filenamesExpected.remove(flowFile.getAttribute(CoreAttributes.FILENAME.key()));
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
        runnerList.assertTransferCount(AbstractListProcessor.REL_SUCCESS, fileCount);
        final List<MockFlowFile> flowFilesList = runnerList.getFlowFilesForRelationship(AbstractListProcessor.REL_SUCCESS);
        // FetchFTP
        final TestRunner runnerFetch = provisionTestRunner(FetchFTP.class);
        for (MockFlowFile flowFile : flowFilesList) {
            runnerFetch.enqueue(flowFile);
        }
        runnerFetch.setProperty(FetchFTP.REMOTE_FILENAME, "${path}/${filename}");
        runnerFetch.run(flowFilesList.size());
        runnerFetch.assertTransferCount(FetchFTP.REL_COMMS_FAILURE, 0);
        runnerFetch.assertTransferCount(FetchFTP.REL_NOT_FOUND, 0);
        runnerFetch.assertTransferCount(FetchFTP.REL_PERMISSION_DENIED, 0);
        runnerFetch.assertTransferCount(FetchFileTransfer.REL_SUCCESS, fileCount);
        final List<MockFlowFile> flowFilesFetch = runnerFetch.getFlowFilesForRelationship(AbstractListProcessor.REL_SUCCESS);
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

        final String valueOverrides = System.getProperty(FTPCharsetIT.class.getSimpleName());
        if (valueOverrides == null) {
            runner.setProperty(FTPTransfer.HOSTNAME, "localhost");
            runner.setProperty(FTPTransfer.PORT, String.valueOf(listeningPort));
            runner.setProperty(FTPTransfer.USERNAME, USER);
            runner.setProperty(FTPTransfer.PASSWORD, PASSWORD);
        } else {
            final String[] serverParameters = valueOverrides.split(",");
            runner.setProperty(FTPTransfer.HOSTNAME, serverParameters[0]);
            runner.setProperty(FTPTransfer.PORT, serverParameters[1]);
            runner.setProperty(FTPTransfer.USERNAME, serverParameters[2]);
            runner.setProperty(FTPTransfer.PASSWORD, serverParameters[3]);
        }

        runner.setProperty(FTPTransfer.UTF8_ENCODING, USE_UTF8);
        runner.setProperty(FTPTransfer.CONNECTION_TIMEOUT, TIMEOUT);
        runner.setProperty(FTPTransfer.DATA_TIMEOUT, TIMEOUT);
        return runner;
    }
}
