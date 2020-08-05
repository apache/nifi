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
package org.apache.nifi.processors.smb;

import com.hierynomus.msdtyp.FileTime;
import com.hierynomus.msfscc.FileAttributes;
import com.hierynomus.msfscc.fileinformation.FileAllInformation;
import com.hierynomus.msfscc.fileinformation.FileBasicInformation;
import com.hierynomus.msfscc.fileinformation.FileIdBothDirectoryInformation;
import com.hierynomus.msfscc.fileinformation.FileStandardInformation;
import com.hierynomus.mssmb2.SMB2CreateDisposition;
import com.hierynomus.smbj.SMBClient;
import com.hierynomus.smbj.auth.AuthenticationContext;
import com.hierynomus.smbj.connection.Connection;
import com.hierynomus.smbj.session.Session;
import com.hierynomus.smbj.share.DiskShare;
import com.hierynomus.smbj.share.File;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;

public class GetSmbFileTest {
    private TestRunner testRunner;

    private SMBClient smbClient;
    private Connection connection;
    private Session session;
    private DiskShare diskShare;
    private ByteArrayOutputStream baOutputStream;

    private final static String HOSTNAME = "host";
    private final static String SHARE = "share";
    private final static String DIRECTORY = "nifi\\input";
    private final static String DOMAIN = "";
    private final static String USERNAME = "user";
    private final static String PASSWORD = "pass";

    private void setupSmbProcessor() throws IOException {
        smbClient = mock(SMBClient.class);
        connection = mock(Connection.class);
        session = mock(Session.class);
        diskShare = mock(DiskShare.class);

        baOutputStream = new ByteArrayOutputStream();

        when(smbClient.connect(any(String.class))).thenReturn(connection);
        when(connection.authenticate(any(AuthenticationContext.class))).thenReturn(session);
        when(session.connectShare(SHARE)).thenReturn(diskShare);


        testRunner.setProperty(GetSmbFile.HOSTNAME, HOSTNAME);
        testRunner.setProperty(GetSmbFile.SHARE, SHARE);
        testRunner.setProperty(GetSmbFile.DIRECTORY, DIRECTORY);
        if (!DOMAIN.isEmpty()) {
            testRunner.setProperty(GetSmbFile.DOMAIN, DOMAIN);
        }
        testRunner.setProperty(GetSmbFile.USERNAME, USERNAME);
        testRunner.setProperty(GetSmbFile.PASSWORD, PASSWORD);

        GetSmbFile GetSmbFile = (GetSmbFile) testRunner.getProcessor();
        GetSmbFile.initSmbClient(smbClient);

    }

    private FileIdBothDirectoryInformation mockFile(String path, String filename, String fileContent, long fileAttributes) {
        File smbfile = mock(File.class);
        final String fullpath = path + "\\" + filename;
        when(diskShare.openFile(
                eq(fullpath),
                anySet(),
                anySet(),
                anySet(),
                any(SMB2CreateDisposition.class),
                anySet()
        )).thenReturn(smbfile);
        when(smbfile.getFileName()).thenReturn(filename);

        if (fileContent != null) {
            InputStream is = new ByteArrayInputStream(fileContent.getBytes(StandardCharsets.UTF_8));
            when(smbfile.getInputStream()).thenReturn(is);
        }

        FileIdBothDirectoryInformation fdInfo = mock(FileIdBothDirectoryInformation.class);
        when(fdInfo.getFileName()).thenReturn(filename);
        when(fdInfo.getFileAttributes()).thenReturn(fileAttributes);

        FileAllInformation fileAllInfo = mock(FileAllInformation.class);
        FileTime fileTime = FileTime.ofEpochMillis(0);
        FileBasicInformation fileBasicInfo = new FileBasicInformation(fileTime, fileTime, fileTime, fileTime, 0);
        FileStandardInformation fileStandardInformation = mock(FileStandardInformation.class);

        when(smbfile.getFileInformation()).thenReturn(fileAllInfo);
        when(fileAllInfo.getBasicInformation()).thenReturn(fileBasicInfo);
        when(fileAllInfo.getStandardInformation()).thenReturn(fileStandardInformation);
        when(fileStandardInformation.getEndOfFile()).thenReturn((long) 0);

        return fdInfo;
    }

    private FileIdBothDirectoryInformation mockFile(String path, String filename, String fileContent) {
        return mockFile(path, filename, fileContent, FileAttributes.FILE_ATTRIBUTE_NORMAL.getValue());
    }

    private void verifyOpenFile(String path, String filename, int times) {
        final String fullpath = path + "\\" + filename;
        verify(diskShare, times(times)).openFile(
            eq(fullpath),
            anySet(),
            anySet(),
            anySet(),
            any(SMB2CreateDisposition.class),
            anySet()
        );
    }

    private FileIdBothDirectoryInformation mockDir(String path, List<FileIdBothDirectoryInformation> files) {
        final String[] fileSplits = path.split("\\\\");
        final String filename = fileSplits[fileSplits.length - 1];
        when(diskShare.folderExists(path)).thenReturn(true);
        when(diskShare.list(path)).thenReturn(files);

        FileIdBothDirectoryInformation fdInfo = mock(FileIdBothDirectoryInformation.class);
        when(fdInfo.getFileName()).thenReturn(filename);
        when(fdInfo.getFileAttributes()).thenReturn(FileAttributes.FILE_ATTRIBUTE_DIRECTORY.getValue());
        return fdInfo;
    }

    @Before
    public void init() throws IOException {
        testRunner = TestRunners.newTestRunner(GetSmbFile.class);
        MockitoAnnotations.initMocks(this);
        setupSmbProcessor();
    }

    @Test
    public void testOpenFileCalled() throws IOException {
        FileIdBothDirectoryInformation file1 = mockFile(DIRECTORY, "file1.txt", "abc");
        mockDir(DIRECTORY, new ArrayList<FileIdBothDirectoryInformation>(){{
            add(file1);
        }});
        testRunner.run();
        verifyOpenFile(DIRECTORY, "file1.txt", 1);
        verifyOpenFile(DIRECTORY, "file2.txt", 0);
    }

    @Test
    public void testHiddenFile() throws IOException {
        testRunner.setProperty(GetSmbFile.IGNORE_HIDDEN_FILES, "true");
        FileIdBothDirectoryInformation file1 = mockFile(DIRECTORY, "file1.txt", "abc", FileAttributes.FILE_ATTRIBUTE_HIDDEN.getValue());
        FileIdBothDirectoryInformation file2 = mockFile(DIRECTORY, "file2.txt", "abc", FileAttributes.FILE_ATTRIBUTE_NORMAL.getValue());
        mockDir(DIRECTORY, new ArrayList<FileIdBothDirectoryInformation>(){{
            add(file1);
            add(file2);
        }});
        testRunner.run();
        verifyOpenFile(DIRECTORY, "file1.txt", 0);
        verifyOpenFile(DIRECTORY, "file2.txt", 1);
    }

    @Test
    public void testFileFilter() throws IOException {
        testRunner.setProperty(GetSmbFile.FILE_FILTER, "file[0-9]\\.txt");
        mockDir(DIRECTORY, new ArrayList<FileIdBothDirectoryInformation>(){{
            add(mockFile(DIRECTORY, "something_else.txt", "abc"));
            add(mockFile(DIRECTORY, "file1.txt", "abc"));
            add(mockFile(DIRECTORY, "file2.txt", "abc"));
        }});
        testRunner.run();
        verifyOpenFile(DIRECTORY, "something_else.txt", 0);
        verifyOpenFile(DIRECTORY, "file1.txt", 1);
        verifyOpenFile(DIRECTORY, "file2.txt", 1);
        testRunner.assertTransferCount(GetSmbFile.REL_SUCCESS, 2);
    }

    @Test
    public void testNonRecurse() throws IOException {
        testRunner.setProperty(GetSmbFile.RECURSE, "false");
        String subdir = DIRECTORY + "\\subdir1";
        mockDir(DIRECTORY, new ArrayList<FileIdBothDirectoryInformation>(){{
            add(mockFile(DIRECTORY, "file1.txt", "abc"));
            add(mockFile(DIRECTORY, "file2.txt", "abc"));
            add(mockDir(subdir, new ArrayList<FileIdBothDirectoryInformation>(){{
                add(mockFile(subdir, "file3.txt", "abc"));
            }}));
        }});

        testRunner.run();
        verifyOpenFile(DIRECTORY, "file1.txt", 1);
        verifyOpenFile(DIRECTORY, "file2.txt", 1);
        verifyOpenFile(subdir, "file3.txt", 0);
        testRunner.assertTransferCount(GetSmbFile.REL_SUCCESS, 2);
    }

    @Test
    public void testRecurse() throws IOException {
        testRunner.setProperty(GetSmbFile.RECURSE, "true");
        String subdir = DIRECTORY + "\\subdir1";
        mockDir(DIRECTORY, new ArrayList<FileIdBothDirectoryInformation>(){{
            add(mockFile(DIRECTORY, "file1.txt", "abc"));
            add(mockFile(DIRECTORY, "file2.txt", "abc"));
            add(mockDir(subdir, new ArrayList<FileIdBothDirectoryInformation>(){{
                add(mockFile(subdir, "file3.txt", "abc"));
            }}));
        }});


        testRunner.run();
        verifyOpenFile(DIRECTORY, "file1.txt", 1);
        verifyOpenFile(DIRECTORY, "file2.txt", 1);
        verifyOpenFile(subdir, "file3.txt", 1);
        testRunner.assertTransferCount(GetSmbFile.REL_SUCCESS, 3);
    }

    @Test
    public void testPathFilter() throws IOException {
        testRunner.setProperty(GetSmbFile.RECURSE, "true");
        testRunner.setProperty(GetSmbFile.PATH_FILTER, ".*\\\\subdir[0-9]");
        String subdir1 = DIRECTORY + "\\subdir1";
        String subdir2 = DIRECTORY + "\\subdir2";
        String subdir3 = DIRECTORY + "\\foo";
        mockDir(DIRECTORY, new ArrayList<FileIdBothDirectoryInformation>(){{
            add(mockDir(subdir1, new ArrayList<FileIdBothDirectoryInformation>(){{
                add(mockFile(subdir1, "file1.txt", "abc"));
            }}));
            add(mockDir(subdir2, new ArrayList<FileIdBothDirectoryInformation>(){{
                add(mockFile(subdir2, "file2.txt", "abc"));
            }}));
            add(mockDir(subdir3, new ArrayList<FileIdBothDirectoryInformation>(){{
                add(mockFile(subdir3, "file3.txt", "abc"));
            }}));
        }});


        testRunner.run();
        verifyOpenFile(subdir1, "file1.txt", 1);
        verifyOpenFile(subdir2, "file2.txt", 1);
        verifyOpenFile(subdir3, "file3.txt", 0);
        testRunner.assertTransferCount(GetSmbFile.REL_SUCCESS, 2);
    }

    @Test
    public void testBatchSize() throws IOException {
        int batchSize = 10;
        testRunner.setProperty(GetSmbFile.BATCH_SIZE, Integer.toString(batchSize));
        List<FileIdBothDirectoryInformation> fileList = new ArrayList<FileIdBothDirectoryInformation>();
        for (int i=0; i<batchSize*2; i++) {
            fileList.add(mockFile(DIRECTORY, "file" + i + ".txt", Integer.toString(i)));
        }
        mockDir(DIRECTORY, fileList);

        testRunner.run();
        testRunner.assertTransferCount(GetSmbFile.REL_SUCCESS, batchSize);
        testRunner.run();
        testRunner.assertTransferCount(GetSmbFile.REL_SUCCESS, batchSize*2);
        List<String> queuedFileNames = testRunner.getFlowFilesForRelationship(GetSmbFile.REL_SUCCESS).stream()
                .map(f -> f.getAttribute(CoreAttributes.FILENAME.key()))
                .collect(Collectors.toList());
        for (int i=0; i<batchSize*2; i++) {
            queuedFileNames.contains("file" + i + ".txt");
        }
    }
}
