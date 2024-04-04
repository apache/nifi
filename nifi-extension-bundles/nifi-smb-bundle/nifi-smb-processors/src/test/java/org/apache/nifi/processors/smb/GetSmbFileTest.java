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
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class GetSmbFileTest {
    private TestRunner testRunner;

    @Mock
    private SMBClient smbClient;

    @Mock
    private Connection connection;

    @Mock
    private Session session;

    @Mock
    private DiskShare diskShare;

    private final static String HOSTNAME = "host";
    private final static String SHARE = "share";
    private final static String DIRECTORY = "nifi\\input";
    private final static String USERNAME = "user";
    private final static String PASSWORD = "pass";

    private void setupSmbProcessor() throws IOException {
        when(smbClient.connect(any(String.class))).thenReturn(connection);
        when(connection.authenticate(any(AuthenticationContext.class))).thenReturn(session);
        when(session.connectShare(SHARE)).thenReturn(diskShare);


        testRunner.setProperty(GetSmbFile.HOSTNAME, HOSTNAME);
        testRunner.setProperty(GetSmbFile.SHARE, SHARE);
        testRunner.setProperty(GetSmbFile.DIRECTORY, DIRECTORY);
        testRunner.setProperty(GetSmbFile.USERNAME, USERNAME);
        testRunner.setProperty(GetSmbFile.PASSWORD, PASSWORD);
    }

    private FileIdBothDirectoryInformation mockFile(String path, String filename, String fileContent, long fileAttributes) {
        File smbfile = mock(File.class);
        final String fullpath = path + "\\" + filename;
        lenient().when(diskShare.openFile(
                eq(fullpath),
                anySet(),
                anySet(),
                anySet(),
                any(SMB2CreateDisposition.class),
                anySet()
        )).thenReturn(smbfile);
        lenient().when(smbfile.getUncPath()).thenReturn(filename);

        if (fileContent != null) {
            InputStream is = new ByteArrayInputStream(fileContent.getBytes(StandardCharsets.UTF_8));
            lenient().when(smbfile.getInputStream()).thenReturn(is);
        }

        FileIdBothDirectoryInformation fdInfo = mock(FileIdBothDirectoryInformation.class);
        lenient().when(fdInfo.getFileName()).thenReturn(filename);
        lenient().when(fdInfo.getFileAttributes()).thenReturn(fileAttributes);

        FileAllInformation fileAllInfo = mock(FileAllInformation.class);
        FileTime fileTime = FileTime.ofEpochMillis(0);
        FileBasicInformation fileBasicInfo = new FileBasicInformation(fileTime, fileTime, fileTime, fileTime, 0);
        FileStandardInformation fileStandardInformation = mock(FileStandardInformation.class);

        lenient().when(smbfile.getFileInformation()).thenReturn(fileAllInfo);
        lenient().when(fileAllInfo.getBasicInformation()).thenReturn(fileBasicInfo);
        lenient().when(fileAllInfo.getStandardInformation()).thenReturn(fileStandardInformation);
        lenient().when(fileStandardInformation.getEndOfFile()).thenReturn((long) 0);

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
        lenient().when(diskShare.folderExists(path)).thenReturn(true);
        lenient().when(diskShare.list(path)).thenReturn(files);

        FileIdBothDirectoryInformation fdInfo = mock(FileIdBothDirectoryInformation.class);
        lenient().when(fdInfo.getFileName()).thenReturn(filename);
        lenient().when(fdInfo.getFileAttributes()).thenReturn(FileAttributes.FILE_ATTRIBUTE_DIRECTORY.getValue());
        return fdInfo;
    }

    @BeforeEach
    public void init() throws IOException {
        testRunner = TestRunners.newTestRunner(new GetSmbFile() {
            @Override
            SMBClient initSmbClient(ProcessContext context) {
                return smbClient;
            }
        });
        setupSmbProcessor();
    }

    @Test
    public void testOpenFileCalled() {
        FileIdBothDirectoryInformation file1 = mockFile(DIRECTORY, "file1.txt", "abc");
        mockDir(DIRECTORY, new ArrayList<>(){{
            add(file1);
        }});
        testRunner.run();
        verifyOpenFile(DIRECTORY, "file1.txt", 1);
        verifyOpenFile(DIRECTORY, "file2.txt", 0);
    }

    @Test
    public void testHiddenFile() {
        testRunner.setProperty(GetSmbFile.IGNORE_HIDDEN_FILES, "true");
        FileIdBothDirectoryInformation file1 = mockFile(DIRECTORY, "file1.txt", "abc", FileAttributes.FILE_ATTRIBUTE_HIDDEN.getValue());
        FileIdBothDirectoryInformation file2 = mockFile(DIRECTORY, "file2.txt", "abc", FileAttributes.FILE_ATTRIBUTE_NORMAL.getValue());
        mockDir(DIRECTORY, new ArrayList<>(){{
            add(file1);
            add(file2);
        }});
        testRunner.run();
        verifyOpenFile(DIRECTORY, "file1.txt", 0);
        verifyOpenFile(DIRECTORY, "file2.txt", 1);
    }

    @Test
    public void testFileFilter() {
        testRunner.setProperty(GetSmbFile.FILE_FILTER, "file[0-9]\\.txt");
        mockDir(DIRECTORY, new ArrayList<>(){{
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
    public void testNonRecurse() {
        testRunner.setProperty(GetSmbFile.RECURSE, "false");
        String subdir = DIRECTORY + "\\subdir1";
        mockDir(DIRECTORY, new ArrayList<>(){{
            add(mockFile(DIRECTORY, "file1.txt", "abc"));
            add(mockFile(DIRECTORY, "file2.txt", "abc"));
            add(mockDir(subdir, new ArrayList<>(){{
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
    public void testRecurse() {
        testRunner.setProperty(GetSmbFile.RECURSE, "true");
        String subdir = DIRECTORY + "\\subdir1";
        mockDir(DIRECTORY, new ArrayList<>(){{
            add(mockFile(DIRECTORY, "file1.txt", "abc"));
            add(mockFile(DIRECTORY, "file2.txt", "abc"));
            add(mockDir(subdir, new ArrayList<>(){{
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
    public void testPathFilter() {
        testRunner.setProperty(GetSmbFile.RECURSE, "true");
        testRunner.setProperty(GetSmbFile.PATH_FILTER, ".*\\\\subdir[0-9]");
        String subdir1 = DIRECTORY + "\\subdir1";
        String subdir2 = DIRECTORY + "\\subdir2";
        String subdir3 = DIRECTORY + "\\foo";
        mockDir(DIRECTORY, new ArrayList<>(){{
            add(mockDir(subdir1, new ArrayList<>(){{
                add(mockFile(subdir1, "file1.txt", "abc"));
            }}));
            add(mockDir(subdir2, new ArrayList<>(){{
                add(mockFile(subdir2, "file2.txt", "abc"));
            }}));
            add(mockDir(subdir3, new ArrayList<>(){{
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
    public void testBatchSize() {
        final int batchSize = 10;
        final int totalSize = batchSize * 2;
        testRunner.setProperty(GetSmbFile.BATCH_SIZE, Integer.toString(batchSize));

        final List<FileIdBothDirectoryInformation> files = new ArrayList<>();
        final String fileNamePrefix = "file-";
        for (int i = 0; i < totalSize; i++) {
            files.add(mockFile(DIRECTORY, fileNamePrefix + i, Integer.toString(i)));
        }
        mockDir(DIRECTORY, files);

        // Avoid stopping on finish and run initialization
        testRunner.run(1, false, true);
        testRunner.assertTransferCount(GetSmbFile.REL_SUCCESS, batchSize);
        // Stop on finish and skip initialization to avoid clearing internal queue of files
        testRunner.run(1, true, false);
        testRunner.assertTransferCount(GetSmbFile.REL_SUCCESS, totalSize);

        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(GetSmbFile.REL_SUCCESS);
        final List<String> flowFileNames = flowFiles.stream()
                .map(flowFile -> flowFile.getAttribute(CoreAttributes.FILENAME.key()))
                .toList();

        for (int i = 0; i < totalSize; i++) {
            final String flowFileName = flowFileNames.get(0);
            assertTrue(flowFileNames.contains(flowFileName), String.format("FlowFile Name [%s] not found", flowFileName));
        }
    }
}
