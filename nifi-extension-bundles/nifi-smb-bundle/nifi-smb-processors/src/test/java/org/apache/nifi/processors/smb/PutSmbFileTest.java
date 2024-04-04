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

import com.hierynomus.mssmb2.SMB2CreateDisposition;
import com.hierynomus.mssmb2.SMB2ShareAccess;
import com.hierynomus.smbj.SMBClient;
import com.hierynomus.smbj.auth.AuthenticationContext;
import com.hierynomus.smbj.connection.Connection;
import com.hierynomus.smbj.server.ServerList;
import com.hierynomus.smbj.session.Session;
import com.hierynomus.smbj.share.DiskEntry;
import com.hierynomus.smbj.share.DiskShare;
import com.hierynomus.smbj.share.File;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Set;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class PutSmbFileTest {

    private TestRunner testRunner;

    private SMBClient smbClient;
    private Connection connection;
    private Session session;
    private DiskShare diskShare;
    private DiskEntry diskEntry;
    private File smbfile;
    private ServerList serverList;
    private ByteArrayOutputStream baOutputStream;

    private final static String HOSTNAME = "smbhostname";
    private final static String SHARE = "smbshare";
    private final static String DIRECTORY = "smbdirectory\\subdir";
    private final static String DOMAIN = "mydomain";
    private final static String USERNAME = "myusername";
    private final static String PASSWORD = "mypassword";

    @Captor
    private ArgumentCaptor<Set<SMB2ShareAccess>> shareAccessSet;
    @Captor
    private ArgumentCaptor<AuthenticationContext> authenticationContext;

    private void setupSmbProcessor() throws IOException {
        smbClient = mock(SMBClient.class);
        connection = mock(Connection.class);
        session = mock(Session.class);
        diskShare = mock(DiskShare.class);
        diskEntry = mock(DiskEntry.class);
        smbfile = mock(File.class);
        serverList = mock(ServerList.class);
        baOutputStream = new ByteArrayOutputStream();

        when(smbClient.connect(any(String.class))).thenReturn(connection);
        when(smbClient.getServerList()).thenReturn(serverList);

        when(connection.authenticate(any(AuthenticationContext.class))).thenReturn(session);
        when(session.connectShare(SHARE)).thenReturn(diskShare);
        when(diskShare.openFile(
                any(String.class),
                anySet(),
                anySet(),
                anySet(),
                any(SMB2CreateDisposition.class),
                anySet()
        )).thenReturn(smbfile);
        when(diskShare.open(
                any(String.class),
                anySet(),
                anySet(),
                anySet(),
                any(SMB2CreateDisposition.class),
                anySet()
        )).thenReturn(diskEntry);
        when(smbfile.getOutputStream()).thenReturn(baOutputStream);

        testRunner.setProperty(PutSmbFile.HOSTNAME, HOSTNAME);
        testRunner.setProperty(PutSmbFile.SHARE, SHARE);
        testRunner.setProperty(PutSmbFile.DIRECTORY, DIRECTORY);
        testRunner.setProperty(PutSmbFile.DOMAIN, DOMAIN);
        testRunner.setProperty(PutSmbFile.USERNAME, USERNAME);
        testRunner.setProperty(PutSmbFile.PASSWORD, PASSWORD);
    }

    private void testDirectoryCreation(String dirFlag, int times) throws IOException {
        when(diskShare.folderExists(DIRECTORY)).thenReturn(false);

        testRunner.setProperty(PutSmbFile.CREATE_DIRS, dirFlag);
        testRunner.enqueue("data");
        testRunner.run();

        verify(diskShare, times(times)).mkdir(DIRECTORY);
    }

    private Set<SMB2ShareAccess> testOpenFileShareAccess() throws IOException {
        testRunner.enqueue("data");
        testRunner.run();

        verify(diskShare, times(1)).openFile(
            any(String.class),
            anySet(),
            anySet(),
            shareAccessSet.capture(),
            any(SMB2CreateDisposition.class),
            anySet()
        );
        return shareAccessSet.getValue();
    }

    private AutoCloseable mockCloseable;

    @BeforeEach
    public void init() throws IOException {
        testRunner = TestRunners.newTestRunner(new PutSmbFile() {
            @Override
            SMBClient initSmbClient(ProcessContext context) {
                return smbClient;
            }
        });
        mockCloseable = MockitoAnnotations.openMocks(this);
        setupSmbProcessor();
    }

    @AfterEach
    public void closeMocks() throws Exception {
        if (mockCloseable != null) {
            mockCloseable.close();
        }
    }

    @Test
    public void testNormalAuth() throws IOException {
        testRunner.enqueue("data");
        testRunner.run();

        verify(connection).authenticate(authenticationContext.capture());
        AuthenticationContext acObj = authenticationContext.getValue();
        assertEquals(acObj.getUsername(), USERNAME);
        assertEquals(acObj.getDomain(), DOMAIN);
        assertArrayEquals(acObj.getPassword(), PASSWORD.toCharArray());
    }

    @Test
    public void testAnonymousAuth() throws IOException {
        testRunner.removeProperty(PutSmbFile.USERNAME);
        testRunner.enqueue("data");
        testRunner.run();

        verify(connection).authenticate(authenticationContext.capture());
        AuthenticationContext acObj = authenticationContext.getValue();
        AuthenticationContext compAc = AuthenticationContext.anonymous();
        assertEquals(acObj.getUsername(), compAc.getUsername());
        assertEquals(acObj.getDomain(), compAc.getDomain());
        assertArrayEquals(acObj.getPassword(), compAc.getPassword());
    }

    @Test
    public void testDirExistsWithoutCreate() throws IOException {
        testDirectoryCreation("false", 0);
    }

    @Test
    public void testDirExistsWithCreate() throws IOException {
        testDirectoryCreation("true", 1);
    }

    @Test
    public void testDirectoriesCreatedWhenDontExists() throws IOException {
        final String directory = new java.io.File("a\\b\\c\\b\\e").getPath();
        final int count = directory.split(java.util.regex.Pattern.quote(java.io.File.separator)).length;
        when(diskShare.folderExists(DIRECTORY)).thenReturn(false);

        testRunner.setProperty(PutSmbFile.CREATE_DIRS, "true");
        testRunner.setProperty(PutSmbFile.DIRECTORY, directory);
        testRunner.enqueue("data");
        testRunner.run();

        verify(diskShare, times(count)).mkdir(
            any(String.class)
        );
    }

    @Test
    public void testFileShareNone() throws IOException {
        testRunner.setProperty(PutSmbFile.SHARE_ACCESS, PutSmbFile.SHARE_ACCESS_NONE);
        testRunner.setProperty(PutSmbFile.CREATE_DIRS, "true");
        Set<SMB2ShareAccess> shareAccessSet = testOpenFileShareAccess();
        assertTrue(shareAccessSet.isEmpty());
    }

    @Test
    public void testFileShareRead() throws IOException {
        testRunner.setProperty(PutSmbFile.SHARE_ACCESS, PutSmbFile.SHARE_ACCESS_READ);
        testRunner.setProperty(PutSmbFile.CREATE_DIRS, "true");
        Set<SMB2ShareAccess> shareAccessSet = testOpenFileShareAccess();
        assertTrue(shareAccessSet.contains(SMB2ShareAccess.FILE_SHARE_READ));
    }

    @Test
    public void testFileShareReadWriteDelete() throws IOException {
        testRunner.setProperty(PutSmbFile.SHARE_ACCESS, PutSmbFile.SHARE_ACCESS_READWRITEDELETE);
        testRunner.setProperty(PutSmbFile.CREATE_DIRS, "true");
        Set<SMB2ShareAccess> shareAccessSet = testOpenFileShareAccess();
        assertTrue(shareAccessSet.contains(SMB2ShareAccess.FILE_SHARE_READ));
        assertTrue(shareAccessSet.contains(SMB2ShareAccess.FILE_SHARE_WRITE));
        assertTrue(shareAccessSet.contains(SMB2ShareAccess.FILE_SHARE_DELETE));
    }

    @Test
    public void testFileExistsFail() throws IOException {
        testRunner.setProperty(PutSmbFile.CONFLICT_RESOLUTION, PutSmbFile.FAIL_RESOLUTION);
        when(diskShare.fileExists(any(String.class))).thenReturn(true);
        testRunner.assertAllFlowFilesTransferred(PutSmbFile.REL_FAILURE);
    }

    @Test
    public void testFileExistsIgnore() throws IOException {
        testRunner.setProperty(PutSmbFile.CONFLICT_RESOLUTION, PutSmbFile.IGNORE_RESOLUTION);
        when(diskShare.fileExists(any(String.class))).thenReturn(true);
        testRunner.assertAllFlowFilesTransferred(PutSmbFile.REL_SUCCESS);
    }

    @Test
    public void testTemporarySuffixIsUnset() throws IOException {
        testRunner.enqueue("data");
        testRunner.run();

        verify(diskShare, never()).open(
            any(String.class),
            anySet(),
            anySet(),
            anySet(),
            any(SMB2CreateDisposition.class),
            anySet()
        );
    }

    @Test
    public void testTemporarySuffixIsSet() throws IOException {
        final String suffix = ".test";

        testRunner.setProperty(PutSmbFile.RENAME_SUFFIX, suffix);
        testRunner.setProperty(PutSmbFile.CREATE_DIRS, "true");
        testRunner.enqueue("data");
        testRunner.run();

        ArgumentCaptor<String> filename = ArgumentCaptor.forClass(String.class);

        verify(diskShare, times(1)).open(
            filename.capture(),
            anySet(),
            anySet(),
            anySet(),
            any(SMB2CreateDisposition.class),
            anySet()
        );

        assertTrue(filename.getValue().endsWith(suffix), "Suffix is not present");
    }

    @Test
    public void testTemporarySuffixIsSetRenameIsCalled() throws IOException {
        final String suffix = ".test";

        testRunner.setProperty(PutSmbFile.RENAME_SUFFIX, suffix);
        testRunner.setProperty(PutSmbFile.CREATE_DIRS, "true");
        testRunner.enqueue("data");
        testRunner.run();

        ArgumentCaptor<String> initialFilename = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> finalFilename = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Boolean> replace = ArgumentCaptor.forClass(Boolean.class);

        verify(diskShare, times(1)).open(
            initialFilename.capture(),
            anySet(),
            anySet(),
            anySet(),
            any(SMB2CreateDisposition.class),
            anySet()
        );

        verify(diskEntry, times(1)).rename(
            finalFilename.capture(),
            replace.capture()
        );

        assertTrue(initialFilename.getValue().endsWith(suffix), "Suffix is not present and it should be");
        assertTrue(!finalFilename.getValue().endsWith(suffix), "Suffix is present and it shouldn't be");
        assertTrue(replace.getValue(), "Replace flag shold be true");
    }

    @Test
    public void testConnectionError() throws IOException {
        String emsg = "mock connection exception";
        when(smbClient.connect(any(String.class))).thenThrow(new IOException(emsg));

        testRunner.enqueue("1");
        testRunner.enqueue("2");
        testRunner.enqueue("3");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutSmbFile.REL_FAILURE, 3);
    }
}
