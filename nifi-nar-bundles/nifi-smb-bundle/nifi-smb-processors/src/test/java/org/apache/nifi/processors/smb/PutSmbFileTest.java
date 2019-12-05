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

import com.hierynomus.smbj.share.File;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;
import org.mockito.ArgumentCaptor;

import com.hierynomus.smbj.SMBClient;
import com.hierynomus.smbj.connection.Connection;
import com.hierynomus.smbj.auth.AuthenticationContext;
import com.hierynomus.smbj.share.DiskShare;
import com.hierynomus.smbj.session.Session;
import com.hierynomus.mssmb2.SMB2CreateDisposition;
import com.hierynomus.mssmb2.SMB2ShareAccess;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Set;


public class PutSmbFileTest {

    private TestRunner testRunner;

    private SMBClient smbClient;
    private Connection connection;
    private Session session;
    private DiskShare diskShare;
    private File smbfile;
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
        smbfile = mock(File.class);
        baOutputStream = new ByteArrayOutputStream();

        when(smbClient.connect(any(String.class))).thenReturn(connection);
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
        when(smbfile.getOutputStream()).thenReturn(baOutputStream);

        testRunner.setProperty(PutSmbFile.HOSTNAME, HOSTNAME);
        testRunner.setProperty(PutSmbFile.SHARE, SHARE);
        testRunner.setProperty(PutSmbFile.DIRECTORY, DIRECTORY);
        testRunner.setProperty(PutSmbFile.DOMAIN, DOMAIN);
        testRunner.setProperty(PutSmbFile.USERNAME, USERNAME);
        testRunner.setProperty(PutSmbFile.PASSWORD, PASSWORD);


        PutSmbFile PutSmbFile = (PutSmbFile) testRunner.getProcessor();
        PutSmbFile.initSmbClient(smbClient);
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

    @Before
    public void init() throws IOException {
        testRunner = TestRunners.newTestRunner(PutSmbFile.class);
        MockitoAnnotations.initMocks(this);
        setupSmbProcessor();
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
    public void testFileShareNone() throws IOException {
        testRunner.setProperty(PutSmbFile.SHARE_ACCESS, PutSmbFile.SHARE_ACCESS_NONE);
        Set<SMB2ShareAccess> shareAccessSet = testOpenFileShareAccess();
        assertTrue(shareAccessSet.isEmpty());
    }

    @Test
    public void testFileShareRead() throws IOException {
        testRunner.setProperty(PutSmbFile.SHARE_ACCESS, PutSmbFile.SHARE_ACCESS_READ);
        Set<SMB2ShareAccess> shareAccessSet = testOpenFileShareAccess();
        assertTrue(shareAccessSet.contains(SMB2ShareAccess.FILE_SHARE_READ));
    }

    @Test
    public void testFileShareReadWriteDelete() throws IOException {
        testRunner.setProperty(PutSmbFile.SHARE_ACCESS, PutSmbFile.SHARE_ACCESS_READWRITEDELETE);
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
