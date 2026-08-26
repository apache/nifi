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
package org.apache.nifi.services.smb;

import com.hierynomus.msdtyp.FileTime;
import com.hierynomus.mserref.NtStatus;
import com.hierynomus.msfscc.FileAttributes;
import com.hierynomus.msfscc.fileinformation.FileIdBothDirectoryInformation;
import com.hierynomus.mssmb2.SMBApiException;
import com.hierynomus.smbj.session.Session;
import com.hierynomus.smbj.share.Directory;
import com.hierynomus.smbj.share.DiskShare;
import com.hierynomus.smbj.share.File;
import org.apache.nifi.logging.ComponentLog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SmbjClientServiceTest {

    private static final FileTime EPOCH = new FileTime(0L);

    @Mock
    Session session;

    @Mock
    DiskShare share;

    @Mock
    ComponentLog logger;

    @Mock
    File file;

    @Mock
    Directory directory;

    @Mock
    Directory subDirectory;

    @Mock
    FileIdBothDirectoryInformation fileInfo;

    @Mock
    FileIdBothDirectoryInformation subdirInfo;

    @InjectMocks
    SmbjClientService underTest;

    @BeforeEach
    void beforeEach() {
        MockitoAnnotations.openMocks(this);

        when(session.connectShare(anyString())).thenReturn(share);
    }

    @Test
    void closeShouldCloseSession() throws Exception {
        underTest.close();

        verify(session).close();
    }

    @Test
    void closeShouldHandleExceptionSilently() throws Exception {
        doThrow(new RuntimeException("close failed")).when(session).close();

        assertDoesNotThrow(() -> underTest.close());
    }

    @Test
    void folderExistsShouldDelegateToShare() {
        when(share.folderExists("existing")).thenReturn(true);
        assertTrue(underTest.folderExists("existing"));

        when(share.folderExists("missing")).thenReturn(false);
        assertFalse(underTest.folderExists("missing"));
    }

    @Test
    void fileExistsShouldDelegateToShare() {
        when(share.fileExists("existing.txt")).thenReturn(true);
        assertTrue(underTest.fileExists("existing.txt"));

        when(share.fileExists("missing.txt")).thenReturn(false);
        assertFalse(underTest.fileExists("missing.txt"));
    }

    @Test
    void ensureDirectoryShouldCreateDirectoriesRecursively() {
        when(share.folderExists("directory")).thenReturn(true);
        when(share.folderExists("directory/path")).thenReturn(false);
        when(share.folderExists("directory/path/to")).thenReturn(false);
        when(share.folderExists("directory/path/to/create")).thenReturn(false);

        underTest.ensureDirectory("directory/path/to/create");

        verify(share, never()).mkdir("directory");
        verify(share).mkdir("directory/path");
        verify(share).mkdir("directory/path/to");
        verify(share).mkdir("directory/path/to/create");
    }

    @Test
    void ensureDirectoryShouldHandleRaceConditionWhenFolderCreatedConcurrently() {
        when(share.folderExists("newdir")).thenReturn(false, true);
        doThrow(new SMBApiException(NtStatus.STATUS_OBJECT_NAME_COLLISION.getValue(), null, null))
                .when(share).mkdir("newdir");

        assertDoesNotThrow(() -> underTest.ensureDirectory("newdir"));
    }

    @Test
    void ensureDirectoryShouldThrowWhenCollisionOccursAndFolderStillMissing() {
        when(share.folderExists("newdir")).thenReturn(false, false);
        doThrow(new SMBApiException(NtStatus.STATUS_OBJECT_NAME_COLLISION.getValue(), null, null))
                .when(share).mkdir("newdir");

        assertThrows(SmbException.class, () -> underTest.ensureDirectory("newdir"));
    }

    @Test
    void listFilesShouldHandlePermissionErrors() {
        mockOpenDirectory("dir1", NtStatus.STATUS_ACCESS_DENIED);
        mockOpenDirectory("dir2", NtStatus.STATUS_BAD_NETWORK_NAME);
        mockOpenDirectory("dir3", NtStatus.STATUS_OTHER);

        assertEquals(0, underTest.listFiles("dir1", true).count());
        assertEquals(0, underTest.listFiles("dir2", true).count());
        assertThrows(SMBApiException.class, () -> underTest.listFiles("dir3", true).count());
    }

    @Test
    void listFilesShouldReturnFiles() {
        when(share.openDirectory(eq("root"), any(), any(), any(), any(), any())).thenReturn(directory);
        setupFileInfo(fileInfo, "file.txt", 0L);
        when(directory.spliterator()).thenReturn(List.of(fileInfo).spliterator());

        assertEquals(1, underTest.listFiles("root", false).count());
    }

    @Test
    void listFilesShouldFilterSpecialDirectories() {
        when(share.openDirectory(eq("root"), any(), any(), any(), any(), any())).thenReturn(directory);
        setupFileInfo(fileInfo, ".", FileAttributes.FILE_ATTRIBUTE_DIRECTORY.getValue());
        setupFileInfo(subdirInfo, "..", FileAttributes.FILE_ATTRIBUTE_DIRECTORY.getValue());
        when(directory.spliterator()).thenReturn(List.of(fileInfo, subdirInfo).spliterator());

        assertEquals(0, underTest.listFiles("root", false).count());
    }

    @Test
    void listFilesShouldListFilesRecursively() {
        when(share.openDirectory(eq("root"), any(), any(), any(), any(), any())).thenReturn(directory);
        setupFileInfo(subdirInfo, "subdir", FileAttributes.FILE_ATTRIBUTE_DIRECTORY.getValue());
        when(directory.spliterator()).thenReturn(List.of(subdirInfo).spliterator());

        when(share.openDirectory(eq("root/subdir"), any(), any(), any(), any(), any())).thenReturn(subDirectory);
        setupFileInfo(fileInfo, "file.txt", 0L);
        when(subDirectory.spliterator()).thenReturn(List.of(fileInfo).spliterator());

        assertEquals(1, underTest.listFiles("root", true).count());
    }

    @Test
    void listFilesShouldHandleNpeFromGetDiskEntry() {
        NullPointerException npe = npeWithStackTrace(
                new StackTraceElement(DiskShare.class.getName(), "getDiskEntry", "DiskShare.java", 1)
        );
        when(share.openDirectory(eq("dfsDir"), any(), any(), any(), any(), any())).thenThrow(npe);

        assertEquals(0, underTest.listFiles("dfsDir", true).count());
    }

    @Test
    void listFilesShouldRethrowNpeFromOtherSource() {
        NullPointerException npe = npeWithStackTrace(
                new StackTraceElement("OtherClass", "otherMethod", "Other.java", 1)
        );
        when(share.openDirectory(eq("dir"), any(), any(), any(), any(), any())).thenThrow(npe);

        assertThrows(NullPointerException.class, () -> underTest.listFiles("dir", true).count());
    }

    @Test
    void readFileShouldReadFileContent() throws Exception {
        OutputStream outputStream = mock(OutputStream.class);
        when(share.openFile(eq("file.txt"), any(), any(), any(), any(), any())).thenReturn(file);

        underTest.readFile("file.txt", outputStream, SmbShareAccess.READ);

        verify(file).read(outputStream);
    }

    @Test
    void readFileShouldWrapException() {
        OutputStream outputStream = mock(OutputStream.class);
        when(share.openFile(eq("file.txt"), any(), any(), any(), any(), any()))
                .thenThrow(new SMBApiException(NtStatus.STATUS_ACCESS_DENIED.getValue(), null, null));

        assertThrows(SmbException.class, () -> underTest.readFile("file.txt", outputStream, SmbShareAccess.READ));
    }

    @Test
    void writeFileShouldWriteFileContent() {
        InputStream inputStream = mock(InputStream.class);
        when(share.openFile(eq("file.txt"), any(), any(), any(), any(), any())).thenReturn(file);

        underTest.writeFile("file.txt", inputStream, SmbShareAccess.NONE);

        verify(file).write(any());
    }

    @Test
    void writeFileShouldWrapException() {
        InputStream inputStream = mock(InputStream.class);
        when(share.openFile(eq("file.txt"), any(), any(), any(), any(), any()))
                .thenThrow(new SMBApiException(NtStatus.STATUS_ACCESS_DENIED.getValue(), null, null));

        assertThrows(SmbException.class, () -> underTest.writeFile("file.txt", inputStream, SmbShareAccess.NONE));
    }

    @Test
    void renameFileShouldConvertPathSeparatorsToBackslash() {
        when(share.openFile(eq("old/path.txt"), any(), any(), any(), any(), any())).thenReturn(file);

        underTest.renameFile("old/path.txt", "new/path.txt", false);

        verify(file).rename("new\\path.txt", false);
    }

    @Test
    void renameFileShouldWrapException() {
        when(share.openFile(eq("file.txt"), any(), any(), any(), any(), any()))
                .thenThrow(new SMBApiException(NtStatus.STATUS_ACCESS_DENIED.getValue(), null, null));

        assertThrows(SmbException.class, () -> underTest.renameFile("file.txt", "new.txt", false));
    }

    @Test
    void moveFileShouldConstructCorrectTargetPath() {
        when(share.openFile(eq("source/file.txt"), any(), any(), any(), any(), any())).thenReturn(file);

        underTest.moveFile("source/file.txt", "target");

        verify(file).rename("target\\file.txt", false);
    }

    @Test
    void deleteFileShouldCallShareRm() {
        underTest.deleteFile("file.txt");

        verify(share).rm("file.txt");
    }

    @Test
    void deleteFileShouldWrapException() {
        doThrow(new SMBApiException(NtStatus.STATUS_ACCESS_DENIED.getValue(), null, null)).when(share).rm("file.txt");

        assertThrows(SmbException.class, () -> underTest.deleteFile("file.txt"));
    }

    private static NullPointerException npeWithStackTrace(StackTraceElement... elements) {
        NullPointerException npe = new NullPointerException() {
            @Override
            public synchronized Throwable fillInStackTrace() {
                return this;
            }
        };
        npe.setStackTrace(elements);
        return npe;
    }

    private void mockOpenDirectory(String directoryName, NtStatus responseStatus) {
        when(share.openDirectory(eq(directoryName), any(), any(), any(), any(), any()))
                .thenThrow(new SMBApiException(responseStatus.getValue(), null, null));
    }

    private void setupFileInfo(FileIdBothDirectoryInformation info, String fileName, long fileAttributes) {
        when(info.getFileName()).thenReturn(fileName);
        when(info.getShortName()).thenReturn("");
        when(info.getFileAttributes()).thenReturn(fileAttributes);
        when(info.getEndOfFile()).thenReturn(0L);
        when(info.getAllocationSize()).thenReturn(0L);
        when(info.getLastWriteTime()).thenReturn(EPOCH);
        when(info.getCreationTime()).thenReturn(EPOCH);
        when(info.getChangeTime()).thenReturn(EPOCH);
        when(info.getLastAccessTime()).thenReturn(EPOCH);
    }
}
