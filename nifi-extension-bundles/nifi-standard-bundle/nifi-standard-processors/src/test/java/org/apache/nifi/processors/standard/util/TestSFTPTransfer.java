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
package org.apache.nifi.processors.standard.util;

import com.hierynomus.sshj.sftp.RemoteResourceSelector;
import net.schmizz.sshj.sftp.FileAttributes;
import net.schmizz.sshj.sftp.FileMode;
import net.schmizz.sshj.sftp.PathComponents;
import net.schmizz.sshj.sftp.RemoteResourceInfo;
import net.schmizz.sshj.sftp.Response;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.sftp.SFTPException;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.file.transfer.FileInfo;
import org.apache.nifi.processor.util.file.transfer.FileTransfer;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockPropertyValue;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestSFTPTransfer {
    private static final int FILE_MASK_UNKNOWN_777 = Integer.parseInt("000777", 8);
    private static final int FILE_MASK_REGULAR_777 = Integer.parseInt("100777", 8);

    private static final PropertyValue BOOLEAN_FALSE_PROPERTY_VALUE = new MockPropertyValue(Boolean.FALSE.toString());
    private static final PropertyValue BOOLEAN_TRUE_PROPERTY_VALUE = new MockPropertyValue(Boolean.TRUE.toString());

    private SFTPTransfer createSftpTransfer(ProcessContext processContext, SFTPClient sftpClient) {
        final ComponentLog componentLog = mock(ComponentLog.class);
        return new SFTPTransfer(processContext, componentLog) {
            @Override
            protected SFTPClient getSFTPClient(FlowFile flowFile) {
                return sftpClient;
            }
        };
    }

    @Test
    public void testEnsureDirectoryExistsAlreadyExisted() throws IOException {
        final ProcessContext processContext = mock(ProcessContext.class);
        final SFTPClient sftpClient = mock(SFTPClient.class);
        final SFTPTransfer sftpTransfer = createSftpTransfer(processContext, sftpClient);
        final MockFlowFile flowFile = new MockFlowFile(0);
        final File remoteDir = new File("/dir1/dir2/dir3");
        sftpTransfer.ensureDirectoryExists(flowFile, remoteDir);

        // Dir existence check should be done by stat
        verify(sftpClient).stat(eq("/dir1/dir2/dir3"));
    }

    @Test
    public void testEnsureDirectoryExistsFailedToStat() throws IOException {
        final ProcessContext processContext = mock(ProcessContext.class);
        final SFTPClient sftpClient = mock(SFTPClient.class);
        // stat for the parent was successful, simulating that dir2 exists, but no dir3.
        when(sftpClient.stat("/dir1/dir2/dir3")).thenThrow(new SFTPException(Response.StatusCode.FAILURE, "Failure"));

        final SFTPTransfer sftpTransfer = createSftpTransfer(processContext, sftpClient);
        final MockFlowFile flowFile = new MockFlowFile(0);
        final File remoteDir = new File("/dir1/dir2/dir3");
        final IOException e = assertThrows(IOException.class, () -> {
            sftpTransfer.ensureDirectoryExists(flowFile, remoteDir);
        });
        assertEquals("Failed to determine if remote directory exists at /dir1/dir2/dir3 due to 4: Failure", e.getMessage());

        // Dir existence check should be done by stat
        verify(sftpClient).stat(eq("/dir1/dir2/dir3"));
    }

    @Test
    public void testEnsureDirectoryExistsNotExisted() throws IOException {
        final ProcessContext processContext = mock(ProcessContext.class);
        final SFTPClient sftpClient = mock(SFTPClient.class);
        // stat for the parent was successful, simulating that dir2 exists, but no dir3.
        when(sftpClient.stat("/dir1/dir2/dir3")).thenThrow(new SFTPException(Response.StatusCode.NO_SUCH_FILE, "No such file"));

        final SFTPTransfer sftpTransfer = createSftpTransfer(processContext, sftpClient);
        final MockFlowFile flowFile = new MockFlowFile(0);
        final File remoteDir = new File("/dir1/dir2/dir3");
        sftpTransfer.ensureDirectoryExists(flowFile, remoteDir);

        // Dir existence check should be done by stat
        verify(sftpClient).stat(eq("/dir1/dir2/dir3")); // dir3 was not found
        verify(sftpClient).stat(eq("/dir1/dir2")); // so, dir2 was checked
        verify(sftpClient).mkdir(eq("/dir1/dir2/dir3")); // dir2 existed, so dir3 was created.
    }

    @Test
    public void testEnsureDirectoryExistsParentNotExisted() throws IOException {
        final ProcessContext processContext = mock(ProcessContext.class);
        final SFTPClient sftpClient = mock(SFTPClient.class);

        // stat for the dir1 was successful, simulating that dir1 exists, but no dir2 and dir3.
        when(sftpClient.stat("/dir1/dir2/dir3")).thenThrow(new SFTPException(Response.StatusCode.NO_SUCH_FILE, "No such file"));
        when(sftpClient.stat("/dir1/dir2")).thenThrow(new SFTPException(Response.StatusCode.NO_SUCH_FILE, "No such file"));

        final SFTPTransfer sftpTransfer = createSftpTransfer(processContext, sftpClient);
        final MockFlowFile flowFile = new MockFlowFile(0);
        final File remoteDir = new File("/dir1/dir2/dir3");
        sftpTransfer.ensureDirectoryExists(flowFile, remoteDir);

        // Dir existence check should be done by stat
        verify(sftpClient).stat(eq("/dir1/dir2/dir3")); // dir3 was not found
        verify(sftpClient).stat(eq("/dir1/dir2")); // dir2 was not found, too
        verify(sftpClient).stat(eq("/dir1")); // dir1 was found
        verify(sftpClient).mkdir(eq("/dir1/dir2")); // dir1 existed, so dir2 was created.
        verify(sftpClient).mkdir(eq("/dir1/dir2/dir3")); // then dir3 was created.
    }

    @Test
    public void testEnsureDirectoryExistsNotExistedFailedToCreate() throws IOException {
        final ProcessContext processContext = mock(ProcessContext.class);
        final SFTPClient sftpClient = mock(SFTPClient.class);

        // stat for the parent was successful, simulating that dir2 exists, but no dir3.
        when(sftpClient.stat("/dir1/dir2/dir3")).thenThrow(new SFTPException(Response.StatusCode.NO_SUCH_FILE, "No such file"));
        // Failed to create dir3.
        doThrow(new SFTPException(Response.StatusCode.FAILURE, "Failed")).when(sftpClient).mkdir(eq("/dir1/dir2/dir3"));

        final SFTPTransfer sftpTransfer = createSftpTransfer(processContext, sftpClient);
        final MockFlowFile flowFile = new MockFlowFile(0);
        final File remoteDir = new File("/dir1/dir2/dir3");
        final IOException e = assertThrows(IOException.class, () -> {
            sftpTransfer.ensureDirectoryExists(flowFile, remoteDir);
        });
        assertEquals("Failed to create remote directory /dir1/dir2/dir3 due to 4: Failed", e.getMessage());

        // Dir existence check should be done by stat
        verify(sftpClient).stat(eq("/dir1/dir2/dir3")); // dir3 was not found
        verify(sftpClient).stat(eq("/dir1/dir2")); // so, dir2 was checked
        verify(sftpClient).mkdir(eq("/dir1/dir2/dir3")); // dir2 existed, so dir3 was created.
    }

    @Test
    public void testEnsureDirectoryExistsBlindlyNotExisted() throws IOException {
        final ProcessContext processContext = mock(ProcessContext.class);
        when(processContext.getProperty(SFTPTransfer.DISABLE_DIRECTORY_LISTING)).thenReturn(new MockPropertyValue("true"));

        final SFTPClient sftpClient = mock(SFTPClient.class);
        final SFTPTransfer sftpTransfer = createSftpTransfer(processContext, sftpClient);
        final MockFlowFile flowFile = new MockFlowFile(0);
        final File remoteDir = new File("/dir1/dir2/dir3");
        sftpTransfer.ensureDirectoryExists(flowFile, remoteDir);

        // stat should not be called.
        verify(sftpClient, times(0)).stat(eq("/dir1/dir2/dir3"));
        verify(sftpClient).mkdir(eq("/dir1/dir2/dir3")); // dir3 was created blindly.
    }

    @Test
    public void testEnsureDirectoryExistsBlindlyParentNotExisted() throws IOException {
        final ProcessContext processContext = mock(ProcessContext.class);
        when(processContext.getProperty(SFTPTransfer.DISABLE_DIRECTORY_LISTING)).thenReturn(new MockPropertyValue("true"));

        final SFTPClient sftpClient = mock(SFTPClient.class);
        final AtomicInteger mkdirCount = new AtomicInteger(0);
        doAnswer(invocation -> {
            final int cnt = mkdirCount.getAndIncrement();
            if (cnt == 0) {
                // If the parent dir does not exist, no such file exception is thrown.
                throw new SFTPException(Response.StatusCode.NO_SUCH_FILE, "Failure");
            }
            return true;
        }).when(sftpClient).mkdir(eq("/dir1/dir2/dir3"));

        final SFTPTransfer sftpTransfer = createSftpTransfer(processContext, sftpClient);
        final MockFlowFile flowFile = new MockFlowFile(0);
        final File remoteDir = new File("/dir1/dir2/dir3");
        sftpTransfer.ensureDirectoryExists(flowFile, remoteDir);

        // stat should not be called.
        verify(sftpClient, times(0)).stat(eq("/dir1/dir2/dir3"));
        // dir3 was created blindly, but failed for the 1st time, and succeeded for the 2nd time.
        verify(sftpClient, times(2)).mkdir(eq("/dir1/dir2/dir3"));
        verify(sftpClient).mkdir(eq("/dir1/dir2")); // dir2 was created successfully.
    }

    @Test
    public void testEnsureDirectoryExistsBlindlyAlreadyExisted() throws IOException {
        final ProcessContext processContext = mock(ProcessContext.class);
        when(processContext.getProperty(SFTPTransfer.DISABLE_DIRECTORY_LISTING)).thenReturn(new MockPropertyValue("true"));

        final SFTPClient sftpClient = mock(SFTPClient.class);
        // If the dir existed, a failure exception is thrown, but should be swallowed.
        doThrow(new SFTPException(Response.StatusCode.FAILURE, "Failure")).when(sftpClient).mkdir(eq("/dir1/dir2/dir3"));

        final SFTPTransfer sftpTransfer = createSftpTransfer(processContext, sftpClient);
        final MockFlowFile flowFile = new MockFlowFile(0);
        final File remoteDir = new File("/dir1/dir2/dir3");
        sftpTransfer.ensureDirectoryExists(flowFile, remoteDir);

        // stat should not be called.
        verify(sftpClient, times(0)).stat(eq("/dir1/dir2/dir3"));
        verify(sftpClient).mkdir(eq("/dir1/dir2/dir3")); // dir3 was created blindly.
    }

    @Test
    public void testEnsureDirectoryExistsBlindlyFailed() throws IOException {
        final ProcessContext processContext = mock(ProcessContext.class);
        when(processContext.getProperty(SFTPTransfer.DISABLE_DIRECTORY_LISTING)).thenReturn(new MockPropertyValue("true"));

        final SFTPClient sftpClient = mock(SFTPClient.class);
        doThrow(new SFTPException(Response.StatusCode.PERMISSION_DENIED, "Permission denied")).when(sftpClient).mkdir(eq("/dir1/dir2/dir3"));

        final SFTPTransfer sftpTransfer = createSftpTransfer(processContext, sftpClient);
        final MockFlowFile flowFile = new MockFlowFile(0);
        final File remoteDir = new File("/dir1/dir2/dir3");
        final IOException e = assertThrows(IOException.class, () -> {
            sftpTransfer.ensureDirectoryExists(flowFile, remoteDir);
        });
        assertEquals("Could not blindly create remote directory due to Permission denied", e.getMessage());

        // stat should not be called.
        verify(sftpClient, times(0)).stat(eq("/dir1/dir2/dir3"));
        verify(sftpClient).mkdir(eq("/dir1/dir2/dir3")); // dir3 was created blindly.
    }

    @Test
    public void testFileTypeUnknown() throws IOException {
        final ProcessContext processContext = mock(ProcessContext.class);
        when(processContext.getProperty(eq(FileTransfer.REMOTE_PATH))).thenReturn(new MockPropertyValue("."));
        when(processContext.getProperty(eq(FileTransfer.IGNORE_DOTTED_FILES))).thenReturn(BOOLEAN_TRUE_PROPERTY_VALUE);
        when(processContext.getProperty(eq(FileTransfer.RECURSIVE_SEARCH))).thenReturn(BOOLEAN_FALSE_PROPERTY_VALUE);
        when(processContext.getProperty(eq(FileTransfer.FOLLOW_SYMLINK))).thenReturn(BOOLEAN_FALSE_PROPERTY_VALUE);
        when(processContext.getProperty(eq(FileTransfer.FILE_FILTER_REGEX))).thenReturn(new MockPropertyValue(".*"));
        when(processContext.getProperty(eq(FileTransfer.PATH_FILTER_REGEX))).thenReturn(new MockPropertyValue(".*"));
        when(processContext.getProperty(eq(FileTransfer.REMOTE_PATH))).thenReturn(new MockPropertyValue("."));

        try (SFTPClient sftpClient = mock(SFTPClient.class)) {
            when(sftpClient.ls(any(), ArgumentMatchers.<RemoteResourceSelector>any())).then(invocation -> {
                final Map<String, String> extended = new LinkedHashMap<>();
                final List<RemoteResourceInfo> list = new ArrayList<>();
                list.add(new RemoteResourceInfo(
                        new PathComponents("./", "unknown.txt", "/"),
                        new FileAttributes(FileAttributes.Flag.MODE.get(), 0, 0, 0, new FileMode(FILE_MASK_UNKNOWN_777), 0, 0, extended)));
                list.add(new RemoteResourceInfo(
                        new PathComponents("./", "regular.txt", "/"),
                        new FileAttributes(FileAttributes.Flag.MODE.get(), 0, 0, 0, new FileMode(FILE_MASK_REGULAR_777), 0, 0, extended)));
                final RemoteResourceSelector selector = invocation.getArgument(1, RemoteResourceSelector.class);
                list.forEach(selector::select);
                return list;
            });

            final SFTPTransfer sftpTransfer = createSftpTransfer(processContext, sftpClient);
            final List<FileInfo> listing = sftpTransfer.getListing(false);
            assertEquals(2, listing.size());
        }
    }
}
