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

import org.apache.commons.net.ftp.FTPClient;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.file.transfer.FetchFileTransfer;
import org.apache.nifi.processors.standard.util.FTPTransfer;
import org.apache.nifi.processor.util.file.transfer.FileTransfer;
import org.apache.nifi.processor.util.file.transfer.FileInfo;
import org.apache.nifi.processor.util.file.transfer.PermissionDeniedException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

public class TestFetchFTP {

    private TestableFetchFTP proc;
    private TestRunner runner;

    @BeforeEach
    public void setUp() throws Exception {
        proc = new TestableFetchFTP();
        runner = TestRunners.newTestRunner(proc);
        runner.setValidateExpressionUsage(false);

        runner.setProperty(FetchFileTransfer.HOSTNAME, "localhost");
        runner.setProperty(FetchFileTransfer.UNDEFAULTED_PORT, "11");
        runner.setProperty(FetchFileTransfer.REMOTE_FILENAME, "${filename}");

        MockProcessContext ctx = (MockProcessContext) runner.getProcessContext();
        setDefaultValues(ctx, FTPTransfer.BUFFER_SIZE, FTPTransfer.DATA_TIMEOUT, FTPTransfer.CONNECTION_TIMEOUT,
                FTPTransfer.CONNECTION_MODE, FTPTransfer.TRANSFER_MODE);
        ctx.setProperty(FTPTransfer.USERNAME, "foo");
        ctx.setProperty(FTPTransfer.PASSWORD, "bar");
    }

    private void setDefaultValues(MockProcessContext ctx, PropertyDescriptor... propertyDescriptors) {
        Arrays.stream(propertyDescriptors).forEach(d -> ctx.setProperty(d, d.getDefaultValue()));
    }

    private void addFileAndEnqueue(String filename) {
        proc.addContent(filename, "world".getBytes());
        runner.enqueue(new byte[0], Collections.singletonMap("filename", filename));
    }

    @Test
    public void testContentFetched() {
        addFileAndEnqueue("hello.txt");

        runner.run(1, false, false);
        runner.assertAllFlowFilesTransferred(FetchFileTransfer.REL_SUCCESS, 1);
        assertFalse(proc.isClosed);
        runner.getFlowFilesForRelationship(FetchFileTransfer.REL_SUCCESS).get(0).assertContentEquals("world");
    }

    @Test
    public void testFilenameContainsPath() {
        addFileAndEnqueue("./here/is/my/path/hello.txt");

        runner.run(1, false, false);
        runner.assertAllFlowFilesTransferred(FetchFileTransfer.REL_SUCCESS, 1);
        assertFalse(proc.isClosed);
        MockFlowFile transferredFlowFile = runner.getFlowFilesForRelationship(FetchFileTransfer.REL_SUCCESS).get(0);
        transferredFlowFile.assertContentEquals("world");
        transferredFlowFile.assertAttributeExists(CoreAttributes.PATH.key());
        transferredFlowFile.assertAttributeEquals(CoreAttributes.PATH.key(), "./here/is/my/path");
    }

    @Test
    public void testContentNotFound() {
        runner.enqueue(new byte[0], Collections.singletonMap("filename", "hello.txt"));

        runner.run(1, false, false);
        runner.assertAllFlowFilesTransferred(FetchFileTransfer.REL_NOT_FOUND, 1);
        runner.assertAllFlowFilesContainAttribute(FetchFileTransfer.FAILURE_REASON_ATTRIBUTE);
        MockFlowFile transferredFlowFile = runner.getPenalizedFlowFiles().get(0);
        assertEquals(FetchFileTransfer.REL_NOT_FOUND.getName(), transferredFlowFile.getAttribute(FetchFileTransfer.FAILURE_REASON_ATTRIBUTE));
    }

    @Test
    public void testInsufficientPermissions() {
        addFileAndEnqueue("hello.txt");
        proc.allowAccess = false;

        runner.run(1, false, false);
        runner.assertAllFlowFilesTransferred(FetchFileTransfer.REL_PERMISSION_DENIED, 1);
        runner.assertAllFlowFilesContainAttribute(FetchFileTransfer.FAILURE_REASON_ATTRIBUTE);
        MockFlowFile transferredFlowFile = runner.getPenalizedFlowFiles().get(0);
        assertEquals(FetchFileTransfer.REL_PERMISSION_DENIED.getName(), transferredFlowFile.getAttribute(FetchFileTransfer.FAILURE_REASON_ATTRIBUTE));
    }

    @Test
    public void testInsufficientPermissionsDoesNotCloseConnection() {
        addFileAndEnqueue("hello1.txt");
        addFileAndEnqueue("hello2.txt");
        proc.allowAccess = false;

        runner.run(2, false, false);
        runner.assertAllFlowFilesTransferred(FetchFileTransfer.REL_PERMISSION_DENIED, 2);
        runner.assertAllFlowFilesContainAttribute(FetchFileTransfer.FAILURE_REASON_ATTRIBUTE);

        assertEquals(1, proc.numberOfFileTransfers);
        assertFalse(proc.isClosed);
    }

    @Test
    public void testFileNotFoundDoesNotCloseConnection() {
        addFileAndEnqueue("hello1.txt");
        addFileAndEnqueue("hello2.txt");
        proc.isFileNotFound = true;

        runner.run(2, false, false);
        runner.assertAllFlowFilesTransferred(FetchFileTransfer.REL_NOT_FOUND, 2);
        runner.assertAllFlowFilesContainAttribute(FetchFileTransfer.FAILURE_REASON_ATTRIBUTE);

        assertEquals(1, proc.numberOfFileTransfers);
        assertFalse(proc.isClosed);
    }

    @Test
    public void testCommunicationFailureClosesConnection() {
        addFileAndEnqueue("hello.txt");
        proc.isCommFailure = true;

        runner.run(1, false, false);
        runner.assertAllFlowFilesTransferred(FetchFileTransfer.REL_COMMS_FAILURE, 1);
        runner.assertAllFlowFilesContainAttribute(FetchFileTransfer.FAILURE_REASON_ATTRIBUTE);
        MockFlowFile transferredFlowFile = runner.getPenalizedFlowFiles().get(0);
        assertEquals(FetchFileTransfer.REL_COMMS_FAILURE.getName(), transferredFlowFile.getAttribute(FetchFileTransfer.FAILURE_REASON_ATTRIBUTE));

        assertTrue(proc.isClosed);
    }

    @Test
    public void testMoveFileWithNoTrailingSlashDirName() {
        runner.setProperty(FetchFileTransfer.COMPLETION_STRATEGY, FetchFileTransfer.COMPLETION_MOVE.getValue());
        runner.setProperty(FetchFileTransfer.MOVE_DESTINATION_DIR, "/moved");
        runner.setProperty(FetchFileTransfer.MOVE_CREATE_DIRECTORY, "true");

        addFileAndEnqueue("hello.txt");

        runner.run(1, false, false);
        runner.assertAllFlowFilesTransferred(FetchFileTransfer.REL_SUCCESS, 1);

        proc.fileContents.containsKey("/moved/hello.txt");
        assertEquals(1, proc.fileContents.size());
    }

    @Test
    public void testMoveFileWithTrailingSlashDirName() {
        runner.setProperty(FetchFileTransfer.COMPLETION_STRATEGY, FetchFileTransfer.COMPLETION_MOVE.getValue());
        runner.setProperty(FetchFileTransfer.MOVE_DESTINATION_DIR, "/moved/");

        addFileAndEnqueue("hello.txt");

        runner.run(1, false, false);
        runner.assertAllFlowFilesTransferred(FetchFileTransfer.REL_SUCCESS, 1);

        proc.fileContents.containsKey("/moved/hello.txt");
        assertEquals(1, proc.fileContents.size());
    }

    @Test
    public void testDeleteFile() {
        runner.setProperty(FetchFileTransfer.COMPLETION_STRATEGY, FetchFileTransfer.COMPLETION_DELETE.getValue());

        addFileAndEnqueue("hello.txt");

        runner.run(1, false, false);
        runner.assertAllFlowFilesTransferred(FetchFileTransfer.REL_SUCCESS, 1);
        assertTrue(proc.fileContents.isEmpty());
    }

    @Test
    public void testDeleteFails() {
        runner.setProperty(FetchFileTransfer.COMPLETION_STRATEGY, FetchFileTransfer.COMPLETION_DELETE.getValue());
        proc.allowDelete = false;

        addFileAndEnqueue("hello.txt");

        runner.run(1, false, false);
        runner.assertAllFlowFilesTransferred(FetchFileTransfer.REL_SUCCESS, 1);
        assertFalse(proc.fileContents.isEmpty());
    }

    @Test
    public void testRenameFails() {
        runner.setProperty(FetchFileTransfer.COMPLETION_STRATEGY, FetchFileTransfer.COMPLETION_MOVE.getValue());
        runner.setProperty(FetchFileTransfer.MOVE_DESTINATION_DIR, "/moved/");
        proc.allowDelete = false;
        proc.allowRename = false;

        addFileAndEnqueue("hello.txt");

        runner.run(1, false, false);
        runner.assertAllFlowFilesTransferred(FetchFileTransfer.REL_SUCCESS, 1);
        assertEquals(1, proc.fileContents.size());

        assertTrue(proc.fileContents.containsKey("hello.txt"));
    }

    @Test
    public void testMoveConflictReplace() {
        runner.setProperty(FetchFileTransfer.COMPLETION_STRATEGY, FetchFileTransfer.COMPLETION_MOVE.getValue());
        runner.setProperty(FetchFileTransfer.MOVE_DESTINATION_DIR, "/moved/");
        runner.setProperty(FetchFileTransfer.MOVE_CONFLICT_RESOLUTION, FileTransfer.CONFLICT_RESOLUTION_REPLACE);

        // Destination exists
        proc.addContent("/moved/hello.txt", "old".getBytes());
        addFileAndEnqueue("hello.txt");

        runner.run(1, false, false);
        runner.assertAllFlowFilesTransferred(FetchFileTransfer.REL_SUCCESS, 1);

        assertFalse(proc.fileContents.containsKey("hello.txt"));
        assertTrue(proc.fileContents.containsKey("/moved/hello.txt"));
    }


    @Test
    public void testCreateDirFails() {
        runner.setProperty(FetchFileTransfer.COMPLETION_STRATEGY, FetchFileTransfer.COMPLETION_MOVE.getValue());
        runner.setProperty(FetchFileTransfer.MOVE_DESTINATION_DIR, "/moved/");
        runner.setProperty(FetchFileTransfer.MOVE_CREATE_DIRECTORY, "true");

        addFileAndEnqueue("hello.txt");

        proc.allowCreateDir = false;

        runner.run(1, false, false);
        runner.assertAllFlowFilesTransferred(FetchFileTransfer.REL_SUCCESS, 1);
        assertEquals(1, proc.fileContents.size());

        assertTrue(proc.fileContents.containsKey("hello.txt"));
    }


    private static class TestableFetchFTP extends FetchFTP {
        private boolean allowAccess = true;
        private boolean allowDelete = true;
        private boolean allowCreateDir = true;
        private boolean allowRename = true;
        private boolean isClosed = false;
        private boolean isFileNotFound = false;
        private boolean isCommFailure = false;
        private int numberOfFileTransfers = 0;
        private final Map<String, byte[]> fileContents = new HashMap<>();
        private final FTPClient mockFtpClient = Mockito.mock(FTPClient.class);

        private TestableFetchFTP() throws IOException {
            when(mockFtpClient.retrieveFileStream(anyString()))
                .then((Answer) invocationOnMock -> {
                    byte[] content = fileContents.get(invocationOnMock.getArgument(0));
                    if (content == null) {
                        throw new FileNotFoundException();
                    }
                    return new ByteArrayInputStream(content);
                });
            when(mockFtpClient.login(anyString(), anyString())).thenReturn(true);
            when(mockFtpClient.setFileType(anyInt())).thenReturn(true);

        }

        public void addContent(final String filename, final byte[] content) {
            this.fileContents.put(filename, content);
        }

        @Override
        protected FileTransfer createFileTransfer(final ProcessContext context) {
            numberOfFileTransfers++;
            return new FTPTransfer(context, getLogger()) {

                @Override
                protected FTPClient createClient(final PropertyContext context, final Map<String, String> attributes) {
                    return mockFtpClient;
                }

                @Override
                public FlowFile getRemoteFile(String remoteFileName, FlowFile flowFile, ProcessSession session) throws ProcessException, IOException {
                    if (!allowAccess) {
                        throw new PermissionDeniedException("test permission denied");
                    }
                    if (isFileNotFound) {
                        throw new FileNotFoundException("test file not found");
                    }
                    if (isCommFailure) {
                        throw new IOException("test communication failure");
                    }
                    return super.getRemoteFile(remoteFileName, flowFile, session);
                }

                @Override
                public void deleteFile(FlowFile flowFile, String path, String remoteFileName) throws IOException {
                    if (!allowDelete) {
                        throw new PermissionDeniedException("test permission denied");
                    }

                    String key;
                    if (path == null) {
                        key = remoteFileName;
                    } else {
                        key = (path.endsWith("/") ? path.substring(0, path.length() - 1) : path) + "/" + remoteFileName;
                    }
                    key = key.replaceAll("/+", "/");

                    if (!fileContents.containsKey(key)) {
                        throw new FileNotFoundException();
                    }

                    fileContents.remove(key);
                }

                @Override
                public void rename(FlowFile flowFile, String source, String target) throws IOException {
                    if (!allowRename) {
                        throw new PermissionDeniedException("test permission denied");
                    }

                    final String normalizedSource = source.replaceAll("/+", "/");
                    final String normalizedTarget = target.replaceAll("/+", "/");

                    if (!fileContents.containsKey(normalizedSource)) {
                        throw new FileNotFoundException();
                    }

                    final byte[] content = fileContents.remove(normalizedSource);
                    fileContents.put(normalizedTarget, content);
                }

                @Override
                public void ensureDirectoryExists(FlowFile flowFile, File remoteDirectory) throws IOException {
                    if (!allowCreateDir) {
                        throw new PermissionDeniedException("test permission denied");
                    }
                }

                @Override
                public String getAbsolutePath(FlowFile flowFile, String remotePath) throws IOException {
                    final String abs;
                    if (!remotePath.startsWith("/") && !remotePath.startsWith("\\")) {
                        abs = new File(getHomeDirectory(flowFile), remotePath).getPath();
                    } else {
                        abs = remotePath;
                    }
                    String norm = abs.replace("\\", "/");
                    norm = norm.replaceAll("/+", "/");
                    if (norm.endsWith("/") && norm.length() > 1) {
                        norm = norm.substring(0, norm.length() - 1);
                    }
                    return norm;
                }

                @Override
                public void close() throws IOException {
                    super.close();
                    isClosed = true;
                }

                @Override
                public String getHomeDirectory(FlowFile flowFile) throws IOException {
                    return "/";
                }

                @Override
                public FileInfo getRemoteFileInfo(FlowFile flowFile, String path, String remoteFileName) throws IOException {
                    final String dir = path == null ? "/" : path;
                    String key = (dir.endsWith("/") ? dir.substring(0, dir.length() - 1) : dir) + "/" + remoteFileName;
                    key = key.replaceAll("/+", "/");
                    final byte[] content = fileContents.get(key);
                    if (content == null) {
                        return null;
                    }
                    return new FileInfo.Builder()
                            .filename(remoteFileName)
                            .fullPathFileName(key)
                            .directory(false)
                            .size(content.length)
                            .build();
                }
            };
        }
    }
}
