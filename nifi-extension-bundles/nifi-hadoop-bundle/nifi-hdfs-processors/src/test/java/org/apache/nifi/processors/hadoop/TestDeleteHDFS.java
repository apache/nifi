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
package org.apache.nifi.processors.hadoop;

import com.google.common.collect.Maps;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.ietf.jgss.GSSException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.processors.hadoop.AbstractHadoopProcessor.HADOOP_FILE_URL_ATTRIBUTE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestDeleteHDFS {
    private FileSystem mockFileSystem;

    @BeforeEach
    public void setup() throws Exception {
        mockFileSystem = mock(FileSystem.class);
    }

    //Tests the case where a file is found and deleted but there was no incoming connection
    @Test
    public void testSuccessfulDelete() throws Exception {
        Path filePath = new Path("/some/path/to/file.txt");
        when(mockFileSystem.exists(any(Path.class))).thenReturn(true);
        when(mockFileSystem.getUri()).thenReturn(new URI("hdfs://0.example.com:8020"));
        when(mockFileSystem.delete(any(Path.class), anyBoolean())).thenReturn(true);
        DeleteHDFS deleteHDFS = new TestableDeleteHDFS(mockFileSystem);
        TestRunner runner = TestRunners.newTestRunner(deleteHDFS);
        runner.setIncomingConnection(false);
        runner.assertNotValid();
        runner.setProperty(DeleteHDFS.FILE_OR_DIRECTORY, filePath.toString());
        runner.assertValid();
        runner.run();
        // Even if there's no incoming relationship, a FlowFile is created to indicate which path is deleted.
        runner.assertTransferCount(DeleteHDFS.REL_SUCCESS, 1);
        runner.assertTransferCount(DeleteHDFS.REL_FAILURE, 0);

        final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(1, provenanceEvents.size());
        assertEquals(ProvenanceEventType.REMOTE_INVOCATION, provenanceEvents.get(0).getEventType());
        assertEquals("hdfs://0.example.com:8020/some/path/to/file.txt", provenanceEvents.get(0).getTransitUri());

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(DeleteHDFS.REL_SUCCESS).get(0);
        assertEquals("hdfs://0.example.com:8020/some/path/to/file.txt", flowFile.getAttribute(HADOOP_FILE_URL_ATTRIBUTE));

    }

    @Test
    public void testDeleteFromIncomingFlowFile() throws Exception {
        Path filePath = new Path("/some/path/to/file.txt");
        when(mockFileSystem.exists(any(Path.class))).thenReturn(true);
        when(mockFileSystem.getUri()).thenReturn(new URI("hdfs://0.example.com:8020"));
        when(mockFileSystem.delete(any(Path.class), anyBoolean())).thenReturn(true);
        DeleteHDFS deleteHDFS = new TestableDeleteHDFS(mockFileSystem);
        TestRunner runner = TestRunners.newTestRunner(deleteHDFS);
        runner.setProperty(DeleteHDFS.FILE_OR_DIRECTORY, "${hdfs.file}");
        Map<String, String> attributes = Maps.newHashMap();
        attributes.put("hdfs.file", filePath.toString());
        runner.enqueue("foo", attributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(DeleteHDFS.REL_SUCCESS);
        runner.assertTransferCount(DeleteHDFS.REL_SUCCESS, 1);
    }

    @Test
    public void testIOException() throws Exception {
        Path filePath = new Path("/some/path/to/file.txt");
        when(mockFileSystem.exists(any(Path.class))).thenThrow(new IOException());
        DeleteHDFS deleteHDFS = new TestableDeleteHDFS(mockFileSystem);
        TestRunner runner = TestRunners.newTestRunner(deleteHDFS);
        runner.setProperty(DeleteHDFS.FILE_OR_DIRECTORY, "${hdfs.file}");
        Map<String, String> attributes = Maps.newHashMap();
        attributes.put("hdfs.file", filePath.toString());
        runner.enqueue("foo", attributes);
        runner.run();
        runner.assertTransferCount(DeleteHDFS.REL_FAILURE, 1);
    }

    @Test
    public void testGSSException() throws Exception {
        Path filePath = new Path("/some/path/to/file.txt");
        when(mockFileSystem.exists(any(Path.class))).thenThrow(new IOException(new GSSException(13)));
        DeleteHDFS deleteHDFS = new TestableDeleteHDFS(mockFileSystem);
        TestRunner runner = TestRunners.newTestRunner(deleteHDFS);
        runner.setProperty(DeleteHDFS.FILE_OR_DIRECTORY, "${hdfs.file}");
        Map<String, String> attributes = Maps.newHashMap();
        attributes.put("hdfs.file", filePath.toString());
        runner.enqueue("foo", attributes);
        runner.run();
        // GSS Auth exceptions should cause rollback
        runner.assertTransferCount(DeleteHDFS.REL_SUCCESS, 0);
        runner.assertTransferCount(DeleteHDFS.REL_FAILURE, 0);
    }

    @Test
    public void testPermissionIOException() throws Exception {
        Path filePath = new Path("/some/path/to/file.txt");
        when(mockFileSystem.exists(any(Path.class))).thenReturn(true);
        when(mockFileSystem.delete(any(Path.class), any(Boolean.class))).thenThrow(new IOException("Permissions Error"));
        DeleteHDFS deleteHDFS = new TestableDeleteHDFS(mockFileSystem);
        TestRunner runner = TestRunners.newTestRunner(deleteHDFS);
        runner.setProperty(DeleteHDFS.FILE_OR_DIRECTORY, "${hdfs.file}");
        Map<String, String> attributes = Maps.newHashMap();
        attributes.put("hdfs.file", filePath.toString());
        runner.enqueue("foo", attributes);
        runner.run();
        runner.assertTransferCount(DeleteHDFS.REL_FAILURE, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(DeleteHDFS.REL_FAILURE).get(0);
        assertEquals("file.txt", flowFile.getAttribute("hdfs.filename"));
        assertEquals("/some/path/to", flowFile.getAttribute("hdfs.path"));
        assertEquals("Permissions Error", flowFile.getAttribute("hdfs.error.message"));
    }

    @Test
    public void testNoFlowFilesWithIncomingConnection() {
        Path filePath = new Path("${hdfs.file}");
        DeleteHDFS deleteHDFS = new TestableDeleteHDFS(mockFileSystem);
        TestRunner runner = TestRunners.newTestRunner(deleteHDFS);
        runner.setProperty(DeleteHDFS.FILE_OR_DIRECTORY, filePath.toString());
        runner.setIncomingConnection(true);
        runner.run();
        runner.assertQueueEmpty();
        runner.assertTransferCount(DeleteHDFS.REL_SUCCESS, 0);
        runner.assertTransferCount(DeleteHDFS.REL_FAILURE, 0);
    }

    @Test
    public void testDeleteNotExistingFile() throws Exception {
        Path filePath = new Path("/some/path/to/file.txt");
        when(mockFileSystem.exists(any(Path.class))).thenReturn(false);
        DeleteHDFS deleteHDFS = new TestableDeleteHDFS(mockFileSystem);
        TestRunner runner = TestRunners.newTestRunner(deleteHDFS);
        runner.setIncomingConnection(false);
        runner.assertNotValid();
        runner.setProperty(DeleteHDFS.FILE_OR_DIRECTORY, filePath.toString());
        runner.assertValid();
        runner.run();
        runner.assertTransferCount(DeleteHDFS.REL_FAILURE, 0);
    }

    @Test
    public void testFailedDelete() throws Exception {
        final Path filePath = new Path("/some/path/to/file.txt");
        when(mockFileSystem.exists(any(Path.class))).thenReturn(true);
        when(mockFileSystem.delete(any(Path.class), anyBoolean())).thenReturn(false);
        final DeleteHDFS deleteHDFS = new TestableDeleteHDFS(mockFileSystem);
        final TestRunner runner = TestRunners.newTestRunner(deleteHDFS);
        runner.setIncomingConnection(false);
        runner.assertNotValid();
        runner.setProperty(DeleteHDFS.FILE_OR_DIRECTORY, filePath.toString());
        runner.assertValid();
        runner.run();
        runner.assertTransferCount(DeleteHDFS.REL_SUCCESS, 0);
        runner.assertTransferCount(DeleteHDFS.REL_FAILURE, 1);
    }

    @Test
    public void testGlobDelete() throws Exception {
        Path glob = new Path("/data/for/2017/08/05/*");
        int fileCount = 300;
        FileStatus[] fileStatuses = new FileStatus[fileCount];
        for (int i = 0; i < fileCount; i++) {
            Path file = new Path("/data/for/2017/08/05/file" + i);
            FileStatus fileStatus = mock(FileStatus.class);
            when(fileStatus.getPath()).thenReturn(file);
            fileStatuses[i] = fileStatus;
        }
        when(mockFileSystem.exists(any(Path.class))).thenReturn(true);
        when(mockFileSystem.globStatus(any(Path.class))).thenReturn(fileStatuses);
        when(mockFileSystem.getUri()).thenReturn(new URI("hdfs://0.example.com:8020"));
        when(mockFileSystem.delete(any(Path.class), anyBoolean())).thenReturn(true);
        DeleteHDFS deleteHDFS = new TestableDeleteHDFS(mockFileSystem);
        TestRunner runner = TestRunners.newTestRunner(deleteHDFS);
        runner.setIncomingConnection(false);
        runner.assertNotValid();
        runner.setProperty(DeleteHDFS.FILE_OR_DIRECTORY, glob.toString());
        runner.assertValid();
        runner.run();
        runner.assertTransferCount(DeleteHDFS.REL_SUCCESS, 1);
    }

    @Test
    public void testFailedGlobDelete() throws Exception {
        final Path glob = new Path("/data/for/2017/08/05/*");
        final int fileCount = 10;
        final FileStatus[] fileStatuses = new FileStatus[fileCount];
        for (int i = 0; i < fileCount; i++) {
            final Path file = new Path("/data/for/2017/08/05/file" + i);
            final FileStatus fileStatus = mock(FileStatus.class);
            when(fileStatus.getPath()).thenReturn(file);
            fileStatuses[i] = fileStatus;
        }
        when(mockFileSystem.exists(any(Path.class))).thenReturn(true);
        when(mockFileSystem.globStatus(any(Path.class))).thenReturn(fileStatuses);
        when(mockFileSystem.getUri()).thenReturn(new URI("hdfs://0.example.com:8020"));
        when(mockFileSystem.delete(any(Path.class), anyBoolean())).thenReturn(false);
        final DeleteHDFS deleteHDFS = new TestableDeleteHDFS(mockFileSystem);
        final TestRunner runner = TestRunners.newTestRunner(deleteHDFS);
        runner.setIncomingConnection(false);
        runner.assertNotValid();
        runner.setProperty(DeleteHDFS.FILE_OR_DIRECTORY, glob.toString());
        runner.assertValid();
        runner.run();
        runner.assertTransferCount(DeleteHDFS.REL_SUCCESS, 0);
        runner.assertTransferCount(DeleteHDFS.REL_FAILURE, fileCount);
    }

    @Test
    public void testMixedGlobDelete() throws Exception {
        final Path glob = new Path("/data/for/2017/08/05/*");
        final int fileCount = 3;
        final FileStatus[] fileStatuses = new FileStatus[fileCount];
        for (int i = 0; i < fileCount; i++) {
            final Path file = new Path("/data/for/2017/08/05/file" + i);
            final FileStatus fileStatus = mock(FileStatus.class);
            when(fileStatus.getPath()).thenReturn(file);
            fileStatuses[i] = fileStatus;
        }
        when(mockFileSystem.exists(any(Path.class))).thenReturn(true);
        when(mockFileSystem.globStatus(any(Path.class))).thenReturn(fileStatuses);
        when(mockFileSystem.getUri()).thenReturn(new URI("hdfs://0.example.com:8020"));
        when(mockFileSystem.delete(any(Path.class), anyBoolean()))
                .thenReturn(false)
                .thenReturn(true)
                .thenReturn(false);
        final DeleteHDFS deleteHDFS = new TestableDeleteHDFS(mockFileSystem);
        final TestRunner runner = TestRunners.newTestRunner(deleteHDFS);
        runner.setIncomingConnection(false);
        runner.assertNotValid();
        runner.setProperty(DeleteHDFS.FILE_OR_DIRECTORY, glob.toString());
        runner.assertValid();
        runner.run();
        runner.assertTransferCount(DeleteHDFS.REL_SUCCESS, 0);
        runner.assertTransferCount(DeleteHDFS.REL_FAILURE, 2);
    }

    @Test
    public void testGlobDeleteFromIncomingFlowFile() throws Exception {
        Path glob = new Path("/data/for/2017/08/05/*");
        int fileCount = 300;
        FileStatus[] fileStatuses = new FileStatus[fileCount];
        for (int i = 0; i < fileCount; i++) {
            Path file = new Path("/data/for/2017/08/05/file" + i);
            FileStatus fileStatus = mock(FileStatus.class);
            when(fileStatus.getPath()).thenReturn(file);
            fileStatuses[i] = fileStatus;
        }
        when(mockFileSystem.exists(any(Path.class))).thenReturn(true);
        when(mockFileSystem.globStatus(any(Path.class))).thenReturn(fileStatuses);
        when(mockFileSystem.getUri()).thenReturn(new URI("hdfs://0.example.com:8020"));
        when(mockFileSystem.delete(any(Path.class), anyBoolean())).thenReturn(true);
        DeleteHDFS deleteHDFS = new TestableDeleteHDFS(mockFileSystem);
        TestRunner runner = TestRunners.newTestRunner(deleteHDFS);
        runner.setIncomingConnection(true);
        Map<String, String> attributes = Maps.newHashMap();
        runner.enqueue("foo", attributes);
        runner.setProperty(DeleteHDFS.FILE_OR_DIRECTORY, glob.toString());
        runner.assertValid();
        runner.run();
        runner.assertAllFlowFilesTransferred(DeleteHDFS.REL_SUCCESS);
        runner.assertTransferCount(DeleteHDFS.REL_SUCCESS, 1);
    }

    private static class TestableDeleteHDFS extends DeleteHDFS {
        private final FileSystem mockFileSystem;

        public TestableDeleteHDFS(FileSystem mockFileSystem) {
            this.mockFileSystem = mockFileSystem;
        }

        @Override
        protected FileSystem getFileSystem() {
            return mockFileSystem;
        }
    }

    @Test
    public void testGlobMatcher() {
        DeleteHDFS deleteHDFS = new DeleteHDFS();
        assertTrue(deleteHDFS.GLOB_MATCHER.reset("/data/for/08/09/*").find());
        assertTrue(deleteHDFS.GLOB_MATCHER.reset("/data/for/08/09/[01-04]").find());
        assertTrue(deleteHDFS.GLOB_MATCHER.reset("/data/for/0?/09/").find());
        assertFalse(deleteHDFS.GLOB_MATCHER.reset("/data/for/08/09").find());
    }
}
