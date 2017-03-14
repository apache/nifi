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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.nifi.hadoop.KerberosProperties;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;

public class TestDeleteHDFS {
    private NiFiProperties mockNiFiProperties;
    private FileSystem mockFileSystem;
    private KerberosProperties kerberosProperties;

    @Before
    public void setup() throws Exception {
        mockNiFiProperties = mock(NiFiProperties.class);
        when(mockNiFiProperties.getKerberosConfigurationFile()).thenReturn(null);
        kerberosProperties = new KerberosProperties(null);
        mockFileSystem = mock(FileSystem.class);
    }

    //Tests the case where a file is found and deleted but there was no incoming connection
    @Test
    public void testSuccessfulDelete() throws Exception {
        Path filePath = new Path("/some/path/to/file.txt");
        when(mockFileSystem.exists(any(Path.class))).thenReturn(true);
        DeleteHDFS deleteHDFS = new TestableDeleteHDFS(kerberosProperties, mockFileSystem);
        TestRunner runner = TestRunners.newTestRunner(deleteHDFS);
        runner.setIncomingConnection(false);
        runner.assertNotValid();
        runner.setProperty(DeleteHDFS.FILE_OR_DIRECTORY, filePath.toString());
        runner.assertValid();
        runner.run();
        runner.assertTransferCount(DeleteHDFS.REL_SUCCESS, 0);
        runner.assertTransferCount(DeleteHDFS.REL_FAILURE, 0);
    }

    @Test
    public void testDeleteFromIncomingFlowFile() throws Exception {
        Path filePath = new Path("/some/path/to/file.txt");
        when(mockFileSystem.exists(any(Path.class))).thenReturn(true);
        DeleteHDFS deleteHDFS = new TestableDeleteHDFS(kerberosProperties, mockFileSystem);
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
        DeleteHDFS deleteHDFS = new TestableDeleteHDFS(kerberosProperties, mockFileSystem);
        TestRunner runner = TestRunners.newTestRunner(deleteHDFS);
        runner.setProperty(DeleteHDFS.FILE_OR_DIRECTORY, "${hdfs.file}");
        Map<String, String> attributes = Maps.newHashMap();
        attributes.put("hdfs.file", filePath.toString());
        runner.enqueue("foo", attributes);
        runner.run();
        runner.assertTransferCount(DeleteHDFS.REL_FAILURE, 1);
    }

    @Test
    public void testPermissionIOException() throws Exception {
        Path filePath = new Path("/some/path/to/file.txt");
        when(mockFileSystem.exists(any(Path.class))).thenReturn(true);
        when(mockFileSystem.delete(any(Path.class), any(Boolean.class))).thenThrow(new IOException("Permissions Error"));
        DeleteHDFS deleteHDFS = new TestableDeleteHDFS(kerberosProperties, mockFileSystem);
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
    public void testNoFlowFilesWithIncomingConnection() throws Exception {
        Path filePath = new Path("${hdfs.file}");
        DeleteHDFS deleteHDFS = new TestableDeleteHDFS(kerberosProperties, mockFileSystem);
        TestRunner runner = TestRunners.newTestRunner(deleteHDFS);
        runner.setProperty(DeleteHDFS.FILE_OR_DIRECTORY, filePath.toString());
        runner.setIncomingConnection(true);
        runner.run();
        runner.assertQueueEmpty();
        runner.assertTransferCount(DeleteHDFS.REL_SUCCESS, 0);
        runner.assertTransferCount(DeleteHDFS.REL_FAILURE, 0);
    }

    @Test
    public void testUnsuccessfulDelete() throws Exception {
        Path filePath = new Path("/some/path/to/file.txt");
        when(mockFileSystem.exists(any(Path.class))).thenReturn(false);
        DeleteHDFS deleteHDFS = new TestableDeleteHDFS(kerberosProperties, mockFileSystem);
        TestRunner runner = TestRunners.newTestRunner(deleteHDFS);
        runner.setIncomingConnection(false);
        runner.assertNotValid();
        runner.setProperty(DeleteHDFS.FILE_OR_DIRECTORY, filePath.toString());
        runner.assertValid();
        runner.run();
        runner.assertTransferCount(DeleteHDFS.REL_FAILURE, 0);
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
        DeleteHDFS deleteHDFS = new TestableDeleteHDFS(kerberosProperties, mockFileSystem);
        TestRunner runner = TestRunners.newTestRunner(deleteHDFS);
        runner.setIncomingConnection(false);
        runner.assertNotValid();
        runner.setProperty(DeleteHDFS.FILE_OR_DIRECTORY, glob.toString());
        runner.assertValid();
        runner.run();
        runner.assertTransferCount(DeleteHDFS.REL_SUCCESS, 0);
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
        DeleteHDFS deleteHDFS = new TestableDeleteHDFS(kerberosProperties, mockFileSystem);
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
        private KerberosProperties testKerberosProperties;
        private FileSystem mockFileSystem;

        public TestableDeleteHDFS(KerberosProperties kerberosProperties, FileSystem mockFileSystem) {
            this.testKerberosProperties = kerberosProperties;
            this.mockFileSystem = mockFileSystem;
        }

        @Override
        protected KerberosProperties getKerberosProperties(File kerberosConfigFile) {
            return testKerberosProperties;
        }

        @Override
        protected FileSystem getFileSystem() {
            return mockFileSystem;
        }
    }

    @Test
    public void testGlobMatcher() throws Exception {
        DeleteHDFS deleteHDFS = new DeleteHDFS();
        assertTrue(deleteHDFS.GLOB_MATCHER.reset("/data/for/08/09/*").find());
        assertTrue(deleteHDFS.GLOB_MATCHER.reset("/data/for/08/09/[01-04]").find());
        assertTrue(deleteHDFS.GLOB_MATCHER.reset("/data/for/0?/09/").find());
        assertFalse(deleteHDFS.GLOB_MATCHER.reset("/data/for/08/09").find());
    }
}
