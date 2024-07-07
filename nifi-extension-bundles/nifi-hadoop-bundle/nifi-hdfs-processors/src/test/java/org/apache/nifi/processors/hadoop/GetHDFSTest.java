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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.ietf.jgss.GSSException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.io.InputStream;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@DisabledOnOs(OS.WINDOWS)
public class GetHDFSTest {

    @Test
    public void getPathDifferenceTest() {
        assertEquals("", GetHDFS.getPathDifference(new Path("/root"), new Path("/file")));
        assertEquals("", GetHDFS.getPathDifference(new Path("/root"), new Path("/root/file")));
        assertEquals("one", GetHDFS.getPathDifference(new Path("/root"), new Path("/root/one/file")));
        assertEquals("one/two", GetHDFS.getPathDifference(new Path("/root"), new Path("/root/one/two/file")));
        assertEquals("one/two/three", GetHDFS.getPathDifference(new Path("/root"), new Path("/root/one/two/three/file")));

        assertEquals("", GetHDFS.getPathDifference(new Path("root"), new Path("/file")));
        assertEquals("", GetHDFS.getPathDifference(new Path("root"), new Path("/root/file")));
        assertEquals("one", GetHDFS.getPathDifference(new Path("root"), new Path("/root/one/file")));
        assertEquals("one/two", GetHDFS.getPathDifference(new Path("root"), new Path("/root/one/two/file")));
        assertEquals("one/two/three", GetHDFS.getPathDifference(new Path("root"), new Path("/base/root/one/two/three/file")));

        assertEquals("", GetHDFS.getPathDifference(new Path("/foo/bar"), new Path("/file")));
        assertEquals("", GetHDFS.getPathDifference(new Path("/foo/bar"), new Path("/foo/bar/file")));
        assertEquals("one", GetHDFS.getPathDifference(new Path("/foo/bar"), new Path("/foo/bar/one/file")));
        assertEquals("one/two", GetHDFS.getPathDifference(new Path("/foo/bar"), new Path("/foo/bar/one/two/file")));
        assertEquals("one/two/three", GetHDFS.getPathDifference(new Path("/foo/bar"), new Path("/foo/bar/one/two/three/file")));

        assertEquals("", GetHDFS.getPathDifference(new Path("foo/bar"), new Path("/file")));
        assertEquals("", GetHDFS.getPathDifference(new Path("foo/bar"), new Path("/foo/bar/file")));
        assertEquals("one", GetHDFS.getPathDifference(new Path("foo/bar"), new Path("/foo/bar/one/file")));
        assertEquals("one/two", GetHDFS.getPathDifference(new Path("foo/bar"), new Path("/foo/bar/one/two/file")));
        assertEquals("one/two/three", GetHDFS.getPathDifference(new Path("foo/bar"), new Path("/base/foo/bar/one/two/three/file")));

        assertEquals("one/two/three", GetHDFS.getPathDifference(new Path("foo/bar"), new Path("/base/base2/base3/foo/bar/one/two/three/file")));
    }

    @Test
    public void testValidators() {
        GetHDFS proc = new GetHDFS();
        TestRunner runner = TestRunners.newTestRunner(proc);
        Collection<ValidationResult> results;
        ProcessContext pc;

        results = new HashSet<>();
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        assertEquals(1, results.size());
        for (ValidationResult vr : results) {
            assertTrue(vr.toString().contains("is invalid because Directory is required"));
        }

        results = new HashSet<>();
        runner.setProperty(PutHDFS.DIRECTORY, "target");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        assertEquals(0, results.size());

        results = new HashSet<>();
        runner.setProperty(GetHDFS.DIRECTORY, "/target");
        runner.setProperty(GetHDFS.MIN_AGE, "10 secs");
        runner.setProperty(GetHDFS.MAX_AGE, "5 secs");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        assertEquals(1, results.size());
        for (ValidationResult vr : results) {
            assertTrue(vr.toString().contains("is invalid because Minimum File Age cannot be greater than Maximum File Age"));
        }
    }

    @Test
    public void testGetFilesWithFilter() {
        GetHDFS proc = new GetHDFS();
        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutHDFS.DIRECTORY, "src/test/resources/testdata");
        runner.setProperty(GetHDFS.FILE_FILTER_REGEX, "random.*");
        runner.setProperty(GetHDFS.KEEP_SOURCE_FILE, "true");
        runner.run();
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetHDFS.REL_SUCCESS);
        assertEquals(4, flowFiles.size());
        for (MockFlowFile flowFile : flowFiles) {
            assertTrue(flowFile.getAttribute(CoreAttributes.FILENAME.key()).startsWith("random"));
        }
    }

    @Test
    public void testDirectoryDoesNotExist() {
        GetHDFS proc = new GetHDFS();
        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutHDFS.DIRECTORY, "does/not/exist/${now():format('yyyyMMdd')}");
        runner.setProperty(GetHDFS.KEEP_SOURCE_FILE, "true");
        runner.run();
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetHDFS.REL_SUCCESS);
        assertEquals(0, flowFiles.size());
    }

    @Test
    public void testAutomaticDecompression() throws IOException {
        GetHDFS proc = new GetHDFS();
        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutHDFS.DIRECTORY, "src/test/resources/testdata");
        runner.setProperty(GetHDFS.FILE_FILTER_REGEX, "random.*.gz");
        runner.setProperty(GetHDFS.KEEP_SOURCE_FILE, "true");
        runner.setProperty(GetHDFS.COMPRESSION_CODEC, "AUTOMATIC");
        runner.run();

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetHDFS.REL_SUCCESS);
        assertEquals(1, flowFiles.size());

        MockFlowFile flowFile = flowFiles.get(0);
        assertEquals("randombytes-1", flowFile.getAttribute(CoreAttributes.FILENAME.key()));
        InputStream expected = getClass().getResourceAsStream("/testdata/randombytes-1");
        flowFile.assertContentEquals(expected);
    }

    @Test
    public void testInferCompressionCodecDisabled() throws IOException {
        GetHDFS proc = new GetHDFS();
        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutHDFS.DIRECTORY, "src/test/resources/testdata");
        runner.setProperty(GetHDFS.FILE_FILTER_REGEX, "random.*.gz");
        runner.setProperty(GetHDFS.KEEP_SOURCE_FILE, "true");
        runner.setProperty(GetHDFS.COMPRESSION_CODEC, "NONE");
        runner.run();

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetHDFS.REL_SUCCESS);
        assertEquals(1, flowFiles.size());

        MockFlowFile flowFile = flowFiles.get(0);
        assertEquals("randombytes-1.gz", flowFile.getAttribute(CoreAttributes.FILENAME.key()));
        InputStream expected = getClass().getResourceAsStream("/testdata/randombytes-1.gz");
        flowFile.assertContentEquals(expected);
    }

    @Test
    public void testFileExtensionNotACompressionCodec() throws IOException {
        GetHDFS proc = new GetHDFS();
        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutHDFS.DIRECTORY, "src/test/resources/testdata");
        runner.setProperty(GetHDFS.FILE_FILTER_REGEX, ".*.zip");
        runner.setProperty(GetHDFS.KEEP_SOURCE_FILE, "true");
        runner.setProperty(GetHDFS.COMPRESSION_CODEC, "AUTOMATIC");
        runner.run();

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetHDFS.REL_SUCCESS);
        assertEquals(1, flowFiles.size());

        MockFlowFile flowFile = flowFiles.get(0);
        assertEquals("13545423550275052.zip", flowFile.getAttribute(CoreAttributes.FILENAME.key()));
        InputStream expected = getClass().getResourceAsStream("/testdata/13545423550275052.zip");
        flowFile.assertContentEquals(expected);
    }

    @Test
    public void testDirectoryUsesValidEL() throws IOException {
        GetHDFS proc = new GetHDFS();
        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutHDFS.DIRECTORY, "src/test/resources/${literal('testdata'):substring(0,8)}");
        runner.setProperty(GetHDFS.FILE_FILTER_REGEX, ".*.zip");
        runner.setProperty(GetHDFS.KEEP_SOURCE_FILE, "true");
        runner.setProperty(GetHDFS.COMPRESSION_CODEC, "AUTOMATIC");
        runner.run();

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetHDFS.REL_SUCCESS);
        assertEquals(1, flowFiles.size());

        MockFlowFile flowFile = flowFiles.get(0);
        assertEquals("13545423550275052.zip", flowFile.getAttribute(CoreAttributes.FILENAME.key()));
        InputStream expected = getClass().getResourceAsStream("/testdata/13545423550275052.zip");
        flowFile.assertContentEquals(expected);
        final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(1, provenanceEvents.size());
        final ProvenanceEventRecord receiveEvent = provenanceEvents.get(0);
        assertEquals(ProvenanceEventType.RECEIVE, receiveEvent.getEventType());
        // If it runs with a real HDFS, the protocol will be "hdfs://", but with a local filesystem, just assert the filename.
        assertTrue(receiveEvent.getTransitUri().endsWith("13545423550275052.zip"));
    }

    @Test
    public void testDirectoryUsesUnrecognizedEL() {
        GetHDFS proc = new GetHDFS();
        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutHDFS.DIRECTORY, "data_${literal('testing'):substring(0,4)%7D");
        runner.setProperty(GetHDFS.FILE_FILTER_REGEX, ".*.zip");
        runner.setProperty(GetHDFS.KEEP_SOURCE_FILE, "true");
        runner.setProperty(GetHDFS.COMPRESSION_CODEC, "AUTOMATIC");
        runner.assertNotValid();
    }

    @Test
    public void testDirectoryUsesInvalidEL() {
        GetHDFS proc = new GetHDFS();
        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutHDFS.DIRECTORY, "data_${literal('testing'):foo()}");
        runner.setProperty(GetHDFS.FILE_FILTER_REGEX, ".*.zip");
        runner.setProperty(GetHDFS.KEEP_SOURCE_FILE, "true");
        runner.setProperty(GetHDFS.COMPRESSION_CODEC, "AUTOMATIC");
        runner.assertNotValid();
    }

    @Test
    public void testDirectoryCheckWrappedInUGICallWhenDirectoryExists() throws IOException, InterruptedException {
        // GIVEN, WHEN
        boolean directoryExists = true;

        // THEN
        directoryExistsWrappedInUGICall(directoryExists);
    }

    @Test
    public void testDirectoryCheckWrappedInUGICallWhenDirectoryDoesNotExist() throws IOException, InterruptedException {
        // GIVEN, WHEN
        boolean directoryExists = false;

        // THEN
        directoryExistsWrappedInUGICall(directoryExists);
    }

    private void directoryExistsWrappedInUGICall(boolean directoryExists) throws IOException, InterruptedException {
        // GIVEN
        FileSystem mockFileSystem = mock(FileSystem.class);
        UserGroupInformation mockUserGroupInformation = mock(UserGroupInformation.class);

        GetHDFS testSubject = new TestableGetHDFSForUGI(mockFileSystem, mockUserGroupInformation);
        TestRunner runner = TestRunners.newTestRunner(testSubject);
        runner.setProperty(GetHDFS.DIRECTORY, "src/test/resources/testdata");

        // WHEN
        Answer<?> answer = new Answer<>() {
            private int callCounter = 0;
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                final Object result;
                if (callCounter == 0) {
                    when(mockFileSystem.exists(any(Path.class))).thenReturn(directoryExists);
                    result = ((PrivilegedExceptionAction<?>) invocationOnMock.getArgument(0)).run();
                    verify(mockUserGroupInformation, times(callCounter + 1)).doAs(any(PrivilegedExceptionAction.class));
                    verify(mockFileSystem).exists(any(Path.class));
                } else {
                    when(mockFileSystem.listStatus(any(Path.class))).thenReturn(new FileStatus[0]);
                    result = ((PrivilegedExceptionAction<?>) invocationOnMock.getArgument(0)).run();
                    verify(mockUserGroupInformation, times(callCounter + 1)).doAs(any(PrivilegedExceptionAction.class));
                    verify(mockFileSystem).listStatus(any(Path.class));
                }
                ++callCounter;
                return result;
            }
        };
        when(mockUserGroupInformation.doAs(any(PrivilegedExceptionAction.class))).thenAnswer(answer);
        runner.run();

        // THEN
        verify(mockFileSystem).getUri();
        verifyNoMoreInteractions(mockUserGroupInformation);
    }

    @Test
    public void testGSSExceptionOnExists() throws Exception {
        FileSystem mockFileSystem = mock(FileSystem.class);
        UserGroupInformation mockUserGroupInformation = mock(UserGroupInformation.class);

        GetHDFS testSubject = new TestableGetHDFSForUGI(mockFileSystem, mockUserGroupInformation);
        TestRunner runner = TestRunners.newTestRunner(testSubject);
        runner.setProperty(GetHDFS.DIRECTORY, "src/test/resources/testdata");
        when(mockUserGroupInformation.doAs(any(PrivilegedExceptionAction.class))).thenThrow(new IOException(new GSSException(13)));
        runner.run();

        // Assert session rollback
        runner.assertTransferCount(GetHDFS.REL_SUCCESS, 0);
        // assert that no files were penalized
        runner.assertPenalizeCount(0);
    }

    private static class TestableGetHDFSForUGI extends GetHDFS {
        private final FileSystem mockFileSystem;
        private final UserGroupInformation mockUserGroupInformation;

        public TestableGetHDFSForUGI(FileSystem mockFileSystem, UserGroupInformation mockUserGroupInformation) {
            this.mockFileSystem = mockFileSystem;
            this.mockUserGroupInformation = mockUserGroupInformation;
        }

        @Override
        protected FileSystem getFileSystem() {
            return mockFileSystem;
        }

        @Override
        protected UserGroupInformation getUserGroupInformation() {
            return mockUserGroupInformation;
        }
    }
}
