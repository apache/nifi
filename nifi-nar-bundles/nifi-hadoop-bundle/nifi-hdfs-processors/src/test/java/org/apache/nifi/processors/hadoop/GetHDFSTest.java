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

import org.apache.hadoop.fs.Path;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.hadoop.KerberosProperties;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GetHDFSTest {

    private NiFiProperties mockNiFiProperties;
    private KerberosProperties kerberosProperties;

    @Before
    public void setup() {
        mockNiFiProperties = mock(NiFiProperties.class);
        when(mockNiFiProperties.getKerberosConfigurationFile()).thenReturn(null);
        kerberosProperties = new KerberosProperties(null);
    }

    @Test
    public void getPathDifferenceTest() {
        Assert.assertEquals("", GetHDFS.getPathDifference(new Path("/root"), new Path("/file")));
        Assert.assertEquals("", GetHDFS.getPathDifference(new Path("/root"), new Path("/root/file")));
        Assert.assertEquals("one", GetHDFS.getPathDifference(new Path("/root"), new Path("/root/one/file")));
        Assert.assertEquals("one/two", GetHDFS.getPathDifference(new Path("/root"), new Path("/root/one/two/file")));
        Assert.assertEquals("one/two/three", GetHDFS.getPathDifference(new Path("/root"), new Path("/root/one/two/three/file")));

        Assert.assertEquals("", GetHDFS.getPathDifference(new Path("root"), new Path("/file")));
        Assert.assertEquals("", GetHDFS.getPathDifference(new Path("root"), new Path("/root/file")));
        Assert.assertEquals("one", GetHDFS.getPathDifference(new Path("root"), new Path("/root/one/file")));
        Assert.assertEquals("one/two", GetHDFS.getPathDifference(new Path("root"), new Path("/root/one/two/file")));
        Assert.assertEquals("one/two/three", GetHDFS.getPathDifference(new Path("root"), new Path("/base/root/one/two/three/file")));

        Assert.assertEquals("", GetHDFS.getPathDifference(new Path("/foo/bar"), new Path("/file")));
        Assert.assertEquals("", GetHDFS.getPathDifference(new Path("/foo/bar"), new Path("/foo/bar/file")));
        Assert.assertEquals("one", GetHDFS.getPathDifference(new Path("/foo/bar"), new Path("/foo/bar/one/file")));
        Assert.assertEquals("one/two", GetHDFS.getPathDifference(new Path("/foo/bar"), new Path("/foo/bar/one/two/file")));
        Assert.assertEquals("one/two/three", GetHDFS.getPathDifference(new Path("/foo/bar"), new Path("/foo/bar/one/two/three/file")));

        Assert.assertEquals("", GetHDFS.getPathDifference(new Path("foo/bar"), new Path("/file")));
        Assert.assertEquals("", GetHDFS.getPathDifference(new Path("foo/bar"), new Path("/foo/bar/file")));
        Assert.assertEquals("one", GetHDFS.getPathDifference(new Path("foo/bar"), new Path("/foo/bar/one/file")));
        Assert.assertEquals("one/two", GetHDFS.getPathDifference(new Path("foo/bar"), new Path("/foo/bar/one/two/file")));
        Assert.assertEquals("one/two/three", GetHDFS.getPathDifference(new Path("foo/bar"), new Path("/base/foo/bar/one/two/three/file")));

        Assert.assertEquals("one/two/three", GetHDFS.getPathDifference(new Path("foo/bar"), new Path("/base/base2/base3/foo/bar/one/two/three/file")));
    }

    @Test
    public void testValidators() {
        GetHDFS proc = new TestableGetHDFS(kerberosProperties);
        TestRunner runner = TestRunners.newTestRunner(proc);
        Collection<ValidationResult> results;
        ProcessContext pc;

        results = new HashSet<>();
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        Assert.assertEquals(1, results.size());
        for (ValidationResult vr : results) {
            Assert.assertTrue(vr.toString().contains("is invalid because Directory is required"));
        }

        results = new HashSet<>();
        runner.setProperty(PutHDFS.DIRECTORY, "target");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        Assert.assertEquals(0, results.size());

        results = new HashSet<>();
        runner.setProperty(GetHDFS.DIRECTORY, "/target");
        runner.setProperty(GetHDFS.MIN_AGE, "10 secs");
        runner.setProperty(GetHDFS.MAX_AGE, "5 secs");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        Assert.assertEquals(1, results.size());
        for (ValidationResult vr : results) {
            Assert.assertTrue(vr.toString().contains("is invalid because Minimum File Age cannot be greater than Maximum File Age"));
        }
    }

    @Test
    public void testGetFilesWithFilter() {
        GetHDFS proc = new TestableGetHDFS(kerberosProperties);
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
    public void testAutomaticDecompression() throws IOException {
        GetHDFS proc = new TestableGetHDFS(kerberosProperties);
        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutHDFS.DIRECTORY, "src/test/resources/testdata");
        runner.setProperty(GetHDFS.FILE_FILTER_REGEX, "random.*.gz");
        runner.setProperty(GetHDFS.KEEP_SOURCE_FILE, "true");
        runner.setProperty(GetHDFS.COMPRESSION_CODEC, "AUTOMATIC");
        runner.run();

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetHDFS.REL_SUCCESS);
        assertEquals(1, flowFiles.size());

        MockFlowFile flowFile = flowFiles.get(0);
        assertTrue(flowFile.getAttribute(CoreAttributes.FILENAME.key()).equals("randombytes-1"));
        InputStream expected = getClass().getResourceAsStream("/testdata/randombytes-1");
        flowFile.assertContentEquals(expected);
    }

    @Test
    public void testInferCompressionCodecDisabled() throws IOException {
        GetHDFS proc = new TestableGetHDFS(kerberosProperties);
        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutHDFS.DIRECTORY, "src/test/resources/testdata");
        runner.setProperty(GetHDFS.FILE_FILTER_REGEX, "random.*.gz");
        runner.setProperty(GetHDFS.KEEP_SOURCE_FILE, "true");
        runner.setProperty(GetHDFS.COMPRESSION_CODEC, "NONE");
        runner.run();

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetHDFS.REL_SUCCESS);
        assertEquals(1, flowFiles.size());

        MockFlowFile flowFile = flowFiles.get(0);
        assertTrue(flowFile.getAttribute(CoreAttributes.FILENAME.key()).equals("randombytes-1.gz"));
        InputStream expected = getClass().getResourceAsStream("/testdata/randombytes-1.gz");
        flowFile.assertContentEquals(expected);
    }

    @Test
    public void testFileExtensionNotACompressionCodec() throws IOException {
        GetHDFS proc = new TestableGetHDFS(kerberosProperties);
        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutHDFS.DIRECTORY, "src/test/resources/testdata");
        runner.setProperty(GetHDFS.FILE_FILTER_REGEX, ".*.zip");
        runner.setProperty(GetHDFS.KEEP_SOURCE_FILE, "true");
        runner.setProperty(GetHDFS.COMPRESSION_CODEC, "AUTOMATIC");
        runner.run();

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetHDFS.REL_SUCCESS);
        assertEquals(1, flowFiles.size());

        MockFlowFile flowFile = flowFiles.get(0);
        assertTrue(flowFile.getAttribute(CoreAttributes.FILENAME.key()).equals("13545423550275052.zip"));
        InputStream expected = getClass().getResourceAsStream("/testdata/13545423550275052.zip");
        flowFile.assertContentEquals(expected);
    }

    @Test
    public void testDirectoryUsesValidEL() throws IOException {
        GetHDFS proc = new TestableGetHDFS(kerberosProperties);
        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutHDFS.DIRECTORY, "src/test/resources/${literal('testdata'):substring(0,8)}");
        runner.setProperty(GetHDFS.FILE_FILTER_REGEX, ".*.zip");
        runner.setProperty(GetHDFS.KEEP_SOURCE_FILE, "true");
        runner.setProperty(GetHDFS.COMPRESSION_CODEC, "AUTOMATIC");
        runner.run();

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetHDFS.REL_SUCCESS);
        assertEquals(1, flowFiles.size());

        MockFlowFile flowFile = flowFiles.get(0);
        assertTrue(flowFile.getAttribute(CoreAttributes.FILENAME.key()).equals("13545423550275052.zip"));
        InputStream expected = getClass().getResourceAsStream("/testdata/13545423550275052.zip");
        flowFile.assertContentEquals(expected);
    }

    @Test
    public void testDirectoryUsesUnrecognizedEL() throws IOException {
        GetHDFS proc = new TestableGetHDFS(kerberosProperties);
        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutHDFS.DIRECTORY, "data_${literal('testing'):substring(0,4)%7D");
        runner.setProperty(GetHDFS.FILE_FILTER_REGEX, ".*.zip");
        runner.setProperty(GetHDFS.KEEP_SOURCE_FILE, "true");
        runner.setProperty(GetHDFS.COMPRESSION_CODEC, "AUTOMATIC");
        runner.assertNotValid();
    }

    @Test
    public void testDirectoryUsesInvalidEL() throws IOException {
        GetHDFS proc = new TestableGetHDFS(kerberosProperties);
        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutHDFS.DIRECTORY, "data_${literal('testing'):foo()}");
        runner.setProperty(GetHDFS.FILE_FILTER_REGEX, ".*.zip");
        runner.setProperty(GetHDFS.KEEP_SOURCE_FILE, "true");
        runner.setProperty(GetHDFS.COMPRESSION_CODEC, "AUTOMATIC");
        runner.assertNotValid();
    }

    private static class TestableGetHDFS extends GetHDFS {

        private final KerberosProperties testKerberosProperties;

        public TestableGetHDFS(KerberosProperties testKerberosProperties) {
            this.testKerberosProperties = testKerberosProperties;
        }

        @Override
        protected KerberosProperties getKerberosProperties(File kerberosConfigFile) {
            return testKerberosProperties;
        }
    }

}
