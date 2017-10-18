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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.hadoop.KerberosProperties;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PutHDFSTest {

    private NiFiProperties mockNiFiProperties;
    private KerberosProperties kerberosProperties;

    @BeforeClass
    public static void setUpClass() throws Exception {
        /*
         * Running Hadoop on Windows requires a special build which will produce required binaries and native modules [1]. Since functionality
         * provided by this module and validated by these test does not have any native implication we do not distribute required binaries and native modules
         * to support running these tests in Windows environment, therefore they are ignored. You can also get more info from this StackOverflow thread [2]
         *
         * [1] https://wiki.apache.org/hadoop/Hadoop2OnWindows
         * [2] http://stackoverflow.com/questions/19620642/failed-to-locate-the-winutils-binary-in-the-hadoop-binary-path
         */
    }

    @Before
    public void setup() {
        mockNiFiProperties = mock(NiFiProperties.class);
        when(mockNiFiProperties.getKerberosConfigurationFile()).thenReturn(null);
        kerberosProperties = new KerberosProperties(null);
    }

    @Test
    public void testValidators() {
        PutHDFS proc = new TestablePutHDFS(kerberosProperties);
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
        runner.setProperty(PutHDFS.DIRECTORY, "/target");
        runner.setProperty(PutHDFS.REPLICATION_FACTOR, "-1");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        Assert.assertEquals(1, results.size());
        for (ValidationResult vr : results) {
            assertTrue(vr.toString().contains("is invalid because short integer must be greater than zero"));
        }

        proc = new TestablePutHDFS(kerberosProperties);
        runner = TestRunners.newTestRunner(proc);
        results = new HashSet<>();
        runner.setProperty(PutHDFS.DIRECTORY, "/target");
        runner.setProperty(PutHDFS.REPLICATION_FACTOR, "0");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        Assert.assertEquals(1, results.size());
        for (ValidationResult vr : results) {
            assertTrue(vr.toString().contains("is invalid because short integer must be greater than zero"));
        }

        proc = new TestablePutHDFS(kerberosProperties);
        runner = TestRunners.newTestRunner(proc);
        results = new HashSet<>();
        runner.setProperty(PutHDFS.DIRECTORY, "/target");
        runner.setProperty(PutHDFS.UMASK, "-1");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        Assert.assertEquals(1, results.size());
        for (ValidationResult vr : results) {
            assertTrue(vr.toString().contains("is invalid because octal umask [-1] cannot be negative"));
        }

        proc = new TestablePutHDFS(kerberosProperties);
        runner = TestRunners.newTestRunner(proc);
        results = new HashSet<>();
        runner.setProperty(PutHDFS.DIRECTORY, "/target");
        runner.setProperty(PutHDFS.UMASK, "18");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        Assert.assertEquals(1, results.size());
        for (ValidationResult vr : results) {
            assertTrue(vr.toString().contains("is invalid because [18] is not a valid short octal number"));
        }

        results = new HashSet<>();
        runner.setProperty(PutHDFS.DIRECTORY, "/target");
        runner.setProperty(PutHDFS.UMASK, "2000");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        Assert.assertEquals(1, results.size());
        for (ValidationResult vr : results) {
            assertTrue(vr.toString().contains("is invalid because octal umask [2000] is not a valid umask"));
        }

        results = new HashSet<>();
        proc = new TestablePutHDFS(kerberosProperties);
        runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutHDFS.DIRECTORY, "/target");
        runner.setProperty(PutHDFS.COMPRESSION_CODEC, CompressionCodec.class.getName());
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        Assert.assertEquals(1, results.size());
        for (ValidationResult vr : results) {
            Assert.assertTrue(vr.toString().contains("is invalid because Given value not found in allowed set"));
        }
    }

    @Test
    public void testPutFile() throws IOException {
        // Refer to comment in the BeforeClass method for an explanation
        assumeTrue(isNotWindows());

        PutHDFS proc = new TestablePutHDFS(kerberosProperties);
        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutHDFS.DIRECTORY, "target/test-classes");
        runner.setProperty(PutHDFS.CONFLICT_RESOLUTION, "replace");
        try (FileInputStream fis = new FileInputStream("src/test/resources/testdata/randombytes-1");) {
            Map<String, String> attributes = new HashMap<String, String>();
            attributes.put(CoreAttributes.FILENAME.key(), "randombytes-1");
            runner.enqueue(fis, attributes);
            runner.run();
        }

        Configuration config = new Configuration();
        FileSystem fs = FileSystem.get(config);

        List<MockFlowFile> failedFlowFiles = runner
                .getFlowFilesForRelationship(new Relationship.Builder().name("failure").build());
        assertTrue(failedFlowFiles.isEmpty());

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutHDFS.REL_SUCCESS);
        assertEquals(1, flowFiles.size());
        MockFlowFile flowFile = flowFiles.get(0);
        assertTrue(fs.exists(new Path("target/test-classes/randombytes-1")));
        assertEquals("randombytes-1", flowFile.getAttribute(CoreAttributes.FILENAME.key()));
        assertEquals("target/test-classes", flowFile.getAttribute(PutHDFS.ABSOLUTE_HDFS_PATH_ATTRIBUTE));
    }

    @Test
    public void testPutFileWithCompression() throws IOException {
        // Refer to comment in the BeforeClass method for an explanation
        assumeTrue(isNotWindows());

        PutHDFS proc = new TestablePutHDFS(kerberosProperties);
        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutHDFS.DIRECTORY, "target/test-classes");
        runner.setProperty(PutHDFS.CONFLICT_RESOLUTION, "replace");
        runner.setProperty(PutHDFS.COMPRESSION_CODEC, "GZIP");
        try (FileInputStream fis = new FileInputStream("src/test/resources/testdata/randombytes-1");) {
            Map<String, String> attributes = new HashMap<String, String>();
            attributes.put(CoreAttributes.FILENAME.key(), "randombytes-1");
            runner.enqueue(fis, attributes);
            runner.run();
        }

        Configuration config = new Configuration();
        FileSystem fs = FileSystem.get(config);

        List<MockFlowFile> failedFlowFiles = runner
                .getFlowFilesForRelationship(new Relationship.Builder().name("failure").build());
        assertTrue(failedFlowFiles.isEmpty());

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutHDFS.REL_SUCCESS);
        assertEquals(1, flowFiles.size());
        MockFlowFile flowFile = flowFiles.get(0);
        assertTrue(fs.exists(new Path("target/test-classes/randombytes-1.gz")));
        assertEquals("randombytes-1.gz", flowFile.getAttribute(CoreAttributes.FILENAME.key()));
        assertEquals("target/test-classes", flowFile.getAttribute(PutHDFS.ABSOLUTE_HDFS_PATH_ATTRIBUTE));
    }

    @Test
    public void testPutFileWithException() throws IOException {
        // Refer to comment in the BeforeClass method for an explanation
        assumeTrue(isNotWindows());

        String dirName = "target/testPutFileWrongPermissions";
        File file = new File(dirName);
        file.mkdirs();
        Configuration config = new Configuration();
        FileSystem fs = FileSystem.get(config);
        Path p = new Path(dirName).makeQualified(fs.getUri(), fs.getWorkingDirectory());

        final KerberosProperties testKerberosProperties = kerberosProperties;
        TestRunner runner = TestRunners.newTestRunner(new PutHDFS() {
            @Override
            protected void changeOwner(ProcessContext context, FileSystem hdfs, Path name, FlowFile flowFile) {
                throw new ProcessException("Forcing Exception to get thrown in order to verify proper handling");
            }

            @Override
            protected KerberosProperties getKerberosProperties(File kerberosConfigFile) {
                return testKerberosProperties;
            }
        });
        runner.setProperty(PutHDFS.DIRECTORY, dirName);
        runner.setProperty(PutHDFS.CONFLICT_RESOLUTION, "replace");

        try (FileInputStream fis = new FileInputStream("src/test/resources/testdata/randombytes-1");) {
            Map<String, String> attributes = new HashMap<String, String>();
            attributes.put(CoreAttributes.FILENAME.key(), "randombytes-1");
            runner.enqueue(fis, attributes);
            runner.run();
        }

        List<MockFlowFile> failedFlowFiles = runner
                .getFlowFilesForRelationship(new Relationship.Builder().name("failure").build());
        assertFalse(failedFlowFiles.isEmpty());
        assertTrue(failedFlowFiles.get(0).isPenalized());

        fs.delete(p, true);
    }

    @Test
    public void testPutFileWhenDirectoryUsesValidELFunction() throws IOException {
        // Refer to comment in the BeforeClass method for an explanation
        assumeTrue(isNotWindows());

        PutHDFS proc = new TestablePutHDFS(kerberosProperties);
        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutHDFS.DIRECTORY, "target/data_${literal('testing'):substring(0,4)}");
        runner.setProperty(PutHDFS.CONFLICT_RESOLUTION, "replace");
        try (FileInputStream fis = new FileInputStream("src/test/resources/testdata/randombytes-1");) {
            Map<String, String> attributes = new HashMap<String, String>();
            attributes.put(CoreAttributes.FILENAME.key(), "randombytes-1");
            runner.enqueue(fis, attributes);
            runner.run();
        }

        Configuration config = new Configuration();
        FileSystem fs = FileSystem.get(config);

        List<MockFlowFile> failedFlowFiles = runner
                .getFlowFilesForRelationship(new Relationship.Builder().name("failure").build());
        assertTrue(failedFlowFiles.isEmpty());

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutHDFS.REL_SUCCESS);
        assertEquals(1, flowFiles.size());
        MockFlowFile flowFile = flowFiles.get(0);
        assertTrue(fs.exists(new Path("target/test-classes/randombytes-1")));
        assertEquals("randombytes-1", flowFile.getAttribute(CoreAttributes.FILENAME.key()));
        assertEquals("target/data_test", flowFile.getAttribute(PutHDFS.ABSOLUTE_HDFS_PATH_ATTRIBUTE));
    }

    @Test
    public void testPutFileWhenDirectoryUsesUnrecognizedEL() throws IOException {
        // Refer to comment in the BeforeClass method for an explanation
        assumeTrue(isNotWindows());

        PutHDFS proc = new TestablePutHDFS(kerberosProperties);
        TestRunner runner = TestRunners.newTestRunner(proc);

        // this value somehow causes NiFi to not even recognize the EL, and thus it returns successfully from calling
        // evaluateAttributeExpressions and then tries to create a Path with the exact value below and blows up
        runner.setProperty(PutHDFS.DIRECTORY, "data_${literal('testing'):substring(0,4)%7D");

        runner.setProperty(PutHDFS.CONFLICT_RESOLUTION, "replace");
        try (FileInputStream fis = new FileInputStream("src/test/resources/testdata/randombytes-1");) {
            Map<String, String> attributes = new HashMap<String, String>();
            attributes.put(CoreAttributes.FILENAME.key(), "randombytes-1");
            runner.enqueue(fis, attributes);
            runner.run();
        }

        runner.assertAllFlowFilesTransferred(PutHDFS.REL_FAILURE);
    }

    @Test
    public void testPutFileWhenDirectoryUsesInvalidEL() throws IOException {
        // Refer to comment in the BeforeClass method for an explanation
        assumeTrue(isNotWindows());

        PutHDFS proc = new TestablePutHDFS(kerberosProperties);
        TestRunner runner = TestRunners.newTestRunner(proc);
        // the validator should pick up the invalid EL
        runner.setProperty(PutHDFS.DIRECTORY, "target/data_${literal('testing'):foo()}");
        runner.setProperty(PutHDFS.CONFLICT_RESOLUTION, "replace");
        runner.assertNotValid();
    }

    private boolean isNotWindows() {
        return !System.getProperty("os.name").startsWith("Windows");
    }

    private static class TestablePutHDFS extends PutHDFS {

        private KerberosProperties testKerberosProperties;

        public TestablePutHDFS(KerberosProperties testKerberosProperties) {
            this.testKerberosProperties = testKerberosProperties;
        }

        @Override
        protected KerberosProperties getKerberosProperties(File kerberosConfigFile) {
            return testKerberosProperties;
        }
    }

}
