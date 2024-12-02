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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.fileresource.service.StandardFileResourceService;
import org.apache.nifi.fileresource.service.api.FileResourceService;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processors.transfer.ResourceTransferProperties;
import org.apache.nifi.processors.transfer.ResourceTransferSource;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.hadoop.util.MockFileSystem;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.ietf.jgss.GSSException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import javax.security.sasl.SaslException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.Set;
import java.util.Collections;

import static org.apache.nifi.processors.hadoop.CompressionType.GZIP;
import static org.apache.nifi.processors.hadoop.CompressionType.NONE;
import static org.apache.nifi.processors.hadoop.PutHDFS.APPEND_RESOLUTION;
import static org.apache.nifi.processors.hadoop.PutHDFS.AVRO_APPEND_MODE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class PutHDFSTest {
    private final static String TARGET_DIRECTORY = "target/test-classes";
    private final static String AVRO_TARGET_DIRECTORY = TARGET_DIRECTORY + "/testdata-avro";
    private final static String SOURCE_DIRECTORY = "src/test/resources/testdata";
    private final static String AVRO_SOURCE_DIRECTORY = "src/test/resources/testdata-avro";
    private final static String FILE_NAME = "randombytes-1";
    private final static String AVRO_FILE_NAME = "input.avro";

    private MockFileSystem mockFileSystem;

    @BeforeEach
    public void setup() {
        mockFileSystem = new MockFileSystem();
    }

    @Test
    public void testValidators() {
        PutHDFS proc = new TestablePutHDFS(mockFileSystem);
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
        runner.setProperty(PutHDFS.DIRECTORY, "/target");
        runner.setProperty(PutHDFS.REPLICATION_FACTOR, "-1");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        assertEquals(1, results.size());
        for (ValidationResult vr : results) {
            assertTrue(vr.toString().contains("is invalid because short integer must be greater than zero"));
        }

        proc = new TestablePutHDFS(mockFileSystem);
        runner = TestRunners.newTestRunner(proc);
        results = new HashSet<>();
        runner.setProperty(PutHDFS.DIRECTORY, "/target");
        runner.setProperty(PutHDFS.REPLICATION_FACTOR, "0");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        assertEquals(1, results.size());
        for (ValidationResult vr : results) {
            assertTrue(vr.toString().contains("is invalid because short integer must be greater than zero"));
        }

        proc = new TestablePutHDFS(mockFileSystem);
        runner = TestRunners.newTestRunner(proc);
        results = new HashSet<>();
        runner.setProperty(PutHDFS.DIRECTORY, "/target");
        runner.setProperty(PutHDFS.UMASK, "-1");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        assertEquals(1, results.size());
        for (ValidationResult vr : results) {
            assertTrue(vr.toString().contains("is invalid because octal umask [-1] cannot be negative"));
        }

        proc = new TestablePutHDFS(mockFileSystem);
        runner = TestRunners.newTestRunner(proc);
        results = new HashSet<>();
        runner.setProperty(PutHDFS.DIRECTORY, "/target");
        runner.setProperty(PutHDFS.UMASK, "18");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        assertEquals(1, results.size());
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
        assertEquals(1, results.size());
        for (ValidationResult vr : results) {
            assertTrue(vr.toString().contains("is invalid because octal umask [2000] is not a valid umask"));
        }

        results = new HashSet<>();
        proc = new TestablePutHDFS(mockFileSystem);
        runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutHDFS.DIRECTORY, "/target");
        runner.setProperty(PutHDFS.COMPRESSION_CODEC, CompressionCodec.class.getName());
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        assertEquals(1, results.size());
        for (ValidationResult vr : results) {
            assertTrue(vr.toString().contains("is invalid because Given value not found in allowed set"));
        }

        results = new HashSet<>();
        runner.setProperty(PutHDFS.DIRECTORY, "target");
        runner.setProperty(PutHDFS.APPEND_MODE, AVRO_APPEND_MODE);
        runner.setProperty(PutHDFS.CONFLICT_RESOLUTION, APPEND_RESOLUTION);
        runner.setProperty(PutHDFS.COMPRESSION_CODEC, GZIP.name());
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        assertEquals(1, results.size());
        for (ValidationResult vr : results) {
            assertEquals(vr.getSubject(), "Codec");
            assertEquals(vr.getExplanation(), "Compression codec cannot be set when used in 'append avro' mode");
        }

        results = new HashSet<>();
        runner.setProperty(PutHDFS.DIRECTORY, "target");
        runner.setProperty(PutHDFS.APPEND_MODE, AVRO_APPEND_MODE);
        runner.setProperty(PutHDFS.CONFLICT_RESOLUTION, APPEND_RESOLUTION);
        runner.setProperty(PutHDFS.COMPRESSION_CODEC, NONE.name());
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        assertEquals(0, results.size());
    }

    @Test
    public void testPutFile() throws IOException {
        // given
        final FileSystem spyFileSystem = Mockito.spy(mockFileSystem);
        final PutHDFS proc = new TestablePutHDFS(spyFileSystem);
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutHDFS.DIRECTORY, TARGET_DIRECTORY);
        runner.setProperty(PutHDFS.CONFLICT_RESOLUTION, PutHDFS.REPLACE_RESOLUTION);

        // when
        try (final FileInputStream fis = new FileInputStream(SOURCE_DIRECTORY + "/" + FILE_NAME)) {
            final Map<String, String> attributes = new HashMap<>();
            attributes.put(CoreAttributes.FILENAME.key(), FILE_NAME);
            runner.enqueue(fis, attributes);
            runner.run();
        }

        // then
        final List<MockFlowFile> failedFlowFiles = runner.getFlowFilesForRelationship(PutHDFS.REL_FAILURE);
        assertTrue(failedFlowFiles.isEmpty());

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutHDFS.REL_SUCCESS);
        assertEquals(1, flowFiles.size());

        final MockFlowFile flowFile = flowFiles.get(0);
        assertTrue(spyFileSystem.exists(new Path(TARGET_DIRECTORY + "/" + FILE_NAME)));
        assertEquals(FILE_NAME, flowFile.getAttribute(CoreAttributes.FILENAME.key()));
        assertEquals(TARGET_DIRECTORY, flowFile.getAttribute(PutHDFS.ABSOLUTE_HDFS_PATH_ATTRIBUTE));
        assertEquals("true", flowFile.getAttribute(PutHDFS.TARGET_HDFS_DIR_CREATED_ATTRIBUTE));

        final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(1, provenanceEvents.size());
        final ProvenanceEventRecord sendEvent = provenanceEvents.get(0);
        assertEquals(ProvenanceEventType.SEND, sendEvent.getEventType());
        // If it runs with a real HDFS, the protocol will be "hdfs://", but with a local filesystem, just assert the filename.
        assertTrue(sendEvent.getTransitUri().endsWith(TARGET_DIRECTORY + "/" + FILE_NAME));
        assertTrue(flowFile.getAttribute(PutHDFS.HADOOP_FILE_URL_ATTRIBUTE).endsWith(TARGET_DIRECTORY + "/" + FILE_NAME));

        verify(spyFileSystem, times(1)).rename(any(Path.class), any(Path.class));
    }

    @Test
    public void testPutFileWithAppendAvroModeNewFileCreated() throws IOException {
        // given
        final FileSystem spyFileSystem = Mockito.spy(mockFileSystem);
        final PutHDFS proc = new TestablePutHDFS(spyFileSystem);
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutHDFS.DIRECTORY, AVRO_TARGET_DIRECTORY);
        runner.setProperty(PutHDFS.CONFLICT_RESOLUTION, APPEND_RESOLUTION);
        runner.setProperty(PutHDFS.APPEND_MODE, AVRO_APPEND_MODE);
        final Path targetPath = new Path(AVRO_TARGET_DIRECTORY + "/" + AVRO_FILE_NAME);

        // when
        try (final FileInputStream fis = new FileInputStream(AVRO_SOURCE_DIRECTORY + "/" + AVRO_FILE_NAME)) {
            runner.enqueue(fis, Map.of(CoreAttributes.FILENAME.key(), AVRO_FILE_NAME));
            assertTrue(runner.isValid());
            runner.run();
        }

        // then
        assertAvroAppendValues(runner, spyFileSystem, targetPath);
        verify(spyFileSystem, times(0)).append(eq(targetPath), anyInt());
        verify(spyFileSystem, times(1)).rename(any(Path.class), eq(targetPath));
        assertEquals(100, spyFileSystem.getFileStatus(targetPath).getLen());
    }

    @Test
    public void testPutFileWithAppendAvroModeWhenTargetFileAlreadyExists() throws IOException {
        // given
        final FileSystem spyFileSystem = Mockito.spy(mockFileSystem);
        final PutHDFS proc = new TestablePutHDFS(spyFileSystem);
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutHDFS.DIRECTORY, AVRO_TARGET_DIRECTORY);
        runner.setProperty(PutHDFS.CONFLICT_RESOLUTION, APPEND_RESOLUTION);
        runner.setProperty(PutHDFS.APPEND_MODE, AVRO_APPEND_MODE);
        spyFileSystem.setConf(new Configuration());
        final Path targetPath = new Path(AVRO_TARGET_DIRECTORY + "/" + AVRO_FILE_NAME);
        spyFileSystem.createNewFile(targetPath);

        // when
        try (final FileInputStream fis = new FileInputStream(AVRO_SOURCE_DIRECTORY + "/" + AVRO_FILE_NAME)) {
            runner.enqueue(fis, Map.of(CoreAttributes.FILENAME.key(), AVRO_FILE_NAME));
            assertTrue(runner.isValid());
            runner.run();
        }

        // then
        assertAvroAppendValues(runner, spyFileSystem, targetPath);
        verify(spyFileSystem).append(eq(targetPath), anyInt());
        verify(spyFileSystem, times(0)).rename(any(Path.class), eq(targetPath));
        assertEquals(200, spyFileSystem.getFileStatus(targetPath).getLen());
    }

    @Test
    public void testPutFileWithSimpleWrite() throws IOException {
        // given
        final FileSystem spyFileSystem = Mockito.spy(mockFileSystem);
        final PutHDFS proc = new TestablePutHDFS(spyFileSystem);
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutHDFS.DIRECTORY, TARGET_DIRECTORY);
        runner.setProperty(PutHDFS.CONFLICT_RESOLUTION, PutHDFS.REPLACE_RESOLUTION);
        runner.setProperty(PutHDFS.WRITING_STRATEGY, PutHDFS.SIMPLE_WRITE);

        // when
        try (final FileInputStream fis = new FileInputStream(SOURCE_DIRECTORY + "/" + FILE_NAME)) {
            final Map<String, String> attributes = new HashMap<>();
            attributes.put(CoreAttributes.FILENAME.key(), FILE_NAME);
            runner.enqueue(fis, attributes);
            runner.run();
        }

        // then
        final List<MockFlowFile> failedFlowFiles = runner.getFlowFilesForRelationship(PutHDFS.REL_FAILURE);
        assertTrue(failedFlowFiles.isEmpty());

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutHDFS.REL_SUCCESS);
        assertEquals(1, flowFiles.size());

        final MockFlowFile flowFile = flowFiles.get(0);
        assertTrue(spyFileSystem.exists(new Path(TARGET_DIRECTORY + "/" + FILE_NAME)));
        assertEquals(FILE_NAME, flowFile.getAttribute(CoreAttributes.FILENAME.key()));
        assertEquals(TARGET_DIRECTORY, flowFile.getAttribute(PutHDFS.ABSOLUTE_HDFS_PATH_ATTRIBUTE));
        assertEquals("true", flowFile.getAttribute(PutHDFS.TARGET_HDFS_DIR_CREATED_ATTRIBUTE));
        // If it runs with a real HDFS, the protocol will be "hdfs://", but with a local filesystem, just assert the filename.
        assertTrue(flowFile.getAttribute(PutHDFS.HADOOP_FILE_URL_ATTRIBUTE).endsWith(TARGET_DIRECTORY + "/" + FILE_NAME));

        verify(spyFileSystem, Mockito.never()).rename(any(Path.class), any(Path.class));
    }

    @Test
    public void testPutFileWhenTargetDirExists() throws IOException {
        String targetDir = "target/test-classes";
        PutHDFS proc = new TestablePutHDFS(mockFileSystem);
        proc.getFileSystem().mkdirs(new Path(targetDir));
        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutHDFS.DIRECTORY, targetDir);
        runner.setProperty(PutHDFS.CONFLICT_RESOLUTION, "replace");
        try (FileInputStream fis = new FileInputStream("src/test/resources/testdata/randombytes-1")) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put(CoreAttributes.FILENAME.key(), "randombytes-1");
            runner.enqueue(fis, attributes);
            runner.run();
        }

        List<MockFlowFile> failedFlowFiles = runner
                .getFlowFilesForRelationship(new Relationship.Builder().name("failure").build());
        assertTrue(failedFlowFiles.isEmpty());

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutHDFS.REL_SUCCESS);
        assertEquals(1, flowFiles.size());
        MockFlowFile flowFile = flowFiles.get(0);
        assertTrue(mockFileSystem.exists(new Path("target/test-classes/randombytes-1")));
        assertEquals("randombytes-1", flowFile.getAttribute(CoreAttributes.FILENAME.key()));
        assertEquals("target/test-classes", flowFile.getAttribute(PutHDFS.ABSOLUTE_HDFS_PATH_ATTRIBUTE));
        assertEquals("false", flowFile.getAttribute(PutHDFS.TARGET_HDFS_DIR_CREATED_ATTRIBUTE));

        final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(1, provenanceEvents.size());
        final ProvenanceEventRecord sendEvent = provenanceEvents.get(0);
        assertEquals(ProvenanceEventType.SEND, sendEvent.getEventType());
        // If it runs with a real HDFS, the protocol will be "hdfs://", but with a local filesystem, just assert the filename.
        assertTrue(sendEvent.getTransitUri().endsWith("target/test-classes/randombytes-1"));
        assertTrue(flowFile.getAttribute(PutHDFS.HADOOP_FILE_URL_ATTRIBUTE).endsWith("target/test-classes/randombytes-1"));
    }

    @Test
    public void testPutFileWithCompression() throws IOException {
        PutHDFS proc = new TestablePutHDFS(mockFileSystem);
        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutHDFS.DIRECTORY, "target/test-classes");
        runner.setProperty(PutHDFS.CONFLICT_RESOLUTION, "replace");
        runner.setProperty(PutHDFS.COMPRESSION_CODEC, "GZIP");
        try (FileInputStream fis = new FileInputStream("src/test/resources/testdata/randombytes-1")) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put(CoreAttributes.FILENAME.key(), "randombytes-1");
            runner.enqueue(fis, attributes);
            runner.run();
        }

        List<MockFlowFile> failedFlowFiles = runner
                .getFlowFilesForRelationship(new Relationship.Builder().name("failure").build());
        assertTrue(failedFlowFiles.isEmpty());

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutHDFS.REL_SUCCESS);
        assertEquals(1, flowFiles.size());
        MockFlowFile flowFile = flowFiles.get(0);
        assertTrue(mockFileSystem.exists(new Path("target/test-classes/randombytes-1.gz")));
        assertEquals("randombytes-1.gz", flowFile.getAttribute(CoreAttributes.FILENAME.key()));
        assertEquals("target/test-classes", flowFile.getAttribute(PutHDFS.ABSOLUTE_HDFS_PATH_ATTRIBUTE));
        assertTrue(flowFile.getAttribute(PutHDFS.HADOOP_FILE_URL_ATTRIBUTE).endsWith(TARGET_DIRECTORY + "/" + FILE_NAME + ".gz"));
    }

    @Test
    public void testPutFileWithGSSException() throws IOException {
        FileSystem noCredentialsFileSystem = new MockFileSystem() {
            @Override
            public FileStatus getFileStatus(Path path) throws IOException {
                throw new IOException("ioe", new SaslException("sasle", new GSSException(13)));
            }
        };
        TestRunner runner = TestRunners.newTestRunner(new TestablePutHDFS(noCredentialsFileSystem));
        runner.setProperty(PutHDFS.DIRECTORY, "target/test-classes");
        runner.setProperty(PutHDFS.CONFLICT_RESOLUTION, "replace");

        try (FileInputStream fis = new FileInputStream("src/test/resources/testdata/randombytes-1")) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put(CoreAttributes.FILENAME.key(), "randombytes-1");
            runner.enqueue(fis, attributes);
            runner.run();
        }

        // assert no flowfiles transferred to outgoing relationships
        runner.assertTransferCount(PutHDFS.REL_SUCCESS, 0);
        runner.assertTransferCount(PutHDFS.REL_FAILURE, 0);
    }

    @Test
    public void testPutFileWithProcessException() throws IOException {
        String dirName = "target/testPutFileWrongPermissions";
        File file = new File(dirName);
        file.mkdirs();
        Path p = new Path(dirName).makeQualified(mockFileSystem.getUri(), mockFileSystem.getWorkingDirectory());

        TestRunner runner = TestRunners.newTestRunner(new TestablePutHDFS(mockFileSystem) {
            @Override
            protected void changeOwner(ProcessContext context, FileSystem hdfs, Path name, FlowFile flowFile) {
                throw new ProcessException("Forcing Exception to get thrown in order to verify proper handling");
            }
        });
        runner.setProperty(PutHDFS.DIRECTORY, dirName);
        runner.setProperty(PutHDFS.CONFLICT_RESOLUTION, "replace");

        try (FileInputStream fis = new FileInputStream("src/test/resources/testdata/randombytes-1")) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put(CoreAttributes.FILENAME.key(), "randombytes-1");
            runner.enqueue(fis, attributes);
            runner.run();
        }

        List<MockFlowFile> failedFlowFiles = runner
                .getFlowFilesForRelationship(new Relationship.Builder().name("failure").build());
        assertFalse(failedFlowFiles.isEmpty());
        assertTrue(failedFlowFiles.get(0).isPenalized());

        mockFileSystem.delete(p, true);
    }

    @Test
    public void testPutFileWhenDirectoryUsesValidELFunction() throws IOException {
        PutHDFS proc = new TestablePutHDFS(mockFileSystem);
        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutHDFS.DIRECTORY, "target/data_${literal('testing'):substring(0,4)}");
        runner.setProperty(PutHDFS.CONFLICT_RESOLUTION, "replace");
        try (FileInputStream fis = new FileInputStream("src/test/resources/testdata/randombytes-1")) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put(CoreAttributes.FILENAME.key(), "randombytes-1");
            runner.enqueue(fis, attributes);
            runner.run();
        }

        List<MockFlowFile> failedFlowFiles = runner
                .getFlowFilesForRelationship(new Relationship.Builder().name("failure").build());
        assertTrue(failedFlowFiles.isEmpty());

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutHDFS.REL_SUCCESS);
        assertEquals(1, flowFiles.size());
        MockFlowFile flowFile = flowFiles.get(0);
        assertTrue(mockFileSystem.exists(new Path("target/data_test/randombytes-1")));
        assertEquals("randombytes-1", flowFile.getAttribute(CoreAttributes.FILENAME.key()));
        assertEquals("target/data_test", flowFile.getAttribute(PutHDFS.ABSOLUTE_HDFS_PATH_ATTRIBUTE));
        assertTrue(flowFile.getAttribute(PutHDFS.HADOOP_FILE_URL_ATTRIBUTE).endsWith("target/data_test/" + FILE_NAME));
    }

    @Test
    public void testPutFileWhenDirectoryUsesUnrecognizedEL() throws IOException {
        PutHDFS proc = new TestablePutHDFS(mockFileSystem);
        TestRunner runner = TestRunners.newTestRunner(proc);

        // this value somehow causes NiFi to not even recognize the EL, and thus it returns successfully from calling
        // evaluateAttributeExpressions and then tries to create a Path with the exact value below and blows up
        runner.setProperty(PutHDFS.DIRECTORY, "data_${literal('testing'):substring(0,4)%7D");

        runner.setProperty(PutHDFS.CONFLICT_RESOLUTION, "replace");
        try (FileInputStream fis = new FileInputStream("src/test/resources/testdata/randombytes-1")) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put(CoreAttributes.FILENAME.key(), "randombytes-1");
            runner.enqueue(fis, attributes);
            runner.run();
        }

        runner.assertAllFlowFilesTransferred(PutHDFS.REL_FAILURE);
    }

    @Test
    public void testPutFileWhenDirectoryUsesInvalidEL() {
        PutHDFS proc = new TestablePutHDFS(mockFileSystem);
        TestRunner runner = TestRunners.newTestRunner(proc);
        // the validator should pick up the invalid EL
        runner.setProperty(PutHDFS.DIRECTORY, "target/data_${literal('testing'):foo()}");
        runner.setProperty(PutHDFS.CONFLICT_RESOLUTION, "replace");
        runner.assertNotValid();
    }

    @Test
    public void testPutFilePermissionsWithProcessorConfiguredUmask() throws IOException {
        // assert the file permission is the same value as processor's property
        MockFileSystem fileSystem = new MockFileSystem();
        PutHDFS proc = new TestablePutHDFS(fileSystem);
        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutHDFS.DIRECTORY, "target/test-classes");
        runner.setProperty(PutHDFS.CONFLICT_RESOLUTION, "replace");
        String umaskPropertyValue = "027";
        runner.setProperty(PutHDFS.UMASK, umaskPropertyValue);
        // invoke the abstractOnScheduled method so the Hadoop configuration is available to apply the MockFileSystem instance
        proc.abstractOnScheduled(runner.getProcessContext());
        fileSystem.setConf(proc.getConfiguration());
        try (FileInputStream fis = new FileInputStream("src/test/resources/testdata/randombytes-1")) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put(CoreAttributes.FILENAME.key(), "randombytes-1");
            runner.enqueue(fis, attributes);
            runner.run();
        }
        assertEquals(FsPermission.getFileDefault().applyUMask(new FsPermission(umaskPropertyValue)), fileSystem.getFileStatus(new Path("target/test-classes/randombytes-1")).getPermission());
    }

    @Test
    public void testPutFilePermissionsWithXmlConfiguredUmask() throws IOException {
        // assert the file permission is the same value as xml
        MockFileSystem fileSystem = new MockFileSystem();
        PutHDFS proc = new TestablePutHDFS(fileSystem);
        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutHDFS.DIRECTORY, "target/test-classes");
        runner.setProperty(PutHDFS.CONFLICT_RESOLUTION, "replace");
        runner.setProperty(PutHDFS.HADOOP_CONFIGURATION_RESOURCES, "src/test/resources/core-site-perms.xml");
        // invoke the abstractOnScheduled method so the Hadoop configuration is available to apply the MockFileSystem instance
        proc.abstractOnScheduled(runner.getProcessContext());
        fileSystem.setConf(proc.getConfiguration());
        try (FileInputStream fis = new FileInputStream("src/test/resources/testdata/randombytes-1")) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put(CoreAttributes.FILENAME.key(), "randombytes-1");
            runner.enqueue(fis, attributes);
            runner.run();
        }
        assertEquals(FsPermission.getFileDefault().applyUMask(new FsPermission("777")), fileSystem.getFileStatus(new Path("target/test-classes/randombytes-1")).getPermission());
    }

    @Test
    public void testPutFilePermissionsWithNoConfiguredUmask() throws IOException {
        // assert the file permission fallback works. It should read FsPermission.DEFAULT_UMASK
        MockFileSystem fileSystem = new MockFileSystem();
        PutHDFS proc = new TestablePutHDFS(fileSystem);
        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutHDFS.DIRECTORY, "target/test-classes");
        runner.setProperty(PutHDFS.CONFLICT_RESOLUTION, "replace");
        // invoke the abstractOnScheduled method so the Hadoop configuration is available to apply the MockFileSystem instance
        proc.abstractOnScheduled(runner.getProcessContext());
        fileSystem.setConf(proc.getConfiguration());
        try (FileInputStream fis = new FileInputStream("src/test/resources/testdata/randombytes-1")) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put(CoreAttributes.FILENAME.key(), "randombytes-1");
            runner.enqueue(fis, attributes);
            runner.run();
        }
        assertEquals(FsPermission.getFileDefault().applyUMask(new FsPermission((short) FsPermission.DEFAULT_UMASK)),
            fileSystem.getFileStatus(new Path("target/test-classes/randombytes-1")).getPermission());
    }

    /**
     * Multiple invocations of PutHDFS on the same target directory should query the remote filesystem ACL once, and
     * use the cached ACL afterwards.
     */
    @Test
    public void testPutHDFSAclCache() {
        final MockFileSystem fileSystem = Mockito.spy(new MockFileSystem());
        final Path directory = new Path("/withACL");
        assertTrue(fileSystem.mkdirs(directory));
        final String acl = "user::rwx,group::rwx,other::rwx";
        final String aclDefault = "default:user::rwx,default:group::rwx,default:other::rwx";
        fileSystem.setAcl(directory, AclEntry.parseAclSpec(String.join(",", acl, aclDefault), true));

        final PutHDFS processor = new TestablePutHDFS(fileSystem);
        final TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setProperty(PutHDFS.DIRECTORY, directory.toString());
        runner.setProperty(PutHDFS.UMASK, "077");
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), "empty");
        runner.enqueue(new byte[16], attributes);
        runner.run(3);  // fetch data once; hit AclCache twice
        verify(fileSystem, times(1)).getAclStatus(any(Path.class));
    }

    /**
     * When no default ACL is present on the remote directory, usage of {@link PutHDFS#UMASK}
     * should be ok.
     */
    @Test
    public void testPutFileWithNoDefaultACL() {
        final List<Boolean> setUmask = Arrays.asList(false, true);
        for (boolean setUmaskIt : setUmask) {
            final MockFileSystem fileSystem = new MockFileSystem();
            final Path directory = new Path("/withNoDACL");
            assertTrue(fileSystem.mkdirs(directory));
            final String acl = "user::rwx,group::rwx,other::rwx";
            fileSystem.setAcl(directory, AclEntry.parseAclSpec(acl, true));

            final PutHDFS processor = new TestablePutHDFS(fileSystem);
            final TestRunner runner = TestRunners.newTestRunner(processor);
            runner.setProperty(PutHDFS.DIRECTORY, directory.toString());
            if (setUmaskIt) {
                runner.setProperty(PutHDFS.UMASK, "077");
            }
            final Map<String, String> attributes = new HashMap<>();
            attributes.put(CoreAttributes.FILENAME.key(), "empty");
            runner.enqueue(new byte[16], attributes);
            runner.run();
            assertEquals(1, runner.getFlowFilesForRelationship(PutHDFS.REL_SUCCESS).size());
            assertEquals(0, runner.getFlowFilesForRelationship(PutHDFS.REL_FAILURE).size());
        }
    }

    /**
     * When default ACL is present on the remote directory, usage of {@link PutHDFS#UMASK}
     * should trigger failure of the flow file.
     */
    @Test
    public void testPutFileWithDefaultACL() {
        final List<Boolean> setUmask = Arrays.asList(false, true);
        for (boolean setUmaskIt : setUmask) {
            final MockFileSystem fileSystem = new MockFileSystem();
            final Path directory = new Path("/withACL");
            assertTrue(fileSystem.mkdirs(directory));
            final String acl = "user::rwx,group::rwx,other::rwx";
            final String aclDefault = "default:user::rwx,default:group::rwx,default:other::rwx";
            fileSystem.setAcl(directory, AclEntry.parseAclSpec(String.join(",", acl, aclDefault), true));

            final PutHDFS processor = new TestablePutHDFS(fileSystem);
            final TestRunner runner = TestRunners.newTestRunner(processor);
            runner.setProperty(PutHDFS.DIRECTORY, directory.toString());
            if (setUmaskIt) {
                runner.setProperty(PutHDFS.UMASK, "077");
            }
            final Map<String, String> attributes = new HashMap<>();
            attributes.put(CoreAttributes.FILENAME.key(), "empty");
            runner.enqueue(new byte[16], attributes);
            runner.run();
            assertEquals(setUmaskIt ? 0 : 1, runner.getFlowFilesForRelationship(PutHDFS.REL_SUCCESS).size());
            assertEquals(setUmaskIt ? 1 : 0, runner.getFlowFilesForRelationship(PutHDFS.REL_FAILURE).size());
        }
    }

    @Test
    public void testPutFileWithCloseException() throws IOException {
        mockFileSystem = new MockFileSystem();
        mockFileSystem.setFailOnClose(true);
        String dirName = "target/testPutFileCloseException";
        File file = new File(dirName);
        file.mkdirs();
        Path p = new Path(dirName).makeQualified(mockFileSystem.getUri(), mockFileSystem.getWorkingDirectory());

        TestRunner runner = TestRunners.newTestRunner(new TestablePutHDFS(mockFileSystem));
        runner.setProperty(PutHDFS.DIRECTORY, dirName);
        runner.setProperty(PutHDFS.CONFLICT_RESOLUTION, "replace");

        try (FileInputStream fis = new FileInputStream("src/test/resources/testdata/randombytes-1")) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put(CoreAttributes.FILENAME.key(), "randombytes-1");
            runner.enqueue(fis, attributes);
            runner.run();
        }

        List<MockFlowFile> failedFlowFiles = runner
                .getFlowFilesForRelationship(PutHDFS.REL_FAILURE);
        assertFalse(failedFlowFiles.isEmpty());
        assertTrue(failedFlowFiles.get(0).isPenalized());

        mockFileSystem.delete(p, true);
    }

    @Test
    public void testPutFileFromLocalFile(@TempDir java.nio.file.Path tempDir) throws Exception {
        final FileSystem spyFileSystem = Mockito.spy(mockFileSystem);
        final PutHDFS proc = new TestablePutHDFS(spyFileSystem);
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutHDFS.DIRECTORY, TARGET_DIRECTORY);
        runner.setProperty(PutHDFS.CONFLICT_RESOLUTION, PutHDFS.REPLACE_RESOLUTION);
        runner.setProperty(PutHDFS.WRITING_STRATEGY, PutHDFS.SIMPLE_WRITE);

        //Adding StandardFileResourceService controller service
        String attributeName = "file.path";

        String serviceId = FileResourceService.class.getSimpleName();
        FileResourceService service = new StandardFileResourceService();
        byte[] EMPTY_CONTENT = new byte[0];
        runner.addControllerService(serviceId, service);
        runner.setProperty(service, StandardFileResourceService.FILE_PATH, String.format("${%s}", attributeName));
        runner.enableControllerService(service);

        runner.setProperty(ResourceTransferProperties.RESOURCE_TRANSFER_SOURCE, ResourceTransferSource.FILE_RESOURCE_SERVICE.getValue());
        runner.setProperty(ResourceTransferProperties.FILE_RESOURCE_SERVICE, serviceId);
        java.nio.file.Path tempFilePath = tempDir.resolve("PutHDFS_testPutFileFromLocalFile_" + System.currentTimeMillis());
        Files.writeString(tempFilePath, "0123456789");

        Map<String, String> attributes = new HashMap<>();

        attributes.put(CoreAttributes.FILENAME.key(), FILE_NAME);
        attributes.put(attributeName, tempFilePath.toString());
        runner.enqueue(EMPTY_CONTENT, attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutHDFS.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutHDFS.REL_SUCCESS).get(0);
        flowFile.assertContentEquals(EMPTY_CONTENT);

        //assert HDFS File and Directory structures
        assertTrue(spyFileSystem.exists(new Path(TARGET_DIRECTORY + "/" + FILE_NAME)));
        assertEquals(FILE_NAME, flowFile.getAttribute(CoreAttributes.FILENAME.key()));
        assertEquals(TARGET_DIRECTORY, flowFile.getAttribute(PutHDFS.ABSOLUTE_HDFS_PATH_ATTRIBUTE));
        assertEquals("true", flowFile.getAttribute(PutHDFS.TARGET_HDFS_DIR_CREATED_ATTRIBUTE));
        // If it runs with a real HDFS, the protocol will be "hdfs://", but with a local filesystem, just assert the filename.
        assertTrue(flowFile.getAttribute(PutHDFS.HADOOP_FILE_URL_ATTRIBUTE).endsWith(TARGET_DIRECTORY + "/" + FILE_NAME));

        verify(spyFileSystem, Mockito.never()).rename(any(Path.class), any(Path.class));

        //assert Provenance events
        Set<ProvenanceEventType> expectedEventTypes = Collections.singleton(ProvenanceEventType.SEND);
        Set<ProvenanceEventType> actualEventTypes = runner.getProvenanceEvents().stream()
                .map(ProvenanceEventRecord::getEventType)
                .collect(Collectors.toSet());
        assertEquals(expectedEventTypes, actualEventTypes);

    }

    @Test
    public void testPutFileWithCreateException() throws IOException {
        mockFileSystem = new MockFileSystem();
        mockFileSystem.setFailOnCreate(true);
        String dirName = "target/testPutFileCreateException";
        File file = new File(dirName);
        file.mkdirs();
        Path p = new Path(dirName).makeQualified(mockFileSystem.getUri(), mockFileSystem.getWorkingDirectory());

        TestRunner runner = TestRunners.newTestRunner(new TestablePutHDFS(mockFileSystem));
        runner.setProperty(PutHDFS.DIRECTORY, dirName);
        runner.setProperty(PutHDFS.CONFLICT_RESOLUTION, "replace");

        try (FileInputStream fis = new FileInputStream("src/test/resources/testdata/randombytes-1")) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put(CoreAttributes.FILENAME.key(), "randombytes-1");
            runner.enqueue(fis, attributes);
            runner.run();
        }

        List<MockFlowFile> failedFlowFiles = runner
                .getFlowFilesForRelationship(PutHDFS.REL_FAILURE);
        assertFalse(failedFlowFiles.isEmpty());
        assertTrue(failedFlowFiles.get(0).isPenalized());

        mockFileSystem.delete(p, true);
    }

    private static void assertAvroAppendValues(TestRunner runner, FileSystem spyFileSystem, Path targetPath) throws IOException {
        final List<MockFlowFile> failedFlowFiles = runner.getFlowFilesForRelationship(PutHDFS.REL_FAILURE);
        assertTrue(failedFlowFiles.isEmpty());

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutHDFS.REL_SUCCESS);
        assertEquals(1, flowFiles.size());

        final MockFlowFile flowFile = flowFiles.get(0);
        assertTrue(spyFileSystem.exists(targetPath));
        assertEquals(AVRO_FILE_NAME, flowFile.getAttribute(CoreAttributes.FILENAME.key()));
        assertEquals(AVRO_TARGET_DIRECTORY, flowFile.getAttribute(PutHDFS.ABSOLUTE_HDFS_PATH_ATTRIBUTE));
        assertEquals("true", flowFile.getAttribute(PutHDFS.TARGET_HDFS_DIR_CREATED_ATTRIBUTE));

        final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(1, provenanceEvents.size());
        final ProvenanceEventRecord sendEvent = provenanceEvents.get(0);
        assertEquals(ProvenanceEventType.SEND, sendEvent.getEventType());
        // If it runs with a real HDFS, the protocol will be "hdfs://", but with a local filesystem, just assert the filename.
        assertTrue(sendEvent.getTransitUri().endsWith(AVRO_TARGET_DIRECTORY + "/" + AVRO_FILE_NAME));
        assertTrue(flowFile.getAttribute(PutHDFS.HADOOP_FILE_URL_ATTRIBUTE).endsWith(AVRO_TARGET_DIRECTORY + "/" + AVRO_FILE_NAME));
    }

    private static class TestablePutHDFS extends PutHDFS {

        private final FileSystem fileSystem;

        TestablePutHDFS(FileSystem fileSystem) {
            this.fileSystem = fileSystem;
        }

        @Override
        protected FileSystem getFileSystem(Configuration config) {
            fileSystem.setConf(config);
            return fileSystem;
        }

        @Override
        protected FileSystem getFileSystem() {
            return fileSystem;
        }
    }
}
