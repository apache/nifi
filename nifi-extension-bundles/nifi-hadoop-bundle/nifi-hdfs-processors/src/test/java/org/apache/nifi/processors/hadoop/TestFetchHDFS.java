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

import org.apache.hadoop.fs.FileSystem;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processors.hadoop.util.MockFileSystem;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.processors.hadoop.AbstractHadoopProcessor.HADOOP_FILE_URL_ATTRIBUTE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestFetchHDFS {

    private TestRunner runner;

    @BeforeEach
    public void setup() {
        FetchHDFS proc = new TestableFetchHDFS();
        runner = TestRunners.newTestRunner(proc);
    }

    @Test
    public void testFetchStaticFileThatExists() {
        final String file = "src/test/resources/testdata/randombytes-1";
        final String fileWithMultipliedSeparators = "src/test////resources//testdata/randombytes-1";
        runner.setProperty(FetchHDFS.FILENAME, fileWithMultipliedSeparators);
        runner.enqueue("trigger flow file");
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchHDFS.REL_SUCCESS, 1);
        final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(1, provenanceEvents.size());
        final ProvenanceEventRecord fetchEvent = provenanceEvents.get(0);
        assertEquals(ProvenanceEventType.FETCH, fetchEvent.getEventType());
        // If it runs with a real HDFS, the protocol will be "hdfs://", but with a local filesystem, just assert the filename.
        assertTrue(fetchEvent.getTransitUri().endsWith(file));

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(DeleteHDFS.REL_SUCCESS).get(0);
        assertTrue(flowFile.getAttribute(HADOOP_FILE_URL_ATTRIBUTE).endsWith(file));
    }

    @Test
    public void testFetchStaticFileThatExistsWithAbsolutePath() {
        final File destination = new File("src/test/resources/testdata/randombytes-1");
        final String file = destination.getAbsolutePath();
        final String fileWithMultipliedSeparators = "/" + file;
        runner.setProperty(FetchHDFS.FILENAME, fileWithMultipliedSeparators);
        runner.enqueue("trigger flow file");
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchHDFS.REL_SUCCESS, 1);
        final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(1, provenanceEvents.size());
        final ProvenanceEventRecord fetchEvent = provenanceEvents.get(0);
        assertEquals(ProvenanceEventType.FETCH, fetchEvent.getEventType());
        // As absolute path call results a different format under Windows, the assertion directly looks for duplication.
        assertFalse(fetchEvent.getTransitUri().contains(File.separator + File.separator));
    }

    @Test
    public void testFetchStaticFileThatDoesNotExist() {
        final String file = "src/test/resources/testdata/doesnotexist";
        runner.setProperty(FetchHDFS.FILENAME, file);
        runner.enqueue("trigger flow file");
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchHDFS.REL_FAILURE, 1);
    }

    @Test
    public void testFetchFileThatExistsFromIncomingFlowFile() {
        final String file = "src/test/resources/testdata/randombytes-1";
        runner.setProperty(FetchHDFS.FILENAME, "${my.file}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("my.file", file);

        runner.enqueue("trigger flow file", attributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchHDFS.REL_SUCCESS, 1);
    }

    @Test
    public void testFilenameWithValidEL() {
        final String file = "src/test/resources/testdata/${literal('randombytes-1')}";
        runner.setProperty(FetchHDFS.FILENAME, file);
        runner.enqueue("trigger flow file");
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchHDFS.REL_SUCCESS, 1);
    }

    @Test
    public void testFilenameWithInvalidEL() {
        final String file = "src/test/resources/testdata/${literal('randombytes-1'):foo()}";
        runner.setProperty(FetchHDFS.FILENAME, file);
        runner.assertNotValid();
    }

    @Test
    public void testFilenameWithUnrecognizedEL() {
        final String file = "data_${literal('testing'):substring(0,4)%7D";
        runner.setProperty(FetchHDFS.FILENAME, file);
        runner.enqueue("trigger flow file");
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchHDFS.REL_FAILURE, 1);
    }

    @Test
    public void testAutomaticDecompression() throws IOException {
        FetchHDFS proc = new TestableFetchHDFS();
        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(FetchHDFS.FILENAME, "src/test/resources/testdata/randombytes-1.gz");
        runner.setProperty(FetchHDFS.COMPRESSION_CODEC, "AUTOMATIC");
        runner.enqueue("trigger flow file");
        runner.run();

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(FetchHDFS.REL_SUCCESS);
        assertEquals(1, flowFiles.size());

        MockFlowFile flowFile = flowFiles.get(0);
        assertEquals("randombytes-1", flowFile.getAttribute(CoreAttributes.FILENAME.key()));
        InputStream expected = getClass().getResourceAsStream("/testdata/randombytes-1");
        flowFile.assertContentEquals(expected);
    }

    @Test
    public void testInferCompressionCodecDisabled() throws IOException {
        FetchHDFS proc = new TestableFetchHDFS();
        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(FetchHDFS.FILENAME, "src/test/resources/testdata/randombytes-1.gz");
        runner.setProperty(FetchHDFS.COMPRESSION_CODEC, "NONE");
        runner.enqueue("trigger flow file");
        runner.run();

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(FetchHDFS.REL_SUCCESS);
        assertEquals(1, flowFiles.size());

        MockFlowFile flowFile = flowFiles.get(0);
        assertEquals("randombytes-1.gz", flowFile.getAttribute(CoreAttributes.FILENAME.key()));
        InputStream expected = getClass().getResourceAsStream("/testdata/randombytes-1.gz");
        flowFile.assertContentEquals(expected);
    }

    @Test
    public void testFileExtensionNotACompressionCodec() throws IOException {
        FetchHDFS proc = new TestableFetchHDFS();
        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(FetchHDFS.FILENAME, "src/test/resources/testdata/13545423550275052.zip");
        runner.setProperty(FetchHDFS.COMPRESSION_CODEC, "AUTOMATIC");
        runner.enqueue("trigger flow file");
        runner.run();

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(FetchHDFS.REL_SUCCESS);
        assertEquals(1, flowFiles.size());

        MockFlowFile flowFile = flowFiles.get(0);
        assertEquals("13545423550275052.zip", flowFile.getAttribute(CoreAttributes.FILENAME.key()));
        InputStream expected = getClass().getResourceAsStream("/testdata/13545423550275052.zip");
        flowFile.assertContentEquals(expected);
    }

    @Test
    public void testGSSException() {
        MockFileSystem fileSystem = new MockFileSystem();
        fileSystem.setFailOnOpen(true);
        FetchHDFS proc = new TestableFetchHDFS(fileSystem);
        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(FetchHDFS.FILENAME, "src/test/resources/testdata/randombytes-1.gz");
        runner.setProperty(FetchHDFS.COMPRESSION_CODEC, "NONE");
        runner.enqueue("trigger flow file");
        runner.run();

        runner.assertTransferCount(FetchHDFS.REL_SUCCESS, 0);
        runner.assertTransferCount(FetchHDFS.REL_FAILURE, 0);
        runner.assertTransferCount(FetchHDFS.REL_COMMS_FAILURE, 0);
        // assert that no files were penalized
        runner.assertPenalizeCount(0);
        fileSystem.setFailOnOpen(false);
    }

    @Test
    public void testRuntimeException() {
        MockFileSystem fileSystem = new MockFileSystem();
        fileSystem.setRuntimeFailOnOpen(true);
        FetchHDFS proc = new TestableFetchHDFS(fileSystem);
        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(FetchHDFS.FILENAME, "src/test/resources/testdata/randombytes-1.gz");
        runner.setProperty(FetchHDFS.COMPRESSION_CODEC, "NONE");
        runner.enqueue("trigger flow file");
        runner.run();

        runner.assertTransferCount(FetchHDFS.REL_SUCCESS, 0);
        runner.assertTransferCount(FetchHDFS.REL_FAILURE, 0);
        runner.assertTransferCount(FetchHDFS.REL_COMMS_FAILURE, 1);
        // assert that the file was penalized
        runner.assertPenalizeCount(1);
        fileSystem.setRuntimeFailOnOpen(false);
    }

    private static class TestableFetchHDFS extends FetchHDFS {
        private final FileSystem fileSystem;

        public TestableFetchHDFS() {
            this.fileSystem = null;
        }
        public TestableFetchHDFS(final FileSystem fileSystem) {
            this.fileSystem = fileSystem;
        }

        @Override
        protected FileSystem getFileSystem() {
            return fileSystem == null ? super.getFileSystem() : fileSystem;
        }
    }
}
