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

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.hadoop.KerberosProperties;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestFetchHDFS {

    private TestRunner runner;
    private TestableFetchHDFS proc;
    private NiFiProperties mockNiFiProperties;
    private KerberosProperties kerberosProperties;

    @Before
    public void setup() {
        mockNiFiProperties = mock(NiFiProperties.class);
        when(mockNiFiProperties.getKerberosConfigurationFile()).thenReturn(null);
        kerberosProperties = new KerberosProperties(null);

        proc = new TestableFetchHDFS(kerberosProperties);
        runner = TestRunners.newTestRunner(proc);
    }

    @Test
    public void testFetchStaticFileThatExists() throws IOException {
        final String file = "src/test/resources/testdata/randombytes-1";
        runner.setProperty(FetchHDFS.FILENAME, file);
        runner.enqueue(new String("trigger flow file"));
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchHDFS.REL_SUCCESS, 1);
        final List<ProvenanceEventRecord> provenanceEvents = runner.getProvenanceEvents();
        assertEquals(1, provenanceEvents.size());
        final ProvenanceEventRecord fetchEvent = provenanceEvents.get(0);
        assertEquals(ProvenanceEventType.FETCH, fetchEvent.getEventType());
        // If it runs with a real HDFS, the protocol will be "hdfs://", but with a local filesystem, just assert the filename.
        assertTrue(fetchEvent.getTransitUri().endsWith(file));
    }

    @Test
    public void testFetchStaticFileThatDoesNotExist() throws IOException {
        final String file = "src/test/resources/testdata/doesnotexist";
        runner.setProperty(FetchHDFS.FILENAME, file);
        runner.enqueue(new String("trigger flow file"));
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchHDFS.REL_FAILURE, 1);
    }

    @Test
    public void testFetchFileThatExistsFromIncomingFlowFile() throws IOException {
        final String file = "src/test/resources/testdata/randombytes-1";
        runner.setProperty(FetchHDFS.FILENAME, "${my.file}");

        final Map<String,String> attributes = new HashMap<>();
        attributes.put("my.file", file);

        runner.enqueue(new String("trigger flow file"), attributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchHDFS.REL_SUCCESS, 1);
    }

    @Test
    public void testFilenameWithValidEL() throws IOException {
        final String file = "src/test/resources/testdata/${literal('randombytes-1')}";
        runner.setProperty(FetchHDFS.FILENAME, file);
        runner.enqueue(new String("trigger flow file"));
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchHDFS.REL_SUCCESS, 1);
    }

    @Test
    public void testFilenameWithInvalidEL() throws IOException {
        final String file = "src/test/resources/testdata/${literal('randombytes-1'):foo()}";
        runner.setProperty(FetchHDFS.FILENAME, file);
        runner.assertNotValid();
    }

    @Test
    public void testFilenameWithUnrecognizedEL() throws IOException {
        final String file = "data_${literal('testing'):substring(0,4)%7D";
        runner.setProperty(FetchHDFS.FILENAME, file);
        runner.enqueue(new String("trigger flow file"));
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchHDFS.REL_FAILURE, 1);
    }

    @Test
    public void testAutomaticDecompression() throws IOException {
        FetchHDFS proc = new TestableFetchHDFS(kerberosProperties);
        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(FetchHDFS.FILENAME, "src/test/resources/testdata/randombytes-1.gz");
        runner.setProperty(FetchHDFS.COMPRESSION_CODEC, "AUTOMATIC");
        runner.enqueue(new String("trigger flow file"));
        runner.run();

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(FetchHDFS.REL_SUCCESS);
        assertEquals(1, flowFiles.size());

        MockFlowFile flowFile = flowFiles.get(0);
        assertTrue(flowFile.getAttribute(CoreAttributes.FILENAME.key()).equals("randombytes-1"));
        InputStream expected = getClass().getResourceAsStream("/testdata/randombytes-1");
        flowFile.assertContentEquals(expected);
    }

    @Test
    public void testInferCompressionCodecDisabled() throws IOException {
        FetchHDFS proc = new TestableFetchHDFS(kerberosProperties);
        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(FetchHDFS.FILENAME, "src/test/resources/testdata/randombytes-1.gz");
        runner.setProperty(FetchHDFS.COMPRESSION_CODEC, "NONE");
        runner.enqueue(new String("trigger flow file"));
        runner.run();

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(FetchHDFS.REL_SUCCESS);
        assertEquals(1, flowFiles.size());

        MockFlowFile flowFile = flowFiles.get(0);
        assertTrue(flowFile.getAttribute(CoreAttributes.FILENAME.key()).equals("randombytes-1.gz"));
        InputStream expected = getClass().getResourceAsStream("/testdata/randombytes-1.gz");
        flowFile.assertContentEquals(expected);
    }

    @Test
    public void testFileExtensionNotACompressionCodec() throws IOException {
        FetchHDFS proc = new TestableFetchHDFS(kerberosProperties);
        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(FetchHDFS.FILENAME, "src/test/resources/testdata/13545423550275052.zip");
        runner.setProperty(FetchHDFS.COMPRESSION_CODEC, "AUTOMATIC");
        runner.enqueue(new String("trigger flow file"));
        runner.run();

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(FetchHDFS.REL_SUCCESS);
        assertEquals(1, flowFiles.size());

        MockFlowFile flowFile = flowFiles.get(0);
        assertTrue(flowFile.getAttribute(CoreAttributes.FILENAME.key()).equals("13545423550275052.zip"));
        InputStream expected = getClass().getResourceAsStream("/testdata/13545423550275052.zip");
        flowFile.assertContentEquals(expected);
    }

    private static class TestableFetchHDFS extends FetchHDFS {
        private final KerberosProperties testKerberosProps;

        public TestableFetchHDFS(KerberosProperties testKerberosProps) {
            this.testKerberosProps = testKerberosProps;
        }

        @Override
        protected KerberosProperties getKerberosProperties(File kerberosConfigFile) {
            return testKerberosProps;
        }


    }
}
