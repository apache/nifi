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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class TestCompressContent {

    @Test
    public void testBzip2DecompressConcatenated() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(CompressContent.class);
        runner.setProperty(CompressContent.MODE, "decompress");
        runner.setProperty(CompressContent.COMPRESSION_FORMAT, "bzip2");
        runner.setProperty(CompressContent.UPDATE_FILENAME, "false");

        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFileConcat.txt.bz2"));
        runner.run();

        runner.assertAllFlowFilesTransferred(CompressContent.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(CompressContent.REL_SUCCESS).get(0);
        flowFile.assertContentEquals(Paths.get("src/test/resources/CompressedData/SampleFileConcat.txt"));
        flowFile.assertAttributeEquals("filename", "SampleFileConcat.txt.bz2"); // not updating filename
    }

    @Test
    public void testBzip2Decompress() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(CompressContent.class);
        runner.setProperty(CompressContent.MODE, "decompress");
        runner.setProperty(CompressContent.COMPRESSION_FORMAT, "bzip2");
        runner.setProperty(CompressContent.UPDATE_FILENAME, "true");

        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile.txt.bz2"));
        runner.run();

        runner.assertAllFlowFilesTransferred(CompressContent.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(CompressContent.REL_SUCCESS).get(0);
        flowFile.assertContentEquals(Paths.get("src/test/resources/CompressedData/SampleFile.txt"));
        flowFile.assertAttributeEquals("filename", "SampleFile.txt");

        runner.clearTransferState();
        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile1.txt.bz2"));
        runner.run();

        runner.assertAllFlowFilesTransferred(CompressContent.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(CompressContent.REL_SUCCESS).get(0);
        flowFile.assertContentEquals(Paths.get("src/test/resources/CompressedData/SampleFile.txt"));
        flowFile.assertAttributeEquals("filename", "SampleFile1.txt");
    }

    @Test
    public void testGzipDecompress() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(CompressContent.class);
        runner.setProperty(CompressContent.MODE, "decompress");
        runner.setProperty(CompressContent.COMPRESSION_FORMAT, "gzip");
        assertTrue(runner.setProperty(CompressContent.UPDATE_FILENAME, "true").isValid());

        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile.txt.gz"));
        runner.run();

        runner.assertAllFlowFilesTransferred(CompressContent.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(CompressContent.REL_SUCCESS).get(0);
        flowFile.assertContentEquals(Paths.get("src/test/resources/CompressedData/SampleFile.txt"));
        flowFile.assertAttributeEquals("filename", "SampleFile.txt");

        runner.clearTransferState();
        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile1.txt.gz"));
        runner.run();

        runner.assertAllFlowFilesTransferred(CompressContent.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(CompressContent.REL_SUCCESS).get(0);
        flowFile.assertContentEquals(Paths.get("src/test/resources/CompressedData/SampleFile.txt"));
        flowFile.assertAttributeEquals("filename", "SampleFile1.txt");
    }

    @Test
    public void testFilenameUpdatedOnCompress() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(CompressContent.class);
        runner.setProperty(CompressContent.MODE, "compress");
        runner.setProperty(CompressContent.COMPRESSION_FORMAT, "gzip");
        assertTrue(runner.setProperty(CompressContent.UPDATE_FILENAME, "true").isValid());

        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(CompressContent.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(CompressContent.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("filename", "SampleFile.txt.gz");

    }

    @Test
    public void testDecompressFailure() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(CompressContent.class);
        runner.setProperty(CompressContent.MODE, "decompress");
        runner.setProperty(CompressContent.COMPRESSION_FORMAT, "gzip");

        byte[] data = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        runner.enqueue(data);

        assertTrue(runner.setProperty(CompressContent.UPDATE_FILENAME, "true").isValid());
        runner.run();
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(CompressContent.REL_FAILURE, 1);

        runner.getFlowFilesForRelationship(CompressContent.REL_FAILURE).get(0).assertContentEquals(data);
    }
}
