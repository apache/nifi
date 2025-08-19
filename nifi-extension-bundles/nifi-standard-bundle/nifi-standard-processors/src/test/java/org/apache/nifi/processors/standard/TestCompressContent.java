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

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestCompressContent {

    @Test
    public void testSnappyCompress() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(CompressContent.class);
        runner.setProperty(CompressContent.MODE, CompressContent.MODE_COMPRESS);
        runner.setProperty(CompressContent.COMPRESSION_FORMAT, CompressContent.COMPRESSION_FORMAT_SNAPPY);
        runner.setProperty(CompressContent.UPDATE_FILENAME, "true");

        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(CompressContent.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(CompressContent.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/x-snappy");
        flowFile.assertAttributeEquals("filename", "SampleFile.txt.snappy");
    }

    @Test
    public void testSnappyDecompress() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(CompressContent.class);
        runner.setProperty(CompressContent.MODE, CompressContent.MODE_DECOMPRESS);
        runner.setProperty(CompressContent.COMPRESSION_FORMAT, CompressContent.COMPRESSION_FORMAT_SNAPPY);
        runner.setProperty(CompressContent.UPDATE_FILENAME, "true");

        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile.txt.snappy"));
        runner.run();

        runner.assertAllFlowFilesTransferred(CompressContent.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(CompressContent.REL_SUCCESS).get(0);
        flowFile.assertContentEquals(Paths.get("src/test/resources/CompressedData/SampleFile.txt"));
        flowFile.assertAttributeEquals("filename", "SampleFile.txt");
    }

    @Test
    public void testSnappyHadoopCompress() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(CompressContent.class);
        runner.setProperty(CompressContent.MODE, CompressContent.MODE_COMPRESS);
        runner.setProperty(CompressContent.COMPRESSION_FORMAT, CompressContent.COMPRESSION_FORMAT_SNAPPY_HADOOP);
        runner.setProperty(CompressContent.UPDATE_FILENAME, "true");

        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(CompressContent.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(CompressContent.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/x-snappy-hadoop");
        flowFile.assertAttributeEquals("filename", "SampleFile.txt.snappy");
    }

    @Test
    public void testSnappyHadoopDecompress() {
        final TestRunner runner = TestRunners.newTestRunner(CompressContent.class);
        runner.setProperty(CompressContent.MODE, CompressContent.MODE_DECOMPRESS);
        runner.setProperty(CompressContent.COMPRESSION_FORMAT, CompressContent.COMPRESSION_FORMAT_SNAPPY_HADOOP);
        runner.setProperty(CompressContent.UPDATE_FILENAME, "true");

        runner.assertNotValid();
    }

    @Test
    public void testSnappyFramedCompress() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(CompressContent.class);
        runner.setProperty(CompressContent.MODE, CompressContent.MODE_COMPRESS);
        runner.setProperty(CompressContent.COMPRESSION_FORMAT, CompressContent.COMPRESSION_FORMAT_SNAPPY_FRAMED);
        runner.setProperty(CompressContent.UPDATE_FILENAME, "true");

        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(CompressContent.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(CompressContent.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/x-snappy-framed");
        flowFile.assertAttributeEquals("filename", "SampleFile.txt.sz");
    }

    @Test
    public void testSnappyFramedDecompress() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(CompressContent.class);
        runner.setProperty(CompressContent.MODE, CompressContent.MODE_DECOMPRESS);
        runner.setProperty(CompressContent.COMPRESSION_FORMAT, CompressContent.COMPRESSION_FORMAT_SNAPPY_FRAMED);
        runner.setProperty(CompressContent.UPDATE_FILENAME, "true");

        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile.txt.sz"));
        runner.run();

        runner.assertAllFlowFilesTransferred(CompressContent.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(CompressContent.REL_SUCCESS).get(0);
        flowFile.assertContentEquals(Paths.get("src/test/resources/CompressedData/SampleFile.txt"));
        flowFile.assertAttributeEquals("filename", "SampleFile.txt");
    }

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
    public void testProperMimeTypeFromBzip2() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(CompressContent.class);
        runner.setProperty(CompressContent.MODE, "compress");
        runner.setProperty(CompressContent.COMPRESSION_FORMAT, "bzip2");
        runner.setProperty(CompressContent.UPDATE_FILENAME, "false");

        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(CompressContent.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(CompressContent.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("mime.type", "application/x-bzip2");
    }

    @Test
    public void testBzip2DecompressWithBothMimeTypes() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(CompressContent.class);
        runner.setProperty(CompressContent.MODE, "decompress");
        runner.setProperty(CompressContent.COMPRESSION_FORMAT, CompressContent.COMPRESSION_FORMAT_ATTRIBUTE);
        runner.setProperty(CompressContent.UPDATE_FILENAME, "true");

        // ensure that we can decompress with a mime type of application/x-bzip2
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("mime.type", "application/x-bzip2");
        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile.txt.bz2"), attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(CompressContent.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(CompressContent.REL_SUCCESS).get(0);
        flowFile.assertContentEquals(Paths.get("src/test/resources/CompressedData/SampleFile.txt"));
        flowFile.assertAttributeEquals("filename", "SampleFile.txt");

        // ensure that we can decompress with a mime type of application/bzip2. The appropriate mime type is
        // application/x-bzip2, but we used to use application/bzip2. We want to ensure that we are still
        // backward compatible.
        runner.clearTransferState();
        attributes.put("mime.type", "application/bzip2");
        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile1.txt.bz2"), attributes);
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

        runner.clearTransferState();
        runner.setProperty(CompressContent.COMPRESSION_FORMAT, CompressContent.COMPRESSION_FORMAT_ATTRIBUTE);
        Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), "application/x-gzip");
        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile.txt.gz"), attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(CompressContent.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(CompressContent.REL_SUCCESS).get(0);
        flowFile.assertContentEquals(Paths.get("src/test/resources/CompressedData/SampleFile.txt"));
        flowFile.assertAttributeEquals("filename", "SampleFile.txt");
    }


    @Test
    public void testDeflateDecompress() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(CompressContent.class);
        runner.setProperty(CompressContent.MODE, "decompress");
        runner.setProperty(CompressContent.COMPRESSION_FORMAT, "deflate");
        assertTrue(runner.setProperty(CompressContent.UPDATE_FILENAME, "true").isValid());

        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile.txt.zlib"));
        runner.run();

        runner.assertAllFlowFilesTransferred(CompressContent.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(CompressContent.REL_SUCCESS).get(0);
        flowFile.assertContentEquals(Paths.get("src/test/resources/CompressedData/SampleFile.txt"));
        flowFile.assertAttributeEquals("filename", "SampleFile.txt");
    }


    @Test
    public void testDeflateCompress() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(CompressContent.class);
        runner.setProperty(CompressContent.MODE, "compress");
        runner.setProperty(CompressContent.COMPRESSION_LEVEL, "6");
        runner.setProperty(CompressContent.COMPRESSION_FORMAT, "deflate");
        assertTrue(runner.setProperty(CompressContent.UPDATE_FILENAME, "true").isValid());

        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(CompressContent.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(CompressContent.REL_SUCCESS).get(0);
        flowFile.assertContentEquals(Paths.get("src/test/resources/CompressedData/SampleFile.txt.zlib"));
        flowFile.assertAttributeEquals("filename", "SampleFile.txt.zlib");
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

    @Test
    public void testLz4FramedCompress() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(CompressContent.class);
        runner.setProperty(CompressContent.MODE, CompressContent.MODE_COMPRESS);
        runner.setProperty(CompressContent.COMPRESSION_FORMAT, CompressContent.COMPRESSION_FORMAT_LZ4_FRAMED);
        runner.setProperty(CompressContent.UPDATE_FILENAME, "true");

        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(CompressContent.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(CompressContent.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/x-lz4-framed");
        flowFile.assertAttributeEquals("filename", "SampleFile.txt.lz4");
    }

    @Test
    public void testLz4FramedDecompress() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(CompressContent.class);
        runner.setProperty(CompressContent.MODE, CompressContent.MODE_DECOMPRESS);
        runner.setProperty(CompressContent.COMPRESSION_FORMAT, CompressContent.COMPRESSION_FORMAT_LZ4_FRAMED);
        runner.setProperty(CompressContent.UPDATE_FILENAME, "true");

        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile.txt.lz4"));
        runner.run();

        runner.assertAllFlowFilesTransferred(CompressContent.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(CompressContent.REL_SUCCESS).get(0);
        flowFile.assertContentEquals(Paths.get("src/test/resources/CompressedData/SampleFile.txt"));
        flowFile.assertAttributeEquals("filename", "SampleFile.txt");
    }

    @Test
    public void testZstdCompress() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(CompressContent.class);
        runner.setProperty(CompressContent.MODE, CompressContent.MODE_COMPRESS);
        runner.setProperty(CompressContent.COMPRESSION_FORMAT, CompressContent.COMPRESSION_FORMAT_ZSTD);
        runner.setProperty(CompressContent.UPDATE_FILENAME, "true");

        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(CompressContent.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(CompressContent.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/zstd");
        flowFile.assertAttributeEquals("filename", "SampleFile.txt.zst");
    }

    @Test
    public void testZstdDecompress() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(CompressContent.class);
        runner.setProperty(CompressContent.MODE, CompressContent.MODE_DECOMPRESS);
        runner.setProperty(CompressContent.COMPRESSION_FORMAT, CompressContent.COMPRESSION_FORMAT_ZSTD);
        runner.setProperty(CompressContent.UPDATE_FILENAME, "true");
        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile.txt.zst"));
        runner.run();
        runner.assertAllFlowFilesTransferred(CompressContent.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(CompressContent.REL_SUCCESS).get(0);
        flowFile.assertContentEquals(Paths.get("src/test/resources/CompressedData/SampleFile.txt"));
        flowFile.assertAttributeEquals("filename", "SampleFile.txt");
    }

    @Test
    public void testBrotliCompress() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(CompressContent.class);
        runner.setProperty(CompressContent.MODE, CompressContent.MODE_COMPRESS);
        runner.setProperty(CompressContent.COMPRESSION_FORMAT, CompressContent.COMPRESSION_FORMAT_BROTLI);
        runner.setProperty(CompressContent.UPDATE_FILENAME, "true");

        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(CompressContent.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(CompressContent.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/x-brotli");
        flowFile.assertAttributeEquals("filename", "SampleFile.txt.br");
    }

    @Test
    public void testBrotliDecompress() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(CompressContent.class);
        runner.setProperty(CompressContent.MODE, CompressContent.MODE_DECOMPRESS);
        runner.setProperty(CompressContent.COMPRESSION_FORMAT, CompressContent.COMPRESSION_FORMAT_BROTLI);
        runner.setProperty(CompressContent.UPDATE_FILENAME, "true");
        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile.txt.br"));
        runner.run();
        runner.assertAllFlowFilesTransferred(CompressContent.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(CompressContent.REL_SUCCESS).get(0);
        flowFile.assertContentEquals(Paths.get("src/test/resources/CompressedData/SampleFile.txt"));
        flowFile.assertAttributeEquals("filename", "SampleFile.txt");
    }
}
