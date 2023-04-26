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
package org.apache.nifi.processors.compress;

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processors.compress.util.CompressionInfo;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static org.apache.nifi.processors.compress.ModifyCompression.ORIGINAL_FILENAME;
import static org.apache.nifi.processors.compress.ModifyCompression.UPDATE_FILENAME;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestModifyCompression {

    private TestRunner runner;

    @BeforeEach
    public void setRunner() {
        runner = TestRunners.newTestRunner(ModifyCompression.class);
    }

    @Test
    public void testSnappyCompress() throws Exception {
        runner.setProperty(ModifyCompression.OUTPUT_COMPRESSION, CompressionInfo.COMPRESSION_FORMAT_SNAPPY.getValue());
        runner.setProperty(ModifyCompression.OUTPUT_FILENAME_STRATEGY, UPDATE_FILENAME.getValue());

        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), CompressionInfo.COMPRESSION_FORMAT_SNAPPY.getMimeTypes()[0]);
        flowFile.assertAttributeEquals("filename", "SampleFile.txt.snappy");
    }

    @Test
    public void testSnappyDecompress() throws Exception {
        runner.setProperty(ModifyCompression.INPUT_COMPRESSION, CompressionInfo.COMPRESSION_FORMAT_SNAPPY.getValue());
        runner.setProperty(ModifyCompression.OUTPUT_FILENAME_STRATEGY, UPDATE_FILENAME.getValue());

        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile.txt.snappy"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).get(0);
        flowFile.assertContentEquals(Paths.get("src/test/resources/CompressedData/SampleFile.txt"));
        flowFile.assertAttributeEquals("filename", "SampleFile.txt");
    }

    @Test
    public void testSnappyHadoopCompress() throws Exception {
        runner.setProperty(ModifyCompression.OUTPUT_COMPRESSION, CompressionInfo.COMPRESSION_FORMAT_SNAPPY_HADOOP.getValue());
        runner.setProperty(ModifyCompression.OUTPUT_FILENAME_STRATEGY, UPDATE_FILENAME.getValue());

        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), CompressionInfo.COMPRESSION_FORMAT_SNAPPY_HADOOP.getMimeTypes()[0]);
        flowFile.assertAttributeEquals("filename", "SampleFile.txt.snappy");
    }

    @Test
    public void testSnappyHadoopDecompress() {
        runner.setProperty(ModifyCompression.INPUT_COMPRESSION, CompressionInfo.COMPRESSION_FORMAT_SNAPPY_HADOOP.getValue());
        runner.setProperty(ModifyCompression.OUTPUT_FILENAME_STRATEGY, UPDATE_FILENAME.getValue());

        runner.assertNotValid();
    }

    @Test
    public void testSnappyFramedCompress() throws Exception {
        runner.setProperty(ModifyCompression.OUTPUT_COMPRESSION, CompressionInfo.COMPRESSION_FORMAT_SNAPPY_FRAMED.getValue());
        runner.setProperty(ModifyCompression.OUTPUT_FILENAME_STRATEGY, UPDATE_FILENAME.getValue());

        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), CompressionInfo.COMPRESSION_FORMAT_SNAPPY_FRAMED.getMimeTypes()[0]);
        flowFile.assertAttributeEquals("filename", "SampleFile.txt.sz");
    }

    @Test
    public void testSnappyFramedDecompress() throws Exception {
        runner.setProperty(ModifyCompression.INPUT_COMPRESSION, CompressionInfo.COMPRESSION_FORMAT_SNAPPY_FRAMED.getValue());
        runner.setProperty(ModifyCompression.OUTPUT_FILENAME_STRATEGY, UPDATE_FILENAME.getValue());

        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile.txt.sz"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).get(0);
        flowFile.assertContentEquals(Paths.get("src/test/resources/CompressedData/SampleFile.txt"));
        flowFile.assertAttributeEquals("filename", "SampleFile.txt");
    }

    @Test
    public void testBzip2DecompressConcatenated() throws Exception {
        runner.setProperty(ModifyCompression.INPUT_COMPRESSION, CompressionInfo.COMPRESSION_FORMAT_BZIP2.getValue());
        runner.setProperty(ModifyCompression.OUTPUT_FILENAME_STRATEGY, ORIGINAL_FILENAME.getValue());

        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFileConcat.txt.bz2"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).get(0);
        flowFile.assertContentEquals(Paths.get("src/test/resources/CompressedData/SampleFileConcat.txt"));
        flowFile.assertAttributeEquals("filename", "SampleFileConcat.txt.bz2"); // not updating filename
    }

    @Test
    public void testBzip2DecompressLz4FramedCompress() throws Exception {
        runner.setProperty(ModifyCompression.INPUT_COMPRESSION, CompressionInfo.COMPRESSION_FORMAT_BZIP2.getValue());
        runner.setProperty(ModifyCompression.OUTPUT_COMPRESSION, CompressionInfo.COMPRESSION_FORMAT_LZ4_FRAMED.getValue());
        runner.setProperty(ModifyCompression.OUTPUT_FILENAME_STRATEGY, UPDATE_FILENAME.getValue());

        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile.txt.bz2"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("filename", "SampleFile.txt.lz4");

        runner.clearTransferState();
        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile1.txt.bz2"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("filename", "SampleFile1.txt.lz4");
    }

    @Test
    public void testProperMimeTypeFromBzip2() throws Exception {
        runner.setProperty(ModifyCompression.OUTPUT_COMPRESSION, CompressionInfo.COMPRESSION_FORMAT_BZIP2.getValue());
        runner.setProperty(ModifyCompression.OUTPUT_FILENAME_STRATEGY, ORIGINAL_FILENAME.getValue());

        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("mime.type", "application/x-bzip2");
    }

    @Test
    public void testBzip2DecompressWithBothMimeTypes() throws Exception {
        runner.setProperty(ModifyCompression.INPUT_COMPRESSION, CompressionInfo.COMPRESSION_FORMAT_ATTRIBUTE.getValue());
        runner.setProperty(ModifyCompression.OUTPUT_FILENAME_STRATEGY, UPDATE_FILENAME.getValue());

        // ensure that we can decompress with a mime type of application/x-bzip2
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("mime.type", CompressionInfo.COMPRESSION_FORMAT_BZIP2.getMimeTypes()[0]);
        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile.txt.bz2"), attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).get(0);
        flowFile.assertContentEquals(Paths.get("src/test/resources/CompressedData/SampleFile.txt"));
        flowFile.assertAttributeEquals("filename", "SampleFile.txt");

        // ensure that we can decompress with a mime type of application/bzip2. The appropriate mime type is
        // application/x-bzip2, but we used to use application/bzip2. We want to ensure that we are still
        // backward compatible.
        runner.clearTransferState();
        attributes.put("mime.type", CompressionInfo.COMPRESSION_FORMAT_BZIP2.getMimeTypes()[0]);
        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile1.txt.bz2"), attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).get(0);
        flowFile.assertContentEquals(Paths.get("src/test/resources/CompressedData/SampleFile.txt"));
        flowFile.assertAttributeEquals("filename", "SampleFile1.txt");
    }


    @Test
    public void testGzipDecompress() throws Exception {
        runner.setProperty(ModifyCompression.INPUT_COMPRESSION, CompressionInfo.COMPRESSION_FORMAT_GZIP.getValue());
        assertTrue(runner.setProperty(ModifyCompression.OUTPUT_FILENAME_STRATEGY, UPDATE_FILENAME.getValue()).isValid());

        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile.txt.gz"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).get(0);
        flowFile.assertContentEquals(Paths.get("src/test/resources/CompressedData/SampleFile.txt"));
        flowFile.assertAttributeEquals("filename", "SampleFile.txt");

        runner.clearTransferState();
        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile1.txt.gz"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).get(0);
        flowFile.assertContentEquals(Paths.get("src/test/resources/CompressedData/SampleFile.txt"));
        flowFile.assertAttributeEquals("filename", "SampleFile1.txt");

        runner.clearTransferState();
        runner.setProperty(ModifyCompression.INPUT_COMPRESSION, CompressionInfo.COMPRESSION_FORMAT_ATTRIBUTE.getValue());
        Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), CompressionInfo.COMPRESSION_FORMAT_GZIP.getMimeTypes()[0]);
        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile.txt.gz"), attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).get(0);
        flowFile.assertContentEquals(Paths.get("src/test/resources/CompressedData/SampleFile.txt"));
        flowFile.assertAttributeEquals("filename", "SampleFile.txt");
    }


    @Test
    public void testDeflateDecompress() throws Exception {
        runner.setProperty(ModifyCompression.INPUT_COMPRESSION, CompressionInfo.COMPRESSION_FORMAT_DEFLATE.getValue());
        assertTrue(runner.setProperty(ModifyCompression.OUTPUT_FILENAME_STRATEGY, UPDATE_FILENAME.getValue()).isValid());

        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile.txt.zlib"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).get(0);
        System.err.println(new String(flowFile.toByteArray()));
        flowFile.assertContentEquals(Paths.get("src/test/resources/CompressedData/SampleFile.txt"));
        flowFile.assertAttributeEquals("filename", "SampleFile.txt");
    }


    @Test
    public void testDeflateCompress() throws Exception {
        runner.setProperty(ModifyCompression.COMPRESSION_LEVEL, "6");
        runner.setProperty(ModifyCompression.OUTPUT_COMPRESSION, CompressionInfo.COMPRESSION_FORMAT_DEFLATE.getValue());
        assertTrue(runner.setProperty(ModifyCompression.OUTPUT_FILENAME_STRATEGY, UPDATE_FILENAME.getValue()).isValid());

        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).get(0);
        flowFile.assertContentEquals(Paths.get("src/test/resources/CompressedData/SampleFile.txt.zlib"));
        flowFile.assertAttributeEquals("filename", "SampleFile.txt.zlib");
    }

    @Test
    public void testFilenameUpdatedOnCompress() throws IOException {
        runner.setProperty(ModifyCompression.OUTPUT_COMPRESSION, CompressionInfo.COMPRESSION_FORMAT_GZIP.getValue());
        assertTrue(runner.setProperty(ModifyCompression.OUTPUT_FILENAME_STRATEGY, UPDATE_FILENAME.getValue()).isValid());

        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("filename", "SampleFile.txt.gz");

    }

    @Test
    public void testDecompressFailure() throws IOException {
        runner.setProperty(ModifyCompression.INPUT_COMPRESSION, CompressionInfo.COMPRESSION_FORMAT_GZIP.getValue());

        byte[] data = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        runner.enqueue(data);

        assertTrue(runner.setProperty(ModifyCompression.OUTPUT_FILENAME_STRATEGY, UPDATE_FILENAME.getValue()).isValid());
        runner.run();
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_FAILURE, 1);

        runner.getFlowFilesForRelationship(ModifyCompression.REL_FAILURE).get(0).assertContentEquals(data);
    }

    @Test
    public void testLz4FramedCompress() throws Exception {
        runner.setProperty(ModifyCompression.OUTPUT_COMPRESSION, CompressionInfo.COMPRESSION_FORMAT_LZ4_FRAMED.getValue());
        runner.setProperty(ModifyCompression.OUTPUT_FILENAME_STRATEGY, UPDATE_FILENAME.getValue());

        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), CompressionInfo.COMPRESSION_FORMAT_LZ4_FRAMED.getMimeTypes()[0]);
        flowFile.assertAttributeEquals("filename", "SampleFile.txt.lz4");
    }

    @Test
    public void testLz4FramedDecompress() throws Exception {
        runner.setProperty(ModifyCompression.INPUT_COMPRESSION, CompressionInfo.COMPRESSION_FORMAT_LZ4_FRAMED.getValue());
        runner.setProperty(ModifyCompression.OUTPUT_FILENAME_STRATEGY, UPDATE_FILENAME.getValue());

        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile.txt.lz4"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).get(0);
        flowFile.assertContentEquals(Paths.get("src/test/resources/CompressedData/SampleFile.txt"));
        flowFile.assertAttributeEquals("filename", "SampleFile.txt");
    }

    @Test
    public void testZstdCompress() throws Exception {
        runner.setProperty(ModifyCompression.OUTPUT_COMPRESSION, CompressionInfo.COMPRESSION_FORMAT_ZSTD.getValue());
        runner.setProperty(ModifyCompression.OUTPUT_FILENAME_STRATEGY, UPDATE_FILENAME.getValue());

        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), CompressionInfo.COMPRESSION_FORMAT_ZSTD.getMimeTypes()[0]);
        flowFile.assertAttributeEquals("filename", "SampleFile.txt.zst");
    }

    @Test
    public void testZstdDecompress() throws Exception {
        runner.setProperty(ModifyCompression.INPUT_COMPRESSION, CompressionInfo.COMPRESSION_FORMAT_ZSTD.getValue());
        runner.setProperty(ModifyCompression.OUTPUT_FILENAME_STRATEGY, UPDATE_FILENAME.getValue());
        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile.txt.zst"));
        runner.run();
        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).get(0);
        flowFile.assertContentEquals(Paths.get("src/test/resources/CompressedData/SampleFile.txt"));
        flowFile.assertAttributeEquals("filename", "SampleFile.txt");
    }

    @Test
    public void testBrotliCompress() throws Exception {
        runner.setProperty(ModifyCompression.OUTPUT_COMPRESSION, CompressionInfo.COMPRESSION_FORMAT_BROTLI.getValue());
        runner.setProperty(ModifyCompression.OUTPUT_FILENAME_STRATEGY, UPDATE_FILENAME.getValue());

        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/x-brotli");
        flowFile.assertAttributeEquals("filename", "SampleFile.txt.br");
    }

    @Test
    public void testBrotliDecompress() throws Exception {
        runner.setProperty(ModifyCompression.INPUT_COMPRESSION, CompressionInfo.COMPRESSION_FORMAT_BROTLI.getValue());
        runner.setProperty(ModifyCompression.OUTPUT_FILENAME_STRATEGY, UPDATE_FILENAME.getValue());
        runner.enqueue(Paths.get("src/test/resources/CompressedData/SampleFile.txt.br"));
        runner.run();
        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).get(0);
        flowFile.assertContentEquals(Paths.get("src/test/resources/CompressedData/SampleFile.txt"));
        flowFile.assertAttributeEquals("filename", "SampleFile.txt");
    }
}