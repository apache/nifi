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
import org.apache.nifi.processors.compress.property.CompressionStrategy;
import org.apache.nifi.processors.compress.property.FilenameStrategy;
import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestModifyCompression {

    private TestRunner runner;

    @BeforeEach
    public void setRunner() {
        runner = TestRunners.newTestRunner(ModifyCompression.class);
    }

    @Test
    public void testSnappyCompress() throws Exception {
        runner.setProperty(ModifyCompression.OUTPUT_COMPRESSION_STRATEGY, CompressionStrategy.SNAPPY);
        runner.setProperty(ModifyCompression.OUTPUT_FILENAME_STRATEGY, FilenameStrategy.UPDATED);

        runner.enqueue(getSamplePath("SampleFile.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).getFirst();
        flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), CompressionStrategy.SNAPPY.getMimeTypes()[0]);
        flowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), "SampleFile.txt.snappy");
    }

    @Test
    public void testSnappyDecompress() throws Exception {
        runner.setProperty(ModifyCompression.INPUT_COMPRESSION_STRATEGY, CompressionStrategy.SNAPPY);
        runner.setProperty(ModifyCompression.OUTPUT_FILENAME_STRATEGY, FilenameStrategy.UPDATED);

        runner.enqueue(getSamplePath("SampleFile.txt.snappy"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).getFirst();
        flowFile.assertContentEquals(getSamplePath("SampleFile.txt"));
        flowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), "SampleFile.txt");
    }

    @Test
    public void testSnappyHadoopCompress() throws Exception {
        runner.setProperty(ModifyCompression.OUTPUT_COMPRESSION_STRATEGY, CompressionStrategy.SNAPPY_HADOOP);
        runner.setProperty(ModifyCompression.OUTPUT_FILENAME_STRATEGY, FilenameStrategy.UPDATED);

        runner.enqueue(getSamplePath("SampleFile.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).getFirst();
        flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), CompressionStrategy.SNAPPY_HADOOP.getMimeTypes()[0]);
        flowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), "SampleFile.txt.snappy");
    }

    @Test
    public void testSnappyHadoopDecompress() {
        runner.setProperty(ModifyCompression.INPUT_COMPRESSION_STRATEGY, CompressionStrategy.SNAPPY_HADOOP);
        runner.setProperty(ModifyCompression.OUTPUT_FILENAME_STRATEGY, FilenameStrategy.UPDATED);

        runner.assertNotValid();
    }

    @Test
    public void testSnappyFramedCompress() throws Exception {
        runner.setProperty(ModifyCompression.OUTPUT_COMPRESSION_STRATEGY, CompressionStrategy.SNAPPY_FRAMED);
        runner.setProperty(ModifyCompression.OUTPUT_FILENAME_STRATEGY, FilenameStrategy.UPDATED);

        runner.enqueue(getSamplePath("SampleFile.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).getFirst();
        flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), CompressionStrategy.SNAPPY_FRAMED.getMimeTypes()[0]);
        flowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), "SampleFile.txt.sz");
    }

    @Test
    public void testSnappyFramedDecompress() throws Exception {
        runner.setProperty(ModifyCompression.INPUT_COMPRESSION_STRATEGY, CompressionStrategy.SNAPPY_FRAMED);
        runner.setProperty(ModifyCompression.OUTPUT_FILENAME_STRATEGY, FilenameStrategy.UPDATED);

        runner.enqueue(getSamplePath("SampleFile.txt.sz"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).getFirst();
        flowFile.assertContentEquals(getSamplePath("SampleFile.txt"));
        flowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), "SampleFile.txt");
    }

    @Test
    public void testBzip2DecompressConcatenated() throws Exception {
        runner.setProperty(ModifyCompression.INPUT_COMPRESSION_STRATEGY, CompressionStrategy.BZIP2);
        runner.setProperty(ModifyCompression.OUTPUT_FILENAME_STRATEGY, FilenameStrategy.ORIGINAL);

        runner.enqueue(getSamplePath("SampleFileConcat.txt.bz2"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).getFirst();
        flowFile.assertContentEquals(getSamplePath("SampleFileConcat.txt"));
        flowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), "SampleFileConcat.txt.bz2"); // not updating filename
    }

    @Test
    public void testBzip2DecompressLz4FramedCompress() throws Exception {
        runner.setProperty(ModifyCompression.INPUT_COMPRESSION_STRATEGY, CompressionStrategy.BZIP2);
        runner.setProperty(ModifyCompression.OUTPUT_COMPRESSION_STRATEGY, CompressionStrategy.LZ4_FRAMED);
        runner.setProperty(ModifyCompression.OUTPUT_FILENAME_STRATEGY, FilenameStrategy.UPDATED);

        runner.enqueue(getSamplePath("SampleFile.txt.bz2"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).getFirst();
        flowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), "SampleFile.txt.lz4");

        runner.clearTransferState();
        runner.enqueue(getSamplePath("SampleFile1.txt.bz2"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).getFirst();
        flowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), "SampleFile1.txt.lz4");
    }

    @Test
    public void testProperMimeTypeFromBzip2() throws Exception {
        runner.setProperty(ModifyCompression.OUTPUT_COMPRESSION_STRATEGY, CompressionStrategy.BZIP2);
        runner.setProperty(ModifyCompression.OUTPUT_FILENAME_STRATEGY, FilenameStrategy.ORIGINAL);

        runner.enqueue(getSamplePath("SampleFile.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).getFirst();
        flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/x-bzip2");
    }

    @Test
    public void testBzip2DecompressWithBothMimeTypes() throws Exception {
        runner.setProperty(ModifyCompression.INPUT_COMPRESSION_STRATEGY, CompressionStrategy.MIME_TYPE_ATTRIBUTE);
        runner.setProperty(ModifyCompression.OUTPUT_FILENAME_STRATEGY, FilenameStrategy.UPDATED);

        // ensure that we can decompress with a mime type of application/x-bzip2
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), CompressionStrategy.BZIP2.getMimeTypes()[0]);
        runner.enqueue(getSamplePath("SampleFile.txt.bz2"), attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).getFirst();
        flowFile.assertContentEquals(getSamplePath("SampleFile.txt"));
        flowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), "SampleFile.txt");

        // ensure that we can decompress with a mime type of application/bzip2. The appropriate mime type is
        // application/x-bzip2, but we used to use application/bzip2. We want to ensure that we are still
        // backward compatible.
        runner.clearTransferState();
        attributes.put(CoreAttributes.MIME_TYPE.key(), CompressionStrategy.BZIP2.getMimeTypes()[0]);
        runner.enqueue(getSamplePath("SampleFile1.txt.bz2"), attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).getFirst();
        flowFile.assertContentEquals(getSamplePath("SampleFile.txt"));
        flowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), "SampleFile1.txt");
    }

    @Test
    public void testGzipDecompress() throws Exception {
        runner.setProperty(ModifyCompression.INPUT_COMPRESSION_STRATEGY, CompressionStrategy.GZIP);
        assertTrue(runner.setProperty(ModifyCompression.OUTPUT_FILENAME_STRATEGY, FilenameStrategy.UPDATED).isValid());

        runner.enqueue(getSamplePath("/SampleFile.txt.gz"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).getFirst();
        flowFile.assertContentEquals(getSamplePath("SampleFile.txt"));
        flowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), "SampleFile.txt");

        runner.clearTransferState();
        runner.enqueue(getSamplePath("SampleFile1.txt.gz"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).getFirst();
        flowFile.assertContentEquals(getSamplePath("SampleFile.txt"));
        flowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), "SampleFile1.txt");

        runner.clearTransferState();
        runner.setProperty(ModifyCompression.INPUT_COMPRESSION_STRATEGY, CompressionStrategy.MIME_TYPE_ATTRIBUTE);
        Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), CompressionStrategy.GZIP.getMimeTypes()[0]);
        runner.enqueue(getSamplePath("SampleFile.txt.gz"), attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).getFirst();
        flowFile.assertContentEquals(getSamplePath("SampleFile.txt"));
        flowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), "SampleFile.txt");
    }

    @Test
    public void testDeflateDecompress() throws Exception {
        runner.setProperty(ModifyCompression.INPUT_COMPRESSION_STRATEGY, CompressionStrategy.DEFLATE);
        assertTrue(runner.setProperty(ModifyCompression.OUTPUT_FILENAME_STRATEGY, FilenameStrategy.UPDATED).isValid());

        runner.enqueue(getSamplePath("SampleFile.txt.zlib"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).getFirst();
        flowFile.assertContentEquals(getSamplePath("SampleFile.txt"));
        flowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), "SampleFile.txt");
    }

    @Test
    public void testDeflateCompress() throws Exception {
        runner.setProperty(ModifyCompression.OUTPUT_COMPRESSION_LEVEL, "6");
        runner.setProperty(ModifyCompression.OUTPUT_COMPRESSION_STRATEGY, CompressionStrategy.DEFLATE);
        assertTrue(runner.setProperty(ModifyCompression.OUTPUT_FILENAME_STRATEGY, FilenameStrategy.UPDATED).isValid());

        runner.enqueue(getSamplePath("SampleFile.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).getFirst();
        flowFile.assertContentEquals(getSamplePath("SampleFile.txt.zlib"));
        flowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), "SampleFile.txt.zlib");
    }

    @Test
    public void testFilenameUpdatedOnCompress() throws IOException {
        runner.setProperty(ModifyCompression.OUTPUT_COMPRESSION_STRATEGY, CompressionStrategy.GZIP);
        assertTrue(runner.setProperty(ModifyCompression.OUTPUT_FILENAME_STRATEGY, FilenameStrategy.UPDATED).isValid());

        runner.enqueue(getSamplePath("SampleFile.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).getFirst();
        flowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), "SampleFile.txt.gz");

    }

    @Test
    public void testDecompressFailure() throws IOException {
        runner.setProperty(ModifyCompression.INPUT_COMPRESSION_STRATEGY, CompressionStrategy.GZIP);

        byte[] data = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        runner.enqueue(data);

        assertTrue(runner.setProperty(ModifyCompression.OUTPUT_FILENAME_STRATEGY, FilenameStrategy.UPDATED).isValid());
        runner.run();
        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_FAILURE, 1);

        runner.getFlowFilesForRelationship(ModifyCompression.REL_FAILURE).getFirst().assertContentEquals(data);

        final LogMessage errorMessage = runner.getLogger().getErrorMessages().getFirst();
        assertNotNull(errorMessage);

        final Optional<Object> exceptionFound = Arrays.stream(errorMessage.getArgs()).filter(Exception.class::isInstance).findFirst();
        assertTrue(exceptionFound.isPresent());
    }

    @Test
    public void testLz4FramedCompress() throws Exception {
        runner.setProperty(ModifyCompression.OUTPUT_COMPRESSION_STRATEGY, CompressionStrategy.LZ4_FRAMED);
        runner.setProperty(ModifyCompression.OUTPUT_FILENAME_STRATEGY, FilenameStrategy.UPDATED);

        runner.enqueue(getSamplePath("SampleFile.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).getFirst();
        flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), CompressionStrategy.LZ4_FRAMED.getMimeTypes()[0]);
        flowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), "SampleFile.txt.lz4");
    }

    @Test
    public void testLz4FramedDecompress() throws Exception {
        runner.setProperty(ModifyCompression.INPUT_COMPRESSION_STRATEGY, CompressionStrategy.LZ4_FRAMED);
        runner.setProperty(ModifyCompression.OUTPUT_FILENAME_STRATEGY, FilenameStrategy.UPDATED);

        runner.enqueue(getSamplePath("SampleFile.txt.lz4"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).getFirst();
        flowFile.assertContentEquals(getSamplePath("SampleFile.txt"));
        flowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), "SampleFile.txt");
    }

    @Test
    public void testZstdCompress() throws Exception {
        runner.setProperty(ModifyCompression.OUTPUT_COMPRESSION_STRATEGY, CompressionStrategy.ZSTD);
        runner.setProperty(ModifyCompression.OUTPUT_FILENAME_STRATEGY, FilenameStrategy.UPDATED);

        runner.enqueue(getSamplePath("SampleFile.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).getFirst();
        flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), CompressionStrategy.ZSTD.getMimeTypes()[0]);
        flowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), "SampleFile.txt.zst");
    }

    @Test
    public void testZstdDecompress() throws Exception {
        runner.setProperty(ModifyCompression.INPUT_COMPRESSION_STRATEGY, CompressionStrategy.ZSTD);
        runner.setProperty(ModifyCompression.OUTPUT_FILENAME_STRATEGY, FilenameStrategy.UPDATED);
        runner.enqueue(getSamplePath("SampleFile.txt.zst"));
        runner.run();
        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).getFirst();
        flowFile.assertContentEquals(getSamplePath("SampleFile.txt"));
        flowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), "SampleFile.txt");
    }

    @Test
    public void testBrotliCompress() throws Exception {
        runner.setProperty(ModifyCompression.OUTPUT_COMPRESSION_STRATEGY, CompressionStrategy.BROTLI);
        runner.setProperty(ModifyCompression.OUTPUT_FILENAME_STRATEGY, FilenameStrategy.UPDATED);

        runner.enqueue(getSamplePath("SampleFile.txt"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).getFirst();
        flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/x-brotli");
        flowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), "SampleFile.txt.br");
    }

    @Test
    public void testBrotliDecompress() throws Exception {
        runner.setProperty(ModifyCompression.INPUT_COMPRESSION_STRATEGY, CompressionStrategy.BROTLI);
        runner.setProperty(ModifyCompression.OUTPUT_FILENAME_STRATEGY, FilenameStrategy.UPDATED);
        runner.enqueue(getSamplePath("SampleFile.txt.br"));
        runner.run();
        runner.assertAllFlowFilesTransferred(ModifyCompression.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ModifyCompression.REL_SUCCESS).getFirst();
        flowFile.assertContentEquals(getSamplePath("SampleFile.txt"));
        flowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), "SampleFile.txt");
    }

    private Path getSamplePath(final String relativePath) {
        final String sourcePath = String.format("src/test/resources/CompressedData/%s", relativePath);
        return Paths.get(sourcePath);
    }
}