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

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.compress.BlockDecompressorStream;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.lz4.Lz4Decompressor;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.io.*;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;

public class TestLz4HadoopCompressor
{
    private final int BUFFER_SIZE = Integer.parseInt(Lz4HadoopCompressor.BUFFER_SIZE.getDefaultValue());
    private final int BYTE_SIZE = BUFFER_SIZE / 3; // The number of raw bytes to compress at a time

    private static final Random rnd = new Random(12345l);

    private int decompress(InputStream is, OutputStream os) throws IOException {
        int bytesTotal = 0;

        byte[] decompressed = new byte[BUFFER_SIZE];
        int decompressedBytes;
        // Connect the decompressor stream to the input stream of compressed data
        CompressionInputStream inflateFilter = new BlockDecompressorStream(is, new Lz4Decompressor(BUFFER_SIZE), BUFFER_SIZE);

        // Read and decompress the data in the input stream

        while ((decompressedBytes = inflateFilter.read(decompressed, 0, BUFFER_SIZE)) != -1) {
            os.write(decompressed, 0, decompressedBytes);
            os.flush();
            bytesTotal += decompressedBytes;
        }

        return bytesTotal;
    }

    private static byte[] generate(int size) {
        byte[] array = new byte[size];
        for (int i = 0; i < size; i++)
            array[i] = (byte)rnd.nextInt(16);
        return array;
    }

    /**
     * Test the Lz4Hadoop compressor by compressing a small byte array by executing a flow to generate a
     * compressed representation of the data.
     *
     * Then, use the Hadoop Lz4 Decompressor to ingest this generated data and
     * confirm that it's valid
     *
     * @throws IOException
     */
    @Test
    public void testSimple() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new Lz4HadoopCompressor());

        byte[] rawData = generate(BYTE_SIZE);

        runner.enqueue(rawData);
        runner.run();

        runner.assertAllFlowFilesTransferred(Lz4HadoopCompressor.REL_SUCCESS, 1);
        final MockFlowFile output = runner.getFlowFilesForRelationship(Lz4HadoopCompressor.REL_SUCCESS).get(0);

        BufferedInputStream is = new BufferedInputStream(new ByteArrayInputStream(output.toByteArray()));

        BufferedOutputStream os = new BufferedOutputStream(new ByteArrayOutputStream(BUFFER_SIZE));

        int decompressedBytes = decompress(is, os);

        byte[] decompressed = new byte[decompressedBytes];
        os.write(decompressed, 0, decompressedBytes);

        assertArrayEquals("original array not equals compress/decompressed array", decompressed,
                rawData);
    }

    /**
     * Test the Lz4Hadoop compressor by compressing a small byte array by executing a flow to generate a
     * compressed representation of the data.
     *
     * Then, use the Hadoop Lz4 Decompressor to ingest this generated data and confirm that it's valid. This second
     * test confirms that datasets of size greater than the buffer_size available for use by the Lz4HadoopCompressor
     * are still compressed correctly.
     *
     * @throws IOException
     */
    @Test
    public void testMultiBuffer() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new Lz4HadoopCompressor());

        byte[] rawData = generate(BUFFER_SIZE * 50);

        runner.enqueue(rawData);
        runner.run();

        runner.assertAllFlowFilesTransferred(Lz4HadoopCompressor.REL_SUCCESS, 1);
        final MockFlowFile output = runner.getFlowFilesForRelationship(Lz4HadoopCompressor.REL_SUCCESS).get(0);

        BufferedInputStream is = new BufferedInputStream(new ByteArrayInputStream(output.toByteArray()));

        BufferedOutputStream os = new BufferedOutputStream(new ByteArrayOutputStream(BUFFER_SIZE));

        int decompressedBytes = decompress(is, os);

        byte[] decompressed = new byte[decompressedBytes];
        os.write(decompressed, 0, decompressedBytes);

        assertArrayEquals("original array not equals compress/decompressed array", decompressed,
                rawData);
    }
}
