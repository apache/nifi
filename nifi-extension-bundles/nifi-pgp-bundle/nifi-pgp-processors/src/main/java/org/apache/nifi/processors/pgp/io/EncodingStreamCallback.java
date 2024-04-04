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
package org.apache.nifi.processors.pgp.io;

import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processors.pgp.attributes.CompressionAlgorithm;
import org.apache.nifi.processors.pgp.attributes.FileEncoding;
import org.apache.nifi.processors.pgp.exception.PGPProcessException;
import org.apache.nifi.stream.io.StreamUtils;
import org.bouncycastle.bcpg.ArmoredOutputStream;
import org.bouncycastle.openpgp.PGPCompressedDataGenerator;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPLiteralData;
import org.bouncycastle.openpgp.PGPLiteralDataGenerator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.Objects;

/**
 * Encoding Stream Callback handles writing PGP messages using configured properties
 */
public class EncodingStreamCallback implements StreamCallback {
    protected static final int OUTPUT_BUFFER_SIZE = 8192;

    private final FileEncoding fileEncoding;

    private final CompressionAlgorithm compressionAlgorithm;

    private final String filename;

    public EncodingStreamCallback(final FileEncoding fileEncoding, final CompressionAlgorithm compressionAlgorithm, final String filename) {
        this.fileEncoding = Objects.requireNonNull(fileEncoding, "File Encoding required");
        this.compressionAlgorithm = Objects.requireNonNull(compressionAlgorithm, "Compression Algorithm required");
        this.filename = Objects.requireNonNull(filename, "Filename required");
    }

    /**
     * Process Input Stream and write encoded contents to Output Stream
     *
     * @param inputStream  Input Stream
     * @param outputStream Output Stream for encrypted contents
     * @throws IOException Thrown when unable to read or write streams
     */
    @Override
    public void process(final InputStream inputStream, final OutputStream outputStream) throws IOException {
        try (final OutputStream encodingOutputStream = getEncodingOutputStream(outputStream)) {
            processEncoding(inputStream, encodingOutputStream);
        } catch (final PGPException e) {
            throw new PGPProcessException("PGP Stream Processing Failed", e);
        }
    }

    /**
     * Create Output Buffer byte array with size of 8192
     *
     * @return New empty array of 8192 bytes
     */
    protected byte[] createOutputBuffer() {
        return new byte[OUTPUT_BUFFER_SIZE];
    }

    /**
     * Process Encoding passing Input Stream through Compression Output Stream
     *
     * @param inputStream          Input Stream
     * @param encodingOutputStream Output Stream configured according to File Encoding
     * @throws IOException  Thrown when unable to read or write streams
     * @throws PGPException Thrown when unable to process compression
     */
    protected void processEncoding(final InputStream inputStream, final OutputStream encodingOutputStream) throws IOException, PGPException {
        final PGPCompressedDataGenerator compressedDataGenerator = new PGPCompressedDataGenerator(compressionAlgorithm.getId());
        try (final OutputStream compressedOutputStream = compressedDataGenerator.open(encodingOutputStream, createOutputBuffer())) {
            processCompression(inputStream, compressedOutputStream);
        }
        compressedDataGenerator.close();
    }

    /**
     * Process Compression passing Input Stream through Literal Data Output Stream
     *
     * @param inputStream            Input Stream
     * @param compressedOutputStream Output Stream configured according to Compression Algorithm
     * @throws IOException Thrown when unable to read or write streams
     * @throws PGPException Thrown when unable to process streams using PGP operations
     */
    protected void processCompression(final InputStream inputStream, final OutputStream compressedOutputStream) throws IOException, PGPException {
        final PGPLiteralDataGenerator generator = new PGPLiteralDataGenerator();
        try (final OutputStream literalOutputStream = openLiteralOutputStream(generator, compressedOutputStream)) {
            StreamUtils.copy(inputStream, literalOutputStream);
        }
        generator.close();
    }

    /**
     * Open Literal Data Output Stream using binary indicator with configured filename and current date indicating modification
     *
     * @param generator              PGP Literal Data Generator
     * @param compressedOutputStream Output Stream configured according to Compression Algorithm
     * @return Literal Data Output Stream
     * @throws IOException Thrown when unable to open Literal Data Output Stream
     */
    protected OutputStream openLiteralOutputStream(final PGPLiteralDataGenerator generator, final OutputStream compressedOutputStream) throws IOException {
        return generator.open(compressedOutputStream, PGPLiteralData.BINARY, filename, new Date(), createOutputBuffer());
    }

    private OutputStream getEncodingOutputStream(final OutputStream outputStream) {
        return FileEncoding.ASCII.equals(fileEncoding) ? new ArmoredOutputStream(outputStream) : outputStream;
    }
}
