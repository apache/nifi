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

import org.apache.nifi.processors.pgp.attributes.CompressionAlgorithm;
import org.apache.nifi.processors.pgp.attributes.FileEncoding;
import org.apache.nifi.stream.io.StreamUtils;
import org.bouncycastle.openpgp.PGPCompressedData;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPLiteralData;
import org.bouncycastle.openpgp.PGPObjectFactory;
import org.bouncycastle.openpgp.PGPUtil;
import org.bouncycastle.openpgp.jcajce.JcaPGPObjectFactory;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class EncodingStreamCallbackTest {
    private static final String FILENAME = String.class.getName();

    private static final byte[] DATA = String.class.getSimpleName().getBytes(StandardCharsets.UTF_8);

    @Test
    public void testProcessBinaryCompressionZip() throws IOException, PGPException {
        final CompressionAlgorithm compressionAlgorithm = CompressionAlgorithm.ZIP;
        final EncodingStreamCallback callback = new EncodingStreamCallback(FileEncoding.BINARY, compressionAlgorithm, FILENAME);
        final ByteArrayInputStream inputStream = new ByteArrayInputStream(DATA);
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        callback.process(inputStream, outputStream);

        final InputStream processed = new ByteArrayInputStream(outputStream.toByteArray());
        final InputStream compressedInputStream = assertCompressDataEquals(processed, compressionAlgorithm);
        assertLiteralDataEquals(compressedInputStream);
    }

    @Test
    public void testProcessAsciiCompressionBzip2() throws IOException, PGPException {
        final CompressionAlgorithm compressionAlgorithm = CompressionAlgorithm.BZIP2;
        final EncodingStreamCallback callback = new EncodingStreamCallback(FileEncoding.ASCII, compressionAlgorithm, FILENAME);
        final ByteArrayInputStream inputStream = new ByteArrayInputStream(DATA);
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        callback.process(inputStream, outputStream);

        final InputStream processed = PGPUtil.getDecoderStream(new ByteArrayInputStream(outputStream.toByteArray()));
        final InputStream compressedInputStream = assertCompressDataEquals(processed, compressionAlgorithm);
        assertLiteralDataEquals(compressedInputStream);
    }

    private InputStream assertCompressDataEquals(final InputStream processed, final CompressionAlgorithm compressionAlgorithm) throws IOException, PGPException {
        final PGPObjectFactory objectFactory = new JcaPGPObjectFactory(processed);
        final Object firstObject = objectFactory.nextObject();
        assertNotNull(firstObject);
        assertEquals(PGPCompressedData.class, firstObject.getClass());

        final PGPCompressedData compressedData = (PGPCompressedData) firstObject;
        assertEquals(compressionAlgorithm.getId(), compressedData.getAlgorithm());
        return compressedData.getDataStream();
    }

    private void assertLiteralDataEquals(final InputStream inputStream) throws IOException {
        final PGPObjectFactory compressedObjectFactory = new JcaPGPObjectFactory(inputStream);
        final Object firstCompressedObject = compressedObjectFactory.nextObject();
        assertNotNull(firstCompressedObject);
        assertEquals(PGPLiteralData.class, firstCompressedObject.getClass());

        final PGPLiteralData literalData = (PGPLiteralData) firstCompressedObject;
        assertEquals(FILENAME, literalData.getFileName());
        assertEquals(PGPLiteralData.BINARY, literalData.getFormat());

        final ByteArrayOutputStream literalOutputStream = new ByteArrayOutputStream();
        StreamUtils.copy(literalData.getDataStream(), literalOutputStream);
        assertArrayEquals(DATA, literalOutputStream.toByteArray());
    }
}
