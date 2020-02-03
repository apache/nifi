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
package org.apache.nifi.processors.standard.pgp;

import org.apache.nifi.processor.exception.ProcessException;
import org.bouncycastle.bcpg.ArmoredOutputStream;
import org.bouncycastle.openpgp.PGPCompressedData;
import org.bouncycastle.openpgp.PGPCompressedDataGenerator;
import org.bouncycastle.openpgp.PGPEncryptedDataGenerator;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPLiteralData;
import org.bouncycastle.openpgp.PGPLiteralDataGenerator;
import org.bouncycastle.util.io.Streams;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.zip.Deflater;

/**
 * This class encapsulates an encrypt operation over a pair of input and output streams.
 *
 */
class EncryptStreamCallback implements ExtendedStreamCallback {
    private static final PGPLiteralDataGenerator literalGenerator = new PGPLiteralDataGenerator();
    private static final PGPCompressedDataGenerator compressedGenerator = new PGPCompressedDataGenerator(PGPCompressedData.ZIP, Deflater.BEST_SPEED);
    private static final int BUFFER_SIZE = org.apache.nifi.processors.standard.util.PGPUtil.BUFFER_SIZE;
    private final EncryptStreamSession session;

    public EncryptStreamCallback(EncryptStreamSession session) {
        this.session = session;
    }

    /**
     * Provides a managed output stream for use. The input stream is
     * automatically opened and closed though it is ok to close the stream
     * manually - and quite important if any streams wrapping these streams open
     * resources which should be cleared.
     *
     * @param in  the stream to read bytes from
     * @param out the stream to write bytes to
     * @throws IOException if issues occur reading or writing the underlying streams
     */
    @Override
    public void process(InputStream in, OutputStream out) throws IOException {
        try {
            encrypt(in, out, session);
        } catch (PGPException e) {
            session.getLogger().error("e 0a:" + e);
            throw new ProcessException(e);
        }
    }

    /**
     * Use the specified key and algorithm to encrypt the input stream, writing to the output stream.
     *
     * @param input the input stream to encrypt
     * @param output the output stream to receive encrypted data
     * @throws IOException when input cannot be read, output cannot be written, etc.
     */
    static void encrypt(InputStream input, OutputStream output, EncryptStreamSession session) throws IOException, PGPException {
        PGPEncryptedDataGenerator generator = session.getDataGenerator();
        boolean armor = session.getArmor();
        OutputStream out = armor ? new ArmoredOutputStream(output) : output;

        try (OutputStream encryptedOutput = generator.open(out, new byte[BUFFER_SIZE])) {
            try (OutputStream compressedOutput = compressedGenerator.open(encryptedOutput, new byte[BUFFER_SIZE])) {
                try (OutputStream literalOutput = literalGenerator.open(compressedOutput, PGPLiteralData.BINARY, "", new Date(), new byte[BUFFER_SIZE])) {
                    Streams.pipeAll(input, literalOutput);
                }
            }
        } finally {
            if (armor)
                out.close();
        }
    }
}
