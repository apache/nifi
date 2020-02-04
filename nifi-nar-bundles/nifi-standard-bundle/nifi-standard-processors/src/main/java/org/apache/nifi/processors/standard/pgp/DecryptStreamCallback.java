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
import org.bouncycastle.openpgp.PGPCompressedData;
import org.bouncycastle.openpgp.PGPEncryptedData;
import org.bouncycastle.openpgp.PGPEncryptedDataList;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPLiteralData;
import org.bouncycastle.openpgp.PGPObjectFactory;
import org.bouncycastle.openpgp.PGPPBEEncryptedData;
import org.bouncycastle.openpgp.PGPPrivateKey;
import org.bouncycastle.openpgp.PGPPublicKeyEncryptedData;
import org.bouncycastle.openpgp.PGPUtil;
import org.bouncycastle.openpgp.bc.BcPGPObjectFactory;
import org.bouncycastle.openpgp.operator.bc.BcKeyFingerprintCalculator;
import org.bouncycastle.util.io.Streams;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

/**
 * This class encapsulates a decrypt operation over a pair of input and output streams.
 *
 */
class DecryptStreamCallback implements ExtendedStreamCallback {
    private final DecryptStreamSession options;

    public DecryptStreamCallback(DecryptStreamSession options) {
        this.options = options;
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
            decrypt(in, out, options);
        } catch (final IOException e) {
            throw new ProcessException(e);
        }
    }

    /**
     * Use the specified private key to decrypt the input stream, writing to the output stream.
     *
     * @param input the input stream of cipher text
     * @param output the output stream to receive decrypted data
     * @param options the {@link PGPPrivateKey} to decrypt the input
     * @throws IOException when input cannot be read, output cannot be written, etc.
     */
    static void decrypt(InputStream input, OutputStream output, DecryptStreamSession options) throws IOException {
        try (InputStream decodedInput = PGPUtil.getDecoderStream(input)) {
            PGPObjectFactory inputFactory = new PGPObjectFactory(decodedInput, new BcKeyFingerprintCalculator());
            Object head = inputFactory.nextObject();

            if (!(head instanceof PGPEncryptedDataList)) {
                head = inputFactory.nextObject();
                if (!(head instanceof PGPEncryptedDataList)) {
                    throw new IOException("Input stream does not contain PGP encrypted data.");
                }
            }

            try {
                Iterator objects = ((PGPEncryptedDataList) head).getEncryptedDataObjects();
                PGPEncryptedData data;

                while (objects.hasNext()) {
                    Object obj = objects.next();
                    InputStream clearInput;

                    if (obj instanceof PGPPublicKeyEncryptedData) {
                        data = (PGPEncryptedData) obj;
                    } else if (obj instanceof PGPPBEEncryptedData) {
                        data = (PGPEncryptedData) obj;
                    } else {
                        throw new IOException("Expected encrypted data");
                    }
                    clearInput = options.getInputStream(data);

                    PGPObjectFactory plainFactory = new BcPGPObjectFactory(clearInput);
                    Object plain = plainFactory.nextObject();

                    if (plain instanceof PGPCompressedData) {
                        PGPCompressedData compressed = (PGPCompressedData) plain;
                        plain = (new BcPGPObjectFactory(compressed.getDataStream())).nextObject();
                    }

                    if (plain instanceof PGPLiteralData) {
                        PGPLiteralData literalObject = (PGPLiteralData) plain;
                        try (InputStream literalInput = literalObject.getInputStream()) {
                            Streams.pipeAll(literalInput, output);
                        }
                    } else {
                        throw new PGPException("message is not a simple encrypted file - type unknown.");
                    }

                    if (data.isIntegrityProtected()) {
                        if (!data.verify()) {
                            throw new PGPException("Failed message integrity check");
                        }
                    } else {
                        options.getLogger().warn("Encrypted data packet does not have integrity check.");
                    }
                }
            } catch (final Exception e) {
                throw new IOException(e.getMessage());
            }
        }
    }
}

