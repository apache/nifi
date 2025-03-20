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
package org.apache.nifi.pgp.util;

import org.bouncycastle.bcpg.HashAlgorithmTags;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPLiteralDataGenerator;
import org.bouncycastle.openpgp.PGPOnePassSignature;
import org.bouncycastle.openpgp.PGPPrivateKey;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.PGPSignature;
import org.bouncycastle.openpgp.PGPSignatureGenerator;
import org.bouncycastle.openpgp.operator.KeyFingerPrintCalculator;
import org.bouncycastle.openpgp.operator.PGPContentSignerBuilder;
import org.bouncycastle.openpgp.operator.jcajce.JcaKeyFingerprintCalculator;
import org.bouncycastle.openpgp.operator.jcajce.JcaPGPContentSignerBuilder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Date;

/**
 * Pretty Good Privacy Operation Utilities
 */
public class PGPOperationUtils {
    private static final boolean NESTED_SIGNATURE_DISABLED = false;

    private static final int BUFFER_SIZE = 2048;

    private static final long MODIFIED_MILLISECONDS = 86400000;

    private static final Date MODIFIED = new Date(MODIFIED_MILLISECONDS);

    private static final String FILE_NAME = String.class.getSimpleName();

    private static final char FILE_TYPE = PGPLiteralDataGenerator.BINARY;

    /**
     * Get data signed using one-pass signature generator
     *
     * @param contents Byte array contents to be signed
     * @param privateKey Private Key used for signing
     * @return Signed byte array
     * @throws PGPException Thrown when signature initialization failed
     * @throws IOException Thrown when signature generation failed
     */
    public static byte[] getOnePassSignedData(final byte[] contents, final PGPPrivateKey privateKey) throws IOException, PGPException {
        final PGPSignatureGenerator signatureGenerator = getSignatureGenerator(privateKey);

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final PGPOnePassSignature onePassSignature = signatureGenerator.generateOnePassVersion(NESTED_SIGNATURE_DISABLED);
        onePassSignature.encode(outputStream);

        outputStream.write(contents);
        signatureGenerator.update(contents);

        final PGPSignature signature = signatureGenerator.generate();
        signature.encode(outputStream);
        return outputStream.toByteArray();
    }

    /**
     * Get data signed using one-pass signature generator wrapping literal data
     *
     * @param contents Byte array contents to be signed
     * @param privateKey Private Key used for signing
     * @return Signed byte array
     * @throws PGPException Thrown when signature initialization failed
     * @throws IOException Thrown when signature generation failed
     */
    public static byte[] getOnePassSignedLiteralData(final byte[] contents, final PGPPrivateKey privateKey) throws IOException, PGPException {
        final PGPSignatureGenerator signatureGenerator = getSignatureGenerator(privateKey);

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final PGPOnePassSignature onePassSignature = signatureGenerator.generateOnePassVersion(NESTED_SIGNATURE_DISABLED);
        onePassSignature.encode(outputStream);

        final PGPLiteralDataGenerator generator = new PGPLiteralDataGenerator();
        final byte[] buffer = new byte[BUFFER_SIZE];
        try (final OutputStream literalStream = generator.open(outputStream, FILE_TYPE, FILE_NAME, MODIFIED, buffer)) {
            literalStream.write(contents);
            signatureGenerator.update(contents);
        }

        final PGPSignature signature = signatureGenerator.generate();
        signature.encode(outputStream);
        return outputStream.toByteArray();
    }

    private static PGPSignatureGenerator getSignatureGenerator(final PGPPrivateKey privateKey) throws PGPException {
        final PGPContentSignerBuilder contentSignerBuilder = new JcaPGPContentSignerBuilder(privateKey.getPublicKeyPacket().getAlgorithm(), HashAlgorithmTags.SHA512);
        final KeyFingerPrintCalculator keyFingerprintCalculator = new JcaKeyFingerprintCalculator();
        final PGPPublicKey pgpPublicKey = new PGPPublicKey(privateKey.getPublicKeyPacket(), keyFingerprintCalculator);
        final PGPSignatureGenerator signatureGenerator = new PGPSignatureGenerator(contentSignerBuilder, pgpPublicKey);
        signatureGenerator.init(PGPSignature.BINARY_DOCUMENT, privateKey);
        return signatureGenerator;
    }
}
