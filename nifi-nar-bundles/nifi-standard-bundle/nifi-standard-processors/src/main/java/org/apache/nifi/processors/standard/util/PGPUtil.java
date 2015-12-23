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
package org.apache.nifi.processors.standard.util;

import org.apache.nifi.processors.standard.EncryptContent;
import org.bouncycastle.bcpg.ArmoredOutputStream;
import org.bouncycastle.openpgp.PGPCompressedData;
import org.bouncycastle.openpgp.PGPCompressedDataGenerator;
import org.bouncycastle.openpgp.PGPEncryptedData;
import org.bouncycastle.openpgp.PGPEncryptedDataGenerator;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPLiteralData;
import org.bouncycastle.openpgp.PGPLiteralDataGenerator;
import org.bouncycastle.openpgp.operator.PGPKeyEncryptionMethodGenerator;
import org.bouncycastle.openpgp.operator.jcajce.JcePGPDataEncryptorBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.SecureRandom;
import java.util.Date;
import java.util.zip.Deflater;

/**
 * This class contains static utility methods to assist with common PGP operations.
 */
public class PGPUtil {
    private static final Logger logger = LoggerFactory.getLogger(PGPUtil.class);

    public static final int BUFFER_SIZE = 65536;
    public static final int BLOCK_SIZE = 4096;

    public static void encrypt(InputStream in, OutputStream out, String algorithm, String provider, int cipher, String filename, PGPKeyEncryptionMethodGenerator encryptionMethodGenerator) throws IOException, PGPException {
        final boolean isArmored = EncryptContent.isPGPArmoredAlgorithm(algorithm);
        OutputStream output = out;
        if (isArmored) {
            output = new ArmoredOutputStream(out);
        }

        // Default value, do not allow null encryption
        if (cipher == PGPEncryptedData.NULL) {
            logger.warn("Null encryption not allowed; defaulting to AES-128");
            cipher = PGPEncryptedData.AES_128;
        }

        try {
            // TODO: Can probably hardcode provider to BC and remove one method parameter
            PGPEncryptedDataGenerator encryptedDataGenerator = new PGPEncryptedDataGenerator(
                    new JcePGPDataEncryptorBuilder(cipher).setWithIntegrityPacket(true).setSecureRandom(new SecureRandom()).setProvider(provider));

            encryptedDataGenerator.addMethod(encryptionMethodGenerator);

            try (OutputStream encryptedOut = encryptedDataGenerator.open(output, new byte[BUFFER_SIZE])) {
                PGPCompressedDataGenerator compressedDataGenerator = new PGPCompressedDataGenerator(PGPCompressedData.ZIP, Deflater.BEST_SPEED);
                try (OutputStream compressedOut = compressedDataGenerator.open(encryptedOut, new byte[BUFFER_SIZE])) {
                    PGPLiteralDataGenerator literalDataGenerator = new PGPLiteralDataGenerator();
                    try (OutputStream literalOut = literalDataGenerator.open(compressedOut, PGPLiteralData.BINARY, filename, new Date(), new byte[BUFFER_SIZE])) {

                        final byte[] buffer = new byte[BLOCK_SIZE];
                        int len;
                        while ((len = in.read(buffer)) >= 0) {
                            literalOut.write(buffer, 0, len);
                        }
                    }
                }
            }
        } finally {
            if (isArmored) {
                output.close();
            }
        }
    }
}
