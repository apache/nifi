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
package org.apache.nifi.key.service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.AlgorithmParameters;
import java.security.GeneralSecurityException;
import java.util.HexFormat;
import javax.crypto.Cipher;
import javax.crypto.EncryptedPrivateKeyInfo;

/**
 * Encodes PKCS8 EncryptedPrivateKeyInfo for testing without using external libraries
 */
public final class EncryptedPrivateKeyInfoEncoder {
    private static final HexFormat HEX_FORMAT = HexFormat.of();

    private static final byte[] PBES2_OBJECT_IDENTIFIER = HEX_FORMAT.parseHex("06092a864886f70d01050d");
    private static final String PBES2_CIPHER_PREFIX = "PBEWithHmac";

    private static final int SEQUENCE_TAG = 0x30;
    private static final int OCTET_STRING_TAG = 0x04;
    private static final int LONG_FORM_LENGTH_FLAG = 0x80;

    private EncryptedPrivateKeyInfoEncoder() {
    }

    /**
     * Encode encrypted PKCS8 private key information using DER with constructing PBES2 AlgorithmParameters to avoid Java behavior changes
     *
     * @param cipher Cipher initialized for encryption, including algorithm parameters
     * @param encrypted Encrypted private key
     * @return DER encoded EncryptedPrivateKeyInfo
     */
    public static byte[] encode(final Cipher cipher, final byte[] encrypted) throws GeneralSecurityException, IOException {
        final AlgorithmParameters cipherParameters = cipher.getParameters();
        if (cipherParameters.getAlgorithm().startsWith(PBES2_CIPHER_PREFIX)) {
            final byte[] parameters = cipherParameters.getEncoded();
            final byte[] algorithmIdentifier = getDerEncoded(SEQUENCE_TAG, PBES2_OBJECT_IDENTIFIER, parameters);
            final byte[] encryptedOctetString = getDerEncoded(OCTET_STRING_TAG, encrypted);
            return getDerEncoded(SEQUENCE_TAG, algorithmIdentifier, encryptedOctetString);
        }

        return new EncryptedPrivateKeyInfo(cipherParameters, encrypted).getEncoded();
    }

    private static byte[] getDerEncoded(final int tag, final byte[]... contents) {
        final ByteArrayOutputStream contentsStream = new ByteArrayOutputStream();
        for (final byte[] content : contents) {
            contentsStream.writeBytes(content);
        }

        final ByteArrayOutputStream encodedStream = new ByteArrayOutputStream();
        encodedStream.write(tag);

        final int length = contentsStream.size();
        if (length < LONG_FORM_LENGTH_FLAG) {
            encodedStream.write(length);
        } else {
            final byte[] signedLength = BigInteger.valueOf(length).toByteArray();
            final int lengthOffset = signedLength[0] == 0 ? 1 : 0;
            final int lengthBytes = signedLength.length - lengthOffset;
            encodedStream.write(LONG_FORM_LENGTH_FLAG | lengthBytes);
            encodedStream.write(signedLength, lengthOffset, lengthBytes);
        }

        encodedStream.writeBytes(contentsStream.toByteArray());
        return encodedStream.toByteArray();
    }
}
