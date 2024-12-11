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
package org.apache.nifi.security.ssl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Objects;

/**
 * Standard implementation of PEM Private Key Reader supporting PKCS1 and PKCS8
 */
class StandardPemPrivateKeyReader implements PemPrivateKeyReader {
    static final String RSA_PRIVATE_KEY_HEADER = "-----BEGIN RSA PRIVATE KEY-----";

    static final String RSA_PRIVATE_KEY_FOOTER = "-----END RSA PRIVATE KEY-----";

    static final String PRIVATE_KEY_HEADER = "-----BEGIN PRIVATE KEY-----";

    static final String PRIVATE_KEY_FOOTER = "-----END PRIVATE KEY-----";

    private static final Charset KEY_CHARACTER_SET = StandardCharsets.US_ASCII;

    private static final PrivateKeyAlgorithmReader privateKeyAlgorithmReader = new PrivateKeyAlgorithmReader();

    private static final Base64.Decoder decoder = Base64.getDecoder();

    /**
     * Read Private from PKCS1 or PKCS8 sources with supported algorithms including ECDSA, Ed25519, and RSA
     *
     * @param inputStream Stream containing PEM header and footer
     * @return Parsed Private Key
     */
    @Override
    public PrivateKey readPrivateKey(final InputStream inputStream) {
        Objects.requireNonNull(inputStream, "Input Stream required");

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, KEY_CHARACTER_SET))) {
            final PrivateKey privateKey;

            final String line = reader.readLine();
            if (PRIVATE_KEY_HEADER.contentEquals(line)) {
                final String privateKeyPayload = readPrivateKeyPayload(reader, PRIVATE_KEY_FOOTER);
                privateKey = readPkcs8PrivateKey(privateKeyPayload);
            } else if (RSA_PRIVATE_KEY_HEADER.contentEquals(line)) {
                final String privateKeyPayload = readPrivateKeyPayload(reader, RSA_PRIVATE_KEY_FOOTER);
                privateKey = readPkcs1PrivateKey(privateKeyPayload);
            } else {
                throw new ReadEntityException("Supported Private Key header not found");
            }

            return privateKey;
        } catch (final IOException e) {
            throw new ReadEntityException("Read Private Key failed", e);
        } catch (final GeneralSecurityException e) {
            throw new ReadEntityException("Parsing Private Key failed", e);
        }
    }

    private PrivateKey readPkcs1PrivateKey(final String privateKeyPayload) throws GeneralSecurityException {
        final byte[] privateKeyDecoded = decoder.decode(privateKeyPayload);
        final PrivateKey encodedPrivateKey = new PKCS1EncodedPrivateKey(privateKeyDecoded);
        final KeyFactory keyFactory = KeyFactory.getInstance(encodedPrivateKey.getAlgorithm());
        return (PrivateKey) keyFactory.translateKey(encodedPrivateKey);
    }

    private PrivateKey readPkcs8PrivateKey(final String privateKeyPayload) throws GeneralSecurityException {
        final byte[] privateKeyDecoded = decoder.decode(privateKeyPayload);
        final ByteBuffer privateKeyBuffer = ByteBuffer.wrap(privateKeyDecoded);
        final String keyAlgorithm = privateKeyAlgorithmReader.getAlgorithm(privateKeyBuffer);
        final KeyFactory keyFactory = KeyFactory.getInstance(keyAlgorithm);
        final PKCS8EncodedKeySpec encodedKeySpec = new PKCS8EncodedKeySpec(privateKeyDecoded);
        return keyFactory.generatePrivate(encodedKeySpec);
    }

    private String readPrivateKeyPayload(final BufferedReader reader, final String footer) throws IOException {
        final StringBuilder builder = new StringBuilder();

        String line = reader.readLine();
        while (line != null) {
            if (footer.contentEquals(line)) {
                break;
            } else {
                builder.append(line);
            }

            line = reader.readLine();
        }

        return builder.toString();
    }

    static class PrivateKeyAlgorithmReader {

        private static final int DER_TAG_MASK = 0x1F;

        private static final int DER_LENGTH_MASK = 0xFF;

        private static final int DER_RESERVED_LENGTH_MASK = 0x7F;

        private static final int DER_LENGTH_BITS = 8;

        private static final int DER_INDEFINITE_LENGTH = 0x80;

        private static final byte SEQUENCE_DER_TAG_TYPE = 0x10;

        private static final byte INTEGER_DER_TAG_TYPE = 0x02;

        enum ObjectIdentifier {
            /** ECDSA Object Identifier 1.2.840.10045.2.1 */
            ECDSA("2a8648ce3d0201", "EC"),

            /** Ed25519 Object Identifier 1.3.101.112 */
            ED25519("2b6570", "Ed25519"),

            /** RSA Object Identifier 1.2.840.113549.1.1.1 */
            RSA("2a864886f70d010101", "RSA");

            private final String encoded;

            private final String algorithm;

            ObjectIdentifier(final String encoded, final String algorithm) {
                this.encoded = encoded;
                this.algorithm = algorithm;
            }
        }

        private String getAlgorithm(final ByteBuffer privateKeyDecoded) throws UnrecoverableKeyException {
            final String objectIdentifierEncoded = readObjectIdentifierEncoded(privateKeyDecoded);

            String keyAlgorithm = null;

            for (final ObjectIdentifier objectIdentifier : ObjectIdentifier.values()) {
                if (objectIdentifier.encoded.contentEquals(objectIdentifierEncoded)) {
                    keyAlgorithm = objectIdentifier.algorithm;
                    break;
                }
            }

            if (keyAlgorithm == null) {
                throw new UnrecoverableKeyException("PKCS8 Algorithm Identifier not supported [%s]".formatted(objectIdentifierEncoded));
            }

            return keyAlgorithm;
        }

        private String readObjectIdentifierEncoded(final ByteBuffer buffer) throws UnrecoverableKeyException {
            final byte derTagEncoded = buffer.get();
            final int derTagType = derTagEncoded & DER_TAG_MASK;

            final String objectIdentifier;

            if (SEQUENCE_DER_TAG_TYPE == derTagType) {
                final int sequenceLength = readDerLength(buffer);
                if (sequenceLength == buffer.remaining()) {
                    final byte versionTagType = buffer.get();
                    if (INTEGER_DER_TAG_TYPE == versionTagType) {
                        // Read Private Key Information Version
                        buffer.get();
                        buffer.get();

                        // Read Sequence Tag Type
                        buffer.get();

                        // Read Algorithm Identifier Tag Type and Length
                        buffer.get();
                        buffer.get();

                        final int algorithmIdentifierLength = readDerLength(buffer);
                        final byte[] algorithmIdentifierEncoded = new byte[algorithmIdentifierLength];
                        buffer.get(algorithmIdentifierEncoded);

                        objectIdentifier = HexFormat.of().formatHex(algorithmIdentifierEncoded);
                    } else {
                        throw new UnrecoverableKeyException("PKCS8 DER Version Tag not found");
                    }
                } else {
                    throw new UnrecoverableKeyException("PKCS8 DER Sequence Length not valid");
                }
            } else {
                throw new UnrecoverableKeyException("PKCS8 DER Sequence Tag not found");
            }

            return objectIdentifier;
        }

        private int readDerLength(final ByteBuffer buffer) {
            final int derLength;

            final byte lengthEncoded = buffer.get();
            final int initialByteLength = lengthEncoded & DER_INDEFINITE_LENGTH;
            if (initialByteLength == 0) {
                derLength = lengthEncoded & DER_RESERVED_LENGTH_MASK;
            } else {
                int lengthBytes = lengthEncoded & DER_RESERVED_LENGTH_MASK;
                int sequenceLength = 0;
                for (int i = 0; i < lengthBytes; i++) {
                    sequenceLength <<= DER_LENGTH_BITS;
                    sequenceLength |= buffer.get() & DER_LENGTH_MASK;
                }
                derLength = sequenceLength;
            }

            return derLength;
        }
    }

    static class PKCS1EncodedPrivateKey implements PrivateKey {
        private static final String PKCS1_FORMAT = "PKCS#1";

        private static final String RSA_ALGORITHM = "RSA";

        private final byte[] encoded;

        private PKCS1EncodedPrivateKey(final byte[] encoded) {
            this.encoded = encoded;
        }

        @Override
        public String getAlgorithm() {
            return RSA_ALGORITHM;
        }

        @Override
        public String getFormat() {
            return PKCS1_FORMAT;
        }

        @Override
        public byte[] getEncoded() {
            return encoded.clone();
        }
    }
}
