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
package org.apache.nifi.security.cert.builder;

import java.io.ByteArrayOutputStream;
import java.math.BigInteger;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Locale;

/**
 * Distinguished Encoding Rules encoder for the X.509 certificates
 */
final class DerEncoder {
    private static final int BOOLEAN_TAG = 0x01;
    private static final int INTEGER_TAG = 0x02;
    private static final int BIT_STRING_TAG = 0x03;
    private static final int OCTET_STRING_TAG = 0x04;
    private static final int NULL_TAG = 0x05;
    private static final int OBJECT_IDENTIFIER_TAG = 0x06;
    private static final int SEQUENCE_TAG = 0x30;
    private static final int UTC_TIME_TAG = 0x17;
    private static final int GENERALIZED_TIME_TAG = 0x18;
    private static final int CONTEXT_SPECIFIC_PRIMITIVE = 0x80;
    private static final int CONTEXT_SPECIFIC_CONSTRUCTED = 0xA0;
    private static final int LONG_FORM_LENGTH = 0x80;
    private static final int SHORT_FORM_LENGTH_LIMIT = 128;
    private static final int LENGTH_BYTE_LIMIT = 4;
    private static final int BASE_128_SHIFT = 7;
    private static final int BASE_128_MASK = 0x7F;
    private static final int CONTINUATION_BIT = 0x80;
    private static final String OID_SEPARATOR = "\\.";
    private static final int OID_FIRST_COMPONENT_MULTIPLIER = 40;
    private static final int BYTE_MASK = 0xFF;
    private static final int BITS_PER_BYTE = 8;
    private static final int AUTHORITY_KEY_IDENTIFIER_TAG = 0;
    private static final String KEY_IDENTIFIER_DIGEST_ALGORITHM = "SHA-1";
    private static final int GENERALIZED_TIME_FIRST_YEAR = 2050;
    private static final DateTimeFormatter UTC_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyMMddHHmmss'Z'", Locale.ROOT).withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter GENERALIZED_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHHmmss'Z'", Locale.ROOT).withZone(ZoneOffset.UTC);
    private static final byte[] BOOLEAN_TRUE = new byte[] {BOOLEAN_TAG, 0x01, (byte) 0xFF};
    private static final byte[] NULL_VALUE = new byte[] {NULL_TAG, 0x00};

    private DerEncoder() {
    }

    static byte[] sequence(final byte[]... encodedValues) {
        return tagged(SEQUENCE_TAG, concatenate(encodedValues));
    }

    static byte[] integer(final BigInteger value) {
        return tagged(INTEGER_TAG, value.toByteArray());
    }

    static byte[] objectIdentifier(final String objectIdentifier) {
        final String[] components = objectIdentifier.split(OID_SEPARATOR);
        final ByteArrayOutputStream contents = new ByteArrayOutputStream();
        final int firstComponent = Integer.parseInt(components[0]);
        final int secondComponent = Integer.parseInt(components[1]);
        final int encodedFirstComponents = firstComponent * OID_FIRST_COMPONENT_MULTIPLIER + secondComponent;
        contents.write(encodedFirstComponents);
        for (int i = 2; i < components.length; i++) {
            final long component = Long.parseLong(components[i]);
            writeBase128(contents, component);
        }

        final byte[] encodedObjectIdentifier = contents.toByteArray();
        return tagged(OBJECT_IDENTIFIER_TAG, encodedObjectIdentifier);
    }

    static byte[] octetString(final byte[] contents) {
        return tagged(OCTET_STRING_TAG, contents);
    }

    static byte[] bitString(final int unusedBits, final byte[] contents) {
        final byte[] body = new byte[contents.length + 1];
        body[0] = (byte) unusedBits;
        System.arraycopy(contents, 0, body, 1, contents.length);
        return tagged(BIT_STRING_TAG, body);
    }

    static byte[] booleanTrue() {
        return BOOLEAN_TRUE;
    }

    static byte[] nullValue() {
        return NULL_VALUE;
    }

    static byte[] time(final Date date) {
        final int year = date.toInstant().atZone(ZoneOffset.UTC).getYear();
        if (year < GENERALIZED_TIME_FIRST_YEAR) {
            return tagged(UTC_TIME_TAG, UTC_TIME_FORMATTER.format(date.toInstant()).getBytes(StandardCharsets.US_ASCII));
        }

        return tagged(GENERALIZED_TIME_TAG, GENERALIZED_TIME_FORMATTER.format(date.toInstant()).getBytes(StandardCharsets.US_ASCII));
    }

    static byte[] explicit(final int tagNumber, final byte[] encodedValue) {
        return tagged(CONTEXT_SPECIFIC_CONSTRUCTED | tagNumber, encodedValue);
    }

    static byte[] implicit(final int tagNumber, final byte[] contents) {
        return tagged(CONTEXT_SPECIFIC_PRIMITIVE | tagNumber, contents);
    }

    /**
     * Subject key identifier is the SHA-1 hash of the subject public key bit string, excluding the unused-bits count, which is RFC 5280 method 1
     *
     * @param subjectPublicKeyInfo Subject public key info encoding
     * @return Subject key identifier extension value
     */
    static byte[] subjectKeyIdentifier(final byte[] subjectPublicKeyInfo) {
        return octetString(keyIdentifier(subjectPublicKeyInfo));
    }

    /**
     * Authority key identifier containing only the key identifier as an implicit context-specific tag inside a sequence
     *
     * @param issuerPublicKeyInfo Issuer public key info encoding
     * @return Authority key identifier extension value
     */
    static byte[] authorityKeyIdentifier(final byte[] issuerPublicKeyInfo) {
        final byte[] keyIdentifierEncoded = implicit(AUTHORITY_KEY_IDENTIFIER_TAG, keyIdentifier(issuerPublicKeyInfo));
        return sequence(keyIdentifierEncoded);
    }

    private static byte[] keyIdentifier(final byte[] subjectPublicKeyInfo) {
        try {
            final MessageDigest messageDigest = MessageDigest.getInstance(KEY_IDENTIFIER_DIGEST_ALGORITHM);
            return messageDigest.digest(publicKeyBytes(subjectPublicKeyInfo));
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalStateException("Key identifier digest is not available", e);
        }
    }

    private static byte[] publicKeyBytes(final byte[] subjectPublicKeyInfo) {
        final ByteBuffer buffer = ByteBuffer.wrap(subjectPublicKeyInfo);
        try {
            expectTag(buffer, SEQUENCE_TAG);
            readLength(buffer);

            expectTag(buffer, SEQUENCE_TAG);
            final int algorithmLength = readLength(buffer);
            if (buffer.remaining() < algorithmLength) {
                throw new IllegalArgumentException("Subject public key encoding is not valid");
            }

            buffer.position(buffer.position() + algorithmLength);
            expectTag(buffer, BIT_STRING_TAG);
            final int bitStringLength = readLength(buffer);
            if (bitStringLength < 1 || buffer.remaining() < bitStringLength) {
                throw new IllegalArgumentException("Subject public key encoding is not valid");
            }

            buffer.get();
            final byte[] keyBytes = new byte[bitStringLength - 1];
            buffer.get(keyBytes);
            return keyBytes;
        } catch (final BufferUnderflowException e) {
            throw new IllegalArgumentException("Subject public key encoding is not valid", e);
        }
    }

    private static void expectTag(final ByteBuffer buffer, final int expectedTag) {
        final int tag = buffer.get() & BYTE_MASK;
        if (tag != expectedTag) {
            throw new IllegalArgumentException("Subject public key encoding is not valid");
        }
    }

    private static int readLength(final ByteBuffer buffer) {
        final int firstLengthByte = buffer.get() & BYTE_MASK;
        if (firstLengthByte < LONG_FORM_LENGTH) {
            return firstLengthByte;
        }

        final int lengthByteCount = firstLengthByte & BASE_128_MASK;
        if (lengthByteCount == 0 || lengthByteCount > LENGTH_BYTE_LIMIT) {
            throw new IllegalArgumentException("Subject public key encoding length is not supported");
        }

        int length = 0;
        for (int i = 0; i < lengthByteCount; i++) {
            length = (length << BITS_PER_BYTE) | (buffer.get() & BYTE_MASK);
        }

        return length;
    }

    private static void writeBase128(final ByteArrayOutputStream outputStream, final long value) {
        int shift = 0;
        long remaining = value;
        while (remaining > BASE_128_MASK) {
            shift += BASE_128_SHIFT;
            remaining >>>= BASE_128_SHIFT;
        }

        while (shift > 0) {
            final int encoded = (int) (((value >>> shift) & BASE_128_MASK) | CONTINUATION_BIT);
            outputStream.write(encoded);
            shift -= BASE_128_SHIFT;
        }

        outputStream.write((int) (value & BASE_128_MASK));
    }

    private static byte[] tagged(final int tag, final byte[] contents) {
        final byte[] encodedLength = length(contents.length);
        final byte[] encoded = new byte[1 + encodedLength.length + contents.length];
        encoded[0] = (byte) tag;
        System.arraycopy(encodedLength, 0, encoded, 1, encodedLength.length);
        System.arraycopy(contents, 0, encoded, 1 + encodedLength.length, contents.length);
        return encoded;
    }

    private static byte[] length(final int contentsLength) {
        if (contentsLength < SHORT_FORM_LENGTH_LIMIT) {
            return new byte[] {(byte) contentsLength};
        }

        int byteCount = 0;
        int remaining = contentsLength;
        while (remaining > 0) {
            byteCount++;
            remaining >>>= BITS_PER_BYTE;
        }

        final byte[] encoded = new byte[byteCount + 1];
        encoded[0] = (byte) (LONG_FORM_LENGTH | byteCount);
        for (int i = 0; i < byteCount; i++) {
            encoded[byteCount - i] = (byte) (contentsLength >>> (i * BITS_PER_BYTE));
        }

        return encoded;
    }

    private static byte[] concatenate(final byte[]... encodedValues) {
        int contentsLength = 0;
        for (final byte[] encodedValue : encodedValues) {
            contentsLength += encodedValue.length;
        }

        final byte[] concatenated = new byte[contentsLength];
        int offset = 0;
        for (final byte[] encodedValue : encodedValues) {
            System.arraycopy(encodedValue, 0, concatenated, offset, encodedValue.length);
            offset += encodedValue.length;
        }

        return concatenated;
    }
}
