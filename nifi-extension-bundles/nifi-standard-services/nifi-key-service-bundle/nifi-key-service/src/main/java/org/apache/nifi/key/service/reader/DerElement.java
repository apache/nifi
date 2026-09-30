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
package org.apache.nifi.key.service.reader;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * ASN.1 element encoded using Distinguished Encoding Rules limited to definite lengths and low tag numbers
 */
final class DerElement {
    static final int INTEGER_TAG = 0x02;

    static final int OCTET_STRING_TAG = 0x04;

    static final int OBJECT_IDENTIFIER_TAG = 0x06;

    static final int SEQUENCE_TAG = 0x30;

    private static final int CONSTRUCTED_TAG_FLAG = 0x20;

    private static final int HIGH_TAG_NUMBER_FORM = 0x1F;

    private static final int LONG_FORM_LENGTH_FLAG = 0x80;

    private static final int LENGTH_BYTES_MASK = 0x7F;

    private static final int MAXIMUM_LENGTH_BYTES = 4;

    private static final int BYTE_MASK = 0xFF;

    private static final int BYTE_BITS = 8;

    private static final int ARC_CONTINUATION_FLAG = 0x80;

    private static final int ARC_VALUE_MASK = 0x7F;

    private static final int ARC_VALUE_BITS = 7;

    private static final long MAXIMUM_ARC_BEFORE_SHIFT = Long.MAX_VALUE >> ARC_VALUE_BITS;

    private static final int FIRST_ARC_FACTOR = 40;

    private static final long MAXIMUM_FIRST_ARC = 2;

    private static final char ARC_SEPARATOR = '.';

    private final int tag;

    private final byte[] contents;

    private DerElement(final int tag, final byte[] contents) {
        this.tag = tag;
        this.contents = contents;
    }

    /**
     * Read DER element from encoded bytes containing exactly one element
     *
     * @param encoded DER encoded bytes
     * @return DER element
     */
    static DerElement read(final byte[] encoded) {
        final List<DerElement> elements = readElements(encoded);
        if (elements.size() != 1) {
            throw new PrivateKeyException("DER encoding contains [%d] elements instead of 1".formatted(elements.size()));
        }

        return elements.getFirst();
    }

    /**
     * Get DER tag
     *
     * @return DER tag
     */
    int getTag() {
        return tag;
    }

    /**
     * Get elements contained in a constructed element such as a SEQUENCE
     *
     * @return Contained elements
     */
    List<DerElement> getElements() {
        if ((tag & CONSTRUCTED_TAG_FLAG) == 0) {
            throw new PrivateKeyException("DER tag [0x%02X] not constructed".formatted(tag));
        }

        return readElements(contents);
    }

    /**
     * Get element at the specified position in a constructed element such as a SEQUENCE
     *
     * @param index Element position starting from zero
     * @return Contained element
     */
    DerElement getElement(final int index) {
        final List<DerElement> elements = getElements();
        if (index >= elements.size()) {
            throw new PrivateKeyException("DER tag [0x%02X] element [%d] not found".formatted(tag, index));
        }

        return elements.get(index);
    }

    /**
     * Get INTEGER value
     *
     * @return Signed integer value
     */
    BigInteger getInteger() {
        requireTag(INTEGER_TAG);
        if (contents.length == 0) {
            throw new PrivateKeyException("DER INTEGER contents not found");
        }

        return new BigInteger(contents);
    }

    /**
     * Get OCTET STRING value
     *
     * @return Copy of bytes
     */
    byte[] getOctetString() {
        requireTag(OCTET_STRING_TAG);
        return contents.clone();
    }

    /**
     * Get OBJECT IDENTIFIER value formatted as a dotted decimal string
     *
     * @return Object Identifier such as 1.2.840.113549.1.1.1
     */
    String getObjectIdentifier() {
        requireTag(OBJECT_IDENTIFIER_TAG);
        if (contents.length == 0) {
            throw new PrivateKeyException("DER OBJECT IDENTIFIER contents not found");
        }

        if ((contents[contents.length - 1] & ARC_CONTINUATION_FLAG) == ARC_CONTINUATION_FLAG) {
            throw new PrivateKeyException("DER OBJECT IDENTIFIER truncated");
        }

        final StringBuilder builder = new StringBuilder();
        long arc = 0;
        for (final byte encodedByte : contents) {
            final int arcByte = encodedByte & BYTE_MASK;
            if (arc == 0 && arcByte == ARC_CONTINUATION_FLAG) {
                throw new PrivateKeyException("DER OBJECT IDENTIFIER arc encoding not minimal");
            }

            if (arc > MAXIMUM_ARC_BEFORE_SHIFT) {
                throw new PrivateKeyException("DER OBJECT IDENTIFIER arc exceeds maximum value");
            }

            arc = (arc << ARC_VALUE_BITS) | (arcByte & ARC_VALUE_MASK);
            if ((arcByte & ARC_CONTINUATION_FLAG) == 0) {
                if (builder.isEmpty()) {
                    final long firstArc = Math.min(arc / FIRST_ARC_FACTOR, MAXIMUM_FIRST_ARC);
                    builder.append(firstArc).append(ARC_SEPARATOR).append(arc - firstArc * FIRST_ARC_FACTOR);
                } else {
                    builder.append(ARC_SEPARATOR).append(arc);
                }

                arc = 0;
            }
        }

        return builder.toString();
    }

    private void requireTag(final int expectedTag) {
        if (tag != expectedTag) {
            throw new PrivateKeyException("DER tag [0x%02X] found instead of expected tag [0x%02X]".formatted(tag, expectedTag));
        }
    }

    private static List<DerElement> readElements(final byte[] encoded) {
        final List<DerElement> elements = new ArrayList<>();

        int offset = 0;
        while (offset < encoded.length) {
            final int elementTag = encoded[offset] & BYTE_MASK;
            if ((elementTag & HIGH_TAG_NUMBER_FORM) == HIGH_TAG_NUMBER_FORM) {
                throw new PrivateKeyException("DER tag [0x%02X] high tag number form not supported".formatted(elementTag));
            }

            offset++;
            if (offset == encoded.length) {
                throw new PrivateKeyException("DER tag [0x%02X] length not found".formatted(elementTag));
            }

            final int initialLength = encoded[offset] & BYTE_MASK;
            offset++;

            final long length;
            if ((initialLength & LONG_FORM_LENGTH_FLAG) == 0) {
                length = initialLength;
            } else {
                final int lengthBytes = initialLength & LENGTH_BYTES_MASK;
                if (lengthBytes == 0) {
                    throw new PrivateKeyException("DER tag [0x%02X] indefinite length not supported".formatted(elementTag));
                }

                if (lengthBytes > MAXIMUM_LENGTH_BYTES || lengthBytes > encoded.length - offset) {
                    throw new PrivateKeyException("DER tag [0x%02X] length bytes [%d] not valid".formatted(elementTag, lengthBytes));
                }

                long longFormLength = 0;
                for (int lengthByte = 0; lengthByte < lengthBytes; lengthByte++) {
                    longFormLength = (longFormLength << BYTE_BITS) | (encoded[offset] & BYTE_MASK);
                    offset++;
                }

                length = longFormLength;
            }

            final int remainingBytes = encoded.length - offset;
            if (length > remainingBytes) {
                throw new PrivateKeyException("DER tag [0x%02X] length [%d] exceeds remaining bytes [%d]".formatted(elementTag, length, remainingBytes));
            }

            final int contentsLength = (int) length;
            elements.add(new DerElement(elementTag, Arrays.copyOfRange(encoded, offset, offset + contentsLength)));
            offset += contentsLength;
        }

        return elements;
    }
}
