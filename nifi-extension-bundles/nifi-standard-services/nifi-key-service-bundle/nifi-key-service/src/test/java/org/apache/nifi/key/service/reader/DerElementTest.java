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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DerElementTest {
    private static final HexFormat HEX_FORMAT = HexFormat.of();

    private static final int LONG_FORM_CONTENTS_LENGTH = 300;

    private static final String LONG_FORM_OCTET_STRING_HEADER = "0482012c";

    private static final String SEQUENCE_INTEGER_ONE = "3003020101";

    private static final String INTEGER_EMPTY = "0200";

    @Test
    void testRead() {
        final DerElement sequence = DerElement.read(HEX_FORMAT.parseHex(SEQUENCE_INTEGER_ONE));

        assertEquals(DerElement.SEQUENCE_TAG, sequence.getTag());
        assertEquals(1, sequence.getElements().size());
        assertEquals(BigInteger.ONE, sequence.getElement(0).getInteger());
    }

    @Test
    void testReadLongFormLength() {
        final byte[] header = HEX_FORMAT.parseHex(LONG_FORM_OCTET_STRING_HEADER);
        final byte[] encoded = Arrays.copyOf(header, header.length + LONG_FORM_CONTENTS_LENGTH);

        final DerElement octetString = DerElement.read(encoded);

        assertArrayEquals(new byte[LONG_FORM_CONTENTS_LENGTH], octetString.getOctetString());
    }

    @ParameterizedTest
    @CsvSource({
            "06092a864886f70d01050d,1.2.840.113549.1.5.13",
            "06092b06010401da47040b,1.3.6.1.4.1.11591.4.11",
            "06072a8648ce3d0201,1.2.840.10045.2.1",
            "0603883703,2.999.3"
    })
    void testGetObjectIdentifier(final String encoded, final String expectedObjectIdentifier) {
        final DerElement element = DerElement.read(HEX_FORMAT.parseHex(encoded));

        assertEquals(expectedObjectIdentifier, element.getObjectIdentifier());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "0600",
            "06022a86",
            "06032a8001",
            "060b2affffffffffffffffff7f"
    })
    void testGetObjectIdentifierException(final String encoded) {
        final DerElement element = DerElement.read(HEX_FORMAT.parseHex(encoded));

        assertThrows(PrivateKeyException.class, element::getObjectIdentifier);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "",
            "30",
            "3080",
            "3082",
            "30850100000000",
            "3005020100",
            "1f0100",
            "30030201000500"
    })
    void testReadException(final String encoded) {
        final byte[] bytes = HEX_FORMAT.parseHex(encoded);

        assertThrows(PrivateKeyException.class, () -> DerElement.read(bytes));
    }

    @Test
    void testGetElementException() {
        final DerElement sequence = DerElement.read(HEX_FORMAT.parseHex(SEQUENCE_INTEGER_ONE));

        assertThrows(PrivateKeyException.class, () -> sequence.getElement(1));
        assertThrows(PrivateKeyException.class, sequence::getInteger);
        assertThrows(PrivateKeyException.class, sequence::getOctetString);

        final DerElement integer = sequence.getElement(0);
        assertThrows(PrivateKeyException.class, integer::getElements);
        assertThrows(PrivateKeyException.class, integer::getObjectIdentifier);
    }

    @Test
    void testGetIntegerException() {
        final DerElement integer = DerElement.read(HEX_FORMAT.parseHex(INTEGER_EMPTY));

        assertThrows(PrivateKeyException.class, integer::getInteger);
    }
}
