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

package org.apache.nifi.service.cassandra.mapping;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Covers {@code CharacterCodec}, which lets a NiFi CHAR record field bind to a CQL {@code text} column by
 * treating the character as a one-character string.
 *
 * <p>The interesting direction is decode: {@code text} can hold the empty string, which has no {@code Character}
 * to decode to. The codec answers null rather than throwing, and that choice is what these tests pin.
 */
class CharacterCodecTest {

    private static final ProtocolVersion VERSION = ProtocolVersion.DEFAULT;

    private final CharacterCodec codec = new CharacterCodec();

    @ParameterizedTest(name = "''{0}''")
    @ValueSource(chars = {'a', 'Z', '0', ' ', '\'', 'é', '☃'})
    @DisplayName("A character encodes exactly as the one-character string it represents")
    void testEncodesAsOneCharacterString(final char value) {
        assertEquals(TypeCodecs.TEXT.encode(String.valueOf(value), VERSION), codec.encode(value, VERSION));
    }

    @ParameterizedTest(name = "''{0}''")
    @ValueSource(chars = {'a', 'Z', '0', ' ', '\'', 'é', '☃'})
    @DisplayName("A character round-trips back to the same character")
    void testRoundTrip(final char value) {
        assertEquals(value, codec.decode(codec.encode(value, VERSION), VERSION));
    }

    @Test
    @DisplayName("A null character encodes to null and formats as the CQL NULL literal")
    void testNullHandling() {
        assertNull(codec.encode(null, VERSION));
        assertEquals("NULL", codec.format(null));
    }

    @Test
    @DisplayName("Formatting quotes the character as CQL text, so an embedded quote is escaped rather than breaking the statement")
    void testFormatEscapesQuotes() {
        assertEquals(TypeCodecs.TEXT.format("'"), codec.format('\''));
    }

    @Test
    @DisplayName("An empty text column decodes to null, since there is no character to return")
    void testEmptyStringDecodesToNull() {
        final ByteBuffer empty = TypeCodecs.TEXT.encode("", VERSION);

        assertNull(codec.decode(empty, VERSION));
    }

    @Test
    @DisplayName("A null text column decodes to null")
    void testNullBytesDecodeToNull() {
        assertNull(codec.decode(null, VERSION));
    }

    @Test
    @DisplayName("A multi-character column value decodes to its first character rather than throwing")
    void testMultiCharacterValueDecodesToFirstCharacter() {
        // Nothing stops a text column holding more than one character, so the codec has to answer something.
        // Taking the first character is lossy, and pinning it here is what makes that a decision rather than
        // an accident - if it should instead throw, this test is the place that argues about it.
        final ByteBuffer several = TypeCodecs.TEXT.encode("abc", VERSION);

        assertEquals('a', codec.decode(several, VERSION));
    }

    @Test
    @DisplayName("Parsing follows the same rules as decoding")
    void testParse() {
        assertEquals('a', codec.parse("'a'"));
        assertNull(codec.parse("''"));
        assertNull(codec.parse("NULL"));
        assertNull(codec.parse(null));
    }

    @Test
    @DisplayName("The codec binds the CQL text type")
    void testCqlTypeIsText() {
        assertEquals(TypeCodecs.TEXT.getCqlType(), codec.getCqlType());
    }

    @Test
    @DisplayName("The declared Java type is Character, which is what makes the registry select it for a CHAR field")
    void testDeclaredJavaTypeIsCharacter() {
        assertEquals(Character.class, codec.getJavaType().getRawType());
    }
}
