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
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Covers the three {@code java.sql.*} bridge codecs.
 *
 * <p>These differ from the {@code Flexible*} family in that they convert in both directions: a record field
 * carrying a {@code java.sql} temporal type binds to the CQL column, and a value read back from the column
 * comes out as that same {@code java.sql} type. Round-tripping is therefore the property to assert, and it is
 * the one the integration suite cannot check - the ITs read back through the record layer, which does its own
 * conversion, so a bug in the decode direction here would be masked.
 */
class JavaSqlBridgeCodecTest {

    private static final ProtocolVersion VERSION = ProtocolVersion.DEFAULT;

    private static final LocalDate A_DATE = LocalDate.of(2026, 8, 10);
    private static final LocalTime A_TIME = LocalTime.of(13, 45, 30);
    private static final Instant AN_INSTANT = Instant.parse("2026-08-10T13:45:30Z");

    static Stream<Arguments> codecs() {
        return Stream.of(
                arguments("JavaSQLDateCodec", new JavaSQLDateCodec()),
                arguments("JavaSQLTimeCodec", new JavaSQLTimeCodec()),
                arguments("JavaSQLTimestampCodec", new JavaSQLTimestampCodec()));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("codecs")
    @DisplayName("A null value encodes to null and formats as the CQL NULL literal")
    void testNullHandling(final String name, final TypeCodec<?> codec) {
        assertNull(encode(codec, null));
        assertEquals("NULL", format(codec, null));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("codecs")
    @DisplayName("Decoding null bytes yields null rather than a zero-epoch value")
    void testDecodeNullBytesReturnsNull(final String name, final TypeCodec<?> codec) {
        // The distinction matters: a NULL column must not read back as 1970-01-01.
        assertNull(codec.decode(null, VERSION));
    }

    @Test
    @DisplayName("A java.sql.Timestamp round-trips through the CQL timestamp column unchanged")
    void testTimestampRoundTrip() {
        final JavaSQLTimestampCodec codec = new JavaSQLTimestampCodec();
        final Timestamp original = Timestamp.from(AN_INSTANT);

        final ByteBuffer encoded = codec.encode(original, VERSION);

        assertEquals(TypeCodecs.TIMESTAMP.encode(AN_INSTANT, VERSION), encoded,
                "should bind exactly as the driver's own Instant codec would");
        assertEquals(original, codec.decode(encoded, VERSION));
    }

    @Test
    @DisplayName("A java.sql.Date round-trips through the CQL date column as the same calendar day")
    void testDateRoundTrip() {
        final JavaSQLDateCodec codec = new JavaSQLDateCodec();
        final Date original = Date.valueOf(A_DATE);

        final ByteBuffer encoded = codec.encode(original, VERSION);

        assertEquals(TypeCodecs.DATE.encode(A_DATE, VERSION), encoded,
                "should bind exactly as the driver's own LocalDate codec would");
        // Comparing the calendar day rather than the Date instances: both conversions go through the system
        // default zone, so asserting on toLocalDate is what actually states the intended property.
        assertEquals(A_DATE, codec.decode(encoded, VERSION).toLocalDate());
    }

    @Test
    @DisplayName("A java.sql.Time round-trips through the CQL time column as the same wall-clock time")
    void testTimeRoundTrip() {
        final JavaSQLTimeCodec codec = new JavaSQLTimeCodec();
        final Time original = Time.valueOf(A_TIME);

        final ByteBuffer encoded = codec.encode(original, VERSION);

        assertEquals(TypeCodecs.TIME.encode(A_TIME, VERSION), encoded,
                "should bind exactly as the driver's own LocalTime codec would");
        assertEquals(A_TIME, codec.decode(encoded, VERSION).toLocalTime());
    }

    @Test
    @DisplayName("Timestamp keeps millisecond precision, which is all a CQL timestamp column stores")
    void testTimestampMillisecondPrecision() {
        final JavaSQLTimestampCodec codec = new JavaSQLTimestampCodec();
        final Timestamp original = Timestamp.from(Instant.parse("2026-08-10T13:45:30.123Z"));

        final Timestamp roundTripped = codec.decode(codec.encode(original, VERSION), VERSION);

        assertEquals(original, roundTripped);
        assertEquals(123_000_000, roundTripped.getNanos(), "milliseconds survive; the column holds no finer unit");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("codecs")
    @DisplayName("parse() reverses format(), so a value written into CQL text can be read back out of it")
    void testParseReversesFormat(final String name, final TypeCodec<?> codec) {
        // format/parse is the string-literal path the driver uses for statement logging and for simple
        // statements, and it converts independently of encode/decode - so it needs asserting separately.
        final Object original = codec.decode(encode(codec, canonicalValueFor(name)), VERSION);

        assertEquals(original, codec.parse(format(codec, original)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("codecs")
    @DisplayName("parse() of the CQL NULL literal yields null rather than a zero-epoch value")
    void testParseNullLiteralReturnsNull(final String name, final TypeCodec<?> codec) {
        assertNull(codec.parse("NULL"));
        assertNull(codec.parse(null));
    }

    private static Object canonicalValueFor(final String codecName) {
        return switch (codecName) {
            case "JavaSQLDateCodec" -> Date.valueOf(A_DATE);
            case "JavaSQLTimeCodec" -> Time.valueOf(A_TIME);
            case "JavaSQLTimestampCodec" -> Timestamp.from(AN_INSTANT);
            default -> throw new IllegalArgumentException(codecName);
        };
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("codecs")
    @DisplayName("The declared Java type is the java.sql type, which is what makes the registry select these for it")
    void testDeclaredJavaTypeIsTheSqlType(final String name, final TypeCodec<?> codec) {
        final Class<?> rawType = codec.getJavaType().getRawType();
        assertEquals("java.sql", rawType.getPackageName(),
                () -> name + " declares " + rawType.getName());
    }

    @SuppressWarnings("unchecked")
    private static ByteBuffer encode(final TypeCodec<?> codec, final Object value) {
        return ((TypeCodec<Object>) codec).encode(value, VERSION);
    }

    @SuppressWarnings("unchecked")
    private static String format(final TypeCodec<?> codec, final Object value) {
        return ((TypeCodec<Object>) codec).format(value);
    }
}
