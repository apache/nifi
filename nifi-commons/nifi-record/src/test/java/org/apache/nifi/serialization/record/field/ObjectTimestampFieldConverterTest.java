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
package org.apache.nifi.serialization.record.field;

import org.apache.nifi.serialization.record.RecordFieldType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Timestamp;
import java.util.Date;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ObjectTimestampFieldConverterTest {
    private static final ObjectTimestampFieldConverter CONVERTER = new ObjectTimestampFieldConverter();

    private static final Optional<String> DEFAULT_PATTERN = Optional.of(RecordFieldType.TIMESTAMP.getDefaultFormat());

    private static final String FIELD_NAME = Timestamp.class.getSimpleName();

    private static final String EMPTY = "";

    private static final String DATE_TIME_DEFAULT = "2000-01-01 12:00:00";

    private static final Optional<String> DATE_TIME_NANOSECONDS_PATTERN = Optional.of("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");

    private static final String DATE_TIME_NANOSECONDS = "2000-01-01 12:00:00.123456789";

    @Test
    void testConvertFieldNull() {
        final Timestamp timestamp = CONVERTER.convertField(null, DEFAULT_PATTERN, FIELD_NAME);
        assertNull(timestamp);
    }

    @Test
    void testConvertFieldTimestamp() {
        final Timestamp field = new Timestamp(System.currentTimeMillis());
        final Timestamp timestamp = CONVERTER.convertField(field, DEFAULT_PATTERN, FIELD_NAME);
        assertEquals(field, timestamp);
    }

    @Test
    void testConvertFieldDate() {
        final Date field = new Date();
        final Timestamp timestamp = CONVERTER.convertField(field, DEFAULT_PATTERN, FIELD_NAME);
        assertEquals(field.getTime(), timestamp.getTime());
    }

    @Test
    void testConvertFieldLong() {
        final long field = System.currentTimeMillis();
        final Timestamp timestamp = CONVERTER.convertField(field, DEFAULT_PATTERN, FIELD_NAME);
        assertEquals(field, timestamp.getTime());
    }

    @Test
    void testConvertFieldStringEmpty() {
        final Timestamp timestamp = CONVERTER.convertField(EMPTY, DEFAULT_PATTERN, FIELD_NAME);
        assertNull(timestamp);
    }

    @Test
    void testConvertFieldStringFormatNull() {
        final long currentTime = System.currentTimeMillis();
        final String field = Long.toString(currentTime);
        final Timestamp timestamp = CONVERTER.convertField(field, Optional.empty(), FIELD_NAME);
        assertEquals(currentTime, timestamp.getTime());
    }

    @Test
    void testConvertFieldStringFormatNullNumberFormatException() {
        final String field = String.class.getSimpleName();
        final FieldConversionException exception = assertThrows(FieldConversionException.class, () -> CONVERTER.convertField(field, Optional.empty(), FIELD_NAME));
        assertTrue(exception.getMessage().contains(field));
    }

    @Test
    void testConvertFieldStringFormatDefault() {
        final Timestamp timestamp = CONVERTER.convertField(DATE_TIME_DEFAULT, DEFAULT_PATTERN, FIELD_NAME);
        final Timestamp expected = Timestamp.valueOf(DATE_TIME_DEFAULT);
        assertEquals(expected, timestamp);
    }

    @Test
    void testConvertFieldStringFormatCustomNanoseconds() {
        final Timestamp timestamp = CONVERTER.convertField(DATE_TIME_NANOSECONDS, DATE_TIME_NANOSECONDS_PATTERN, FIELD_NAME);
        final Timestamp expected = Timestamp.valueOf(DATE_TIME_NANOSECONDS);
        assertEquals(expected, timestamp);
    }

    @Test
    void testConvertFieldStringFormatCustomFormatterException() {
        final FieldConversionException exception = assertThrows(FieldConversionException.class, () -> CONVERTER.convertField(DATE_TIME_DEFAULT, DATE_TIME_NANOSECONDS_PATTERN, FIELD_NAME));
        assertTrue(exception.getMessage().contains(DATE_TIME_DEFAULT));
    }

    @Test
    void testConvertFieldStringFormatWithTimeZone() {
        final String originalTimestampHour = "12";
        //NOTE: Antarctica/Casey is the timezone offset chosen in timestamp below
        final String originalTimestamp = "2000-01-01 " + originalTimestampHour + ":00:00+0800";
        final Optional<String> timezonePattern = Optional.of("yyyy-MM-dd HH:mm:ssZ");
        final Timestamp actual = CONVERTER.convertField(originalTimestamp, timezonePattern, FIELD_NAME);
        final String actualString = actual.toString();

        assertFalse(actualString.contains(" " + originalTimestampHour + ":"));
    }

    @ParameterizedTest
    @MethodSource("getPatterns")
    void testTimeZonePattern(String pattern, boolean expected) {
        final Matcher matcher = ObjectTimestampFieldConverter.TIMEZONE_PATTERN.matcher(pattern);
        if (expected) {
            assertTrue(matcher.find());
        } else {
            assertFalse(matcher.find());
        }
    }

    private static Stream<Arguments> getPatterns() {
        return Stream.of(
                Arguments.of("yyyy-MM-dd'T'HH:mm:ssZ", true),
                Arguments.of("Zyyyy-MM-dd'T'HH:mm:ss", true),
                Arguments.of("yyyy-MM-dd'T'ZHH:mm:ss", true),
                Arguments.of("yyyy-MM-ddZ'T'HH:mm:ss", true),
                Arguments.of("yyyy-MM-dd'T'HH:mm:ss'Z'", false),
                Arguments.of("EEEE, MMM dd, yyyy HH:mm:ss a", false),
                Arguments.of("dd-MMM-yyyy", false),
                Arguments.of("MMMM dd, yyyy: EEEE", false)
        );
    }
}
