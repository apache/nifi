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

import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.util.Date;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ObjectOffsetDateTimeFieldConverterTest {
    private static final ObjectOffsetDateTimeFieldConverter CONVERTER = new ObjectOffsetDateTimeFieldConverter();

    private static final String DEFAULT_PATTERN = RecordFieldType.TIMESTAMP.getDefaultFormat();

    private static final String FIELD_NAME = OffsetDateTime.class.getSimpleName();

    private static final String EMPTY = "";

    private static final String DATE_TIME_DEFAULT = "2000-01-01 12:00:00";

    private static final String OFFSET_DATE_TIME_DEFAULT = "2000-01-01T12:30:45Z";

    private static final String OFFSET_DATE_TIME_PATTERN = "yyyy-MM-dd'T'HH:mm:ssX";

    private static final String NANOSECONDS_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSX";

    private static final String DATE_TIME_NANOSECONDS = "2000-01-01T12:00:00.123456789Z";

    @Test
    public void testConvertFieldNull() {
        final OffsetDateTime offsetDateTime = CONVERTER.convertField(null, Optional.of(DEFAULT_PATTERN), FIELD_NAME);
        assertNull(offsetDateTime);
    }

    @Test
    public void testConvertFieldTimestamp() {
        final Timestamp field = new Timestamp(System.currentTimeMillis());
        final OffsetDateTime offsetDateTime = CONVERTER.convertField(field, Optional.of(DEFAULT_PATTERN), FIELD_NAME);
        assertEquals(field.getTime(), offsetDateTime.toInstant().toEpochMilli());
    }

    @Test
    public void testConvertFieldDate() {
        final Date field = new Date();
        final OffsetDateTime offsetDateTime = CONVERTER.convertField(field, Optional.of(DEFAULT_PATTERN), FIELD_NAME);
        assertEquals(field.getTime(), offsetDateTime.toInstant().toEpochMilli());
    }

    @Test
    public void testConvertFieldLong() {
        final long field = System.currentTimeMillis();
        final OffsetDateTime offsetDateTime = CONVERTER.convertField(field, Optional.of(DEFAULT_PATTERN), FIELD_NAME);
        assertEquals(field, offsetDateTime.toInstant().toEpochMilli());
    }

    @Test
    public void testConvertFieldStringEmpty() {
        final OffsetDateTime offsetDateTime = CONVERTER.convertField(EMPTY, Optional.of(DEFAULT_PATTERN), FIELD_NAME);
        assertNull(offsetDateTime);
    }

    @Test
    public void testConvertFieldStringFormatNull() {
        final long currentTime = System.currentTimeMillis();
        final String field = Long.toString(currentTime);
        final OffsetDateTime offsetDateTime = CONVERTER.convertField(field, Optional.empty(), FIELD_NAME);
        assertEquals(currentTime, offsetDateTime.toInstant().toEpochMilli());
    }

    @Test
    public void testConvertFieldStringFormatNullNumberFormatException() {
        final String field = String.class.getSimpleName();
        final FieldConversionException exception = assertThrows(FieldConversionException.class, () -> CONVERTER.convertField(field, Optional.empty(), FIELD_NAME));
        assertTrue(exception.getMessage().contains(field));
    }

    @Test
    public void testConvertFieldStringFormatDefault() {
        final OffsetDateTime offsetDateTime = CONVERTER.convertField(OFFSET_DATE_TIME_DEFAULT, Optional.of(OFFSET_DATE_TIME_PATTERN), FIELD_NAME);
        assertEquals(OFFSET_DATE_TIME_DEFAULT, offsetDateTime.toString());
    }

    @Test
    public void testConvertFieldStringFormatCustomNanoseconds() {
        final OffsetDateTime offsetDateTime = CONVERTER.convertField(DATE_TIME_NANOSECONDS, Optional.of(NANOSECONDS_PATTERN), FIELD_NAME);
        final OffsetDateTime expected = OffsetDateTime.parse(DATE_TIME_NANOSECONDS);
        assertEquals(expected, offsetDateTime);
    }

    @Test
    public void testConvertFieldStringFormatCustomFormatterException() {
        final FieldConversionException exception = assertThrows(FieldConversionException.class, () -> CONVERTER.convertField(DATE_TIME_DEFAULT, Optional.of(NANOSECONDS_PATTERN), FIELD_NAME));
        assertTrue(exception.getMessage().contains(DATE_TIME_DEFAULT));
    }
}
