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
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.zone.ZoneRules;
import java.util.Date;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ObjectTimestampFieldConverterTest {
    private static final ObjectTimestampFieldConverter CONVERTER = new ObjectTimestampFieldConverter();

    private static final Optional<String> DEFAULT_PATTERN = Optional.of(RecordFieldType.TIMESTAMP.getDefaultFormat());

    private static final String FIELD_NAME = Timestamp.class.getSimpleName();

    private static final String EMPTY = "";

    private static final String DATE_TIME_DEFAULT = "2000-01-01 12:00:00";

    private static final String DATE_TIME_ZONE_OFFSET_PATTERN = "yyyy-MM-dd HH:mm:ssZZZZZ";

    private static final String DATE_TIME_UTC_OFFSET = "2000-01-01 12:00:00+00:00";

    private static final Optional<String> DATE_TIME_NANOSECONDS_PATTERN = Optional.of("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");

    private static final String DATE_TIME_NANOSECONDS = "2000-01-01 12:00:00.123456789";

    @Test
    public void testConvertFieldNull() {
        final Timestamp timestamp = CONVERTER.convertField(null, DEFAULT_PATTERN, FIELD_NAME);
        assertNull(timestamp);
    }

    @Test
    public void testConvertFieldTimestamp() {
        final Timestamp field = new Timestamp(System.currentTimeMillis());
        final Timestamp timestamp = CONVERTER.convertField(field, DEFAULT_PATTERN, FIELD_NAME);
        assertEquals(field, timestamp);
    }

    @Test
    public void testConvertFieldDate() {
        final Date field = new Date();
        final Timestamp timestamp = CONVERTER.convertField(field, DEFAULT_PATTERN, FIELD_NAME);
        assertEquals(field.getTime(), timestamp.getTime());
    }

    @Test
    public void testConvertFieldLong() {
        final long field = System.currentTimeMillis();
        final Timestamp timestamp = CONVERTER.convertField(field, DEFAULT_PATTERN, FIELD_NAME);
        assertEquals(field, timestamp.getTime());
    }

    @Test
    public void testConvertFieldStringEmpty() {
        final Timestamp timestamp = CONVERTER.convertField(EMPTY, DEFAULT_PATTERN, FIELD_NAME);
        assertNull(timestamp);
    }

    @Test
    public void testConvertFieldStringFormatNull() {
        final long currentTime = System.currentTimeMillis();
        final String field = Long.toString(currentTime);
        final Timestamp timestamp = CONVERTER.convertField(field, Optional.empty(), FIELD_NAME);
        assertEquals(currentTime, timestamp.getTime());
    }

    @Test
    public void testConvertFieldStringFormatNullNumberFormatException() {
        final String field = String.class.getSimpleName();
        final FieldConversionException exception = assertThrows(FieldConversionException.class, () -> CONVERTER.convertField(field, Optional.empty(), FIELD_NAME));
        assertTrue(exception.getMessage().contains(field));
    }

    @Test
    public void testConvertFieldStringFormatDefault() {
        final Timestamp timestamp = CONVERTER.convertField(DATE_TIME_DEFAULT, DEFAULT_PATTERN, FIELD_NAME);
        final Timestamp expected = Timestamp.valueOf(DATE_TIME_DEFAULT);
        assertEquals(expected, timestamp);
    }

    @Test
    public void testConvertFieldStringFormatCustomNanoseconds() {
        final Timestamp timestamp = CONVERTER.convertField(DATE_TIME_NANOSECONDS, DATE_TIME_NANOSECONDS_PATTERN, FIELD_NAME);
        final Timestamp expected = Timestamp.valueOf(DATE_TIME_NANOSECONDS);
        assertEquals(expected, timestamp);
    }

    @Test
    public void testConvertFieldStringFormatCustomFormatterException() {
        final FieldConversionException exception = assertThrows(FieldConversionException.class, () -> CONVERTER.convertField(DATE_TIME_DEFAULT, DATE_TIME_NANOSECONDS_PATTERN, FIELD_NAME));
        assertTrue(exception.getMessage().contains(DATE_TIME_DEFAULT));
    }

    @Test
    public void testConvertFieldStringFormatCustomZoneOffsetSystemDefault() {
        final String dateTimeZoneOffset = getDateTimeZoneOffset();
        final Timestamp timestamp = CONVERTER.convertField(dateTimeZoneOffset, Optional.of(DATE_TIME_ZONE_OFFSET_PATTERN), FIELD_NAME);
        final Timestamp expected = Timestamp.valueOf(DATE_TIME_DEFAULT);
        assertEquals(expected, timestamp);
    }

    @Test
    public void testConvertFieldStringFormatCustomZoneOffsetCoordinatedUniversalTime() {
        final Timestamp timestamp = CONVERTER.convertField(DATE_TIME_UTC_OFFSET, Optional.of(DATE_TIME_ZONE_OFFSET_PATTERN), FIELD_NAME);
        final Timestamp expected = getDateTimeCoordinatedUniversalTime();
        assertEquals(expected, timestamp);
    }

    private Timestamp getDateTimeCoordinatedUniversalTime() {
        final Timestamp dateTime = Timestamp.valueOf(DATE_TIME_DEFAULT);
        final LocalDateTime localDateTime = dateTime.toLocalDateTime();

        final ZonedDateTime zonedDateTime = ZonedDateTime.of(localDateTime, ZoneOffset.UTC);
        final Instant instant = zonedDateTime.toInstant();
        final LocalDateTime localDateTimeAdjusted = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        return Timestamp.valueOf(localDateTimeAdjusted);
    }

    private String getDateTimeZoneOffset() {
        final Timestamp inputTimestamp = Timestamp.valueOf(DATE_TIME_DEFAULT);
        final LocalDateTime inputLocalDateTime = inputTimestamp.toLocalDateTime();

        final ZoneId systemDefaultZoneId = ZoneOffset.systemDefault();
        final ZoneRules zoneRules = systemDefaultZoneId.getRules();
        final ZoneOffset inputZoneOffset = zoneRules.getOffset(inputLocalDateTime);
        final String inputZoneOffsetId = inputZoneOffset.getId();

        // Get Date Time with Zone Offset from current system configuration
        return DATE_TIME_DEFAULT + inputZoneOffsetId;
    }
}
