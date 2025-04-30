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

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.zone.ZoneRules;
import java.util.Date;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ObjectStringFieldConverterTest {
    private static final ObjectStringFieldConverter CONVERTER = new ObjectStringFieldConverter();

    private static final String DEFAULT_PATTERN = RecordFieldType.TIMESTAMP.getDefaultFormat();

    private static final String FIELD_NAME = Timestamp.class.getSimpleName();

    private static final String DATE_TIME_DEFAULT = "2000-01-01 12:00:00";

    private static final String DATE_TIME_NANOSECONDS_PATTERN = "yyyy-MM-dd HH:mm:ss.SSSSSSSSS";

    private static final String DATE_TIME_NANOSECONDS = "2000-01-01 12:00:00.123456789";

    private static final String DATE_TIME_ZONE_OFFSET_PATTERN = "yyyy-MM-dd HH:mm:ssZZZZZ";

    private static final String EMPTY_ARRAY_STRING = "[]";

    private static final String ARRAY_STRING_ELEMENT = String.class.getSimpleName();

    private static final String ARRAY_STRING = "[String, String, String]";

    @Test
    void testConvertFieldNull() {
        final String string = CONVERTER.convertField(null, Optional.of(DEFAULT_PATTERN), FIELD_NAME);
        assertNull(string);
    }

    @Test
    void testConvertFieldTimestampDefaultPattern() {
        final Timestamp timestamp = Timestamp.valueOf(DATE_TIME_DEFAULT);

        final String string = CONVERTER.convertField(timestamp, Optional.of(DEFAULT_PATTERN), FIELD_NAME);

        assertEquals(DATE_TIME_DEFAULT, string);
    }

    @Test
    void testConvertFieldTimestampNanoseconds() {
        final Timestamp timestamp = Timestamp.valueOf(DATE_TIME_NANOSECONDS);

        final String string = CONVERTER.convertField(timestamp, Optional.of(DATE_TIME_NANOSECONDS_PATTERN), FIELD_NAME);

        assertEquals(DATE_TIME_NANOSECONDS, string);
    }

    @Test
    void testConvertFieldTimestampEmptyPattern() {
        final Timestamp timestamp = Timestamp.valueOf(DATE_TIME_DEFAULT);

        final String string = CONVERTER.convertField(timestamp, Optional.empty(), FIELD_NAME);

        final String expected = Long.toString(timestamp.getTime());
        assertEquals(expected, string);
    }

    @Test
    void testConvertFieldTimestampZoneOffsetPattern() {
        final Timestamp timestamp = Timestamp.valueOf(DATE_TIME_DEFAULT);

        final String string = CONVERTER.convertField(timestamp, Optional.of(DATE_TIME_ZONE_OFFSET_PATTERN), FIELD_NAME);

        final String dateTimeZoneOffsetExpected = getDateTimeZoneOffset();
        assertEquals(dateTimeZoneOffsetExpected, string);
    }

    @Test
    void testConvertFieldDateDefaultPattern() {
        final Date date = new Date(Timestamp.valueOf(DATE_TIME_DEFAULT).getTime());

        final String string = CONVERTER.convertField(date, Optional.of(DEFAULT_PATTERN), FIELD_NAME);

        assertEquals(DATE_TIME_DEFAULT, string);
    }

    @Test
    void testConvertFieldDateEmptyPattern() {
        final Date date = new Date(Timestamp.valueOf(DATE_TIME_DEFAULT).getTime());

        final String string = CONVERTER.convertField(date, Optional.empty(), FIELD_NAME);

        final String expected = Long.toString(date.getTime());
        assertEquals(expected, string);
    }

    @Test
    void testConvertFieldDateZoneOffsetPattern() {
        final Timestamp inputTimestamp = Timestamp.valueOf(DATE_TIME_DEFAULT);
        final long inputTime = inputTimestamp.getTime();
        final Date date = new Date(inputTime);

        final String string = CONVERTER.convertField(date, Optional.of(DATE_TIME_ZONE_OFFSET_PATTERN), FIELD_NAME);

        final String dateTimeZoneOffsetExpected = getDateTimeZoneOffset();
        assertEquals(dateTimeZoneOffsetExpected, string);
    }

    @Test
    void testConvertFieldObjectArrayOfBytes() {
        final String expected = String.class.getSimpleName();
        final byte[] bytes = expected.getBytes(StandardCharsets.UTF_8);

        final Object[] objectArray = new Object[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            objectArray[i] = bytes[i];
        }

        final String string = CONVERTER.convertField(objectArray, Optional.empty(), FIELD_NAME);
        assertEquals(expected, string);
    }

    @Test
    void testConvertFieldObjectArrayOfStrings() {
        final Object[] objectArray = new Object[]{ARRAY_STRING_ELEMENT, ARRAY_STRING_ELEMENT, ARRAY_STRING_ELEMENT};

        final String string = CONVERTER.convertField(objectArray, Optional.empty(), FIELD_NAME);

        assertEquals(ARRAY_STRING, string);
    }

    @Test
    void testConvertFieldObjectArrayEmpty() {
        final Object[] objectArray = new Object[0];

        final String string = CONVERTER.convertField(objectArray, Optional.empty(), FIELD_NAME);
        assertEquals(EMPTY_ARRAY_STRING, string);
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
