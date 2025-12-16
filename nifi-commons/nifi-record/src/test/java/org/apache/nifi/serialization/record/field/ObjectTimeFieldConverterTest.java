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
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Time;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ObjectTimeFieldConverterTest {
    private static final ObjectTimeFieldConverter CONVERTER = new ObjectTimeFieldConverter();

    private static final String DEFAULT_PATTERN = RecordFieldType.TIME.getDefaultFormat();

    private static final String FIELD_NAME = Time.class.getSimpleName();

    private static final String EMPTY = "";

    private static final String TIME_DEFAULT = "12:30:45";

    private static final String TIME_NANOSECONDS_PATTERN = "HH:mm:ss.SSSSSSSSS";

    private static final String TIME_NANOSECONDS = "12:30:45.123456789";

    private static final String TIME_WITH_OFFSET_PATTERN = "HH:mm:ssXXX";

    @Test
    public void testConvertFieldNull() {
        final Time time = CONVERTER.convertField(null, Optional.of(DEFAULT_PATTERN), FIELD_NAME);
        assertNull(time);
    }

    @Test
    public void testConvertFieldStringEmpty() {
        final Time time = CONVERTER.convertField(EMPTY, Optional.of(DEFAULT_PATTERN), FIELD_NAME);
        assertNull(time);
    }

    @Test
    public void testConvertFieldTime() {
        final Time field = Time.valueOf(TIME_DEFAULT);
        final Time time = CONVERTER.convertField(field, Optional.of(DEFAULT_PATTERN), FIELD_NAME);
        assertEquals(field.getTime(), time.getTime());
    }

    @Test
    public void testConvertFieldTimeNanoseconds() {
        final Time time = CONVERTER.convertField(TIME_NANOSECONDS, Optional.of(TIME_NANOSECONDS_PATTERN), FIELD_NAME);
        assertEquals(TIME_DEFAULT, time.toString());
    }

    @Test
    public void testConvertFieldDate() {
        final Date field = new Date();
        final Time time = CONVERTER.convertField(field, Optional.of(DEFAULT_PATTERN), FIELD_NAME);
        assertNotNull(time);
    }

    @Test
    public void testConvertFieldLong() {
        final long field = System.currentTimeMillis();
        final Time time = CONVERTER.convertField(field, Optional.of(DEFAULT_PATTERN), FIELD_NAME);
        assertNotNull(time);
    }

    @ParameterizedTest
    @MethodSource("org.apache.nifi.serialization.record.field.DateTimeTestUtil#offsetSource")
    public void testConvertFieldTimeOffset(final String offsetId) {
        final ZoneOffset zoneOffset = ZoneOffset.of(offsetId);

        final LocalTime localTime = LocalTime.parse(TIME_DEFAULT);
        final ZonedDateTime zonedDateTime = ZonedDateTime.of(LocalDate.EPOCH, localTime, zoneOffset);
        final ZonedDateTime zonedDateTimeAdjusted = zonedDateTime.withZoneSameInstant(ZoneId.systemDefault());
        final Instant instant = zonedDateTimeAdjusted.toInstant();
        final long epochMilliseconds = instant.toEpochMilli();
        final Time expectedTime = new Time(epochMilliseconds);

        final String localTimeWithOffsetId = TIME_DEFAULT + offsetId;
        final Time convertedTime = CONVERTER.convertField(localTimeWithOffsetId, Optional.of(TIME_WITH_OFFSET_PATTERN), FIELD_NAME);

        assertEquals(expectedTime.toString(), convertedTime.toString());
    }
}
