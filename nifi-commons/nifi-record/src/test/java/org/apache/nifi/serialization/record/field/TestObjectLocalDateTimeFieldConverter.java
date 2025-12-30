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

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestObjectLocalDateTimeFieldConverter {
    private static final String FIELD_NAME = "test";
    private static final long MILLIS_TIMESTAMP_LONG = 1707238288351L;
    private static final long MICROS_TIMESTAMP_LONG = 1707238288351567L;
    private static final String MICROS_TIMESTAMP_STRING = Long.toString(MICROS_TIMESTAMP_LONG);
    private static final double MICROS_TIMESTAMP_DOUBLE = ((double) MICROS_TIMESTAMP_LONG) / 1000000D;
    private static final long NANOS_AFTER_SECOND = 351567000L;
    private static final Instant INSTANT_MILLIS_PRECISION = Instant.ofEpochMilli(MILLIS_TIMESTAMP_LONG);
    // Create an instant to represent the same time as the microsecond precision timestamp. We add nanoseconds after second but then have to subtract the milliseconds after the second that are already
    // present in the MILLIS_TIMESTAMP_LONG value.
    private static final Instant INSTANT_MICROS_PRECISION = Instant.ofEpochMilli(MILLIS_TIMESTAMP_LONG).plusNanos(NANOS_AFTER_SECOND).minusMillis(MILLIS_TIMESTAMP_LONG % 1000);
    private static final LocalDateTime LOCAL_DATE_TIME_MILLIS_PRECISION = LocalDateTime.ofInstant(INSTANT_MILLIS_PRECISION, ZoneId.systemDefault());
    private static final LocalDateTime LOCAL_DATE_TIME_MICROS_PRECISION = LocalDateTime.ofInstant(INSTANT_MICROS_PRECISION, ZoneId.systemDefault());

    private final ObjectLocalDateTimeFieldConverter converter = new ObjectLocalDateTimeFieldConverter();


    @Test
    public void testConvertTimestampMillis() {
        final LocalDateTime result = converter.convertField(MILLIS_TIMESTAMP_LONG, Optional.empty(), FIELD_NAME);
        assertEquals(LOCAL_DATE_TIME_MILLIS_PRECISION, result);
    }

    @Test
    public void testConvertTimestampMicros() {
        final LocalDateTime result = converter.convertField(MICROS_TIMESTAMP_LONG, Optional.empty(), FIELD_NAME);
        assertEquals(MILLIS_TIMESTAMP_LONG, result.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());

        final Instant resultInstant = result.atZone(ZoneId.systemDefault()).toInstant();
        assertEquals(NANOS_AFTER_SECOND, resultInstant.getNano());
    }

    @Test
    public void testDoubleAsEpochSeconds() {
        final LocalDateTime result = converter.convertField(MICROS_TIMESTAMP_DOUBLE, Optional.empty(), FIELD_NAME);
        assertEquals(LOCAL_DATE_TIME_MICROS_PRECISION, result);
        assertEquals(NANOS_AFTER_SECOND, result.getNano(), 1D);
    }

    @Test
    public void testDoubleAsEpochSecondsAsString() {
        final LocalDateTime result = converter.convertField(MICROS_TIMESTAMP_STRING, Optional.empty(), FIELD_NAME);
        assertEquals(LOCAL_DATE_TIME_MICROS_PRECISION, result);
        final double expectedNanos = 351567000L;
        assertEquals(expectedNanos, result.getNano(), 1D);
    }

    @Test
    public void testWithDateFormatMillisPrecision() {
        final long millis = System.currentTimeMillis();
        final LocalDateTime result = converter.convertField(millis, Optional.of("yyyy-MM-dd'T'HH:mm:ss.SSS"), FIELD_NAME);
        assertEquals(LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.systemDefault()), result);
    }

    @Test
    public void testWithDateFormatMicrosecondPrecision() {
        final LocalDateTime result = converter.convertField(MICROS_TIMESTAMP_LONG, Optional.of("yyyy-MM-dd'T'HH:mm:ss.SSSSSS"), FIELD_NAME);
        assertEquals(LOCAL_DATE_TIME_MICROS_PRECISION, result);
    }
}
