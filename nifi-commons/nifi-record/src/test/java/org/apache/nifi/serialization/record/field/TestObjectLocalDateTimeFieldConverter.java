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

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestObjectLocalDateTimeFieldConverter {
    private static final String FIELD_NAME = "test";

    private static final long MILLIS_TIMESTAMP_LONG = 1707238288351L;
    private static final long NANO_SECONDS_FROM_TIMESTAMP = (MILLIS_TIMESTAMP_LONG % 1000) * 1_000_000L;
    private static final long EXTRA_NANO_SECONDS = 351567L;

    private static final BigDecimal MILLIS_TIMESTAMP = BigDecimal.valueOf(MILLIS_TIMESTAMP_LONG);
    private static final BigDecimal MILLIS_TIMESTAMP_FRACTIONAL = MILLIS_TIMESTAMP.add(BigDecimal.valueOf(EXTRA_NANO_SECONDS, 6));

    private static final String MILLIS_TIMESTAMP_STRING = Long.toString(MILLIS_TIMESTAMP_LONG);
    private static final String MILLIS_TIMESTAMP_FRACTIONAL_STRING = MILLIS_TIMESTAMP_FRACTIONAL.toString();

    private static final Instant INSTANT_MILLIS_PRECISION = Instant.ofEpochMilli(MILLIS_TIMESTAMP_LONG);
    private static final Instant INSTANT_MICROS_PRECISION = Instant.ofEpochMilli(MILLIS_TIMESTAMP_LONG).plusNanos(EXTRA_NANO_SECONDS);

    private static final LocalDateTime LOCAL_DATE_TIME_MILLIS_PRECISION = LocalDateTime.ofInstant(INSTANT_MILLIS_PRECISION, ZoneId.systemDefault());
    private static final LocalDateTime LOCAL_DATE_TIME_MICROS_PRECISION = LocalDateTime.ofInstant(INSTANT_MICROS_PRECISION, ZoneId.systemDefault());

    private final ObjectLocalDateTimeFieldConverter converter = new ObjectLocalDateTimeFieldConverter();


    @Test
    public void testConvertTimestampLong() {
        final LocalDateTime result = converter.convertField(MILLIS_TIMESTAMP_LONG, Optional.empty(), FIELD_NAME);
        assertEquals(LOCAL_DATE_TIME_MILLIS_PRECISION, result);
    }

    @Test
    public void testConvertTimestampString() {
        final LocalDateTime result = converter.convertField(MILLIS_TIMESTAMP_STRING, Optional.empty(), FIELD_NAME);
        assertEquals(LOCAL_DATE_TIME_MILLIS_PRECISION, result);
    }

    @Test
    public void testConvertTimestampBigDecimal() {
        final LocalDateTime result = converter.convertField(MILLIS_TIMESTAMP_FRACTIONAL, Optional.empty(), FIELD_NAME);
        assertEquals(LOCAL_DATE_TIME_MICROS_PRECISION, result);
        assertEquals(NANO_SECONDS_FROM_TIMESTAMP + EXTRA_NANO_SECONDS, result.getNano());
    }

    @Test
    public void testConvertTimestampStringPrecise() {
        final LocalDateTime result = converter.convertField(MILLIS_TIMESTAMP_FRACTIONAL_STRING, Optional.empty(), FIELD_NAME);
        assertEquals(LOCAL_DATE_TIME_MICROS_PRECISION, result);
    }

    @Test
    public void testConvertTimestampDouble() {
        // Less precise timestamp than other tests as double is less precise than BigDecimal
        final double timestamp = 1764673335503.607;

        final BigDecimal bd = BigDecimal.valueOf(timestamp);
        final BigDecimal[] parts = bd.divideAndRemainder(BigDecimal.ONE);

        final long millis = parts[0].longValueExact();
        final long nanos = parts[1].multiply(BigDecimal.valueOf(1_000_000)).longValue();

        final Instant instant = Instant.ofEpochMilli(millis).plusNanos(nanos);
        final LocalDateTime date = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        final LocalDateTime result = converter.convertField(timestamp, Optional.empty(), FIELD_NAME);

        assertEquals(date, result);
    }

    @Test
    public void testConvertTimestampOversized() {
        // Ensure we truncate extra fractional digits beyond what Java time can represent
        final String timestamp = MILLIS_TIMESTAMP_FRACTIONAL_STRING + "123456";
        final LocalDateTime result = converter.convertField(timestamp, Optional.empty(), FIELD_NAME);
        assertEquals(LOCAL_DATE_TIME_MICROS_PRECISION, result);
    }

    @Test
    public void testWithDateFormatMillisPrecision() {
        final long millis = System.currentTimeMillis();
        final LocalDateTime result = converter.convertField(millis, Optional.of("yyyy-MM-dd'T'HH:mm:ss.SSS"), FIELD_NAME);
        assertEquals(LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.systemDefault()), result);
    }

    @Test
    public void testWithDateFormatMillisecond() {
        final LocalDateTime result = converter.convertField(MILLIS_TIMESTAMP_STRING, Optional.of("yyyy-MM-dd'T'HH:mm:ss.SSSSSS"), FIELD_NAME);
        assertEquals(LOCAL_DATE_TIME_MILLIS_PRECISION, result);
    }

    @Test
    public void testWithDateFormatMillisecondPrecision() {
        final LocalDateTime result = converter.convertField(MILLIS_TIMESTAMP_FRACTIONAL_STRING, Optional.of("yyyy-MM-dd'T'HH:mm:ss.SSSSSS"), FIELD_NAME);
        assertEquals(LOCAL_DATE_TIME_MICROS_PRECISION, result);
    }
}
