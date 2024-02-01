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

    @Test
    public void testConvertTimestampMillis() {
        final ObjectLocalDateTimeFieldConverter converter = new ObjectLocalDateTimeFieldConverter();
        final long now = System.currentTimeMillis();
        final Instant instant = Instant.ofEpochMilli(now);
        final LocalDateTime expected = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());

        final LocalDateTime result = converter.convertField(now, Optional.empty(), "test");
        assertEquals(expected, result);
        assertEquals(now, result.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
    }

    @Test
    public void testConvertTimestampMicros() {
        final ObjectLocalDateTimeFieldConverter converter = new ObjectLocalDateTimeFieldConverter();
        final long now = System.currentTimeMillis();
        final long withMicros =  now * 1000 + 567;

        final LocalDateTime result = converter.convertField(withMicros, Optional.empty(), "test");
        assertEquals(now, result.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());

        final Instant resultInstant = result.atZone(ZoneId.systemDefault()).toInstant();
        final long expectedNanos = now % 1000 * 1_000_000 + 567_000;
        assertEquals(expectedNanos, resultInstant.getNano());
    }

    @Test
    public void testDoubleAsEpochSeconds() {
        final ObjectLocalDateTimeFieldConverter converter = new ObjectLocalDateTimeFieldConverter();
        final long millis = System.currentTimeMillis();
        final double seconds = millis / 1000.0;
        final double withMicros =  seconds + 0.000567;

        final LocalDateTime result = converter.convertField(withMicros, Optional.empty(), "test");
        assertEquals(LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.systemDefault()).toLocalDate(), result.toLocalDate());
        final double expectedNanos = (withMicros - (long) seconds) * 1_000_000_000;
        assertEquals(expectedNanos, result.getNano(), 1000D);
    }

    @Test
    public void testDoubleAsEpochSecondsAsString() {
        final ObjectLocalDateTimeFieldConverter converter = new ObjectLocalDateTimeFieldConverter();
        final long millis = System.currentTimeMillis();
        final double seconds = millis / 1000.0;
        final double withMicros =  seconds + 0.000567;

        final LocalDateTime result = converter.convertField(Double.toString(withMicros), Optional.empty(), "test");
        assertEquals(LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.systemDefault()).toLocalDate(), result.toLocalDate());
        final double expectedNanos = (withMicros - (long) seconds) * 1_000_000_000;
        assertEquals(expectedNanos, result.getNano(), 1000D);
    }

    @Test
    public void testWithDateFormatMillisPrecision() {
        final ObjectLocalDateTimeFieldConverter converter = new ObjectLocalDateTimeFieldConverter();
        final long millis = System.currentTimeMillis();
        final LocalDateTime result = converter.convertField(millis, Optional.of("yyyy-MM-dd'T'HH:mm:ss.SSS"), "test");
        assertEquals(LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.systemDefault()), result);
    }

    @Test
    public void testWIthDateFormatMicrosecondPrecision() {
        final ObjectLocalDateTimeFieldConverter converter = new ObjectLocalDateTimeFieldConverter();
        final long millis = System.currentTimeMillis();
        final long micros = millis * 1000 + 567;
        final LocalDateTime result = converter.convertField(micros, Optional.of("yyyy-MM-dd'T'HH:mm:ss.SSSSSS"), "test");
        assertEquals(LocalDateTime.ofInstant(Instant.ofEpochMilli(millis).plusNanos(567_000), ZoneId.systemDefault()), result);
    }
}
