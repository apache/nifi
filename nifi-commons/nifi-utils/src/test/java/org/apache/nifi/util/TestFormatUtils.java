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
package org.apache.nifi.util;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.text.DecimalFormatSymbols;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestFormatUtils {
    private static final String NEW_YORK_TIME_ZONE_ID = "America/New_York";
    private static final String KIEV_TIME_ZONE_ID = "Europe/Kiev";
    private static final String UTC_TIME_ZONE_ID = ZoneOffset.UTC.getId();


    @ParameterizedTest
    @MethodSource("getParseToInstantUsingFormatterWithoutZones")
    public void testParseToInstantUsingFormatterWithoutZones(String pattern, String parsedDateTime, String systemDefaultZoneId, String expectedUtcDateTime) {
        checkSameResultsWithFormatter(pattern, parsedDateTime, systemDefaultZoneId, null, expectedUtcDateTime);
    }

    private static Stream<Arguments> getParseToInstantUsingFormatterWithoutZones() {
        return Stream.of(/*GMT-*/
            Arguments.of("yyyy-MM-dd HH:mm:ss", "2020-01-01 02:00:00", NEW_YORK_TIME_ZONE_ID, "2020-01-01T07:00:00"),
            Arguments.of("yyyy-MM-dd", "2020-01-01", NEW_YORK_TIME_ZONE_ID, "2020-01-01T05:00:00"),
            Arguments.of("HH:mm:ss", "03:00:00", NEW_YORK_TIME_ZONE_ID, "1970-01-01T08:00:00"),
            Arguments.of("yyyy-MMM-dd", "2020-may-01", NEW_YORK_TIME_ZONE_ID, "2020-05-01T04:00:00"),
            /*GMT+*/
            Arguments.of("yyyy-MM-dd HH:mm:ss", "2020-01-01 02:00:00", KIEV_TIME_ZONE_ID, "2020-01-01T00:00:00"),
            Arguments.of("yyyy-MM-dd", "2020-01-01", KIEV_TIME_ZONE_ID, "2019-12-31T22:00:00"),
            Arguments.of("HH:mm:ss", "03:00:00", KIEV_TIME_ZONE_ID, "1970-01-01T00:00:00"),
            Arguments.of("yyyy-MMM-dd", "2020-may-01", KIEV_TIME_ZONE_ID, "2020-04-30T21:00:00"),
            /*UTC*/
            Arguments.of("yyyy-MM-dd HH:mm:ss", "2020-01-01 02:00:00", ZoneOffset.UTC.getId(), "2020-01-01T02:00:00"),
            Arguments.of("yyyy-MM-dd", "2020-01-01", ZoneOffset.UTC.getId(), "2020-01-01T00:00:00"),
            Arguments.of("HH:mm:ss", "03:00:00", ZoneOffset.UTC.getId(), "1970-01-01T03:00:00"),
            Arguments.of("yyyy-MMM-dd", "2020-may-01", ZoneOffset.UTC.getId(), "2020-05-01T00:00:00"));
    }

    @ParameterizedTest
    @MethodSource("getParseToInstantUsingFormatterWithZone")
    public void testParseToInstantUsingFormatterWithZone(String pattern, String parsedDateTime, String systemDefaultZoneId, String formatZoneId, String expectedUtcDateTime) {
        checkSameResultsWithFormatter(pattern, parsedDateTime, systemDefaultZoneId, formatZoneId, expectedUtcDateTime);
    }

    private static Stream<Arguments> getParseToInstantUsingFormatterWithZone() {
        String pattern = "yyyy-MM-dd HH:mm:ss";
        String parsedDateTime = "2020-01-01 02:00:00";
        return Stream.of(Arguments.of(pattern, parsedDateTime, NEW_YORK_TIME_ZONE_ID, NEW_YORK_TIME_ZONE_ID, "2020-01-01T07:00:00"),
            Arguments.of(pattern, parsedDateTime, NEW_YORK_TIME_ZONE_ID, KIEV_TIME_ZONE_ID, "2020-01-01T00:00:00"),
            Arguments.of(pattern, parsedDateTime, NEW_YORK_TIME_ZONE_ID, UTC_TIME_ZONE_ID, "2020-01-01T02:00:00"),
            Arguments.of(pattern, parsedDateTime, KIEV_TIME_ZONE_ID, NEW_YORK_TIME_ZONE_ID, "2020-01-01T07:00:00"),
            Arguments.of(pattern, parsedDateTime, KIEV_TIME_ZONE_ID, KIEV_TIME_ZONE_ID, "2020-01-01T00:00:00"),
            Arguments.of(pattern, parsedDateTime, KIEV_TIME_ZONE_ID, UTC_TIME_ZONE_ID, "2020-01-01T02:00:00"),
            Arguments.of(pattern, parsedDateTime, UTC_TIME_ZONE_ID, NEW_YORK_TIME_ZONE_ID, "2020-01-01T07:00:00"),
            Arguments.of(pattern, parsedDateTime, UTC_TIME_ZONE_ID, KIEV_TIME_ZONE_ID, "2020-01-01T00:00:00"),
            Arguments.of(pattern, parsedDateTime, UTC_TIME_ZONE_ID, UTC_TIME_ZONE_ID, "2020-01-01T02:00:00"));
    }

    @ParameterizedTest
    @MethodSource("getFormatDataSize")
    public void testFormatDataSize(double dataSize, String expected) {
        assertEquals(expected, FormatUtils.formatDataSize(dataSize));
    }

    private static Stream<Arguments> getFormatDataSize() {
        DecimalFormatSymbols decimalFormatSymbols = DecimalFormatSymbols.getInstance();
        char decimalSeparator = decimalFormatSymbols.getDecimalSeparator();
        char groupingSeparator = decimalFormatSymbols.getGroupingSeparator();

        return Stream.of(Arguments.of(0d, "0 bytes"),
            Arguments.of(10.4d, String.format("10%s4 bytes", decimalSeparator)),
            Arguments.of(1024d, String.format("1%s024 bytes", groupingSeparator)),
            Arguments.of(1025d, "1 KB"),
            Arguments.of(2000d, String.format("1%s95 KB", decimalSeparator)),
            Arguments.of(200_000d, String.format("195%s31 KB", decimalSeparator)),
            Arguments.of(200_000_000d, String.format("190%s73 MB", decimalSeparator)),
            Arguments.of(200_000_000_000d, String.format("186%s26 GB", decimalSeparator)),
            Arguments.of(200_000_000_000_000d, String.format("181%s9 TB", decimalSeparator)));
    }

    @ParameterizedTest
    @MethodSource("getFormatNanos")
    public void testFormatNanos(long nanos, boolean includeTotalNanos, String expected) {
        assertEquals(expected, FormatUtils.formatNanos(nanos, includeTotalNanos));
    }

    private static Stream<Arguments> getFormatNanos() {
        return Stream.of(Arguments.of(0L, false, "0 nanos"),
            Arguments.of(0L, true, "0 nanos (0 nanos)"),
            Arguments.of(1_000_000L, false, "1 millis, 0 nanos"),
            Arguments.of(1_000_000L, true, "1 millis, 0 nanos (1000000 nanos)"),
            Arguments.of(1_000_001L, false, "1 millis, 1 nanos"),
            Arguments.of(1_000_001L, true, "1 millis, 1 nanos (1000001 nanos)"),
            Arguments.of(1_000_000_000L, false, "1 seconds, 0 millis, 0 nanos"),
            Arguments.of(1_000_000_000L, true, "1 seconds, 0 millis, 0 nanos (1000000000 nanos)"),
            Arguments.of(1_001_000_000L, false, "1 seconds, 1 millis, 0 nanos"),
            Arguments.of(1_001_000_000L, true, "1 seconds, 1 millis, 0 nanos (1001000000 nanos)"),
            Arguments.of(1_001_000_001L, false, "1 seconds, 1 millis, 1 nanos"),
            Arguments.of(1_001_000_001L, true, "1 seconds, 1 millis, 1 nanos (1001000001 nanos)"));
    }

    @ParameterizedTest
    @MethodSource("getParseToInstantWithZonePassedInText")
    public void testParseToInstantWithZonePassedInText(String pattern, String parsedDateTime, String systemDefaultZoneId, String expectedUtcDateTime) {
        checkSameResultsWithFormatter(pattern, parsedDateTime, systemDefaultZoneId, null, expectedUtcDateTime);
    }

    private static Stream<Arguments> getParseToInstantWithZonePassedInText() {
        String pattern = "yyyy-MM-dd HH:mm:ss Z";
        return Stream.of(Arguments.of(pattern, "2020-01-01 02:00:00 -0100", NEW_YORK_TIME_ZONE_ID, "2020-01-01T03:00:00"),
            Arguments.of(pattern, "2020-01-01 02:00:00 +0100", NEW_YORK_TIME_ZONE_ID, "2020-01-01T01:00:00"),
            Arguments.of(pattern, "2020-01-01 02:00:00 +0000", NEW_YORK_TIME_ZONE_ID, "2020-01-01T02:00:00"),
            Arguments.of(pattern, "2020-01-01 02:00:00 -0100", KIEV_TIME_ZONE_ID, "2020-01-01T03:00:00"),
            Arguments.of(pattern, "2020-01-01 02:00:00 +0100", KIEV_TIME_ZONE_ID, "2020-01-01T01:00:00"),
            Arguments.of(pattern, "2020-01-01 02:00:00 +0000", KIEV_TIME_ZONE_ID, "2020-01-01T02:00:00"),
            Arguments.of(pattern, "2020-01-01 02:00:00 -0100", UTC_TIME_ZONE_ID, "2020-01-01T03:00:00"),
            Arguments.of(pattern, "2020-01-01 02:00:00 +0100", UTC_TIME_ZONE_ID, "2020-01-01T01:00:00"),
            Arguments.of(pattern, "2020-01-01 02:00:00 +0000", UTC_TIME_ZONE_ID, "2020-01-01T02:00:00"));
    }

    private void checkSameResultsWithFormatter(String pattern, String parsedDateTime, String systemDefaultZoneId, String formatZoneId, String expectedUtcDateTime) {
        TimeZone current = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone(systemDefaultZoneId));
        try {
            checkSameResultsWithFormatter(pattern, parsedDateTime, formatZoneId, expectedUtcDateTime);
        } finally {
            TimeZone.setDefault(current);
        }
    }

    private void checkSameResultsWithFormatter(String pattern, String parsedDateTime, String formatterZoneId, String expectedUtcDateTime) {
        Instant expectedInstant = LocalDateTime.parse(expectedUtcDateTime).atZone(ZoneOffset.UTC).toInstant();

        // current implementation
        DateTimeFormatter dtf = FormatUtils.prepareLenientCaseInsensitiveDateTimeFormatter(pattern);
        if (formatterZoneId != null) {
            dtf = dtf.withZone(ZoneId.of(formatterZoneId));
        }
        Instant result = FormatUtils.parseToInstant(dtf, parsedDateTime);
        assertEquals(expectedInstant, result);
    }

    @ParameterizedTest
    @MethodSource("getFormatTime")
    public void testFormatTime(long sourceDuration, TimeUnit sourceUnit, String expected) {
        assertEquals(expected, FormatUtils.formatHoursMinutesSeconds(sourceDuration, sourceUnit));
    }

    private static Stream<Arguments> getFormatTime() {
        return Stream.of(Arguments.of(0L, TimeUnit.DAYS, "00:00:00.000"),
            Arguments.of(1L, TimeUnit.HOURS, "01:00:00.000"),
            Arguments.of(2L, TimeUnit.HOURS, "02:00:00.000"),
            Arguments.of(1L, TimeUnit.MINUTES, "00:01:00.000"),
            Arguments.of(10L, TimeUnit.SECONDS, "00:00:10.000"),
            Arguments.of(777L, TimeUnit.MILLISECONDS, "00:00:00.777"),
            Arguments.of(7777, TimeUnit.MILLISECONDS, "00:00:07.777"),
            Arguments.of(TimeUnit.MILLISECONDS.convert(20, TimeUnit.HOURS)
                         + TimeUnit.MILLISECONDS.convert(11, TimeUnit.MINUTES)
                         + TimeUnit.MILLISECONDS.convert(36, TimeUnit.SECONDS)
                         + TimeUnit.MILLISECONDS.convert(897, TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS, "20:11:36.897"),
            Arguments.of(TimeUnit.MILLISECONDS.convert(999, TimeUnit.HOURS)
                         + TimeUnit.MILLISECONDS.convert(60, TimeUnit.MINUTES)
                         + TimeUnit.MILLISECONDS.convert(60, TimeUnit.SECONDS)
                         + TimeUnit.MILLISECONDS.convert(1001, TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS, "1000:01:01.001"));
    }
}
