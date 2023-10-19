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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.text.DecimalFormatSymbols;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestFormatUtils {
    private static final String NEW_YORK_TIME_ZONE_ID = "America/New_York";
    private static final String KIEV_TIME_ZONE_ID = "Europe/Kiev";
    private static final String UTC_TIME_ZONE_ID = ZoneOffset.UTC.getId();

    @ParameterizedTest
    @MethodSource("getParse")
    public void testParse(String value, TimeUnit desiredUnit, Long expected) {
        assertEquals(expected, FormatUtils.getTimeDuration(value, desiredUnit));
    }

    private static Stream<Arguments> getParse() {
        return Stream.of(Arguments.of("3000 ms", TimeUnit.SECONDS, 3L),
                Arguments.of("3000 s", TimeUnit.SECONDS, 3000L),
                Arguments.of("999 millis", TimeUnit.SECONDS, 0L),
                Arguments.of("4 days", TimeUnit.NANOSECONDS, 4L * 24L * 60L * 60L * 1000000000L),
                Arguments.of("1 DAY", TimeUnit.HOURS, 24L),
                Arguments.of("1 hr", TimeUnit.MINUTES, 60L),
                Arguments.of("1 Hrs", TimeUnit.MINUTES, 60L));
    }

    @ParameterizedTest
    @ValueSource(strings = {"1 week", "1 wk", "1 w", "1 wks", "1 weeks"})
    public void testGetTimeDurationShouldConvertWeeks(String week) {
        assertEquals(7L, FormatUtils.getTimeDuration(week, TimeUnit.DAYS));
    }


    @ParameterizedTest
    @ValueSource(strings = {"-1 week", "-1 wk", "-1 w", "-1 weeks", "- 1 week"})
    public void testGetTimeDurationShouldHandleNegativeWeeks(String week) {
        IllegalArgumentException iae =
                assertThrows(IllegalArgumentException.class, () -> FormatUtils.getTimeDuration(week, TimeUnit.DAYS));
        assertTrue(iae.getMessage().contains("Value '" + week + "' is not a valid time duration"));
    }

    /**
     * Regression test
     */

    @ParameterizedTest
    @ValueSource(strings = {"1 work", "1 wek", "1 k"})
    public void testGetTimeDurationShouldHandleInvalidAbbreviations(String week) {
        IllegalArgumentException iae =
                assertThrows(IllegalArgumentException.class, () -> FormatUtils.getTimeDuration(week, TimeUnit.DAYS));
        assertTrue(iae.getMessage().contains("Value '" + week + "' is not a valid time duration"));
    }

    /**
     * New feature test
     */

    @ParameterizedTest
    @ValueSource(strings={"1week", "1wk", "1w", "1wks", "1weeks"})
    public void testGetTimeDurationShouldHandleNoSpaceInInput(String week) {
        assertEquals(7L, FormatUtils.getTimeDuration(week, TimeUnit.DAYS));
    }

    /**
     * New feature test
     */

    @ParameterizedTest
    @ValueSource(strings={"10 ms", "10 millis", "10 milliseconds"})
    public void testGetTimeDurationWithWholeNumbers(String whole){
        assertEquals(10L, FormatUtils.getTimeDuration(whole, TimeUnit.MILLISECONDS));
    }

    /**
     * New feature test
     */

    @ParameterizedTest
    @ValueSource(strings={"0.010 s", "0.010 seconds"})
    public void testGetTimeDurationWithDecimalNumbers(String decimal){
        assertEquals(10L, FormatUtils.getTimeDuration(decimal, TimeUnit.MILLISECONDS));
    }

    /**
     * Regression test for custom week logic
     */
    @ParameterizedTest
    @MethodSource("getOneWeekInOtherUnits")
    public void testGetPreciseTimeDurationShouldHandleWeeks(TimeUnit timeUnit, long expected){
        assertEquals(expected,  FormatUtils.getPreciseTimeDuration("1 week", timeUnit));
    }

    private static Stream<Arguments> getOneWeekInOtherUnits() {
        return Stream.of(Arguments.of(TimeUnit.DAYS, 7L),
                Arguments.of(TimeUnit.HOURS, 7 * 24L),
                Arguments.of(TimeUnit.MINUTES, 7 * 24 * 60L),
                Arguments.of(TimeUnit.SECONDS, 7 * 24 * 60 * 60L),
                Arguments.of(TimeUnit.MILLISECONDS, 7 * 24 * 60 * 60 * 1000L),
                Arguments.of(TimeUnit.MICROSECONDS, 7 * 24 * 60 * 60 * 1000 * 1000L),
                Arguments.of(TimeUnit.NANOSECONDS, 7 * 24 * 60 * 60 * (1000 * 1000 * 1000L))
        );
    }

    /**
     * Positive flow test for custom week logic with decimal value
     */
    @ParameterizedTest
    @MethodSource("getOneAndAHalfWeeksInOtherUnits")
    public void testGetPreciseTimeDurationShouldHandleDecimalWeeks(TimeUnit timeUnit, double expected) {
        assertEquals(expected, FormatUtils.getPreciseTimeDuration("1.5 week", timeUnit));
    }

    private static Stream<Arguments> getOneAndAHalfWeeksInOtherUnits() {
        final double oneAndAHalf = 1.5;
        return Stream.of(Arguments.of(TimeUnit.HOURS, 7 * 24 * oneAndAHalf),
                Arguments.of(TimeUnit.MINUTES, 7 * 24 * 60 * oneAndAHalf),
                Arguments.of(TimeUnit.SECONDS, 7 * 24 * 60 * 60 * oneAndAHalf),
                Arguments.of(TimeUnit.MILLISECONDS, 7 * 24 * 60 * 60 * 1000 * oneAndAHalf),
                Arguments.of(TimeUnit.MICROSECONDS, 7 * 24 * 60 * 60 * (1000 * 1000L) * oneAndAHalf),
                Arguments.of(TimeUnit.NANOSECONDS, 7 * 24 * 60 * 60 * (1000 * 1000 * 1000L) * oneAndAHalf));
    }

    @ParameterizedTest
    @ValueSource(strings={"10 ms", "10 millis", "10 milliseconds"})
    public void testGetPreciseTimeDurationWithWholeNumbers(String whole) {
        assertEquals(10.0, FormatUtils.getPreciseTimeDuration(whole, TimeUnit.MILLISECONDS));
    }

    /**
     * Positive flow test for decimal time inputs
     */
    @ParameterizedTest
    @ValueSource(strings={"0.010 s", "0.010 seconds"})
    public void testGetPreciseTimeDurationWithDecimalNumbers(String decimal) {
        assertEquals(10.0, FormatUtils.getPreciseTimeDuration(decimal, TimeUnit.MILLISECONDS));
    }

    /**
     * Positive flow test for decimal inputs that are extremely small
     */
    @ParameterizedTest
    @MethodSource("getTinyDecimalValues")
    public void testGetPreciseTimeDurationShouldHandleSmallDecimalValues(String originalValue, TimeUnit desiredUnit, double expectedValue) {
        assertEquals(expectedValue, FormatUtils.getPreciseTimeDuration(originalValue, desiredUnit));
    }

    private static Stream<Arguments> getTinyDecimalValues() {
        return Stream.of(Arguments.of("123.4 " + TimeUnit.NANOSECONDS.name(), TimeUnit.NANOSECONDS, 123.0),
                Arguments.of("0.9 " + TimeUnit.NANOSECONDS.name(), TimeUnit.NANOSECONDS, 1.0),
                Arguments.of("0.9 " + TimeUnit.NANOSECONDS.name(), TimeUnit.MILLISECONDS, 0.0),
                Arguments.of("123.4 " + TimeUnit.MILLISECONDS.name(), TimeUnit.NANOSECONDS, 123_400_000.0));
    }

    /**
     * Positive flow test for decimal inputs that can be converted (all equal values),
     */
    @ParameterizedTest
    @MethodSource("getDecimalsForMakeWholeNumber")
    public void testMakeWholeNumberTimeShouldHandleDecimals(double decimal, TimeUnit timeUnit) {
        assertEquals(Arrays.asList(10L, TimeUnit.NANOSECONDS), FormatUtils.makeWholeNumberTime(decimal, timeUnit));
    }

    private static Stream<Arguments> getDecimalsForMakeWholeNumber() {
        return Stream.of(Arguments.of(0.000_000_010, TimeUnit.SECONDS),
                Arguments.of(0.000_010, TimeUnit.MILLISECONDS),
                Arguments.of(0.010, TimeUnit.MICROSECONDS)
        );
    }

    /**
     * Positive flow test for decimal inputs that can be converted (metric values)
     */
    @ParameterizedTest
    @MethodSource("getDecimalsForMetricConversions")
    public void testMakeWholeNumberTimeShouldHandleMetricConversions(double originalValue, TimeUnit originalUnits, long expectedValue, TimeUnit expectedUnits) {
        assertEquals(Arrays.asList(expectedValue, expectedUnits),
                FormatUtils.makeWholeNumberTime(originalValue, originalUnits));
    }

    private static Stream<Arguments> getDecimalsForMetricConversions() {
        return Stream.of(Arguments.of(123.4, TimeUnit.SECONDS, 123_400L, TimeUnit.MILLISECONDS),
                Arguments.of(1.000_345, TimeUnit.SECONDS, 1_000_345L, TimeUnit.MICROSECONDS),
                Arguments.of(0.75, TimeUnit.MILLISECONDS, 750L, TimeUnit.MICROSECONDS),
                Arguments.of(123.4, TimeUnit.NANOSECONDS, 123L, TimeUnit.NANOSECONDS),
                Arguments.of(0.123, TimeUnit.NANOSECONDS, 1L, TimeUnit.NANOSECONDS));
    }

    /**
     * Positive flow test for decimal inputs that can be converted (non-metric values)
     */
    @ParameterizedTest
    @MethodSource("getDecimalsForNonMetricConversions")
    public void testMakeWholeNumberTimeShouldHandleNonMetricConversions(double originalValue, TimeUnit originalUnits, long expectedValue, TimeUnit expectedUnits) {
        assertEquals(Arrays.asList(expectedValue, expectedUnits),
                FormatUtils.makeWholeNumberTime(originalValue, originalUnits));
    }

    private static Stream<Arguments> getDecimalsForNonMetricConversions() {
        return Stream.of(Arguments.of(1.5, TimeUnit.DAYS, 36L, TimeUnit.HOURS),
                Arguments.of(1.5, TimeUnit.HOURS, 90L, TimeUnit.MINUTES),
                Arguments.of(.75, TimeUnit.HOURS, 45L, TimeUnit.MINUTES));
    }

    /**
     * Positive flow test for whole inputs
     */
    @ParameterizedTest
    @EnumSource(TimeUnit.class)
    public void testMakeWholeNumberTimeShouldHandleWholeNumbers(TimeUnit timeUnit) {
        assertEquals(Arrays.asList(10L, timeUnit), FormatUtils.makeWholeNumberTime(10.0, timeUnit));
    }

    /**
     * Negative flow test for nanosecond inputs (regardless of value, the unit cannot be converted)
     */
    @ParameterizedTest
    @MethodSource("getNanoSecondsForMakeWholeNumber")
    public void testMakeWholeNumberTimeShouldHandleNanoseconds(double original, long expected) {
        assertEquals(Arrays.asList(expected, TimeUnit.NANOSECONDS),
                FormatUtils.makeWholeNumberTime(original, TimeUnit.NANOSECONDS));
    }

    private static Stream<Arguments> getNanoSecondsForMakeWholeNumber() {
        return Stream.of(Arguments.of(1100.0, 1100L),
                Arguments.of(2.1, 2L),
                Arguments.of(1.0, 1L),
                Arguments.of(0.1, 1L));
    }

    @ParameterizedTest
    @NullSource
    public void testGetSmallerTimeUnitWithNull(TimeUnit timeUnit) {
        IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                () -> FormatUtils.getSmallerTimeUnit(timeUnit));

        assertEquals("Cannot determine a smaller time unit than 'null'", iae.getMessage());
    }

    @Test
    public void testGetSmallerTimeUnitWithNanoseconds() {
        IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                () -> FormatUtils.getSmallerTimeUnit(TimeUnit.NANOSECONDS));

        assertEquals("Cannot determine a smaller time unit than 'NANOSECONDS'", iae.getMessage());
    }

    /**
     * Positive flow test for whole inputs
     */
    @ParameterizedTest
    @MethodSource("getSmallerUnits")
    public void testShouldGetSmallerTimeUnit(TimeUnit original, TimeUnit expected) {
        assertEquals(expected, FormatUtils.getSmallerTimeUnit(original));
    }

    private static Stream<Arguments> getSmallerUnits() {
        return Stream.of(Arguments.of(TimeUnit.MICROSECONDS, TimeUnit.NANOSECONDS),
                Arguments.of(TimeUnit.MILLISECONDS, TimeUnit.MICROSECONDS),
                Arguments.of(TimeUnit.SECONDS, TimeUnit.MILLISECONDS),
                Arguments.of(TimeUnit.MINUTES, TimeUnit.SECONDS),
                Arguments.of(TimeUnit.HOURS, TimeUnit.MINUTES),
                Arguments.of(TimeUnit.DAYS, TimeUnit.HOURS));
    }

    /**
     * Positive flow test for multipliers based on valid time units
     */
    @ParameterizedTest
    @MethodSource("getMultipliers")
    void testShouldCalculateMultiplier(TimeUnit original, TimeUnit newTimeUnit, long expected) {
        assertEquals(expected, FormatUtils.calculateMultiplier(original, newTimeUnit));
    }

    private static Stream<Arguments> getMultipliers() {
        return Stream.of(Arguments.of(TimeUnit.DAYS, TimeUnit.NANOSECONDS, 24 * 60 * 60 * 1_000_000_000L),
                Arguments.of(TimeUnit.MICROSECONDS, TimeUnit.NANOSECONDS, 1_000L),
                Arguments.of(TimeUnit.MILLISECONDS, TimeUnit.NANOSECONDS, 1_000_000L),
                Arguments.of(TimeUnit.MILLISECONDS, TimeUnit.MICROSECONDS, 1_000L),
                Arguments.of(TimeUnit.DAYS, TimeUnit.HOURS, 24L),
                Arguments.of(TimeUnit.DAYS, TimeUnit.SECONDS, 24 * 60 * 60L));
    }

    /**
     * Negative flow test for multipliers based on incorrectly-ordered time units
     */
    @ParameterizedTest
    @MethodSource("getIncorrectUnits")
    public void testCalculateMultiplierShouldHandleIncorrectUnits(TimeUnit original, TimeUnit newTimeUnit) {
        IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                () -> FormatUtils.calculateMultiplier(original, newTimeUnit));
        assertTrue(iae.getMessage().matches("The original time unit '.*' must be larger than the new time unit '.*'"));
    }

    private static Stream<Arguments> getIncorrectUnits() {
        return Stream.of(Arguments.of(TimeUnit.NANOSECONDS, TimeUnit.DAYS),
                Arguments.of(TimeUnit.NANOSECONDS, TimeUnit.MICROSECONDS),
                Arguments.of(TimeUnit.HOURS, TimeUnit.DAYS));
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
    @MethodSource("getParseToInstantUsingFormatterWithoutZones")
    public void testParseToInstantUsingFormatterWithoutZones(String pattern, String parsedDateTime, String systemDefaultZoneId, String expectedUtcDateTime) throws Exception {
        checkSameResultsWithSimpleDateFormat(pattern, parsedDateTime, systemDefaultZoneId, null, expectedUtcDateTime);
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
    public void testParseToInstantUsingFormatterWithZone(String pattern, String parsedDateTime, String systemDefaultZoneId, String formatZoneId, String expectedUtcDateTime) throws Exception {
        checkSameResultsWithSimpleDateFormat(pattern, parsedDateTime, systemDefaultZoneId, formatZoneId, expectedUtcDateTime);
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
    @MethodSource("getParseToInstantWithZonePassedInText")
    public void testParseToInstantWithZonePassedInText(String pattern, String parsedDateTime, String systemDefaultZoneId, String expectedUtcDateTime) throws Exception {
        checkSameResultsWithSimpleDateFormat(pattern, parsedDateTime, systemDefaultZoneId, null, expectedUtcDateTime);
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

    private void checkSameResultsWithSimpleDateFormat(String pattern, String parsedDateTime, String systemDefaultZoneId, String formatZoneId, String expectedUtcDateTime) throws Exception {
        TimeZone current = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone(systemDefaultZoneId));
        try {
            checkSameResultsWithSimpleDateFormat(pattern, parsedDateTime, formatZoneId, expectedUtcDateTime);
        } finally {
            TimeZone.setDefault(current);
        }
    }

    private void checkSameResultsWithSimpleDateFormat(String pattern, String parsedDateTime, String formatterZoneId, String expectedUtcDateTime) throws Exception {
        Instant expectedInstant = LocalDateTime.parse(expectedUtcDateTime).atZone(ZoneOffset.UTC).toInstant();

        // reference implementation
        SimpleDateFormat sdf = new SimpleDateFormat(pattern, Locale.US);
        if (formatterZoneId != null) {
            sdf.setTimeZone(TimeZone.getTimeZone(formatterZoneId));
        }
        Instant simpleDateFormatResult = sdf.parse(parsedDateTime).toInstant();
        assertEquals(expectedInstant, simpleDateFormatResult);

        // current implementation
        DateTimeFormatter dtf = FormatUtils.prepareLenientCaseInsensitiveDateTimeFormatter(pattern);
        if (formatterZoneId != null) {
            dtf = dtf.withZone(ZoneId.of(formatterZoneId));
        }
        Instant result = FormatUtils.parseToInstant(dtf, parsedDateTime);
        assertEquals(expectedInstant, result);
    }
}
