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

package org.apache.nifi.time;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestDurationFormat {

    @ParameterizedTest
    @MethodSource("getParse")
    public void testParse(String value, TimeUnit desiredUnit, Long expected) {
        assertEquals(expected, DurationFormat.getTimeDuration(value, desiredUnit));
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
        assertEquals(7L, DurationFormat.getTimeDuration(week, TimeUnit.DAYS));
    }


    @ParameterizedTest
    @ValueSource(strings = {"-1 week", "-1 wk", "-1 w", "-1 weeks", "- 1 week"})
    public void testGetTimeDurationShouldHandleNegativeWeeks(String week) {
        IllegalArgumentException iae =
            assertThrows(IllegalArgumentException.class, () -> DurationFormat.getTimeDuration(week, TimeUnit.DAYS));
        assertTrue(iae.getMessage().contains("Value '" + week + "' is not a valid time duration"));
    }


    @ParameterizedTest
    @ValueSource(strings = {"1 work", "1 wek", "1 k"})
    public void testGetTimeDurationShouldHandleInvalidAbbreviations(String week) {
        IllegalArgumentException iae =
            assertThrows(IllegalArgumentException.class, () -> DurationFormat.getTimeDuration(week, TimeUnit.DAYS));
        assertTrue(iae.getMessage().contains("Value '" + week + "' is not a valid time duration"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"1week", "1wk", "1w", "1wks", "1weeks"})
    public void testGetTimeDurationShouldHandleNoSpaceInInput(String week) {
        assertEquals(7L, DurationFormat.getTimeDuration(week, TimeUnit.DAYS));
    }


    @ParameterizedTest
    @ValueSource(strings = {"10 ms", "10 millis", "10 milliseconds"})
    public void testGetTimeDurationWithWholeNumbers(String whole) {
        assertEquals(10L, DurationFormat.getTimeDuration(whole, TimeUnit.MILLISECONDS));
    }


    @ParameterizedTest
    @ValueSource(strings = {"0.010 s", "0.010 seconds"})
    public void testGetTimeDurationWithDecimalNumbers(String decimal) {
        assertEquals(10L, DurationFormat.getTimeDuration(decimal, TimeUnit.MILLISECONDS));
    }

    @ParameterizedTest
    @MethodSource("getOneWeekInOtherUnits")
    public void testGetPreciseTimeDurationShouldHandleWeeks(TimeUnit timeUnit, long expected) {
        assertEquals(expected,  DurationFormat.getPreciseTimeDuration("1 week", timeUnit));
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

    @ParameterizedTest
    @MethodSource("getOneAndAHalfWeeksInOtherUnits")
    public void testGetPreciseTimeDurationShouldHandleDecimalWeeks(TimeUnit timeUnit, double expected) {
        assertEquals(expected, DurationFormat.getPreciseTimeDuration("1.5 week", timeUnit));
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
    @ValueSource(strings = {"10 ms", "10 millis", "10 milliseconds"})
    public void testGetPreciseTimeDurationWithWholeNumbers(String whole) {
        assertEquals(10.0, DurationFormat.getPreciseTimeDuration(whole, TimeUnit.MILLISECONDS));
    }

    @ParameterizedTest
    @ValueSource(strings = {"0.010 s", "0.010 seconds"})
    public void testGetPreciseTimeDurationWithDecimalNumbers(String decimal) {
        assertEquals(10.0, DurationFormat.getPreciseTimeDuration(decimal, TimeUnit.MILLISECONDS));
    }

    @ParameterizedTest
    @MethodSource("getTinyDecimalValues")
    public void testGetPreciseTimeDurationShouldHandleSmallDecimalValues(String originalValue, TimeUnit desiredUnit, double expectedValue) {
        assertEquals(expectedValue, DurationFormat.getPreciseTimeDuration(originalValue, desiredUnit));
    }

    private static Stream<Arguments> getTinyDecimalValues() {
        return Stream.of(Arguments.of("123.4 " + TimeUnit.NANOSECONDS.name(), TimeUnit.NANOSECONDS, 123.0),
            Arguments.of("0.9 " + TimeUnit.NANOSECONDS.name(), TimeUnit.NANOSECONDS, 1.0),
            Arguments.of("0.9 " + TimeUnit.NANOSECONDS.name(), TimeUnit.MILLISECONDS, 0.0),
            Arguments.of("123.4 " + TimeUnit.MILLISECONDS.name(), TimeUnit.NANOSECONDS, 123_400_000.0));
    }

    @ParameterizedTest
    @EnumSource(TimeUnit.class)
    public void testMakeWholeNumberTimeShouldHandleWholeNumbers(TimeUnit timeUnit) {
        assertEquals(Arrays.asList(10L, timeUnit), DurationFormat.makeWholeNumberTime(10.0, timeUnit));
    }

    @ParameterizedTest
    @MethodSource("getNanoSecondsForMakeWholeNumber")
    public void testMakeWholeNumberTimeShouldHandleNanoseconds(double original, long expected) {
        assertEquals(Arrays.asList(expected, TimeUnit.NANOSECONDS),
            DurationFormat.makeWholeNumberTime(original, TimeUnit.NANOSECONDS));
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
            () -> DurationFormat.getSmallerTimeUnit(timeUnit));

        assertEquals("Cannot determine a smaller time unit than 'null'", iae.getMessage());
    }

    @Test
    public void testGetSmallerTimeUnitWithNanoseconds() {
        IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
            () -> DurationFormat.getSmallerTimeUnit(TimeUnit.NANOSECONDS));

        assertEquals("Cannot determine a smaller time unit than 'NANOSECONDS'", iae.getMessage());
    }

    @ParameterizedTest
    @MethodSource("getSmallerUnits")
    public void testShouldGetSmallerTimeUnit(TimeUnit original, TimeUnit expected) {
        assertEquals(expected, DurationFormat.getSmallerTimeUnit(original));
    }

    private static Stream<Arguments> getSmallerUnits() {
        return Stream.of(Arguments.of(TimeUnit.MICROSECONDS, TimeUnit.NANOSECONDS),
            Arguments.of(TimeUnit.MILLISECONDS, TimeUnit.MICROSECONDS),
            Arguments.of(TimeUnit.SECONDS, TimeUnit.MILLISECONDS),
            Arguments.of(TimeUnit.MINUTES, TimeUnit.SECONDS),
            Arguments.of(TimeUnit.HOURS, TimeUnit.MINUTES),
            Arguments.of(TimeUnit.DAYS, TimeUnit.HOURS));
    }

    @ParameterizedTest
    @MethodSource("getMultipliers")
    void testShouldCalculateMultiplier(TimeUnit original, TimeUnit newTimeUnit, long expected) {
        assertEquals(expected, DurationFormat.calculateMultiplier(original, newTimeUnit));
    }

    private static Stream<Arguments> getMultipliers() {
        return Stream.of(Arguments.of(TimeUnit.DAYS, TimeUnit.NANOSECONDS, 24 * 60 * 60 * 1_000_000_000L),
            Arguments.of(TimeUnit.MICROSECONDS, TimeUnit.NANOSECONDS, 1_000L),
            Arguments.of(TimeUnit.MILLISECONDS, TimeUnit.NANOSECONDS, 1_000_000L),
            Arguments.of(TimeUnit.MILLISECONDS, TimeUnit.MICROSECONDS, 1_000L),
            Arguments.of(TimeUnit.DAYS, TimeUnit.HOURS, 24L),
            Arguments.of(TimeUnit.DAYS, TimeUnit.SECONDS, 24 * 60 * 60L));
    }

    @ParameterizedTest
    @MethodSource("getIncorrectUnits")
    public void testCalculateMultiplierShouldHandleIncorrectUnits(TimeUnit original, TimeUnit newTimeUnit) {
        IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
            () -> DurationFormat.calculateMultiplier(original, newTimeUnit));
        assertTrue(iae.getMessage().matches("The original time unit '.*' must be larger than the new time unit '.*'"));
    }

    private static Stream<Arguments> getIncorrectUnits() {
        return Stream.of(Arguments.of(TimeUnit.NANOSECONDS, TimeUnit.DAYS),
            Arguments.of(TimeUnit.NANOSECONDS, TimeUnit.MICROSECONDS),
            Arguments.of(TimeUnit.HOURS, TimeUnit.DAYS));
    }


    @ParameterizedTest
    @MethodSource("getDecimalsForMakeWholeNumber")
    public void testMakeWholeNumberTimeShouldHandleDecimals(double decimal, TimeUnit timeUnit) {
        assertEquals(Arrays.asList(10L, TimeUnit.NANOSECONDS), DurationFormat.makeWholeNumberTime(decimal, timeUnit));
    }

    @ParameterizedTest
    @MethodSource("getDecimalsForMetricConversions")
    public void testMakeWholeNumberTimeShouldHandleMetricConversions(double originalValue, TimeUnit originalUnits, long expectedValue, TimeUnit expectedUnits) {
        assertEquals(Arrays.asList(expectedValue, expectedUnits),
            DurationFormat.makeWholeNumberTime(originalValue, originalUnits));
    }

    private static Stream<Arguments> getDecimalsForMetricConversions() {
        return Stream.of(Arguments.of(123.4, TimeUnit.SECONDS, 123_400L, TimeUnit.MILLISECONDS),
            Arguments.of(1.000_345, TimeUnit.SECONDS, 1_000_345L, TimeUnit.MICROSECONDS),
            Arguments.of(0.75, TimeUnit.MILLISECONDS, 750L, TimeUnit.MICROSECONDS),
            Arguments.of(123.4, TimeUnit.NANOSECONDS, 123L, TimeUnit.NANOSECONDS),
            Arguments.of(0.123, TimeUnit.NANOSECONDS, 1L, TimeUnit.NANOSECONDS));
    }

    @ParameterizedTest
    @MethodSource("getDecimalsForNonMetricConversions")
    public void testMakeWholeNumberTimeShouldHandleNonMetricConversions(double originalValue, TimeUnit originalUnits, long expectedValue, TimeUnit expectedUnits) {
        assertEquals(Arrays.asList(expectedValue, expectedUnits),
            DurationFormat.makeWholeNumberTime(originalValue, originalUnits));
    }

    private static Stream<Arguments> getDecimalsForNonMetricConversions() {
        return Stream.of(Arguments.of(1.5, TimeUnit.DAYS, 36L, TimeUnit.HOURS),
            Arguments.of(1.5, TimeUnit.HOURS, 90L, TimeUnit.MINUTES),
            Arguments.of(.75, TimeUnit.HOURS, 45L, TimeUnit.MINUTES));
    }

    private static Stream<Arguments> getDecimalsForMakeWholeNumber() {
        return Stream.of(Arguments.of(0.000_000_010, TimeUnit.SECONDS),
            Arguments.of(0.000_010, TimeUnit.MILLISECONDS),
            Arguments.of(0.010, TimeUnit.MICROSECONDS)
        );
    }
}
