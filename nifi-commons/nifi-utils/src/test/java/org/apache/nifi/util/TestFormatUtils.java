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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestFormatUtils {
    @SuppressWarnings("deprecation")
    @Test
    public void testGetTimeDurationShouldConvertWeeks() {
        final List<String> WEEKS = List.of("1 week", "1 wk", "1 w", "1 wks", "1 weeks");
        final long EXPECTED_DAYS = 7L;

        List<Long> days = WEEKS.stream()
                .map(week -> FormatUtils.getTimeDuration(week, TimeUnit.DAYS))
                .collect(Collectors.toList());

        days.forEach(it -> assertEquals(EXPECTED_DAYS, it));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testGetTimeDurationShouldHandleNegativeWeeks() {
        List<String> WEEKS = List.of("-1 week", "-1 wk", "-1 w", "-1 weeks", "- 1 week");

        WEEKS.forEach(week -> {
            IllegalArgumentException iae =
                    assertThrows(IllegalArgumentException.class, () -> FormatUtils.getTimeDuration(week, TimeUnit.DAYS));
            assertTrue(iae.getMessage().contains("Value '" + week + "' is not a valid time duration"));
        });
    }

    /**
     * Regression test
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testGetTimeDurationShouldHandleInvalidAbbreviations() {
        final List<String> WEEKS = List.of("1 work", "1 wek", "1 k");

        WEEKS.forEach(week -> {
            IllegalArgumentException iae =
                    assertThrows(IllegalArgumentException.class, () -> FormatUtils.getTimeDuration(week, TimeUnit.DAYS));
            assertTrue(iae.getMessage().contains("Value '" + week + "' is not a valid time duration"));
        });
    }

    /**
     * New feature test
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testGetTimeDurationShouldHandleNoSpaceInInput() {
        final List<String> WEEKS = List.of("1week", "1wk", "1w", "1wks", "1weeks");
        final long EXPECTED_DAYS = 7L;

        List<Long> days = WEEKS.stream()
                .map(week -> FormatUtils.getTimeDuration(week, TimeUnit.DAYS))
                .collect(Collectors.toList());

        days.forEach(it -> assertEquals(EXPECTED_DAYS, it));
    }

    /**
     * New feature test
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testGetTimeDurationShouldHandleDecimalValues() {
        final List<String> WHOLE_NUMBERS = List.of("10 ms", "10 millis", "10 milliseconds");
        final List<String> DECIMAL_NUMBERS = List.of("0.010 s", "0.010 seconds");
        final long EXPECTED_MILLIS = 10L;

        List<Long> parsedWholeMillis = WHOLE_NUMBERS.stream()
                .map(whole -> FormatUtils.getTimeDuration(whole, TimeUnit.MILLISECONDS))
                .collect(Collectors.toList());

        List<Long> parsedDecimalMillis = DECIMAL_NUMBERS.stream()
                .map(decimal -> FormatUtils.getTimeDuration(decimal, TimeUnit.MILLISECONDS))
                .collect(Collectors.toList());

        parsedWholeMillis.forEach(it -> assertEquals(EXPECTED_MILLIS, it));
        parsedDecimalMillis.forEach(it -> assertEquals(EXPECTED_MILLIS, it));
    }

    /**
     * Regression test for custom week logic
     */
    @Test
    public void testGetPreciseTimeDurationShouldHandleWeeks() {
        final String ONE_WEEK = "1 week";
        final Map<TimeUnit, Number> ONE_WEEK_IN_OTHER_UNITS = Map.of(
                (TimeUnit.DAYS), 7L,
                (TimeUnit.HOURS), 7 * 24L,
                (TimeUnit.MINUTES), 7 * 24 * 60L,
                (TimeUnit.SECONDS), 7 * 24 * 60 * 60L,
                (TimeUnit.MILLISECONDS), 7 * 24 * 60 * 60 * 1000L,
                (TimeUnit.MICROSECONDS), 7 * 24 * 60 * 60 * 1000 * 1000L,
                (TimeUnit.NANOSECONDS), 7 * 24 * 60 * 60 * (1000 * 1000 * 1000L));

        Map<TimeUnit, Number> oneWeekInOtherUnits = Arrays.stream(TimeUnit.values())
                .collect(Collectors.toMap(destinationUnit -> destinationUnit,
                        destinationUnit -> FormatUtils.getPreciseTimeDuration(ONE_WEEK, destinationUnit)));

        oneWeekInOtherUnits.forEach((key, value) -> assertEquals(ONE_WEEK_IN_OTHER_UNITS.get(key), value.longValue()));
    }

    /**
     * Positive flow test for custom week logic with decimal value
     */
    @Test
    public void testGetPreciseTimeDurationShouldHandleDecimalWeeks() {
        final String ONE_AND_A_HALF_WEEKS = "1.5 week";
        final Map<TimeUnit, Double> ONE_POINT_FIVE_WEEKS_IN_OTHER_UNITS = Map.of(
                        (TimeUnit.HOURS), 7 * 24L,
                        (TimeUnit.MINUTES), 7 * 24 * 60L,
                        (TimeUnit.SECONDS), 7 * 24 * 60 * 60L,
                        (TimeUnit.MILLISECONDS), 7 * 24 * 60 * 60 * 1000L,
                        (TimeUnit.MICROSECONDS), 7 * 24 * 60 * 60 * (1000 * 1000L),
                        (TimeUnit.NANOSECONDS), 7 * 24 * 60 * 60 * (1000 * 1000 * 1000L)
                ).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue() * 1.5));

        Map<TimeUnit, Double> onePointFiveWeeksInOtherUnits = Arrays.stream(TimeUnit.values())
                .filter(timeUnit -> !TimeUnit.DAYS.equals(timeUnit))
                .collect(Collectors.toMap(destinationUnit -> destinationUnit,
                        destinationUnit -> FormatUtils.getPreciseTimeDuration(ONE_AND_A_HALF_WEEKS, destinationUnit)));

        onePointFiveWeeksInOtherUnits.forEach((key, value) ->
                assertEquals(ONE_POINT_FIVE_WEEKS_IN_OTHER_UNITS.get(key), value)
        );
    }

    /**
     * Positive flow test for decimal time inputs
     */
    @Test
    public void testGetPreciseTimeDurationShouldHandleDecimalValues() {
        final List<String> WHOLE_NUMBERS = List.of("10 ms", "10 millis", "10 milliseconds");
        final List<String> DECIMAL_NUMBERS = List.of("0.010 s", "0.010 seconds");
        final double EXPECTED_MILLIS = 10.0;

        List<Double> parsedWholeMillis = WHOLE_NUMBERS.stream()
                .map(whole -> FormatUtils.getPreciseTimeDuration(whole, TimeUnit.MILLISECONDS))
                .collect(Collectors.toList());
        List<Double> parsedDecimalMillis = DECIMAL_NUMBERS.stream()
                .map(decimal -> FormatUtils.getPreciseTimeDuration(decimal, TimeUnit.MILLISECONDS))
                .collect(Collectors.toList());

        parsedWholeMillis.forEach(it -> assertEquals(EXPECTED_MILLIS, it));
        parsedDecimalMillis.forEach(it -> assertEquals(EXPECTED_MILLIS, it));
    }

    /**
     * Positive flow test for decimal inputs that are extremely small
     */
    @Test
    public void testGetPreciseTimeDurationShouldHandleSmallDecimalValues() {
        final Map<String, Map<String, Object>> SCENARIOS = Map.of(
                "decimalNanos", Map.of("originalUnits", TimeUnit.NANOSECONDS, "expectedUnits", TimeUnit.NANOSECONDS, "originalValue", 123.4, "expectedValue", 123.0),
                "lessThanOneNano", Map.of("originalUnits", TimeUnit.NANOSECONDS, "expectedUnits", TimeUnit.NANOSECONDS, "originalValue", 0.9, "expectedValue", 1.0),
                "lessThanOneNanoToMillis", Map.of("originalUnits", TimeUnit.NANOSECONDS, "expectedUnits", TimeUnit.MILLISECONDS, "originalValue", 0.9, "expectedValue", 0.0),
                "decimalMillisToNanos", Map.of("originalUnits", TimeUnit.MILLISECONDS, "expectedUnits", TimeUnit.NANOSECONDS, "originalValue", 123.4, "expectedValue", 123_400_000.0)
        );

        Map<String, Double> results = SCENARIOS.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        entry -> FormatUtils.getPreciseTimeDuration(entry.getValue().get("originalValue") + " " + ((TimeUnit) entry.getValue().get("originalUnits")).name(),
                                (TimeUnit) entry.getValue().get("expectedUnits"))));

        results.forEach((key, value) -> assertEquals(SCENARIOS.get(key).get("expectedValue"), value));
    }

    /**
     * Positive flow test for decimal inputs that can be converted (all equal values)
     */
    @Test
    public void testMakeWholeNumberTimeShouldHandleDecimals() {
        Map<Double, TimeUnit> DECIMAL_TIMES = Map.of(0.000_000_010, TimeUnit.SECONDS,
                0.000_010, TimeUnit.MILLISECONDS, 0.010, TimeUnit.MICROSECONDS);
        final long EXPECTED_NANOS = 10L;

        List<Object> parsedWholeNanos = DECIMAL_TIMES.entrySet().stream()
                .map(entry -> FormatUtils.makeWholeNumberTime(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());

        parsedWholeNanos.forEach(it -> assertEquals(List.of(EXPECTED_NANOS, TimeUnit.NANOSECONDS), it));
    }

    /**
     * Positive flow test for decimal inputs that can be converted (metric values)
     */
    @Test
    public void testMakeWholeNumberTimeShouldHandleMetricConversions() {
        final Map<String, Map<String, Object>> SCENARIOS = Map.of(
                "secondsToMillis", Map.of("originalUnits", TimeUnit.SECONDS, "expectedUnits", TimeUnit.MILLISECONDS, "originalValue", 123.4, "expectedValue", 123_400L),
                "secondsToMicros", Map.of("originalUnits", TimeUnit.SECONDS, "expectedUnits", TimeUnit.MICROSECONDS, "originalValue", 1.000_345, "expectedValue", 1_000_345L),
                "millisToMicros", Map.of("originalUnits", TimeUnit.MILLISECONDS, "expectedUnits", TimeUnit.MICROSECONDS, "originalValue", 0.75, "expectedValue", 750L),
                "nanosToNanosGE1", Map.of("originalUnits", TimeUnit.NANOSECONDS, "expectedUnits", TimeUnit.NANOSECONDS, "originalValue", 123.4, "expectedValue", 123L),
                "nanosToNanosLE1", Map.of("originalUnits", TimeUnit.NANOSECONDS, "expectedUnits", TimeUnit.NANOSECONDS, "originalValue", 0.123, "expectedValue", 1L)
        );

        Map<String, List<Object>> results = SCENARIOS.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        entry -> FormatUtils.makeWholeNumberTime((Double) entry.getValue().get("originalValue"),
                                (TimeUnit) entry.getValue().get("originalUnits"))));

        results.forEach((key, value) -> {
            assertEquals(SCENARIOS.get(key).get("expectedValue"), value.get(0));
            assertEquals(SCENARIOS.get(key).get("expectedUnits"), value.get(1));
        });
    }

    /**
     * Positive flow test for decimal inputs that can be converted (non-metric values)
     */
    @Test
    public void testMakeWholeNumberTimeShouldHandleNonMetricConversions() {
        final Map<String, Map<String, Object>> SCENARIOS = Map.of(
                "daysToHours", Map.of("originalUnits", TimeUnit.DAYS, "expectedUnits", TimeUnit.HOURS, "originalValue", 1.5, "expectedValue", 36L),
                "hoursToMinutes", Map.of("originalUnits", TimeUnit.HOURS, "expectedUnits", TimeUnit.MINUTES, "originalValue", 1.5, "expectedValue", 90L),
                "hoursToMinutes2", Map.of("originalUnits", TimeUnit.HOURS, "expectedUnits", TimeUnit.MINUTES, "originalValue", .75, "expectedValue", 45L));

        Map<String, List<Object>> results = SCENARIOS.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        entry -> FormatUtils.makeWholeNumberTime(((Number) entry.getValue().get("originalValue")).doubleValue(),
                                (TimeUnit) entry.getValue().get("originalUnits"))));

        results.forEach((key, value) -> {
            assertEquals(SCENARIOS.get(key).get("expectedValue"), value.get(0));
            assertEquals(SCENARIOS.get(key).get("expectedUnits"), value.get(1));
        });
    }

    /**
     * Positive flow test for whole inputs
     */
    @Test
    public void testMakeWholeNumberTimeShouldHandleWholeNumbers() {
        List<List<Object>> WHOLE_TIMES = new ArrayList<>();
        for (TimeUnit timeUnit : TimeUnit.values()) {
            WHOLE_TIMES.add(List.of(10.0, timeUnit));
        }

        List<List<Object>> parsedWholeTimes = WHOLE_TIMES.stream()
                .map(wholeTime -> FormatUtils.makeWholeNumberTime((Double) wholeTime.get(0), (TimeUnit) wholeTime.get(1)))
                .collect(Collectors.toList());

        IntStream.range(0, parsedWholeTimes.size())
                .forEach(index -> {
                    List<Object> elements = parsedWholeTimes.get(index);
                    assertEquals(10L, elements.get(0));
                    assertEquals(WHOLE_TIMES.get(index).get(1), elements.get(1));
                });
    }

    /**
     * Negative flow test for nanosecond inputs (regardless of value, the unit cannot be converted)
     */
    @Test
    public void testMakeWholeNumberTimeShouldHandleNanoseconds() {
        final List<List<Object>> WHOLE_TIMES = List.of(
                List.of(1100.0, TimeUnit.NANOSECONDS),
                List.of(2.1, TimeUnit.NANOSECONDS),
                List.of(1.0, TimeUnit.NANOSECONDS),
                List.of(0.1, TimeUnit.NANOSECONDS));

        final List<List<Object>> EXPECTED_TIMES = List.of(
                List.of(1100L, TimeUnit.NANOSECONDS),
                List.of(2L, TimeUnit.NANOSECONDS),
                List.of(1L, TimeUnit.NANOSECONDS),
                List.of(1L, TimeUnit.NANOSECONDS)
        );

        List<List<Object>> parsedWholeTimes = WHOLE_TIMES.stream()
                .map(it -> FormatUtils.makeWholeNumberTime((Double) it.get(0), (TimeUnit) it.get(1)))
                .collect(Collectors.toList());

        assertEquals(EXPECTED_TIMES, parsedWholeTimes);
    }

    /**
     * Positive flow test for whole inputs
     */
    @Test
    public void testShouldGetSmallerTimeUnit() {
        final List<TimeUnit> UNITS = Arrays.asList(TimeUnit.values());

        IllegalArgumentException nullIae = assertThrows(IllegalArgumentException.class,
                () -> FormatUtils.getSmallerTimeUnit(null));
        IllegalArgumentException nanoIae = assertThrows(IllegalArgumentException.class,
                () -> FormatUtils.getSmallerTimeUnit(TimeUnit.NANOSECONDS));

        List<TimeUnit> smallerTimeUnits = UNITS.stream()
                .filter(timeUnit -> !TimeUnit.NANOSECONDS.equals(timeUnit))
                .map(FormatUtils::getSmallerTimeUnit)
                .collect(Collectors.toList());

        assertEquals("Cannot determine a smaller time unit than 'null'", nullIae.getMessage());
        assertEquals("Cannot determine a smaller time unit than 'NANOSECONDS'", nanoIae.getMessage());
        assertEquals(UNITS.subList(0, UNITS.size() - 1), smallerTimeUnits);
    }

    /**
     * Positive flow test for multipliers based on valid time units
     */
    @Test
    void testShouldCalculateMultiplier() {
        final Map<String, Map<String, Object>> SCENARIOS = Map.of(
                "allUnits", Map.of("original", TimeUnit.DAYS, "destination", TimeUnit.NANOSECONDS, "expectedMultiplier", 24 * 60 * 60 * 1_000_000_000L),
                "microsToNanos", Map.of("original", TimeUnit.MICROSECONDS, "destination", TimeUnit.NANOSECONDS, "expectedMultiplier", 1_000L),
                "millisToNanos", Map.of("original", TimeUnit.MILLISECONDS, "destination", TimeUnit.NANOSECONDS, "expectedMultiplier", 1_000_000L),
                "millisToMicros", Map.of("original", TimeUnit.MILLISECONDS, "destination", TimeUnit.MICROSECONDS, "expectedMultiplier", 1_000L),
                "daysToHours", Map.of("original", TimeUnit.DAYS, "destination", TimeUnit.HOURS, "expectedMultiplier", 24L),
                "daysToSeconds", Map.of("original", TimeUnit.DAYS, "destination", TimeUnit.SECONDS, "expectedMultiplier", 24 * 60 * 60L)
        );

        Map<String, Long> results = SCENARIOS.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        entry -> FormatUtils.calculateMultiplier((TimeUnit) entry.getValue().get("original"), (TimeUnit) entry.getValue().get("destination"))));

        results.forEach((key, value) -> assertEquals(SCENARIOS.get(key).get("expectedMultiplier"), value));
    }

    /**
     * Negative flow test for multipliers based on incorrectly-ordered time units
     */
    @Test
    public void testCalculateMultiplierShouldHandleIncorrectUnits() {
        final Map<String, Map<String, TimeUnit>> SCENARIOS = Map.of(
                "allUnits", Map.of("original", TimeUnit.NANOSECONDS, "destination", TimeUnit.DAYS),
                "nanosToMicros", Map.of("original", TimeUnit.NANOSECONDS, "destination", TimeUnit.MICROSECONDS),
                "hoursToDays", Map.of("original", TimeUnit.HOURS, "destination", TimeUnit.DAYS)
        );

        SCENARIOS.forEach((key, value) -> {
            IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                    () -> FormatUtils.calculateMultiplier(value.get("original"), value.get("destination")));
            assertTrue(iae.getMessage().matches("The original time unit '.*' must be larger than the new time unit '.*'"));
        });
    }
}
