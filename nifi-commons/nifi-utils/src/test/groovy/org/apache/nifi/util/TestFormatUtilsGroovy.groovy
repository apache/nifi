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
package org.apache.nifi.util

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit
import java.util.stream.IntStream

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertThrows
import static org.junit.jupiter.api.Assertions.assertTrue

class TestFormatUtilsGroovy {
    private static final Logger logger = LoggerFactory.getLogger(TestFormatUtilsGroovy.class)

    @BeforeAll
    static void setUpOnce() throws Exception {
        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    /**
     * New feature test
     */
    @Test
    void testGetTimeDurationShouldConvertWeeks() {
        // Arrange
        final List WEEKS = ["1 week", "1 wk", "1 w", "1 wks", "1 weeks"]
        final long EXPECTED_DAYS = 7L

        // Act
        List days = WEEKS.collect { String week ->
            FormatUtils.getTimeDuration(week, TimeUnit.DAYS)
        }
        logger.converted(days)

        // Assert
        days.forEach(it -> assertEquals(EXPECTED_DAYS, it))
    }


    @Test
    void testGetTimeDurationShouldHandleNegativeWeeks() {
        // Arrange
        final List WEEKS = ["-1 week", "-1 wk", "-1 w", "-1 weeks", "- 1 week"]

        // Act
        WEEKS.stream().forEach(week -> {
            IllegalArgumentException iae =
                    assertThrows(IllegalArgumentException.class, () -> FormatUtils.getTimeDuration(week, TimeUnit.DAYS))
            // Assert
            assertTrue(iae.message.contains("Value '" + week + "' is not a valid time duration"))
        })
    }

    /**
     * Regression test
     */
    @Test
    void testGetTimeDurationShouldHandleInvalidAbbreviations() {
        // Arrange
        final List WEEKS = ["1 work", "1 wek", "1 k"]

        // Act
        WEEKS.stream().forEach(week -> {
            IllegalArgumentException iae =
                    assertThrows(IllegalArgumentException.class, () -> FormatUtils.getTimeDuration(week, TimeUnit.DAYS))
            // Assert
            assertTrue(iae.message.contains("Value '" + week + "' is not a valid time duration"))
        })

    }

    /**
     * New feature test
     */
    @Test
    void testGetTimeDurationShouldHandleNoSpaceInInput() {
        // Arrange
        final List WEEKS = ["1week", "1wk", "1w", "1wks", "1weeks"]
        final long EXPECTED_DAYS = 7L

        // Act
        List days = WEEKS.collect { String week ->
            FormatUtils.getTimeDuration(week, TimeUnit.DAYS)
        }
        logger.converted(days)

        // Assert
        days.forEach(it -> assertEquals(EXPECTED_DAYS, it))
    }

    /**
     * New feature test
     */
    @Test
    void testGetTimeDurationShouldHandleDecimalValues() {
        // Arrange
        final List WHOLE_NUMBERS = ["10 ms", "10 millis", "10 milliseconds"]
        final List DECIMAL_NUMBERS = ["0.010 s", "0.010 seconds"]
        final long EXPECTED_MILLIS = 10

        // Act
        List parsedWholeMillis = WHOLE_NUMBERS.collect { String whole ->
            FormatUtils.getTimeDuration(whole, TimeUnit.MILLISECONDS)
        }
        logger.converted(parsedWholeMillis)

        List parsedDecimalMillis = DECIMAL_NUMBERS.collect { String decimal ->
            FormatUtils.getTimeDuration(decimal, TimeUnit.MILLISECONDS)
        }
        logger.converted(parsedDecimalMillis)

        // Assert
        parsedWholeMillis.forEach(it -> assertEquals(EXPECTED_MILLIS, it))
        parsedDecimalMillis.forEach(it -> assertEquals(EXPECTED_MILLIS, it))
    }

    /**
     * Regression test for custom week logic
     */
    @Test
    void testGetPreciseTimeDurationShouldHandleWeeks() {
        // Arrange
        final String ONE_WEEK = "1 week"
        final Map ONE_WEEK_IN_OTHER_UNITS = [
                (TimeUnit.DAYS)        : 7,
                (TimeUnit.HOURS)       : 7 * 24,
                (TimeUnit.MINUTES)     : 7 * 24 * 60,
                (TimeUnit.SECONDS)     : (long) 7 * 24 * 60 * 60,
                (TimeUnit.MILLISECONDS): (long) 7 * 24 * 60 * 60 * 1000,
                (TimeUnit.MICROSECONDS): (long) 7 * 24 * 60 * 60 * ((long) 1000 * 1000),
                (TimeUnit.NANOSECONDS) : (long) 7 * 24 * 60 * 60 * ((long) 1000 * 1000 * 1000),
        ]

        // Act
        Map oneWeekInOtherUnits = TimeUnit.values()[0..<-1].collectEntries { TimeUnit destinationUnit ->
            [destinationUnit, FormatUtils.getPreciseTimeDuration(ONE_WEEK, destinationUnit)]
        }
        logger.converted(oneWeekInOtherUnits)

        // Assert
        oneWeekInOtherUnits.entrySet().forEach(entry ->
                        assertEquals(ONE_WEEK_IN_OTHER_UNITS.get(entry.getKey()), entry.getValue()))
    }

    /**
     * Positive flow test for custom week logic with decimal value
     */
    @Test
    void testGetPreciseTimeDurationShouldHandleDecimalWeeks() {
        // Arrange
        final String ONE_AND_A_HALF_WEEKS = "1.5 week"
        final Map ONE_POINT_FIVE_WEEKS_IN_OTHER_UNITS = [
                (TimeUnit.DAYS)        : 7,
                (TimeUnit.HOURS)       : 7 * 24,
                (TimeUnit.MINUTES)     : 7 * 24 * 60,
                (TimeUnit.SECONDS)     : (long) 7 * 24 * 60 * 60,
                (TimeUnit.MILLISECONDS): (long) 7 * 24 * 60 * 60 * 1000,
                (TimeUnit.MICROSECONDS): (long) 7 * 24 * 60 * 60 * ((long) 1000 * 1000),
                (TimeUnit.NANOSECONDS) : (long) 7 * 24 * 60 * 60 * ((long) 1000 * 1000 * 1000),
        ].collectEntries { k, v -> [k, v * 1.5] }

        // Act
        Map onePointFiveWeeksInOtherUnits = TimeUnit.values()[0..<-1].collectEntries { TimeUnit destinationUnit ->
            [destinationUnit, FormatUtils.getPreciseTimeDuration(ONE_AND_A_HALF_WEEKS, destinationUnit)]
        }
        logger.converted(onePointFiveWeeksInOtherUnits)

        // Assert
        onePointFiveWeeksInOtherUnits.entrySet().forEach(entry ->
                assertEquals(ONE_POINT_FIVE_WEEKS_IN_OTHER_UNITS.get(entry.getKey()), entry.getValue()))
    }

    /**
     * Positive flow test for decimal time inputs
     */
    @Test
    void testGetPreciseTimeDurationShouldHandleDecimalValues() {
        // Arrange
        final List WHOLE_NUMBERS = ["10 ms", "10 millis", "10 milliseconds"]
        final List DECIMAL_NUMBERS = ["0.010 s", "0.010 seconds"]
        final float EXPECTED_MILLIS = 10.0

        // Act
        List parsedWholeMillis = WHOLE_NUMBERS.collect { String whole ->
            FormatUtils.getPreciseTimeDuration(whole, TimeUnit.MILLISECONDS)
        }
        logger.converted(parsedWholeMillis)

        List parsedDecimalMillis = DECIMAL_NUMBERS.collect { String decimal ->
            FormatUtils.getPreciseTimeDuration(decimal, TimeUnit.MILLISECONDS)
        }
        logger.converted(parsedDecimalMillis)

        // Assert
        parsedWholeMillis.forEach(it -> assertEquals(EXPECTED_MILLIS, it))
        parsedDecimalMillis.forEach(it -> assertEquals(EXPECTED_MILLIS, it))
    }

    /**
     * Positive flow test for decimal inputs that are extremely small
     */
    @Test
    void testGetPreciseTimeDurationShouldHandleSmallDecimalValues() {
        // Arrange
        final Map SCENARIOS = [
                "decimalNanos"           : [originalUnits: TimeUnit.NANOSECONDS, expectedUnits: TimeUnit.NANOSECONDS, originalValue: 123.4, expectedValue: 123.0],
                "lessThanOneNano"        : [originalUnits: TimeUnit.NANOSECONDS, expectedUnits: TimeUnit.NANOSECONDS, originalValue: 0.9, expectedValue: 1],
                "lessThanOneNanoToMillis": [originalUnits: TimeUnit.NANOSECONDS, expectedUnits: TimeUnit.MILLISECONDS, originalValue: 0.9, expectedValue: 0],
                "decimalMillisToNanos"        : [originalUnits: TimeUnit.MILLISECONDS, expectedUnits: TimeUnit.NANOSECONDS, originalValue: 123.4, expectedValue: 123_400_000],
        ]

        // Act
        Map results = SCENARIOS.collectEntries { String k, Map values ->
            logger.debug("Evaluating ${k}: ${values}")
            String input = "${values.originalValue} ${values.originalUnits.name()}"
            [k, FormatUtils.getPreciseTimeDuration(input, values.expectedUnits)]
        }
        logger.info(results)

        // Assert
        results.entrySet().forEach(entry ->
                assertEquals(SCENARIOS.get(entry.getKey()).expectedValue, entry.getValue()))
    }

    /**
     * Positive flow test for decimal inputs that can be converted (all equal values)
     */
    @Test
    void testMakeWholeNumberTimeShouldHandleDecimals() {
        // Arrange
        final List DECIMAL_TIMES = [
                [0.000_000_010, TimeUnit.SECONDS],
                [0.000_010, TimeUnit.MILLISECONDS],
                [0.010, TimeUnit.MICROSECONDS]
        ]
        final long EXPECTED_NANOS = 10L

        // Act
        List parsedWholeNanos = DECIMAL_TIMES.collect { List it ->
            FormatUtils.makeWholeNumberTime(it[0] as float, it[1] as TimeUnit)
        }
        logger.converted(parsedWholeNanos)

        // Assert
        parsedWholeNanos.forEach(it ->
                assertEquals(Arrays.asList(EXPECTED_NANOS, TimeUnit.NANOSECONDS), it))
    }

    /**
     * Positive flow test for decimal inputs that can be converted (metric values)
     */
    @Test
    void testMakeWholeNumberTimeShouldHandleMetricConversions() {
        // Arrange
        final Map SCENARIOS = [
                "secondsToMillis": [originalUnits: TimeUnit.SECONDS, expectedUnits: TimeUnit.MILLISECONDS, expectedValue: 123_400, originalValue: 123.4],
                "secondsToMicros": [originalUnits: TimeUnit.SECONDS, expectedUnits: TimeUnit.MICROSECONDS, originalValue: 1.000_345, expectedValue: 1_000_345],
                "millisToNanos"  : [originalUnits: TimeUnit.MILLISECONDS, expectedUnits: TimeUnit.NANOSECONDS, originalValue: 0.75, expectedValue: 750_000],
                "nanosToNanosGE1": [originalUnits: TimeUnit.NANOSECONDS, expectedUnits: TimeUnit.NANOSECONDS, originalValue: 123.4, expectedValue: 123],
                "nanosToNanosLE1": [originalUnits: TimeUnit.NANOSECONDS, expectedUnits: TimeUnit.NANOSECONDS, originalValue: 0.123, expectedValue: 1],
        ]

        // Act
        Map results = SCENARIOS.collectEntries { String k, Map values ->
            logger.debug("Evaluating ${k}: ${values}")
            [k, FormatUtils.makeWholeNumberTime(values.originalValue, values.originalUnits)]
        }
        logger.info(results)

        // Assert
        results.every { String key, List values ->
            assertEquals(SCENARIOS[key].expectedValue, values.first())
            assertEquals(SCENARIOS[key].expectedUnits, values.last())
        }
    }

    /**
     * Positive flow test for decimal inputs that can be converted (non-metric values)
     */
    @Test
    void testMakeWholeNumberTimeShouldHandleNonMetricConversions() {
        // Arrange
        final Map SCENARIOS = [
                "daysToHours"    : [originalUnits: TimeUnit.DAYS, expectedUnits: TimeUnit.HOURS, expectedValue: 36, originalValue: 1.5],
                "hoursToMinutes" : [originalUnits: TimeUnit.HOURS, expectedUnits: TimeUnit.MINUTES, originalValue: 1.5, expectedValue: 90],
                "hoursToMinutes2": [originalUnits: TimeUnit.HOURS, expectedUnits: TimeUnit.MINUTES, originalValue: 0.75, expectedValue: 45],
        ]

        // Act
        Map results = SCENARIOS.collectEntries { String k, Map values ->
            logger.debug("Evaluating ${k}: ${values}")
            [k, FormatUtils.makeWholeNumberTime(values.originalValue, values.originalUnits)]
        }
        logger.info(results)

        // Assert
        results.entrySet().forEach(entry -> {
            assertEquals(SCENARIOS.get(entry.getKey()).expectedValue, entry.getValue().get(0))
            assertEquals(SCENARIOS.get(entry.getKey()).expectedUnits, entry.getValue().get(1))
        })
    }

    /**
     * Positive flow test for whole inputs
     */
    @Test
    void testMakeWholeNumberTimeShouldHandleWholeNumbers() {
        // Arrange
        final List WHOLE_TIMES = [
                [10.0, TimeUnit.DAYS],
                [10.0, TimeUnit.HOURS],
                [10.0, TimeUnit.MINUTES],
                [10.0, TimeUnit.SECONDS],
                [10.0, TimeUnit.MILLISECONDS],
                [10.0, TimeUnit.MICROSECONDS],
                [10.0, TimeUnit.NANOSECONDS],
        ]

        // Act
        List<List<Object>> parsedWholeTimes = WHOLE_TIMES.collect { List it ->
            FormatUtils.makeWholeNumberTime(it[0] as float, it[1] as TimeUnit)
        }
        logger.converted(parsedWholeTimes)

        // Assert
        IntStream.range(0, parsedWholeTimes.size())
                .forEach(index -> {
                    List<Object> elements = parsedWholeTimes.get(index)
                    assertEquals(10L, elements.get(0))
                    assertEquals(WHOLE_TIMES[index][1], elements.get(1))
                 })
    }

    /**
     * Negative flow test for nanosecond inputs (regardless of value, the unit cannot be converted)
     */
    @Test
    void testMakeWholeNumberTimeShouldHandleNanoseconds() {
        // Arrange
        final List WHOLE_TIMES = [
                [1100.0, TimeUnit.NANOSECONDS],
                [2.1, TimeUnit.NANOSECONDS],
                [1.0, TimeUnit.NANOSECONDS],
                [0.1, TimeUnit.NANOSECONDS],
        ]

        final List EXPECTED_TIMES = [
                [1100L, TimeUnit.NANOSECONDS],
                [2L, TimeUnit.NANOSECONDS],
                [1L, TimeUnit.NANOSECONDS],
                [1L, TimeUnit.NANOSECONDS],
        ]

        // Act
        List parsedWholeTimes = WHOLE_TIMES.collect { List it ->
            FormatUtils.makeWholeNumberTime(it[0] as float, it[1] as TimeUnit)
        }
        logger.converted(parsedWholeTimes)

        // Assert
        assertEquals(EXPECTED_TIMES, parsedWholeTimes)
    }

    /**
     * Positive flow test for whole inputs
     */
    @Test
    void testShouldGetSmallerTimeUnit() {
        // Arrange
        final List UNITS = TimeUnit.values() as List

        // Act
        IllegalArgumentException nullIae = assertThrows(IllegalArgumentException.class,
                () -> FormatUtils.getSmallerTimeUnit(null))
        IllegalArgumentException nanoIae = assertThrows(IllegalArgumentException.class,
                () -> FormatUtils.getSmallerTimeUnit(TimeUnit.NANOSECONDS))

        List smallerTimeUnits = UNITS[1..-1].collect { TimeUnit unit ->
            FormatUtils.getSmallerTimeUnit(unit)
        }
        logger.converted(smallerTimeUnits)

        // Assert
        assertEquals("Cannot determine a smaller time unit than 'null'", nullIae.getMessage())
        assertEquals("Cannot determine a smaller time unit than 'NANOSECONDS'", nanoIae.getMessage())
        assertEquals(smallerTimeUnits, UNITS.subList(0, UNITS.size() - 1))
    }

    /**
     * Positive flow test for multipliers based on valid time units
     */
    @Test
    void testShouldCalculateMultiplier() {
        // Arrange
        final Map SCENARIOS = [
                "allUnits"      : [original: TimeUnit.DAYS, destination: TimeUnit.NANOSECONDS, expectedMultiplier: (long) 24 * 60 * 60 * (long) 1_000_000_000],
                "microsToNanos" : [original: TimeUnit.MICROSECONDS, destination: TimeUnit.NANOSECONDS, expectedMultiplier: 1_000],
                "millisToNanos" : [original: TimeUnit.MILLISECONDS, destination: TimeUnit.NANOSECONDS, expectedMultiplier: 1_000_000],
                "millisToMicros": [original: TimeUnit.MILLISECONDS, destination: TimeUnit.MICROSECONDS, expectedMultiplier: 1_000],
                "daysToHours"   : [original: TimeUnit.DAYS, destination: TimeUnit.HOURS, expectedMultiplier: 24],
                "daysToSeconds" : [original: TimeUnit.DAYS, destination: TimeUnit.SECONDS, expectedMultiplier: 24 * 60 * 60],
        ]

        // Act
        Map results = SCENARIOS.collectEntries { String k, Map values ->
            logger.debug("Evaluating ${k}: ${values}")
            [k, FormatUtils.calculateMultiplier(values.original, values.destination)]
        }
        logger.converted(results)

        // Assert
        results.entrySet().forEach(entry ->
        assertEquals(SCENARIOS.get(entry.getKey()).expectedMultiplier, entry.getValue()))
    }

    /**
     * Negative flow test for multipliers based on incorrectly-ordered time units
     */
    @Test
    void testCalculateMultiplierShouldHandleIncorrectUnits() {
        // Arrange
        final Map<String, Map<String, TimeUnit>> SCENARIOS = [
                "allUnits"     : [original: TimeUnit.NANOSECONDS, destination: TimeUnit.DAYS],
                "nanosToMicros": [original: TimeUnit.NANOSECONDS, destination: TimeUnit.MICROSECONDS],
                "hoursToDays"  : [original: TimeUnit.HOURS, destination: TimeUnit.DAYS],
        ]

        // Act
        SCENARIOS.entrySet().stream()
        .forEach(entry -> {
            // Assert
            IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                    () -> FormatUtils.calculateMultiplier(entry.getValue().get("original"),
                            entry.getValue().get("destination")))
            assertTrue((iae.getMessage() =~ "The original time unit '.*' must be larger than the new time unit '.*'").find())
        })
    }

    // TODO: Microsecond parsing
}