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


import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit

@RunWith(JUnit4.class)
class TestFormatUtilsGroovy extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(TestFormatUtilsGroovy.class)

    @BeforeClass
    static void setUpOnce() throws Exception {
        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Before
    void setUp() throws Exception {

    }

    @After
    void tearDown() throws Exception {

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
        assert days.every { it == EXPECTED_DAYS }
    }


    @Test
    void testGetTimeDurationShouldHandleNegativeWeeks() {
        // Arrange
        final List WEEKS = ["-1 week", "-1 wk", "-1 w", "-1 weeks", "- 1 week"]

        // Act
        List msgs = WEEKS.collect { String week ->
            shouldFail(IllegalArgumentException) {
                FormatUtils.getTimeDuration(week, TimeUnit.DAYS)
            }
        }

        // Assert
        assert msgs.every { it =~ /Value '.*' is not a valid time duration/ }
    }

    /**
     * Regression test
     */
    @Test
    void testGetTimeDurationShouldHandleInvalidAbbreviations() {
        // Arrange
        final List WEEKS = ["1 work", "1 wek", "1 k"]

        // Act
        List msgs = WEEKS.collect { String week ->
            shouldFail(IllegalArgumentException) {
                FormatUtils.getTimeDuration(week, TimeUnit.DAYS)
            }
        }

        // Assert
        assert msgs.every { it =~ /Value '.*' is not a valid time duration/ }

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
        assert days.every { it == EXPECTED_DAYS }
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
        assert parsedWholeMillis.every { it == EXPECTED_MILLIS }
        assert parsedDecimalMillis.every { it == EXPECTED_MILLIS }
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
        oneWeekInOtherUnits.each { TimeUnit k, double value ->
            assert value == ONE_WEEK_IN_OTHER_UNITS[k]
        }
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
        onePointFiveWeeksInOtherUnits.each { TimeUnit k, double value ->
            assert value == ONE_POINT_FIVE_WEEKS_IN_OTHER_UNITS[k]
        }
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
        assert parsedWholeMillis.every { it == EXPECTED_MILLIS }
        assert parsedDecimalMillis.every { it == EXPECTED_MILLIS }
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
        results.every { String key, double value ->
            assert value == SCENARIOS[key].expectedValue
        }
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
        assert parsedWholeNanos.every { it == [EXPECTED_NANOS, TimeUnit.NANOSECONDS] }
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
            assert values.first() == SCENARIOS[key].expectedValue
            assert values.last() == SCENARIOS[key].expectedUnits
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
        results.every { String key, List values ->
            assert values.first() == SCENARIOS[key].expectedValue
            assert values.last() == SCENARIOS[key].expectedUnits
        }
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
        List parsedWholeTimes = WHOLE_TIMES.collect { List it ->
            FormatUtils.makeWholeNumberTime(it[0] as float, it[1] as TimeUnit)
        }
        logger.converted(parsedWholeTimes)

        // Assert
        parsedWholeTimes.eachWithIndex { List elements, int i ->
            assert elements[0] instanceof Long
            assert elements[0] == 10L
            assert elements[1] == WHOLE_TIMES[i][1]
        }
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
        assert parsedWholeTimes == EXPECTED_TIMES
    }

    /**
     * Positive flow test for whole inputs
     */
    @Test
    void testShouldGetSmallerTimeUnit() {
        // Arrange
        final List UNITS = TimeUnit.values() as List

        // Act
        def nullMsg = shouldFail(IllegalArgumentException) {
            FormatUtils.getSmallerTimeUnit(null)
        }
        logger.expected(nullMsg)

        def nanosMsg = shouldFail(IllegalArgumentException) {
            FormatUtils.getSmallerTimeUnit(TimeUnit.NANOSECONDS)
        }
        logger.expected(nanosMsg)

        List smallerTimeUnits = UNITS[1..-1].collect { TimeUnit unit ->
            FormatUtils.getSmallerTimeUnit(unit)
        }
        logger.converted(smallerTimeUnits)

        // Assert
        assert nullMsg == "Cannot determine a smaller time unit than 'null'"
        assert nanosMsg == "Cannot determine a smaller time unit than 'NANOSECONDS'"
        assert smallerTimeUnits == UNITS[0..<-1]
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
        results.every { String key, long value ->
            assert value == SCENARIOS[key].expectedMultiplier
        }
    }

    /**
     * Negative flow test for multipliers based on incorrectly-ordered time units
     */
    @Test
    void testCalculateMultiplierShouldHandleIncorrectUnits() {
        // Arrange
        final Map SCENARIOS = [
                "allUnits"     : [original: TimeUnit.NANOSECONDS, destination: TimeUnit.DAYS],
                "nanosToMicros": [original: TimeUnit.NANOSECONDS, destination: TimeUnit.MICROSECONDS],
                "hoursToDays"  : [original: TimeUnit.HOURS, destination: TimeUnit.DAYS],
        ]

        // Act
        Map results = SCENARIOS.collectEntries { String k, Map values ->
            logger.debug("Evaluating ${k}: ${values}")
            def msg = shouldFail(IllegalArgumentException) {
                FormatUtils.calculateMultiplier(values.original, values.destination)
            }
            logger.expected(msg)
            [k, msg]
        }

        // Assert
        results.every { String key, String value ->
            assert value =~ "The original time unit '.*' must be larger than the new time unit '.*'"
        }
    }

    // TODO: Microsecond parsing
}