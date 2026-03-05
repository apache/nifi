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
package org.apache.nifi.attribute.expression.language;

import org.apache.nifi.attribute.expression.language.evaluation.util.DateAmountParser;
import org.apache.nifi.attribute.expression.language.exception.AttributeExpressionLanguageException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DateAmountParserTest {

    private static final ZonedDateTime AUG_4_2026 = ZonedDateTime.of(
            2026, 8, 4, 0, 0, 0, 0, ZoneId.systemDefault());

    // ================================================================
    // validate() — valid expressions
    // ================================================================

    @ParameterizedTest
    @ValueSource(strings = {
            "1 nanosecond", "500 nanoseconds",
            "1 second", "30 seconds", "1 minute", "15 minutes",
            "1 hour", "2 hours", "1 day", "7 days",
            "1 week", "2 weeks", "1 month", "3 months",
            "1 year", "2 years", "0 days", "100 hours", "365 days"
    })
    void testValidateAcceptsValidExpressions(final String expression) {
        assertDoesNotThrow(() -> DateAmountParser.validate(expression));
    }

    // ================================================================
    // Space is required between integer and unit
    // ================================================================

    @ParameterizedTest
    @ValueSource(strings = {"1days", "2weeks", "30seconds", "1hour", "3months", "1year", "15minutes"})
    void testValidateRejectsMissingSpace(final String expression) {
        assertThrows(AttributeExpressionLanguageException.class,
                () -> DateAmountParser.validate(expression));
    }

    @ParameterizedTest
    @ValueSource(strings = {"1 days", "2 weeks", "30 seconds", "1 hour", "3 months", "1 year", "15 minutes"})
    void testValidateAcceptsWithSpace(final String expression) {
        assertDoesNotThrow(() -> DateAmountParser.validate(expression));
    }

    // ================================================================
    // Case insensitivity
    // ================================================================

    @ParameterizedTest
    @CsvSource({
            "2 WEEKS,    2026-08-18T00:00:00",
            "2 Weeks,    2026-08-18T00:00:00",
            "2 weeks,    2026-08-18T00:00:00",
            "2 wEeKs,    2026-08-18T00:00:00",
            "1 MONTH,    2026-09-04T00:00:00",
            "1 Month,    2026-09-04T00:00:00",
            "1 YEAR,     2027-08-04T00:00:00",
            "3 HOURS,    2026-08-04T03:00:00",
            "1 DAY,      2026-08-05T00:00:00",
            "5 SECONDS,  2026-08-04T00:00:05",
            "10 MINUTES, 2026-08-04T00:10:00"
    })
    void testCaseInsensitive(final String expression, final String expectedIso) {
        final ZonedDateTime result = DateAmountParser.plus(AUG_4_2026, expression);
        assertEquals(LocalDateTime.parse(expectedIso), result.toLocalDateTime());
    }

    // ================================================================
    // Singular and plural produce identical results
    // ================================================================

    @ParameterizedTest
    @CsvSource({
            "1 nanosecond, 1 nanoseconds",
            "1 second,  1 seconds",
            "1 minute,  1 minutes",
            "1 hour,    1 hours",
            "1 day,     1 days",
            "1 week,    1 weeks",
            "1 month,   1 months",
            "1 year,    1 years"
    })
    void testSingularAndPluralProduceSameResult(final String singular, final String plural) {
        assertEquals(
                DateAmountParser.plus(AUG_4_2026, singular),
                DateAmountParser.plus(AUG_4_2026, plural));
    }

    // ================================================================
    // Whitespace tolerance
    // ================================================================

    @ParameterizedTest
    @ValueSource(strings = {"   2 weeks", "2 weeks   ", "  2   weeks  ", "2    weeks"})
    void testExtraWhitespaceAccepted(final String expression) {
        assertEquals(AUG_4_2026.plusWeeks(2), DateAmountParser.plus(AUG_4_2026, expression));
    }

    // ================================================================
    // Invalid expressions
    // ================================================================

    @ParameterizedTest
    @ValueSource(strings = {
            "1 Monday",          // day name, not a unit
            "B weeks",           // non-numeric amount
            "1 dog",             // nonsense unit
            "abc days",          // alpha amount
            "2 fortnights",      // unsupported unit
            "1 decade",          // unsupported unit
            "500 milliseconds",  // unsupported unit
            "-1 days",           // negative
            "1.5 days",          // decimal
            "1 day 2 hours",     // compound expression
            "1days",             // missing space
            "2weeks",            // missing space
            "",                  // empty
            "   ",               // blank
            "2",                 // number only
            "weeks"              // unit only
    })
    void testValidateRejectsInvalidExpressions(final String expression) {
        assertThrows(AttributeExpressionLanguageException.class,
                () -> DateAmountParser.validate(expression));
    }

    @Test
    void testValidateRejectsNull() {
        assertThrows(AttributeExpressionLanguageException.class,
                () -> DateAmountParser.validate(null));
    }

    // ================================================================
    // Plus — all units from base date (Aug 4, 2026 midnight)
    // ================================================================

    @ParameterizedTest
    @CsvSource({
            "1 nanosecond,   2026-08-04T00:00:00.000000001",
            "1000000000 nanoseconds, 2026-08-04T00:00:01",
            "1 second,   2026-08-04T00:00:01",
            "30 seconds, 2026-08-04T00:00:30",
            "1 minute,   2026-08-04T00:01:00",
            "15 minutes, 2026-08-04T00:15:00",
            "1 hour,     2026-08-04T01:00:00",
            "2 hours,    2026-08-04T02:00:00",
            "1 day,      2026-08-05T00:00:00",
            "7 days,     2026-08-11T00:00:00",
            "1 week,     2026-08-11T00:00:00",
            "2 weeks,    2026-08-18T00:00:00",
            "1 month,    2026-09-04T00:00:00",
            "3 months,   2026-11-04T00:00:00",
            "1 year,     2027-08-04T00:00:00",
            "2 years,    2028-08-04T00:00:00"
    })
    void testPlusAllUnits(final String expression, final String expectedIso) {
        final ZonedDateTime result = DateAmountParser.plus(AUG_4_2026, expression);
        assertEquals(LocalDateTime.parse(expectedIso), result.toLocalDateTime(),
                "Failed for: " + expression);
    }

    // ================================================================
    // Minus — all units from base date
    // ================================================================

    @ParameterizedTest
    @CsvSource({
            "1 nanosecond,   2026-08-03T23:59:59.999999999",
            "1000000000 nanoseconds, 2026-08-03T23:59:59",
            "1 second,   2026-08-03T23:59:59",
            "30 seconds, 2026-08-03T23:59:30",
            "1 minute,   2026-08-03T23:59:00",
            "15 minutes, 2026-08-03T23:45:00",
            "1 hour,     2026-08-03T23:00:00",
            "2 hours,    2026-08-03T22:00:00",
            "1 day,      2026-08-03T00:00:00",
            "7 days,     2026-07-28T00:00:00",
            "1 week,     2026-07-28T00:00:00",
            "2 weeks,    2026-07-21T00:00:00",
            "1 month,    2026-07-04T00:00:00",
            "3 months,   2026-05-04T00:00:00",
            "1 year,     2025-08-04T00:00:00",
            "2 years,    2024-08-04T00:00:00"
    })
    void testMinusAllUnits(final String expression, final String expectedIso) {
        final ZonedDateTime result = DateAmountParser.minus(AUG_4_2026, expression);
        assertEquals(LocalDateTime.parse(expectedIso), result.toLocalDateTime(),
                "Failed for: " + expression);
    }

    // ================================================================
    // Zero amount returns same date
    // ================================================================

    @Test
    void testZeroAmountNoOp() {
        assertEquals(AUG_4_2026, DateAmountParser.plus(AUG_4_2026, "0 days"));
        assertEquals(AUG_4_2026, DateAmountParser.plus(AUG_4_2026, "0 months"));
    }

    // ================================================================
    // Leap year behavior
    // ================================================================

    @Test
    void testPlusOneYearFromLeapDay() {
        // Feb 29, 2024 + 1 year -> Feb 28, 2025 (clamped)
        final ZonedDateTime leapDay = zonedDate(2024, 2, 29);
        assertEquals(zonedDate(2025, 2, 28), DateAmountParser.plus(leapDay, "1 year"));
    }

    @Test
    void testPlusFourYearsFromLeapDay() {
        // Feb 29, 2024 + 4 years -> Feb 29, 2028 (2028 is a leap year)
        final ZonedDateTime leapDay = zonedDate(2024, 2, 29);
        assertEquals(zonedDate(2028, 2, 29), DateAmountParser.plus(leapDay, "4 years"));
    }

    @Test
    void testMinusOneYearFromLeapDay() {
        final ZonedDateTime leapDay = zonedDate(2024, 2, 29);
        assertEquals(zonedDate(2023, 2, 28), DateAmountParser.minus(leapDay, "1 year"));
    }

    // ================================================================
    // Month-end clamping
    // ================================================================

    @Test
    void testPlusOneMonthFromJan31() {
        assertEquals(zonedDate(2026, 2, 28), DateAmountParser.plus(zonedDate(2026, 1, 31), "1 month"));
    }

    @Test
    void testPlusOneMonthFromJan31LeapYear() {
        assertEquals(zonedDate(2024, 2, 29), DateAmountParser.plus(zonedDate(2024, 1, 31), "1 month"));
    }

    @Test
    void testPlusOneMonthFromMarch31() {
        // Mar 31 + 1 month -> Apr 30
        assertEquals(zonedDate(2026, 4, 30), DateAmountParser.plus(zonedDate(2026, 3, 31), "1 month"));
    }

    @Test
    void testMinusOneMonthFromMarch31() {
        // Mar 31 - 1 month -> Feb 28
        assertEquals(zonedDate(2026, 2, 28), DateAmountParser.minus(zonedDate(2026, 3, 31), "1 month"));
    }

    // ================================================================
    // Date-only input + sub-day adjustments (midnight baseline)
    // ================================================================

    @Test
    void testPlusHoursFromMidnight() {
        assertEquals(
                ZonedDateTime.of(2026, 8, 4, 2, 0, 0, 0, ZoneId.systemDefault()),
                DateAmountParser.plus(AUG_4_2026, "2 hours"));
    }

    @Test
    void testMinusOneSecondFromMidnight() {
        assertEquals(
                ZonedDateTime.of(2026, 8, 3, 23, 59, 59, 0, ZoneId.systemDefault()),
                DateAmountParser.minus(AUG_4_2026, "1 second"));
    }

    @Test
    void testPlus25HoursCrossesDay() {
        assertEquals(
                ZonedDateTime.of(2026, 8, 5, 1, 0, 0, 0, ZoneId.systemDefault()),
                DateAmountParser.plus(AUG_4_2026, "25 hours"));
    }

    @Test
    void testPlus90MinutesFromMidnight() {
        assertEquals(
                ZonedDateTime.of(2026, 8, 4, 1, 30, 0, 0, ZoneId.systemDefault()),
                DateAmountParser.plus(AUG_4_2026, "90 minutes"));
    }

    // ================================================================
    // Nanoseconds
    // ================================================================

    @Test
    void testPlusNanosecondFromMidnight() {
        assertEquals(
                ZonedDateTime.of(2026, 8, 4, 0, 0, 0, 1, ZoneId.systemDefault()),
                DateAmountParser.plus(AUG_4_2026, "1 nanosecond"));
    }

    @Test
    void testMinusNanosecondFromMidnight() {
        assertEquals(
                ZonedDateTime.of(2026, 8, 3, 23, 59, 59, 999_999_999, ZoneId.systemDefault()),
                DateAmountParser.minus(AUG_4_2026, "1 nanosecond"));
    }

    @Test
    void testNanosecondSingularAndPluralMatch() {
        assertEquals(
                DateAmountParser.plus(AUG_4_2026, "1 nanosecond"),
                DateAmountParser.plus(AUG_4_2026, "1 nanoseconds"));
    }

    // ================================================================
    // Large amounts
    // ================================================================

    @Test
    void testLargeAmounts() {
        assertEquals(AUG_4_2026.plusDays(365), DateAmountParser.plus(AUG_4_2026, "365 days"));
        assertEquals(AUG_4_2026.plusHours(1000), DateAmountParser.plus(AUG_4_2026, "1000 hours"));
    }

    // ================================================================
    // Helper
    // ================================================================

    private static ZonedDateTime zonedDate(final int year, final int month, final int day) {
        return ZonedDateTime.of(year, month, day, 0, 0, 0, 0, ZoneId.systemDefault());
    }
}
