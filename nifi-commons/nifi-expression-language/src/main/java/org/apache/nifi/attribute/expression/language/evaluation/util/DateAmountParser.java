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
package org.apache.nifi.attribute.expression.language.evaluation.util;

import org.apache.nifi.attribute.expression.language.exception.AttributeExpressionLanguageException;

import java.time.Duration;
import java.time.Period;
import java.time.ZonedDateTime;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses a human-readable duration expression (e.g. {@code "2 weeks"}, {@code "1 month"})
 * and applies calendar-aware arithmetic to a {@link ZonedDateTime}.
 *
 * <p>Format: {@code <positive-integer> <unit>} — a space between number and unit is required.
 * Units are case-insensitive and accept singular/plural: second(s), minute(s), hour(s),
 * day(s), week(s), month(s), year(s).</p>
 */
public final class DateAmountParser {

    // Requires at least one whitespace character (\s+) between the integer and unit.
    private static final Pattern AMOUNT_PATTERN = Pattern.compile(
            "^\\s*(\\d+)\\s+(nanoseconds?|seconds?|minutes?|hours?|days?|weeks?|months?|years?)\\s*$",
            Pattern.CASE_INSENSITIVE
    );

    private static final String EXPECTED_FORMAT =
            "Expected format: '<integer> <unit>' (e.g. '2 weeks', '1 month'). "
                    + "Supported units: nanosecond(s), second(s), minute(s), hour(s), day(s), week(s), month(s), year(s)";

    private record ParsedAmount(long amount, String unit) { }

    private DateAmountParser() { }

    /**
     * Validates that the expression matches {@code <integer> <unit>} format.
     * Called at evaluator construction time for literal arguments so the processor
     * fails validation before it can start.
     */
    public static void validate(final String amountExpression) {
        parse(amountExpression);
    }

    /** Adds the parsed amount to the given date-time. */
    public static ZonedDateTime plus(final ZonedDateTime dateTime, final String amountExpression) {
        return applyAmount(dateTime, parse(amountExpression), true);
    }

    /** Subtracts the parsed amount from the given date-time. */
    public static ZonedDateTime minus(final ZonedDateTime dateTime, final String amountExpression) {
        return applyAmount(dateTime, parse(amountExpression), false);
    }

    /**
     * Parses and validates the amount expression in a single pass.
     * Shared by both {@link #validate} and the arithmetic methods.
     */
    private static ParsedAmount parse(final String amountExpression) {
        if (amountExpression == null || amountExpression.isBlank()) {
            throw new AttributeExpressionLanguageException(
                    "Amount expression cannot be null or empty. " + EXPECTED_FORMAT);
        }

        final Matcher matcher = AMOUNT_PATTERN.matcher(amountExpression);
        if (!matcher.matches()) {
            throw new AttributeExpressionLanguageException(
                    "Invalid amount expression: '" + amountExpression + "'. " + EXPECTED_FORMAT);
        }

        final long amount;
        try {
            amount = Long.parseLong(matcher.group(1));
        } catch (final NumberFormatException e) {
            throw new AttributeExpressionLanguageException(
                    "Numeric overflow in expression: '" + amountExpression + "'. " + EXPECTED_FORMAT, e);
        }

        final String rawUnit = matcher.group(2).toLowerCase(Locale.ROOT);
        // Normalize plural to singular: "seconds" -> "second", "weeks" -> "week"
        final String unit = (rawUnit.length() > 1 && rawUnit.endsWith("s"))
                ? rawUnit.substring(0, rawUnit.length() - 1) : rawUnit;

        return new ParsedAmount(amount, unit);
    }

    private static ZonedDateTime applyAmount(final ZonedDateTime dateTime,
                                             final ParsedAmount parsed, final boolean add) {
        final long amount = parsed.amount();
        return switch (parsed.unit()) {
            case "nanosecond" -> add ? dateTime.plus(Duration.ofNanos(amount))
                    : dateTime.minus(Duration.ofNanos(amount));
            case "second" -> add ? dateTime.plus(Duration.ofSeconds(amount))
                    : dateTime.minus(Duration.ofSeconds(amount));
            case "minute" -> add ? dateTime.plus(Duration.ofMinutes(amount))
                    : dateTime.minus(Duration.ofMinutes(amount));
            case "hour"   -> add ? dateTime.plus(Duration.ofHours(amount))
                    : dateTime.minus(Duration.ofHours(amount));
            case "day"    -> add ? dateTime.plusDays(amount)
                    : dateTime.minusDays(amount);
            case "week"   -> add ? dateTime.plusWeeks(amount)
                    : dateTime.minusWeeks(amount);
            case "month"  -> add ? dateTime.plus(Period.ofMonths(Math.toIntExact(amount)))
                    : dateTime.minus(Period.ofMonths(Math.toIntExact(amount)));
            case "year"   -> add ? dateTime.plus(Period.ofYears(Math.toIntExact(amount)))
                    : dateTime.minus(Period.ofYears(Math.toIntExact(amount)));
            default -> throw new AttributeExpressionLanguageException(
                    "Unsupported time unit: '" + parsed.unit() + "'. " + EXPECTED_FORMAT);
        };
    }
}
