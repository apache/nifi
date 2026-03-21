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
package org.apache.nifi.attribute.expression.language.evaluation.functions;

import org.apache.nifi.attribute.expression.language.EvaluationContext;
import org.apache.nifi.attribute.expression.language.StandardEvaluationContext;
import org.apache.nifi.attribute.expression.language.evaluation.BooleanEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.BooleanQueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.literals.StringLiteralEvaluator;
import org.apache.nifi.util.FormatUtils;

import java.text.ParsePosition;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.zone.ZoneRules;
import java.util.Collections;

/**
 * Evaluator for the {@code isValidDate(format[, timeZone])} Expression Language function.
 * Returns {@code true} if the subject parses successfully against the given format, represents
 * a real calendar date (e.g. rejects February 31), and — when a named timezone is supplied —
 * falls outside any DST gap (e.g. rejects 02:30 when clocks spring forward); {@code false} otherwise.
 */
public class IsValidDateEvaluator extends BooleanEvaluator {

    private final Evaluator<String> subject;
    private final Evaluator<String> format;
    private final Evaluator<String> timeZone;

    private final DateTimeFormatter preparedFormatter;
    private final boolean preparedFormatterHasRequestedTimeZone;
    private final ZoneRules preparedZoneRules;

    public IsValidDateEvaluator(final Evaluator<String> subject, final Evaluator<String> format, final Evaluator<String> timeZone) {
        this.subject = subject;
        this.format = format;
        if (format instanceof StringLiteralEvaluator) {
            final String evaluatedFormat = format.evaluate(new StandardEvaluationContext(Collections.emptyMap())).getValue();
            final DateTimeFormatter dtf = FormatUtils.prepareLenientCaseInsensitiveDateTimeFormatter(evaluatedFormat);
            if (timeZone == null) {
                preparedFormatter = dtf;
                preparedFormatterHasRequestedTimeZone = true;
            } else if (timeZone instanceof StringLiteralEvaluator) {
                preparedFormatter = dtf.withZone(ZoneId.of(timeZone.evaluate(new StandardEvaluationContext(Collections.emptyMap())).getValue()));
                preparedFormatterHasRequestedTimeZone = true;
            } else {
                preparedFormatter = dtf;
                preparedFormatterHasRequestedTimeZone = false;
            }
        } else {
            preparedFormatter = null;
            preparedFormatterHasRequestedTimeZone = false;
        }
        this.timeZone = timeZone;
        if (preparedFormatterHasRequestedTimeZone) {
            final ZoneId zone = preparedFormatter.getZone();
            preparedZoneRules = (zone != null && !(zone instanceof ZoneOffset)) ? zone.getRules() : null;
        } else {
            preparedZoneRules = null;
        }
    }

    @Override
    public QueryResult<Boolean> evaluate(final EvaluationContext evaluationContext) {
        final String subjectValue = subject.evaluate(evaluationContext).getValue();
        if (subjectValue == null) {
            return new BooleanQueryResult(false);
        }
        final String trimmedValue = subjectValue.trim();

        DateTimeFormatter dtf;
        if (preparedFormatter != null) {
            dtf = preparedFormatter;
        } else {
            final String formatValue = format.evaluate(evaluationContext).getValue();
            if (formatValue == null) {
                return new BooleanQueryResult(false);
            }
            try {
                dtf = FormatUtils.prepareLenientCaseInsensitiveDateTimeFormatter(formatValue);
            } catch (final IllegalArgumentException e) {
                return new BooleanQueryResult(false);
            }
        }

        if (!preparedFormatterHasRequestedTimeZone && timeZone != null) {
            final String tz = timeZone.evaluate(evaluationContext).getValue();
            if (tz != null) {
                try {
                    dtf = dtf.withZone(ZoneId.of(tz));
                } catch (final Exception e) {
                    return new BooleanQueryResult(false);
                }
            }
        }

        // Use parseUnresolved to get raw field values without lenient resolution
        final ParsePosition pos = new ParsePosition(0);
        final TemporalAccessor parsed = dtf.parseUnresolved(trimmedValue, pos);
        if (parsed == null || pos.getErrorIndex() >= 0 || pos.getIndex() < trimmedValue.length()) {
            return new BooleanQueryResult(false);
        }

        // If the parsed result contains date fields, validate the calendar combination strictly.
        // LocalDate.of() always validates calendar rules (e.g., rejects February 31).
        int year = 2000; // leap year: allows Feb 29 to be valid when no year in format
        int month = -1;
        int day = -1;
        if (parsed.isSupported(ChronoField.DAY_OF_MONTH) && parsed.isSupported(ChronoField.MONTH_OF_YEAR)) {
            try {
                if (parsed.isSupported(ChronoField.YEAR)) {
                    year = parsed.get(ChronoField.YEAR);
                } else if (parsed.isSupported(ChronoField.YEAR_OF_ERA)) {
                    year = parsed.get(ChronoField.YEAR_OF_ERA);
                }
                month = parsed.get(ChronoField.MONTH_OF_YEAR);
                day = parsed.get(ChronoField.DAY_OF_MONTH);
                LocalDate.of(year, month, day);
            } catch (final DateTimeException e) {
                return new BooleanQueryResult(false);
            }
        }

        // If we have a named timezone (one with DST rules) and time fields, check that the
        // local date-time actually exists — times in a DST gap (e.g. 02:30 when clocks spring
        // forward) have no valid offset and should be rejected.
        final ZoneRules zoneRules;
        if (preparedFormatterHasRequestedTimeZone) {
            zoneRules = preparedZoneRules;
        } else {
            final ZoneId zone = dtf.getZone();
            zoneRules = (zone != null && !(zone instanceof ZoneOffset)) ? zone.getRules() : null;
        }
        if (zoneRules != null && month != -1 && (parsed.isSupported(ChronoField.HOUR_OF_DAY)
                || parsed.isSupported(ChronoField.HOUR_OF_AMPM)
                || parsed.isSupported(ChronoField.CLOCK_HOUR_OF_AMPM))) {
            try {
                final int hour;
                if (parsed.isSupported(ChronoField.HOUR_OF_DAY)) {
                    hour = parsed.get(ChronoField.HOUR_OF_DAY);
                } else {
                    final int hourOfAmPm = parsed.isSupported(ChronoField.HOUR_OF_AMPM)
                            ? parsed.get(ChronoField.HOUR_OF_AMPM)
                            : parsed.get(ChronoField.CLOCK_HOUR_OF_AMPM) % 12;
                    final int amPm = parsed.isSupported(ChronoField.AMPM_OF_DAY) ? parsed.get(ChronoField.AMPM_OF_DAY) : 0;
                    hour = amPm * 12 + hourOfAmPm;
                }
                final int minute = parsed.isSupported(ChronoField.MINUTE_OF_HOUR) ? parsed.get(ChronoField.MINUTE_OF_HOUR) : 0;
                final int second = parsed.isSupported(ChronoField.SECOND_OF_MINUTE) ? parsed.get(ChronoField.SECOND_OF_MINUTE) : 0;
                final LocalDateTime ldt = LocalDateTime.of(year, month, day, hour, minute, second);
                if (zoneRules.getValidOffsets(ldt).isEmpty()) {
                    return new BooleanQueryResult(false);
                }
            } catch (final DateTimeException e) {
                return new BooleanQueryResult(false);
            }
        }

        return new BooleanQueryResult(true);
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return subject;
    }
}
