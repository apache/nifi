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

import org.apache.nifi.serialization.record.util.IllegalTypeConversionException;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.time.temporal.TemporalQuery;
import java.util.Date;
import java.util.Optional;

/**
 * Convert Object to java.time.LocalDateTime using instanceof evaluation and optional format pattern for DateTimeFormatter
 */
class ObjectLocalDateTimeFieldConverter implements FieldConverter<Object, LocalDateTime> {
    private static final long YEAR_TEN_THOUSAND = 253_402_300_800_000L;

    private static final TemporalQuery<LocalDateTime> LOCAL_DATE_TIME_TEMPORAL_QUERY = new LocalDateTimeQuery();

    /**
     * Convert Object field to java.sql.Timestamp using optional format supported in DateTimeFormatter
     *
     * @param field Field can be null or a supported input type
     * @param pattern Format pattern optional for parsing
     * @param name Field name for tracking
     * @return Timestamp or null when input field is null or empty string
     * @throws IllegalTypeConversionException Thrown on parsing failures or unsupported types of input fields
     */
    @Override
    public LocalDateTime convertField(final Object field, final Optional<String> pattern, final String name) {
        switch (field) {
            case null -> {
                return null;
            }
            case LocalDateTime localDateTime -> {
                return localDateTime;
            }
            case Date date -> {
                final Instant instant = Instant.ofEpochMilli(date.getTime());
                return ofInstant(instant);
            }
            case final Number number -> {
                // If value is a floating point number, we consider it as seconds since epoch plus a decimal part for fractions of a second.
                if (field instanceof Double || field instanceof Float) {
                    return toLocalDateTime(number.doubleValue());
                }

                return toLocalDateTime(number.longValue());
            }
            case String ignored -> {
                final String string = field.toString().trim();
                if (string.isEmpty()) {
                    return null;
                }

                if (pattern.isPresent()) {
                    final DateTimeFormatter formatter = DateTimeFormatterRegistry.getDateTimeFormatter(pattern.get());
                    try {
                        return formatter.parse(string, LOCAL_DATE_TIME_TEMPORAL_QUERY);
                    } catch (final DateTimeParseException e) {
                        return tryParseAsNumber(string, name);
                    }
                } else {
                    return tryParseAsNumber(string, name);
                }
            }
            default -> {
            }
        }

        throw new FieldConversionException(LocalDateTime.class, field, name);
    }

    private LocalDateTime tryParseAsNumber(final String value, final String fieldName) {
        try {
            // If decimal, treat as a double and convert to seconds and nanoseconds.
            if (value.contains(".")) {
                final double number = Double.parseDouble(value);
                return toLocalDateTime(number);
            }

            // attempt to parse as a long value
            final long number = Long.parseLong(value);
            return toLocalDateTime(number);
        } catch (final NumberFormatException e) {
            throw new FieldConversionException(LocalDateTime.class, value, fieldName, e);
        }
    }

    private LocalDateTime toLocalDateTime(final double secondsSinceEpoch) {
        // Determine the number of micros past the second by subtracting the number of seconds from the decimal value and multiplying by 1 million.
        final double micros = 1_000_000 * (secondsSinceEpoch - (long) secondsSinceEpoch);
        // Convert micros to nanos. Note that we perform this as a separate operation, rather than multiplying by 1_000,000,000 in order to avoid
        // issues that occur with rounding at high precision.
        final long nanos = (long) micros * 1000L;

        return toLocalDateTime((long) secondsSinceEpoch, nanos);
    }

    private LocalDateTime toLocalDateTime(final long epochSeconds, final long nanosPastSecond) {
        final Instant instant = Instant.ofEpochSecond(epochSeconds).plusNanos(nanosPastSecond);
        return ofInstant(instant);
    }

    private LocalDateTime toLocalDateTime(final long value) {
        if (value > YEAR_TEN_THOUSAND) {
            // Value is too large. Assume microseconds instead of milliseconds.
            final Instant microsInstant = Instant.ofEpochSecond(value / 1_000_000, (value % 1_000_000) * 1_000);
            return ofInstant(microsInstant);
        }

        final Instant instant = Instant.ofEpochMilli(value);

        return ofInstant(instant);
    }

    private static LocalDateTime ofInstant(final Instant instant) {
        return LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
    }

    private static class LocalDateTimeQuery implements TemporalQuery<LocalDateTime> {

        @Override
        public LocalDateTime queryFrom(final TemporalAccessor temporal) {
            final LocalDateTime localDateTime;

            // Query for ZoneId or ZoneOffset to determine time zone handling
            final ZoneId zoneId = temporal.query(TemporalQueries.zone());
            if (zoneId == null) {
                localDateTime = LocalDateTime.from(temporal);
            } else {
                final ZonedDateTime zonedDateTime = ZonedDateTime.from(temporal);
                // Convert Instant to LocalDateTime using system default zone offset to incorporate adjusted hours and minutes
                final Instant instant = zonedDateTime.toInstant();
                localDateTime = ofInstant(instant);
            }

            return localDateTime;
        }
    }
}
