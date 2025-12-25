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

import java.math.BigDecimal;
import java.sql.Timestamp;
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
            case Timestamp timestamp -> {
                return timestamp.toLocalDateTime();
            }
            case Date date -> {
                // java.sql.Date and java.sql.Time do not support the toInstant() method so using getTime() is required
                final Instant instant = Instant.ofEpochMilli(date.getTime());
                return ofInstant(instant);
            }
            case BigDecimal bd -> {
                return toLocalDateTime(bd);
            }
            case Long value -> {
                return toLocalDateTime(value);
            }
            case final Number number -> {
                return toLocalDateTime(number.toString());
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
            return toLocalDateTime(value);
        } catch (final NumberFormatException e) {
            throw new FieldConversionException(LocalDateTime.class, value, fieldName, e);
        }
    }

    private LocalDateTime toLocalDateTime(final BigDecimal epochMilliseconds) {
        final BigDecimal[] parts = epochMilliseconds.divideAndRemainder(BigDecimal.ONE);

        final long wholeMillis = parts[0].longValueExact();
        final BigDecimal fractionalMillis = parts[1];

        final long nanos = fractionalMillis.multiply(BigDecimal.valueOf(1_000_000)).longValue();

        final Instant instant = Instant.ofEpochMilli(wholeMillis).plusNanos(nanos);
        return ofInstant(instant);
    }

    private LocalDateTime toLocalDateTime(final String epochMilliseconds) {
        return toLocalDateTime(new BigDecimal(epochMilliseconds));
    }

    private LocalDateTime toLocalDateTime(final Long epochMilliseconds) {
        final Instant instant = Instant.ofEpochMilli(epochMilliseconds);
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
