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
import org.apache.nifi.serialization.record.util.FractionalSecondsUtils;

import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Convert Object to java.sql.Timestamp using instanceof evaluation and optional format pattern for DateTimeFormatter
 */
class ObjectTimestampFieldConverter implements FieldConverter<Object, Timestamp> {
    /** The timezone characters and their legal cardinality detailed in the regular expression below are all defined in the grammar specified
     * in the javadoc for java.time.format.DateTimeFormatter. The regular expression below checks for these characters as unescaped
     * when specified in a timestamp pattern.*/
    private static final String TIMEZONE_CHARACTERS_WITH_CARDINALITIES = "(?:[O]|[O]{4}|[x]{1,5}|[X]{1,5}|[z]{1,4}|[Z]{1,5})";
    private static final String BEGINNING = "^" + TIMEZONE_CHARACTERS_WITH_CARDINALITIES;
    private static final String LEADING_QUOTE = "'" + TIMEZONE_CHARACTERS_WITH_CARDINALITIES + "[^']";
    private static final String TRAILING_QUOTE = "[^']" + TIMEZONE_CHARACTERS_WITH_CARDINALITIES + "'";
    private static final String END = TIMEZONE_CHARACTERS_WITH_CARDINALITIES + "$";
    static final Pattern TIMEZONE_PATTERN = Pattern.compile(BEGINNING + "|" + LEADING_QUOTE + "|" + TRAILING_QUOTE + "|" + END);

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
    public Timestamp convertField(final Object field, final Optional<String> pattern, final String name) {
        Instant instant = null;
        switch (field) {
            case null -> {
                return null;
            }
            case Timestamp timestamp -> {
                return timestamp;
            }
            case ZonedDateTime zonedDateTime -> {
                instant = zonedDateTime.toInstant();
            }
            case Time time -> {
                // Convert to an Instant object preserving millisecond precision
                final long epochMilli = time.getTime();
                instant = Instant.ofEpochMilli(epochMilli);
            }
            case Date date -> {
                final long epochMilli = date.getTime();
                instant = Instant.ofEpochMilli(epochMilli);
            }
            case Number number -> {
                switch (field) {
                    case Double d -> instant = FractionalSecondsUtils.toInstant(d);
                    case Float f -> instant = FractionalSecondsUtils.toInstant(f.doubleValue());
                    default -> instant = FractionalSecondsUtils.toInstant(number.longValue());
                }
            }
            case String string -> {
                final String stringTrimmed = string.trim();
                if (stringTrimmed.isEmpty()) {
                    return null;
                }

                if (pattern.isPresent()) {
                    final String patternString = pattern.get();
                    final DateTimeFormatter formatter = DateTimeFormatterRegistry.getDateTimeFormatter(patternString);
                    try {
                        // NOTE: In order to calculate any possible timezone offsets, the string must be parsed as a ZoneDateTime.
                        // It is not possible to always parse as a ZoneDateTime as it will fail if the pattern has
                        // no timezone information. Hence, a regular expression is used to determine whether it is necessary
                        // to parse with ZoneDateTime or not.
                        final Matcher matcher = TIMEZONE_PATTERN.matcher(patternString);
                        final ZonedDateTime zonedDateTime;

                        if (matcher.find()) {
                            zonedDateTime = ZonedDateTime.parse(stringTrimmed, formatter);
                        } else {
                            final LocalDateTime localDateTime = LocalDateTime.parse(stringTrimmed, formatter);
                            zonedDateTime = ZonedDateTime.of(localDateTime, ZoneId.systemDefault());
                        }
                        instant = zonedDateTime.toInstant();
                    } catch (final DateTimeParseException e) {
                        return tryParseAsNumber(stringTrimmed, name);
                    }
                } else {
                    return tryParseAsNumber(stringTrimmed, name);
                }
            }
            default -> {
            }
        }

        if (instant != null) {
            return Timestamp.from(instant);
        }
        throw new FieldConversionException(Timestamp.class, field, name);
    }

    private Timestamp tryParseAsNumber(final String value, final String fieldName) {
        try {
            final Instant instant = FractionalSecondsUtils.tryParseAsNumber(value);
            return Timestamp.from(instant);
        } catch (final NumberFormatException e) {
            throw new FieldConversionException(Timestamp.class, value, fieldName, e);
        }
    }
}
