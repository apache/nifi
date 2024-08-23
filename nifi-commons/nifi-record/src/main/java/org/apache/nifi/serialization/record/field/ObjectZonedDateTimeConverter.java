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

import java.sql.Time;
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

public class ObjectZonedDateTimeConverter implements FieldConverter<Object, ZonedDateTime> {
    /** The timezone characters defined in the timezone pattern below are all defined in the grammar specified
     * in the javadoc for java.time.format.DateTimeFormatter.*/
    private static final Pattern TIMEZONE_PATTERN = Pattern.compile("^.*[OvVxXzZ]$");

    /**
     * Convert Object field to java.time.ZonedDateTime using optional format supported in DateTimeFormatter
     *
     * @param field Field can be null or a supported input type
     * @param pattern Format pattern optional for parsing
     * @param name Field name for tracking
     * @return ZonedDateTime or null when input field is null or empty string
     * @throws IllegalTypeConversionException Thrown on parsing failures or unsupported types of input fields
     */
    @Override
    public ZonedDateTime convertField(Object field, Optional<String> pattern, String name) {
        if (field == null) {
            return null;
        }
        if (field instanceof ZonedDateTime) {
            return (ZonedDateTime) field;
        }
        if (field instanceof Time time) {
            // Convert to an Instant object preserving millisecond precision
            final long epochMilli = time.getTime();
            final Instant instant = Instant.ofEpochMilli(epochMilli);
            return ofInstant(instant);
        }
        if (field instanceof Date date) {
            final long epochMilli = date.getTime();
            final Instant instant = Instant.ofEpochMilli(epochMilli);
            return ofInstant(instant);
        }
        if (field instanceof Number number) {
            final Instant instant = Instant.ofEpochMilli(number.longValue());
            return ofInstant(instant);
        }
        if (field instanceof String) {
            final String string = field.toString().trim();
            if (string.isEmpty()) {
                return null;
            }

            if (pattern.isPresent()) {
                final DateTimeFormatter formatter = DateTimeFormatterRegistry.getDateTimeFormatter(pattern.get());
                try {
                    final String patternString = pattern.get();
                    // NOTE: The parsing of a string as a ZoneDateTime will fail if the pattern has no timezone information
                    // hence only parse the string as a ZonedDateTime if the pattern is a timezone pattern otherwise parse as a LocalDateTime.
                    final Matcher matcher = TIMEZONE_PATTERN.matcher(patternString);
                    if (matcher.matches()) {
                        return ZonedDateTime.parse(string, formatter);
                    } else {
                        final LocalDateTime localDateTime = LocalDateTime.parse(string, formatter);
                        return ZonedDateTime.of(localDateTime, ZoneId.systemDefault());
                    }
                } catch (final DateTimeParseException e) {
                    throw new FieldConversionException(ZonedDateTime.class, field, name, e);
                }
            } else {
                try {
                    final long number = Long.parseLong(string);
                    final Instant instant = Instant.ofEpochMilli(number);
                    return ofInstant(instant);
                } catch (final NumberFormatException e) {
                    throw new FieldConversionException(ZonedDateTime.class, field, name, e);
                }
            }
        }

        throw new FieldConversionException(ZonedDateTime.class, field, name);
    }

    private ZonedDateTime ofInstant(final Instant instant) {
        return instant.atZone(ZoneId.systemDefault());
    }
}
