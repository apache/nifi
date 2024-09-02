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

import org.apache.nifi.serialization.record.util.FractionalSecondsUtils;
import org.apache.nifi.serialization.record.util.IllegalTypeConversionException;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.Optional;

/**
 * Convert Object to java.time.LocalDateTime using instanceof evaluation and optional format pattern for DateTimeFormatter
 */
class ObjectLocalDateTimeFieldConverter implements FieldConverter<Object, LocalDateTime> {

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
        if (field == null) {
            return null;
        }
        if (field instanceof LocalDateTime) {
            return (LocalDateTime) field;
        }
        if (field instanceof Date date) {
            final Instant instant = Instant.ofEpochMilli(date.getTime());
            return ofInstant(instant);
        }
        if (field instanceof final Number number) {
            // If value is a floating point number, we consider it as seconds since epoch plus a decimal part for fractions of a second.
            final Instant instant;
            if (field instanceof Double || field instanceof Float) {
                instant = FractionalSecondsUtils.toInstant(number.doubleValue());
            } else {
                instant = FractionalSecondsUtils.toInstant(number.longValue());
            }
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
                    return LocalDateTime.parse(string, formatter);
                } catch (final DateTimeParseException e) {
                    return tryParseAsNumber(string, name);
                }
            } else {
                return tryParseAsNumber(string, name);
            }
        }

        throw new FieldConversionException(LocalDateTime.class, field, name);
    }

    private LocalDateTime tryParseAsNumber(final String value, final String fieldName) {
        try {
            final Instant instant = FractionalSecondsUtils.tryParseAsNumber(value);
            return ofInstant(instant);
        } catch (final NumberFormatException e) {
            throw new FieldConversionException(LocalDateTime.class, value, fieldName, e);
        }
    }

    private LocalDateTime ofInstant(final Instant instant) {
        return LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
    }
}
