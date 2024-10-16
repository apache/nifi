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

import java.sql.Date;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Optional;

/**
 * Convert Object to java.time.LocalDate using instanceof evaluation and optional format pattern for DateTimeFormatter
 */
class ObjectLocalDateFieldConverter implements FieldConverter<Object, LocalDate> {
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
    public LocalDate convertField(final Object field, final Optional<String> pattern, final String name) {
        switch (field) {
            case null -> {
                return null;
            }
            case LocalDate localDate -> {
                return localDate;
            }
            case Date date -> {
                return date.toLocalDate();
            }
            case java.util.Date date -> {
                final Instant instant = date.toInstant();
                return ofInstant(instant);
            }
            case Number number -> {
                final Instant instant = Instant.ofEpochMilli(number.longValue());
                return ofInstant(instant);
            }
            case String ignored -> {
                final String string = field.toString().trim();
                if (string.isEmpty()) {
                    return null;
                }

                if (pattern.isPresent()) {
                    final DateTimeFormatter formatter = DateTimeFormatterRegistry.getDateTimeFormatter(pattern.get());
                    try {
                        return LocalDate.parse(string, formatter);
                    } catch (final DateTimeParseException e) {
                        throw new FieldConversionException(LocalDate.class, field, name, e);
                    }
                } else {
                    try {
                        final long number = Long.parseLong(string);
                        final Instant instant = Instant.ofEpochMilli(number);
                        return ofInstant(instant);
                    } catch (final NumberFormatException e) {
                        throw new FieldConversionException(LocalDate.class, field, name, e);
                    }
                }
            }
            default -> {
            }
        }

        throw new FieldConversionException(LocalDate.class, field, name);
    }

    private LocalDate ofInstant(final Instant instant) {
        return LocalDate.ofInstant(instant, ZoneId.systemDefault());
    }
}
