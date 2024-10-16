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

import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Optional;

/**
 * Convert Object to String using instanceof evaluation and optional format pattern for DateTimeFormatter
 */
class ObjectStringFieldConverter implements FieldConverter<Object, String> {
    /**
     * Convert Object field to String using optional format supported in DateTimeFormatter
     *
     * @param field Field can be null or a supported input type
     * @param pattern Format pattern optional for parsing
     * @param name Field name for tracking
     * @return String or null when input field is null or empty string
     * @throws IllegalTypeConversionException Thrown on parsing failures or unsupported types of input fields
     */
    @Override
    public String convertField(final Object field, final Optional<String> pattern, final String name) {
        switch (field) {
            case null -> {
                return null;
            }
            case String ignored -> {
                return field.toString();
            }
            case Timestamp timestamp -> {
                if (pattern.isEmpty()) {
                    return Long.toString(timestamp.getTime());
                }
                final DateTimeFormatter formatter = DateTimeFormatterRegistry.getDateTimeFormatter(pattern.get());
                final LocalDateTime localDateTime = timestamp.toLocalDateTime();

                // Convert LocalDateTime to ZonedDateTime using system default zone to support offsets in Date Time Formatter
                final ZonedDateTime dateTime = ZonedDateTime.of(localDateTime, ZoneId.systemDefault());
                return formatter.format(dateTime);
            }
            case Date date -> {
                if (pattern.isEmpty()) {
                    return Long.toString(date.getTime());
                }
                final DateTimeFormatter formatter = DateTimeFormatterRegistry.getDateTimeFormatter(pattern.get());
                // java.sql.Date and java.sql.Time do not support toInstant()
                final Instant instant = Instant.ofEpochMilli(date.getTime());
                final ZonedDateTime dateTime = instant.atZone(ZoneId.systemDefault());
                return formatter.format(dateTime);
            }
            case byte[] bytes -> {
                return new String(bytes, StandardCharsets.UTF_16);
            }
            case Byte[] bytes -> {
                final byte[] converted = new byte[bytes.length];
                for (int i = 0; i < bytes.length; i++) {
                    converted[i] = bytes[i];
                }
                return new String(converted, StandardCharsets.UTF_16);
            }
            case Clob clob -> {
                final StringBuilder builder = new StringBuilder();
                final char[] buffer = new char[32768];
                try (Reader reader = clob.getCharacterStream()) {
                    int charsRead;
                    while ((charsRead = reader.read(buffer)) != -1) {
                        builder.append(buffer, 0, charsRead);
                    }
                    return builder.toString();
                } catch (final Exception e) {
                    throw new FieldConversionException(String.class, field, name, e);
                }
            }
            default -> {
            }
        }
        return field.toString();
    }
}
