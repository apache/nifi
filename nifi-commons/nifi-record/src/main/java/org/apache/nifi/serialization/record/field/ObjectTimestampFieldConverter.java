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

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Optional;

/**
 * Convert Object to java.sql.Timestamp using instanceof evaluation and optional format pattern for DateTimeFormatter
 */
class ObjectTimestampFieldConverter implements FieldConverter<Object, Timestamp> {
    private static final ObjectLocalDateTimeFieldConverter CONVERTER = new ObjectLocalDateTimeFieldConverter();

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
        final LocalDateTime localDateTime = CONVERTER.convertField(field, pattern, name);
        return localDateTime == null ? null : Timestamp.valueOf(localDateTime);
    }
}
