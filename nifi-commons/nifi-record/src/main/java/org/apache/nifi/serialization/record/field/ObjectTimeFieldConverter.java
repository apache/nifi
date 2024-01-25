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
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;

/**
 * Convert Object to java.sql.Time using instanceof evaluation and optional format pattern for DateTimeFormatter
 */
class ObjectTimeFieldConverter implements FieldConverter<Object, Time> {
    private static final ObjectLocalTimeFieldConverter CONVERTER = new ObjectLocalTimeFieldConverter();

    /**
     * Convert Object field to java.sql.Time using optional format supported in DateTimeFormatter
     *
     * @param field Field can be null or a supported input type
     * @param pattern Format pattern optional for parsing
     * @param name Field name for tracking
     * @return Timestamp or null when input field is null or empty string
     * @throws IllegalTypeConversionException Thrown on parsing failures or unsupported types of input fields
     */
    @Override
    public Time convertField(final Object field, final Optional<String> pattern, final String name) {
        final LocalTime localTime = CONVERTER.convertField(field, pattern, name);

        final Time time;
        if (localTime == null) {
            time = null;
        } else {
            final ZonedDateTime zonedDateTime = localTime.atDate(LocalDate.ofEpochDay(0)).atZone(ZoneId.systemDefault());
            final long epochMilli = zonedDateTime.toInstant().toEpochMilli();
            time = new Time(epochMilli);
        }
        return time;
    }
}
