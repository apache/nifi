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

import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Objects;

public class StandardFieldConverterRegistry implements FieldConverterRegistry {
    private static final Map<Class<?>, FieldConverter<Object, ?>> FIELD_CONVERTERS = Map.of(
            Time.class, new ObjectTimeFieldConverter(),
            Timestamp.class, new ObjectTimestampFieldConverter(),
            LocalDate.class, new ObjectLocalDateFieldConverter(),
            LocalDateTime.class, new ObjectLocalDateTimeFieldConverter(),
            LocalTime.class, new ObjectLocalTimeFieldConverter(),
            OffsetDateTime.class, new ObjectOffsetDateTimeFieldConverter(),
            String.class, new ObjectStringFieldConverter()
    );

    private static final StandardFieldConverterRegistry REGISTRY = new StandardFieldConverterRegistry();

    private StandardFieldConverterRegistry() {

    }

    /**
     * Get Field Converter Registry instance for access to configured Field Converters
     *
     * @return Field Converter Registry
     */
    public static FieldConverterRegistry getRegistry() {
        return REGISTRY;
    }

    /**
     * Get Field Converter for specified output field type class
     *
     * @param outputFieldType Output Field Type Class
     * @return Field Converter
     * @param <T> Output Field Type
     * @throws IllegalArgumentException Thrown when Field Converter not found for Output Field Type
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> FieldConverter<Object, T> getFieldConverter(final Class<T> outputFieldType) {
        Objects.requireNonNull(outputFieldType, "Output Field Type required");

        final FieldConverter<Object, ?> fieldConverter = FIELD_CONVERTERS.get(outputFieldType);
        if (fieldConverter == null) {
            throw new IllegalArgumentException("Field Converter not found for Output Field Type [%s]".formatted(outputFieldType));
        }

        return (FieldConverter<Object, T>) fieldConverter;
    }
}
