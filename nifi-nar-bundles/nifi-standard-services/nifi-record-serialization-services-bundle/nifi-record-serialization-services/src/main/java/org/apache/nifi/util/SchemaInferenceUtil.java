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
package org.apache.nifi.util;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.nifi.schema.inference.TimeValueInference;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordFieldType;

import java.util.Optional;

public final class SchemaInferenceUtil {
    private SchemaInferenceUtil() {
        // Not intended to be instantiated
    }

    public static DataType getDataType(final String value) {
        return getDataType(value, Optional.empty());
    }

    public static DataType getDataType(final String value, final TimeValueInference timeValueInference) {
        return getDataType(value, Optional.of(timeValueInference));
    }

    private static DataType getDataType(final String value, final Optional<TimeValueInference> timeValueInference) {
        if (value == null || value.isEmpty()) {
            return null;
        }

        if (NumberUtils.isParsable(value)) {
            if (value.contains(".")) {
                try {
                    final double doubleValue = Double.parseDouble(value);

                    if (doubleValue == Double.POSITIVE_INFINITY || doubleValue == Double.NEGATIVE_INFINITY) {
                        return RecordFieldType.DECIMAL.getDecimalDataType(value.length() - 1, value.length() - 1 - value.indexOf("."));
                    }

                    if (doubleValue > Float.MAX_VALUE || doubleValue < Float.MIN_VALUE) {
                        return RecordFieldType.DOUBLE.getDataType();
                    }

                    return RecordFieldType.FLOAT.getDataType();
                } catch (final NumberFormatException nfe) {
                    return RecordFieldType.STRING.getDataType();
                }
            }

            try {
                final long longValue = Long.parseLong(value);
                if (longValue > Integer.MAX_VALUE || longValue < Integer.MIN_VALUE) {
                    return RecordFieldType.LONG.getDataType();
                }

                return RecordFieldType.INT.getDataType();
            } catch (final NumberFormatException nfe) {
                return RecordFieldType.STRING.getDataType();
            }
        }

        if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) {
            return RecordFieldType.BOOLEAN.getDataType();
        }

        if (timeValueInference.isPresent()) {
            final Optional<DataType> timeDataType = timeValueInference.get().getDataType(value);
            return timeDataType.orElse(RecordFieldType.STRING.getDataType());
        } else {
            return RecordFieldType.STRING.getDataType();
        }
    }
}
