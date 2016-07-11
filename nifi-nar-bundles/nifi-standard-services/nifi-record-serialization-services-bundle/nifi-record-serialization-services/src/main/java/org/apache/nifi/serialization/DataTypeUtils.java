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

package org.apache.nifi.serialization;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

public class DataTypeUtils {

    public static Double toDouble(final Object value, final Double defaultValue) {
        if (value == null) {
            return null;
        }

        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }

        if (value instanceof String) {
            return Double.parseDouble((String) value);
        }

        return defaultValue;
    }

    public static Float toFloat(final Object value, final Float defaultValue) {
        if (value == null) {
            return null;
        }

        if (value instanceof Number) {
            return ((Number) value).floatValue();
        }

        if (value instanceof String) {
            return Float.parseFloat((String) value);
        }

        return defaultValue;
    }

    public static Long toLong(final Object value, final Long defaultValue) {
        if (value == null) {
            return null;
        }

        if (value instanceof Number) {
            return ((Number) value).longValue();
        }

        if (value instanceof String) {
            return Long.parseLong((String) value);
        }

        return defaultValue;
    }



    public static Integer toInteger(final Object value, final Integer defaultValue) {
        if (value == null) {
            return null;
        }

        if (value instanceof Number) {
            return ((Number) value).intValue();
        }

        if (value instanceof String) {
            return Integer.parseInt((String) value);
        }

        return defaultValue;
    }


    /**
     * Deduces the type of RecordFieldType that should be used for a value of the given type,
     * or returns <code>null</code> if the value is null
     *
     * @param value the value whose type should be deduced
     * @return the type of RecordFieldType that should be used for a value of the given type,
     *         or <code>null</code> if the value is null
     */
    public static DataType inferDataType(final Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof String) {
            return RecordFieldType.STRING.getDataType();
        }
        if (value instanceof Long) {
            return RecordFieldType.LONG.getDataType();
        }
        if (value instanceof Integer) {
            return RecordFieldType.INT.getDataType();
        }
        if (value instanceof Double) {
            return RecordFieldType.DOUBLE.getDataType();
        }
        if (value instanceof Float) {
            return RecordFieldType.FLOAT.getDataType();
        }
        if (value instanceof Boolean) {
            return RecordFieldType.BOOLEAN.getDataType();
        }
        if (value instanceof Byte) {
            return RecordFieldType.BYTE.getDataType();
        }
        if (value instanceof Character) {
            return RecordFieldType.CHAR.getDataType();
        }
        if (value instanceof Short) {
            return RecordFieldType.SHORT.getDataType();
        }
        if (value instanceof Date) {
            return RecordFieldType.DATE.getDataType();
        }
        if (value instanceof Object[] || value instanceof List) {
            return RecordFieldType.ARRAY.getDataType();
        }
        if (value instanceof Map) {
            @SuppressWarnings("unchecked")
            final Map<String, Object> map = (Map<String, Object>) value;
            final RecordSchema childSchema = determineSchema(map);
            return RecordFieldType.RECORD.getDataType(childSchema);
        }

        return RecordFieldType.RECORD.getDataType();
    }

    public static RecordSchema determineSchema(final Map<String, Object> valueMap) {
        final List<RecordField> fields = new ArrayList<>(valueMap.size());
        for (final Map.Entry<String, Object> entry : valueMap.entrySet()) {
            final DataType valueType = inferDataType(entry.getValue());
            final String fieldName = entry.getKey();
            final RecordField field = new RecordField(fieldName, valueType);
            fields.add(field);
        }
        return new SimpleRecordSchema(fields);
    }
}
