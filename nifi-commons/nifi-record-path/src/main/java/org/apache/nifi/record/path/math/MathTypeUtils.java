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
package org.apache.nifi.record.path.math;

import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.StandardFieldValue;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

public class MathTypeUtils {
    public static FieldValue toNumber(FieldValue fieldValue) {
        Number result = coerceNumber(fieldValue);
        DataType resultType = isLongCompatible(result) ? RecordFieldType.LONG.getDataType() : RecordFieldType.DOUBLE.getDataType();

        return new StandardFieldValue(result, new RecordField("toNumber", resultType), null);
    }

    public static Number coerceNumber(FieldValue fieldValue) {
        final Object value = fieldValue.getValue();

        if (value instanceof Number) {
            return (Number) value;
        }

        final RecordField field = fieldValue.getField();
        final String fieldName = field == null ? "<Anonymous Inner Field>" : field.getFieldName();

        if (DataTypeUtils.isLongTypeCompatible(value)) {
            return DataTypeUtils.toLong(value, fieldName);
        }
        if (DataTypeUtils.isDoubleTypeCompatible(value)) {
            return DataTypeUtils.toDouble(value, fieldName);
        }
        throw new IllegalArgumentException("Cannot coerce field '" + fieldName + "' to number");
    }

    public static boolean isLongCompatible(Number value) {
        return (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte);
    }
}
