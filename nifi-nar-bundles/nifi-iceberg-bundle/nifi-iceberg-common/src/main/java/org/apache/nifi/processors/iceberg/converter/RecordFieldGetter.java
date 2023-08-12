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
package org.apache.nifi.processors.iceberg.converter;

import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.serialization.record.util.IllegalTypeConversionException;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.time.ZoneId;

public class RecordFieldGetter {

    /**
     * Creates an accessor for getting elements in an internal record data structure with the given
     * field name.
     *
     * @param dataType   the element type of the field
     * @param fieldName  the name of the field
     * @param isNullable indicates if the field's value is nullable
     */
    public static FieldGetter createFieldGetter(DataType dataType, String fieldName, boolean isNullable) {
        FieldGetter fieldGetter;
        switch (dataType.getFieldType()) {
            case STRING:
                fieldGetter = record -> record.getAsString(fieldName);
                break;
            case CHAR:
                fieldGetter = record -> DataTypeUtils.toCharacter(record.getValue(fieldName), fieldName);
                break;
            case BOOLEAN:
                fieldGetter = record -> record.getAsBoolean(fieldName);
                break;
            case DECIMAL:
                fieldGetter = record -> DataTypeUtils.toBigDecimal(record.getValue(fieldName), fieldName);
                break;
            case BYTE:
                fieldGetter = record -> DataTypeUtils.toByte(record.getValue(fieldName), fieldName);
                break;
            case SHORT:
                fieldGetter = record -> DataTypeUtils.toShort(record.getValue(fieldName), fieldName);
                break;
            case INT:
                fieldGetter = record -> record.getAsInt(fieldName);
                break;
            case DATE:
                fieldGetter = record -> DataTypeUtils.toLocalDate(record.getValue(fieldName), () -> DataTypeUtils.getDateTimeFormatter(dataType.getFormat(), ZoneId.systemDefault()), fieldName);
                break;
            case TIME:
                fieldGetter = record -> DataTypeUtils.toTime(record.getValue(fieldName), () -> DataTypeUtils.getDateFormat(dataType.getFormat()), fieldName);
                break;
            case LONG:
                fieldGetter = record -> record.getAsLong(fieldName);
                break;
            case BIGINT:
                fieldGetter = record -> DataTypeUtils.toBigInt(record.getValue(fieldName), fieldName);
                break;
            case FLOAT:
                fieldGetter = record -> record.getAsFloat(fieldName);
                break;
            case DOUBLE:
                fieldGetter = record -> record.getAsDouble(fieldName);
                break;
            case TIMESTAMP:
                fieldGetter = record -> DataTypeUtils.toTimestamp(record.getValue(fieldName), () -> DataTypeUtils.getDateFormat(dataType.getFormat()), fieldName);
                break;
            case UUID:
                fieldGetter = record -> DataTypeUtils.toUUID(record.getValue(fieldName));
                break;
            case ARRAY:
                fieldGetter = record -> DataTypeUtils.toArray(record.getValue(fieldName), fieldName, ((ArrayDataType) dataType).getElementType());
                break;
            case MAP:
                fieldGetter = record -> DataTypeUtils.toMap(record.getValue(fieldName), fieldName);
                break;
            case RECORD:
                fieldGetter = record -> record.getAsRecord(fieldName, ((RecordDataType) dataType).getChildSchema());
                break;
            case CHOICE:
                fieldGetter = record -> {
                    final ChoiceDataType choiceDataType = (ChoiceDataType) dataType;
                    final Object value = record.getValue(fieldName);
                    final DataType chosenDataType = DataTypeUtils.chooseDataType(value, choiceDataType);
                    if (chosenDataType == null) {
                        throw new IllegalTypeConversionException(String.format(
                                "Cannot convert value [%s] of type %s for field %s to any of the following available Sub-Types for a Choice: %s",
                                value, value.getClass(), fieldName, choiceDataType.getPossibleSubTypes()));
                    }

                    return DataTypeUtils.convertType(record.getValue(fieldName), chosenDataType, fieldName);
                };
                break;
            default:
                throw new IllegalArgumentException("Unsupported field type: " + dataType.getFieldType());
        }

        if (!isNullable) {
            return fieldGetter;
        }

        return record -> {
            if (record.getValue(fieldName) == null) {
                return null;
            }

            return fieldGetter.getFieldOrNull(record);
        };
    }

    /**
     * Accessor for getting the field of a record during runtime.
     */

    public interface FieldGetter extends Serializable {
        @Nullable
        Object getFieldOrNull(Record record);
    }
}
