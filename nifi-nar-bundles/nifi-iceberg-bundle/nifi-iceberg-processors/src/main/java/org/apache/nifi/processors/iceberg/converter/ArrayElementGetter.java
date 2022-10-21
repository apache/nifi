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
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.serialization.record.util.IllegalTypeConversionException;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.time.ZoneId;

public class ArrayElementGetter {

    private static final String ARRAY_FIELD_NAME = "array element";

    /**
     * Creates an accessor for getting elements in an internal array data structure at the given
     * position.
     *
     * @param dataType the element type of the array
     */
    public static ElementGetter createElementGetter(DataType dataType) {
        ElementGetter elementGetter;
        switch (dataType.getFieldType()) {
            case STRING:
                elementGetter = (array, pos) -> DataTypeUtils.toString(array[pos], ARRAY_FIELD_NAME);
                break;
            case CHAR:
                elementGetter = (array, pos) -> DataTypeUtils.toCharacter(array[pos], ARRAY_FIELD_NAME);
                break;
            case BOOLEAN:
                elementGetter = (array, pos) -> DataTypeUtils.toBoolean(array[pos], ARRAY_FIELD_NAME);
                break;
            case DECIMAL:
                elementGetter = (array, pos) -> DataTypeUtils.toBigDecimal(array[pos], ARRAY_FIELD_NAME);
                break;
            case BYTE:
                elementGetter = (array, pos) -> DataTypeUtils.toByte(array[pos], ARRAY_FIELD_NAME);
                break;
            case SHORT:
                elementGetter = (array, pos) -> DataTypeUtils.toShort(array[pos], ARRAY_FIELD_NAME);
                break;
            case INT:
                elementGetter = (array, pos) -> DataTypeUtils.toInteger(array[pos], ARRAY_FIELD_NAME);
                break;
            case DATE:
                elementGetter = (array, pos) -> DataTypeUtils.toLocalDate(array[pos], () -> DataTypeUtils.getDateTimeFormatter(dataType.getFormat(), ZoneId.systemDefault()), ARRAY_FIELD_NAME);
                break;
            case TIME:
                elementGetter = (array, pos) -> DataTypeUtils.toTime(array[pos], () -> DataTypeUtils.getDateFormat(dataType.getFormat()), ARRAY_FIELD_NAME);
                break;
            case LONG:
                elementGetter = (array, pos) -> DataTypeUtils.toLong(array[pos], ARRAY_FIELD_NAME);
                break;
            case BIGINT:
                elementGetter = (array, pos) -> DataTypeUtils.toBigInt(array[pos], ARRAY_FIELD_NAME);
                break;
            case FLOAT:
                elementGetter = (array, pos) -> DataTypeUtils.toFloat(array[pos], ARRAY_FIELD_NAME);
                break;
            case DOUBLE:
                elementGetter = (array, pos) -> DataTypeUtils.toDouble(array[pos], ARRAY_FIELD_NAME);
                break;
            case TIMESTAMP:
                elementGetter = (array, pos) -> DataTypeUtils.toTimestamp(array[pos], () -> DataTypeUtils.getDateFormat(dataType.getFormat()), ARRAY_FIELD_NAME);
                break;
            case UUID:
                elementGetter = (array, pos) -> DataTypeUtils.toUUID(array[pos]);
                break;
            case ARRAY:
                elementGetter = (array, pos) -> DataTypeUtils.toArray(array[pos], ARRAY_FIELD_NAME, ((ArrayDataType) dataType).getElementType());
                break;
            case MAP:
                elementGetter = (array, pos) -> DataTypeUtils.toMap(array[pos], ARRAY_FIELD_NAME);
                break;
            case RECORD:
                elementGetter = (array, pos) -> DataTypeUtils.toRecord(array[pos], ARRAY_FIELD_NAME);
                break;
            case CHOICE:
                elementGetter = (array, pos) -> {
                    final ChoiceDataType choiceDataType = (ChoiceDataType) dataType;
                    final DataType chosenDataType = DataTypeUtils.chooseDataType(array[pos], choiceDataType);
                    if (chosenDataType == null) {
                        throw new IllegalTypeConversionException(String.format(
                                "Cannot convert value [%s] of type %s for array element to any of the following available Sub-Types for a Choice: %s",
                                array[pos], array[pos].getClass(), choiceDataType.getPossibleSubTypes()));
                    }

                    return DataTypeUtils.convertType(array[pos], chosenDataType, ARRAY_FIELD_NAME);
                };
                break;
            default:
                throw new IllegalArgumentException("Unsupported field type: " + dataType.getFieldType());
        }

        return (array, pos) -> {
            if (array[pos] == null) {
                return null;
            }

            return elementGetter.getElementOrNull(array, pos);
        };
    }

    /**
     * Accessor for getting the elements of an array during runtime.
     */
    public interface ElementGetter extends Serializable {
        @Nullable
        Object getElementOrNull(Object[] array, int pos);
    }
}
