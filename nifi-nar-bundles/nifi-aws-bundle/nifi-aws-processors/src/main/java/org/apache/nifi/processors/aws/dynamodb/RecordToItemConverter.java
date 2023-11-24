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
package org.apache.nifi.processors.aws.dynamodb;

import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class RecordToItemConverter {

    private RecordToItemConverter() {
        // Not intended to be instantiated
    }

    /*
     * https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBMapper.ArbitraryDataMapping.html
     */
    public static void addField(final Record record, final Map<String, AttributeValue> item, final RecordFieldType fieldType, final String fieldName) {
        item.put(fieldName, toAttributeValue(record.getValue(fieldName), fieldType));
    }

    static AttributeValue toAttributeValue(final Object object) {
        final DataType dataType = DataTypeUtils.inferDataType(object, RecordFieldType.STRING.getDataType());
        return toAttributeValue(object, dataType.getFieldType());
    }

    private static AttributeValue toAttributeValue(final Object object, final RecordFieldType fieldType) {
        if (object == null) {
            return null;
        }

        final AttributeValue.Builder builder = AttributeValue.builder();
        switch (fieldType) {
            case BOOLEAN:
                builder.bool(DataTypeUtils.toBoolean(object, null));
                break;
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case BYTE:
            case DOUBLE:
            case BIGINT:
            case DECIMAL:
                builder.n(DataTypeUtils.toString(object, (String) null));
                break;
            case ARRAY:
                final List<AttributeValue> list = Arrays.stream(DataTypeUtils.toArray(object, null, null))
                        .map(RecordToItemConverter::toAttributeValue)
                        .toList();
                builder.l(list);
                break;
            case RECORD:
                // In case of the underlying field is really a record (and not a map for example), schema argument is not used
                builder.m(getRecordFieldAsMap(DataTypeUtils.toRecord(object, null)));
                break;
            case MAP:
                builder.m(getMapFieldAsMap(DataTypeUtils.toMap(object, null)));
                break;
            // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html#HowItWorks.DataTypes.String
            case TIMESTAMP:
            case DATE:
            case TIME:
            case CHAR:
            case ENUM:
            case STRING:
            case CHOICE: // No similar data type is supported by DynamoDB
            default:
                builder.s(DataTypeUtils.toString(object, (String) null));
        }
        return builder.build();
    }

    private static Map<String, AttributeValue> getRecordFieldAsMap(final Record recordField) {
        final Map<String, AttributeValue> result = new HashMap<>();

        for (final RecordField field : recordField.getSchema().getFields()) {
            result.put(field.getFieldName(), toAttributeValue(recordField.getValue(field)));
        }

        return result;
    }

    private static Map<String, AttributeValue> getMapFieldAsMap(final Map<String, Object> mapField) {
        final Map<String, AttributeValue> result = new HashMap<>();

        mapField.forEach((name, value) -> result.put(name, toAttributeValue(value)));
        return result;
    }
}
