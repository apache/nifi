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

import com.amazonaws.services.dynamodbv2.document.Item;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

final class RecordToItemConverter {

    private RecordToItemConverter() {
        // Not intended to be instantiated
    }

    /*
     * https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBMapper.ArbitraryDataMapping.html
     */
    public static void addField(final Record record, final Item item, final RecordFieldType fieldType, final String fieldName) {
        switch (fieldType) {
            case BOOLEAN:
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case BYTE:
            case DOUBLE:
            case STRING:
                item.with(fieldName, record.getValue(fieldName));
                break;
            case BIGINT:
                item.withBigInteger(fieldName, new BigInteger(record.getAsString(fieldName)));
                break;
            case DECIMAL:
                item.withNumber(fieldName, new BigDecimal(record.getAsString(fieldName)));
                break;
            case TIMESTAMP:
            case DATE:
            case TIME:
                // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html#HowItWorks.DataTypes.String
                item.withString(fieldName, record.getAsString(fieldName));
            case CHAR:
                item.withString(fieldName, record.getAsString(fieldName));
                break;
            case ENUM:
                item.withString(fieldName, record.getAsString(fieldName));
                break;
            case ARRAY:
                item.withList(fieldName, record.getAsArray(fieldName));
                break;
            case RECORD:
                // In case of the underlying field is really a record (and not a map for example), schema argument is not used
                item.withMap(fieldName, getRecordFieldAsMap(record.getAsRecord(fieldName, null)));
                break;
            case MAP:
                item.withMap(fieldName, getMapFieldAsMap(record.getValue(fieldName)));
                break;
            case CHOICE: // No similar data type is supported by DynamoDB
            default:
                item.withString(fieldName, record.getAsString(fieldName));
        }
    }

    private static Map<String, Object> getRecordFieldAsMap(final Record recordField) {
        final Map<String, Object> result = new HashMap<>();

        for (final RecordField field : recordField.getSchema().getFields()) {
            result.put(field.getFieldName(), convertToSupportedType(recordField.getValue(field)));
        }

        return result;
    }

    private static Map<String, Object> getMapFieldAsMap(final Object recordField) {
        if (!(recordField instanceof Map)) {
            throw new IllegalArgumentException("Map type is expected");
        }

        final Map<String, Object> result = new HashMap<>();
        ((Map<String, Object>) recordField).forEach((name, value) -> result.put(name, convertToSupportedType(value)));
        return result;
    }

    private static Object convertToSupportedType(Object value) {
        if (value instanceof Record) {
            return getRecordFieldAsMap((Record) value);
        } else if (value instanceof Map) {
            return getMapFieldAsMap(value);
        } else if (value instanceof Character || value instanceof Timestamp || value instanceof Date || value instanceof Time) {
            return ((Character) value).toString();
        } else if (value instanceof Enum) {
            return ((Enum) value).name();
        } else {
            return value;
        }
    }
}
