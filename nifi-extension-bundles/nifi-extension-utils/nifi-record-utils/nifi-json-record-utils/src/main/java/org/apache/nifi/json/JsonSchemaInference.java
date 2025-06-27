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
package org.apache.nifi.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import org.apache.nifi.schema.inference.HierarchicalSchemaInference;
import org.apache.nifi.schema.inference.TimeValueInference;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class JsonSchemaInference extends HierarchicalSchemaInference<JsonNode> {

    private final TimeValueInference timeValueInference;

    public JsonSchemaInference(final TimeValueInference timeValueInference) {
        this.timeValueInference = timeValueInference;
    }


    protected DataType getDataType(final JsonNode jsonNode) {
        if (jsonNode.isTextual()) {
            final String text = jsonNode.textValue();
            if (text == null) {
                return RecordFieldType.STRING.getDataType();
            }

            final Optional<DataType> timeDataType = timeValueInference.getDataType(text);
            return timeDataType.orElse(RecordFieldType.STRING.getDataType());
        }

        if (jsonNode.isObject()) {
            final RecordSchema schema = createSchema(jsonNode);
            return RecordFieldType.RECORD.getRecordDataType(schema);
        }

        if (jsonNode.isIntegralNumber()) {
            if (jsonNode.isBigInteger()) {
                return RecordFieldType.BIGINT.getDataType();
            }
            if (jsonNode.isLong()) {
                return RecordFieldType.LONG.getDataType();
            }
            return RecordFieldType.INT.getDataType();
        }

        if (jsonNode.isBigDecimal()) {
            final DecimalNode decimalNode = (DecimalNode) jsonNode;
            final BigDecimal value = decimalNode.decimalValue();
            return RecordFieldType.DECIMAL.getDecimalDataType(value.precision(), value.scale());
        }

        if (jsonNode.isFloatingPointNumber()) {
            return RecordFieldType.DOUBLE.getDataType();
        }
        if (jsonNode.isBinary()) {
            return RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType());
        }
        if (jsonNode.isBoolean()) {
            return RecordFieldType.BOOLEAN.getDataType();
        }

        return null;
    }

    @Override
    protected boolean isObject(final JsonNode value) {
        return value.isObject();
    }

    @Override
    protected boolean isArray(final JsonNode value) {
        return value.isArray();
    }

    @Override
    protected boolean isEmptyArray(final JsonNode value) {
        return value.isArray() && value.size() == 0;
    }

    @Override
    protected void forEachFieldInRecord(final JsonNode rawRecord, final BiConsumer<String, JsonNode> fieldConsumer) {
        for (Map.Entry<String, JsonNode> entry : rawRecord.properties()) {
            final String fieldName = entry.getKey();
            final JsonNode value = entry.getValue();

            fieldConsumer.accept(fieldName, value);
        }
    }

    @Override
    protected void forEachRawRecordInArray(final JsonNode arrayRecord, final Consumer<JsonNode> rawRecordConsumer) {
        final ArrayNode arrayNode = (ArrayNode) arrayRecord;
        for (final JsonNode element : arrayNode) {
            rawRecordConsumer.accept(element);
        }
    }

    @Override
    protected String getRootName(final JsonNode rawRecord) {
        return null;
    }
}
