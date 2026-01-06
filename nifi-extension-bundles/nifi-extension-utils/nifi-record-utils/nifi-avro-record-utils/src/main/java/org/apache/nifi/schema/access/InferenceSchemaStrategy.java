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

package org.apache.nifi.schema.access;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.RecordDataType;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.nifi.serialization.record.util.DataTypeUtils.mergeDataTypes;

public class InferenceSchemaStrategy implements JsonSchemaAccessStrategy {
    private final Set<SchemaField> schemaFields = EnumSet.noneOf(SchemaField.class);

    @Override
    public RecordSchema getSchema(Map<String, String> variables, InputStream contentStream, RecordSchema readSchema) throws SchemaNotFoundException, IOException {
        byte[] bytes = IOUtils.toByteArray(contentStream);
        ObjectMapper mapper = new ObjectMapper();

        return convertSchema(mapper.readValue(bytes, Map.class));
    }

    protected RecordSchema convertSchema(Map<String, Object> result) {
        List<RecordField> fields = new ArrayList<>();
        for (Map.Entry<String, Object> entry : result.entrySet()) {
            final RecordField field = new RecordField(entry.getKey(), getDataType(entry.getValue()));
            fields.add(field);
        }

        return new SimpleRecordSchema(fields);
    }

    private DataType getDataType(Object value) {
        return switch (value) {
            case Integer ignored -> RecordFieldType.INT.getDataType();
            case Long ignored -> RecordFieldType.LONG.getDataType();
            case Boolean ignored -> RecordFieldType.BOOLEAN.getDataType();
            case Double ignored -> RecordFieldType.DOUBLE.getDataType();
            case Date ignored -> RecordFieldType.DATE.getDataType();

            case BigDecimal bigDecimal ->
                RecordFieldType.DECIMAL.getDecimalDataType(bigDecimal.precision(), bigDecimal.scale());

            case List listField -> {
                DataType mergedDataType = null;

                for (Object listElement : listField) {
                    final DataType inferredDataType = getDataType(listElement);
                    mergedDataType = mergeDataTypes(mergedDataType, inferredDataType);
                }

                if (mergedDataType == null) {
                    mergedDataType = RecordFieldType.STRING.getDataType();
                }

                yield RecordFieldType.ARRAY.getArrayDataType(mergedDataType);
            }

            case Map map -> {
                final RecordSchema nestedSchema = convertSchema(map);
                yield new RecordDataType(nestedSchema);
            }

            case null, default -> RecordFieldType.STRING.getDataType();
        };
    }

    @Override
    public Set<SchemaField> getSuppliedSchemaFields() {
        return schemaFields;
    }

    @Override
    public RecordSchema getSchema(Map<String, String> variables, Map<String, Object> content, RecordSchema readSchema) throws SchemaNotFoundException, IOException {
        return convertSchema(content);
    }
}
