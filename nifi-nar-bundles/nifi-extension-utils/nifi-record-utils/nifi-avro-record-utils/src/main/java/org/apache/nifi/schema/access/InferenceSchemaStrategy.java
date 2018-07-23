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

import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.codehaus.jackson.map.ObjectMapper;
import sun.misc.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class InferenceSchemaStrategy implements JsonSchemaAccessStrategy {
    private final Set<SchemaField> schemaFields = EnumSet.noneOf(SchemaField.class);

    @Override
    public RecordSchema getSchema(Map<String, String> variables, InputStream contentStream, RecordSchema readSchema) throws SchemaNotFoundException, IOException {
        byte[] bytes = IOUtils.readFully(contentStream, -1, true);
        ObjectMapper mapper = new ObjectMapper();

        return convertSchema(mapper.readValue(bytes, Map.class));
    }

    protected RecordSchema convertSchema(Map<String, Object> result) {
        List<RecordField> fields = new ArrayList<>();
        for (Map.Entry<String, Object> entry : result.entrySet()) {

            RecordField field;
            if (entry.getValue() instanceof Integer) {
                field = new RecordField(entry.getKey(), RecordFieldType.INT.getDataType());
            } else if (entry.getValue() instanceof Long) {
                field = new RecordField(entry.getKey(), RecordFieldType.LONG.getDataType());
            } else if (entry.getValue() instanceof Boolean) {
                field = new RecordField(entry.getKey(), RecordFieldType.BOOLEAN.getDataType());
            } else if (entry.getValue() instanceof Double) {
                field = new RecordField(entry.getKey(), RecordFieldType.DOUBLE.getDataType());
            } else if (entry.getValue() instanceof Date) {
                field = new RecordField(entry.getKey(), RecordFieldType.DATE.getDataType());
            } else if (entry.getValue() instanceof List) {
                field = new RecordField(entry.getKey(), RecordFieldType.ARRAY.getDataType());
            } else if (entry.getValue() instanceof Map) {
                RecordSchema nestedSchema = convertSchema((Map)entry.getValue());
                RecordDataType rdt = new RecordDataType(nestedSchema);
                field = new RecordField(entry.getKey(), rdt);
            } else {
                field = new RecordField(entry.getKey(), RecordFieldType.STRING.getDataType());
            }
            fields.add(field);
        }

        return new SimpleRecordSchema(fields);
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
