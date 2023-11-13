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

import org.apache.nifi.schemaregistry.services.SchemaReferenceWriter;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Schema Access Writer that delegates to the configured Schema Reference Writer
 */
public class SchemaReferenceWriterSchemaAccessWriter implements SchemaAccessWriter {
    private final SchemaReferenceWriter schemaReferenceWriter;

    public SchemaReferenceWriterSchemaAccessWriter(final SchemaReferenceWriter schemaReferenceWriter) {
        this.schemaReferenceWriter = Objects.requireNonNull(schemaReferenceWriter, "Schema Reference Writer required");
    }

    @Override
    public void writeHeader(final RecordSchema schema, final OutputStream outputStream) throws IOException {
        schemaReferenceWriter.writeHeader(schema, outputStream);
    }

    @Override
    public Map<String, String> getAttributes(final RecordSchema schema) {
        return schemaReferenceWriter.getAttributes(schema);
    }

    @Override
    public void validateSchema(final RecordSchema schema) throws SchemaNotFoundException {
        schemaReferenceWriter.validateSchema(schema);
    }

    @Override
    public Set<SchemaField> getRequiredSchemaFields() {
        return schemaReferenceWriter.getRequiredSchemaFields();
    }
}
