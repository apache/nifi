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

import java.io.OutputStream;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.nifi.serialization.record.RecordSchema;

public class SchemaTextAsAttribute implements SchemaAccessWriter {
    private static final Set<SchemaField> schemaFields = EnumSet.of(SchemaField.SCHEMA_TEXT, SchemaField.SCHEMA_TEXT_FORMAT);

    @Override
    public void writeHeader(final RecordSchema schema, final OutputStream out) {
    }

    @Override
    public Map<String, String> getAttributes(final RecordSchema schema) {
        final Optional<String> textFormatOption = schema.getSchemaFormat();
        final Optional<String> textOption = schema.getSchemaText();
        return Collections.singletonMap(textFormatOption.get() + ".schema", textOption.get());
    }

    @Override
    public void validateSchema(final RecordSchema schema) throws SchemaNotFoundException {
        final Optional<String> textFormatOption = schema.getSchemaFormat();
        if (!textFormatOption.isPresent()) {
            throw new SchemaNotFoundException("Cannot write Schema Text as Attribute because the Schema's Text Format is not present");
        }

        final Optional<String> textOption = schema.getSchemaText();
        if (!textOption.isPresent()) {
            throw new SchemaNotFoundException("Cannot write Schema Text as Attribute because the Schema's Text is not present");
        }
    }

    @Override
    public Set<SchemaField> getRequiredSchemaFields() {
        return schemaFields;
    }
}
