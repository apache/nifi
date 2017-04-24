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

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;

public class SchemaNameAsAttribute implements SchemaAccessWriter {
    private static final Set<SchemaField> schemaFields = EnumSet.of(SchemaField.SCHEMA_NAME);
    private static final String SCHEMA_NAME_ATTRIBUTE = "schema.name";

    @Override
    public void writeHeader(final RecordSchema schema, final OutputStream out) throws IOException {
    }

    @Override
    public Map<String, String> getAttributes(final RecordSchema schema) {
        final SchemaIdentifier identifier = schema.getIdentifier();
        final Optional<String> nameOption = identifier.getName();
        if (nameOption.isPresent()) {
            return Collections.singletonMap(SCHEMA_NAME_ATTRIBUTE, nameOption.get());
        }
        return Collections.emptyMap();
    }

    @Override
    public void validateSchema(final RecordSchema schema) throws SchemaNotFoundException {
        final SchemaIdentifier schemaId = schema.getIdentifier();
        if (!schemaId.getName().isPresent()) {
            throw new SchemaNotFoundException("Cannot write Schema Name As Attribute because the Schema Name is not known");
        }
    }

    @Override
    public Set<SchemaField> getRequiredSchemaFields() {
        return schemaFields;
    }

}
