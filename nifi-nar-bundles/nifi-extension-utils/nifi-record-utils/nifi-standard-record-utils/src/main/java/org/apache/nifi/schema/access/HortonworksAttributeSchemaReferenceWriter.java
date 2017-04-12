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

import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;

import java.io.IOException;
import java.io.OutputStream;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class HortonworksAttributeSchemaReferenceWriter implements SchemaAccessWriter {
    private static final Set<SchemaField> requiredSchemaFields = EnumSet.of(SchemaField.SCHEMA_IDENTIFIER, SchemaField.SCHEMA_VERSION);
    private static final int LATEST_PROTOCOL_VERSION = 1;

    @Override
    public void writeHeader(RecordSchema schema, OutputStream out) throws IOException {
    }

    @Override
    public Map<String, String> getAttributes(final RecordSchema schema) {
        final Map<String, String> attributes = new HashMap<>(4);
        final SchemaIdentifier id = schema.getIdentifier();

        final long schemaId = id.getIdentifier().getAsLong();
        final int schemaVersion = id.getVersion().getAsInt();

        attributes.put(HortonworksAttributeSchemaReferenceStrategy.SCHEMA_ID_ATTRIBUTE, String.valueOf(schemaId));
        attributes.put(HortonworksAttributeSchemaReferenceStrategy.SCHEMA_VERSION_ATTRIBUTE, String.valueOf(schemaVersion));
        attributes.put(HortonworksAttributeSchemaReferenceStrategy.SCHEMA_PROTOCOL_VERSION_ATTRIBUTE, String.valueOf(LATEST_PROTOCOL_VERSION));

        return attributes;
    }

    @Override
    public void validateSchema(final RecordSchema schema) throws SchemaNotFoundException {
        final SchemaIdentifier id = schema.getIdentifier();
        if (!id.getIdentifier().isPresent()) {
            throw new SchemaNotFoundException("Cannot write Schema Reference as Attributes because it does not contain a Schema Identifier");
        }
        if (!id.getVersion().isPresent()) {
            throw new SchemaNotFoundException("Cannot write Schema Reference as Attributes because it does not contain a Schema Version");
        }
    }

    @Override
    public Set<SchemaField> getRequiredSchemaFields() {
        return requiredSchemaFields;
    }

}
