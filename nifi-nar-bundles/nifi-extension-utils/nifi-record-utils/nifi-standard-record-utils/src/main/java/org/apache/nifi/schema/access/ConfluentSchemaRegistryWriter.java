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
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;

import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;

public class ConfluentSchemaRegistryWriter implements SchemaAccessWriter {
    private static final Set<SchemaField> requiredSchemaFields = EnumSet.of(SchemaField.SCHEMA_IDENTIFIER, SchemaField.SCHEMA_VERSION);

    @Override
    public void writeHeader(final RecordSchema schema, final OutputStream out) throws IOException {
        final SchemaIdentifier identifier = schema.getIdentifier();
        final long id = identifier.getIdentifier().getAsLong();

        // This encoding follows the pattern that is provided for serializing data by the Confluent Schema Registry serializer
        // as it is provided at:
        // http://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format
        // The format consists of the first byte always being 0, to indicate a 'magic byte' followed by 4 bytes
        // representing the schema id.
        final ByteBuffer bb = ByteBuffer.allocate(5);
        bb.put((byte) 0);
        bb.putInt((int) id);

        out.write(bb.array());
    }

    @Override
    public Map<String, String> getAttributes(final RecordSchema schema) {
        return Collections.emptyMap();
    }

    @Override
    public void validateSchema(RecordSchema schema) throws SchemaNotFoundException {
        final SchemaIdentifier identifier = schema.getIdentifier();
        final OptionalLong identifierOption = identifier.getIdentifier();
        if (!identifierOption.isPresent()) {
            throw new SchemaNotFoundException("Cannot write Confluent Schema Registry Reference because the Schema Identifier is not known");
        }

        final OptionalInt versionOption = identifier.getVersion();
        if (!versionOption.isPresent()) {
            throw new SchemaNotFoundException("Cannot write Confluent Schema Registry Reference because the Schema Version is not known");
        }
    }

    @Override
    public Set<SchemaField> getRequiredSchemaFields() {
        return requiredSchemaFields;
    }

}
