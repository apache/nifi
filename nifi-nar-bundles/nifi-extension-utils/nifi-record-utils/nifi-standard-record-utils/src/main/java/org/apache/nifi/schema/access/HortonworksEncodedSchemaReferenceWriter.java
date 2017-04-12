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

public class HortonworksEncodedSchemaReferenceWriter implements SchemaAccessWriter {
    private static final Set<SchemaField> requiredSchemaFields = EnumSet.of(SchemaField.SCHEMA_IDENTIFIER, SchemaField.SCHEMA_VERSION);
    private static final int LATEST_PROTOCOL_VERSION = 1;

    @Override
    public void writeHeader(final RecordSchema schema, final OutputStream out) throws IOException {
        final SchemaIdentifier identifier = schema.getIdentifier();
        final long id = identifier.getIdentifier().getAsLong();
        final int version = identifier.getVersion().getAsInt();

        // This decoding follows the pattern that is provided for serializing data by the Hortonworks Schema Registry serializer
        // as it is provided at:
        // https://github.com/hortonworks/registry/blob/master/schema-registry/serdes/src/main/java/com/hortonworks/registries/schemaregistry/serdes/avro/AvroSnapshotSerializer.java
        final ByteBuffer bb = ByteBuffer.allocate(13);
        bb.put((byte) LATEST_PROTOCOL_VERSION);
        bb.putLong(id);
        bb.putInt(version);

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
            throw new SchemaNotFoundException("Cannot write Encoded Schema Reference because the Schema Identifier is not known");
        }

        final OptionalInt versionOption = identifier.getVersion();
        if (!versionOption.isPresent()) {
            throw new SchemaNotFoundException("Cannot write Encoded Schema Reference because the Schema Version is not known");
        }
    }

    @Override
    public Set<SchemaField> getRequiredSchemaFields() {
        return requiredSchemaFields;
    }

}
