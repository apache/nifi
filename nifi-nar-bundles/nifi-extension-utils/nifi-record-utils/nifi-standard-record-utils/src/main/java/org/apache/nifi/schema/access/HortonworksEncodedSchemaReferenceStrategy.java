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

import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class HortonworksEncodedSchemaReferenceStrategy implements SchemaAccessStrategy {
    private static final int LATEST_PROTOCOL_VERSION = 1;

    private final Set<SchemaField> schemaFields;
    private final SchemaRegistry schemaRegistry;

    public HortonworksEncodedSchemaReferenceStrategy(final SchemaRegistry schemaRegistry) {
        this.schemaRegistry = schemaRegistry;

        schemaFields = new HashSet<>();
        schemaFields.add(SchemaField.SCHEMA_IDENTIFIER);
        schemaFields.add(SchemaField.SCHEMA_VERSION);
        schemaFields.addAll(schemaRegistry == null ? Collections.emptySet() : schemaRegistry.getSuppliedSchemaFields());
    }

    @Override
    public RecordSchema getSchema(final Map<String, String> variables, final InputStream contentStream, final RecordSchema readSchema) throws SchemaNotFoundException, IOException {
        final byte[] buffer = new byte[13];
        try {
            StreamUtils.fillBuffer(contentStream, buffer);
        } catch (final IOException ioe) {
            throw new SchemaNotFoundException("Could not read first 13 bytes from stream", ioe);
        }

        // This encoding follows the pattern that is provided for serializing data by the Hortonworks Schema Registry serializer
        // as it is provided at:
        // https://github.com/hortonworks/registry/blob/master/schema-registry/serdes/src/main/java/com/hortonworks/registries/schemaregistry/serdes/avro/AvroSnapshotSerializer.java
        final ByteBuffer bb = ByteBuffer.wrap(buffer);
        final int protocolVersion = bb.get();
        if (protocolVersion != 1) {
            throw new SchemaNotFoundException("Schema Encoding appears to be of an incompatible version. The latest known Protocol is Version "
                + LATEST_PROTOCOL_VERSION + " but the data was encoded with version " + protocolVersion + " or was not encoded with this data format");
        }

        final long schemaId = bb.getLong();
        final int schemaVersion = bb.getInt();

        return schemaRegistry.retrieveSchema(schemaId, schemaVersion);
    }

    @Override
    public Set<SchemaField> getSuppliedSchemaFields() {
        return schemaFields;
    }
}
