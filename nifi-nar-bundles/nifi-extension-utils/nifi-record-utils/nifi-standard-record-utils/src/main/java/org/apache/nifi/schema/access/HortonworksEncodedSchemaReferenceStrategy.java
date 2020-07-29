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
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class HortonworksEncodedSchemaReferenceStrategy implements SchemaAccessStrategy {

    private final Set<SchemaField> schemaFields;
    private final SchemaRegistry schemaRegistry;

    public HortonworksEncodedSchemaReferenceStrategy(final SchemaRegistry schemaRegistry) {
        this.schemaRegistry = schemaRegistry;

        schemaFields = new HashSet<>();
        schemaFields.add(SchemaField.SCHEMA_IDENTIFIER);
        schemaFields.add(SchemaField.SCHEMA_VERSION);
        schemaFields.add(SchemaField.SCHEMA_VERSION_ID);
        schemaFields.addAll(schemaRegistry == null ? Collections.emptySet() : schemaRegistry.getSuppliedSchemaFields());
    }

    @Override
    public RecordSchema getSchema(final Map<String, String> variables, final InputStream contentStream, final RecordSchema readSchema) throws SchemaNotFoundException, IOException {
        final byte[] buffer = new byte[1];
        try {
            StreamUtils.fillBuffer(contentStream, buffer);
        } catch (final IOException ioe) {
            throw new SchemaNotFoundException("Could not read first byte from stream", ioe);
        }

        // This encoding follows the pattern that is provided for serializing data by the Hortonworks Schema Registry serializer
        // See: https://registry-project.readthedocs.io/en/latest/serdes.html#
        final ByteBuffer bb = ByteBuffer.wrap(buffer);
        final int protocolVersion = bb.get();

        SchemaIdentifier schemaIdentifier;

        switch(protocolVersion) {
            case 1:
                final byte[] bufferv1 = new byte[12];

                try {
                    StreamUtils.fillBuffer(contentStream, bufferv1);
                } catch (final IOException ioe) {
                    throw new SchemaNotFoundException("Could not read bytes from stream", ioe);
                }
                final ByteBuffer bbv1 = ByteBuffer.wrap(bufferv1);

                final long schemaId = bbv1.getLong();
                final int schemaVersion = bbv1.getInt();
                schemaIdentifier = SchemaIdentifier.builder().id(schemaId).version(schemaVersion).build();
                return schemaRegistry.retrieveSchema(schemaIdentifier);

            case 2:
                final byte[] bufferv2 = new byte[8];

                try {
                    StreamUtils.fillBuffer(contentStream, bufferv2);
                } catch (final IOException ioe) {
                    throw new SchemaNotFoundException("Could not read bytes from stream", ioe);
                }
                final ByteBuffer bbv2 = ByteBuffer.wrap(bufferv2);

                final long sviLong = bbv2.getLong();
                schemaIdentifier = SchemaIdentifier.builder().schemaVersionId(sviLong).build();
                return schemaRegistry.retrieveSchema(schemaIdentifier);

            case 3:
                final byte[] bufferv3 = new byte[4];

                try {
                    StreamUtils.fillBuffer(contentStream, bufferv3);
                } catch (final IOException ioe) {
                    throw new SchemaNotFoundException("Could not read bytes from stream", ioe);
                }
                final ByteBuffer bbv3 = ByteBuffer.wrap(bufferv3);

                final int sviInt = bbv3.getInt();
                schemaIdentifier = SchemaIdentifier.builder().schemaVersionId((long) sviInt).build();
                return schemaRegistry.retrieveSchema(schemaIdentifier);

            default:
                throw new SchemaNotFoundException("Schema Encoding appears to be of an incompatible version. Expected Protocol Version to be a value between "
                        + HortonworksProtocolVersions.MIN_VERSION + " and " + HortonworksProtocolVersions.MAX_VERSION
                        + ", but data was encoded with protocol version " + protocolVersion + ".");
        }
    }

    @Override
    public Set<SchemaField> getSuppliedSchemaFields() {
        return schemaFields;
    }
}
