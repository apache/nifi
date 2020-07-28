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
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class HortonworksEncodedSchemaReferenceWriter implements SchemaAccessWriter {

    private final int protocolVersion;

    public HortonworksEncodedSchemaReferenceWriter(final int protocolVersion) {
        this.protocolVersion = protocolVersion;

        if (this.protocolVersion < HortonworksProtocolVersions.MIN_VERSION || this.protocolVersion > HortonworksProtocolVersions.MAX_VERSION) {
            throw new IllegalArgumentException("Unknown Protocol Version '" + this.protocolVersion + "'. Protocol Version must be a value between "
                    + HortonworksProtocolVersions.MIN_VERSION + " and " + HortonworksProtocolVersions.MAX_VERSION + ".");
        }
    }

    @Override
    public void writeHeader(final RecordSchema schema, final OutputStream out) throws IOException {
        final SchemaIdentifier identifier = schema.getIdentifier();

        // This encoding follows the pattern that is provided for serializing data by the Hortonworks Schema Registry serializer
        // See: https://registry-project.readthedocs.io/en/latest/serdes.html#
        switch(protocolVersion) {
            case 1:
                final Long id = identifier.getIdentifier().getAsLong();
                final Integer version = identifier.getVersion().getAsInt();
                final ByteBuffer bbv1 = ByteBuffer.allocate(13);
                bbv1.put((byte) 1);
                bbv1.putLong(id);
                bbv1.putInt(version);
                out.write(bbv1.array());
                return;
            case 2:
                final Long sviV2 = identifier.getSchemaVersionId().getAsLong();
                final ByteBuffer bbv2 = ByteBuffer.allocate(9);
                bbv2.put((byte) 2);
                bbv2.putLong(sviV2);
                out.write(bbv2.array());
                return;
            case 3:
                final Long sviV3 = identifier.getSchemaVersionId().getAsLong();
                final ByteBuffer bbv3 = ByteBuffer.allocate(5);
                bbv3.put((byte) 3);
                bbv3.putInt(sviV3.intValue());
                out.write(bbv3.array());
                return;
            default:
                // Can't reach this point
                throw new IllegalStateException("Unknown Protocol Version: " + this.protocolVersion);
        }
    }

    @Override
    public Map<String, String> getAttributes(final RecordSchema schema) {
        return Collections.emptyMap();
    }

    @Override
    public void validateSchema(RecordSchema schema) throws SchemaNotFoundException {
        final SchemaIdentifier identifier = schema.getIdentifier();

        switch (protocolVersion) {
            case 1:
                if (!identifier.getIdentifier().isPresent()) {
                    throw new SchemaNotFoundException("Cannot write Encoded Schema Reference because the Schema Identifier " +
                            "is not known and is required for Protocol Version " + protocolVersion);
                }
                if (!identifier.getVersion().isPresent()) {
                    throw new SchemaNotFoundException("Cannot write Encoded Schema Reference because the Schema Version " +
                            "is not known and is required for Protocol Version " + protocolVersion);
                }
                break;
            case 2:
            case 3:
                if (!identifier.getSchemaVersionId().isPresent()) {
                    throw new SchemaNotFoundException("Cannot write Encoded Schema Reference because the Schema Version Identifier " +
                            "is not known and is required for Protocol Version " + protocolVersion);
                }
                break;
            default:
                // Can't reach this point
                throw new SchemaNotFoundException("Unknown Protocol Version: " + protocolVersion);
        }
    }

    @Override
    public Set<SchemaField> getRequiredSchemaFields() {
        return HortonworksProtocolVersions.getRequiredSchemaFields(protocolVersion);
    }

}
