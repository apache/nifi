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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class HortonworksAttributeSchemaReferenceWriter implements SchemaAccessWriter {

    static final String SCHEMA_BRANCH_ATTRIBUTE = "schema.branch";

    private final int protocolVersion;

    public HortonworksAttributeSchemaReferenceWriter(final int protocolVersion) {
        this.protocolVersion = protocolVersion;

        if (this.protocolVersion < HortonworksProtocolVersions.MIN_VERSION || this.protocolVersion > HortonworksProtocolVersions.MAX_VERSION) {
            throw new IllegalArgumentException("Unknown Protocol Version '" + this.protocolVersion + "'. Protocol Version must be a value between "
                    + HortonworksProtocolVersions.MIN_VERSION + " and " + HortonworksProtocolVersions.MAX_VERSION + ".");
        }
    }

    @Override
    public void writeHeader(RecordSchema schema, OutputStream out) throws IOException {
    }

    @Override
    public Map<String, String> getAttributes(final RecordSchema schema) {
        final Map<String, String> attributes = new HashMap<>(4);
        final SchemaIdentifier id = schema.getIdentifier();

        switch (protocolVersion) {
            case 1:
                final Long schemaId = id.getIdentifier().getAsLong();
                final Integer schemaVersion = id.getVersion().getAsInt();
                attributes.put(HortonworksAttributeSchemaReferenceStrategy.SCHEMA_ID_ATTRIBUTE, String.valueOf(schemaId));
                attributes.put(HortonworksAttributeSchemaReferenceStrategy.SCHEMA_VERSION_ATTRIBUTE, String.valueOf(schemaVersion));
                break;
            case 2:
            case 3:
                final Long schemaVersionId = id.getSchemaVersionId().getAsLong();
                attributes.put(HortonworksAttributeSchemaReferenceStrategy.SCHEMA_VERSION_ID_ATTRIBUTE, String.valueOf(schemaVersionId));
                break;
            default:
                // Can't reach this point
                throw new IllegalStateException("Unknown Protocol Verison: " + protocolVersion);
        }

        attributes.put(HortonworksAttributeSchemaReferenceStrategy.SCHEMA_PROTOCOL_VERSION_ATTRIBUTE, String.valueOf(protocolVersion));

        if (id.getBranch().isPresent()) {
            attributes.put(SCHEMA_BRANCH_ATTRIBUTE, id.getBranch().get());
        }

        return attributes;
    }

    @Override
    public void validateSchema(final RecordSchema schema) throws SchemaNotFoundException {
        final SchemaIdentifier identifier = schema.getIdentifier();

        switch (protocolVersion) {
            case 1:
                if (!identifier.getIdentifier().isPresent()) {
                    throw new SchemaNotFoundException("Cannot write Schema Reference attributes because the Schema Identifier " +
                            "is not known and is required for Protocol Version " + protocolVersion);
                }
                if (!identifier.getVersion().isPresent()) {
                    throw new SchemaNotFoundException("Cannot write Schema Reference attributes because the Schema Version " +
                            "is not known and is required for Protocol Version " + protocolVersion);
                }
                break;
            case 2:
            case 3:
                if (!identifier.getSchemaVersionId().isPresent()) {
                    throw new SchemaNotFoundException("Cannot write Schema Reference attributes because the Schema Version Identifier " +
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
