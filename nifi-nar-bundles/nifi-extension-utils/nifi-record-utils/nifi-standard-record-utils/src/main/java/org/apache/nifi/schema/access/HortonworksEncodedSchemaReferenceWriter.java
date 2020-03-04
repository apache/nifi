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
import java.util.Set;

import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;

public class HortonworksEncodedSchemaReferenceWriter implements SchemaAccessWriter {
    private static final Set<SchemaField> requiredSchemaFields = EnumSet.of(SchemaField.SCHEMA_IDENTIFIER, SchemaField.SCHEMA_VERSION);
    private static final int LATEST_PROTOCOL_VERSION = 3;

    @Override
    public void writeHeader(final RecordSchema schema, final OutputStream out) throws IOException {
        final SchemaIdentifier identifier = schema.getIdentifier();

        // This encoding follows the pattern that is provided for serializing data by the Hortonworks Schema Registry serializer
        // See: https://registry-project.readthedocs.io/en/latest/serdes.html#
        switch(identifier.getProtocol()) {
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
                final Long sviV2 = identifier.getIdentifier().getAsLong();
                final ByteBuffer bbv2 = ByteBuffer.allocate(9);
                bbv2.put((byte) 2);
                bbv2.putLong(sviV2);
                out.write(bbv2.array());
                return;
            case 3:
                final Long sviV3 = identifier.getIdentifier().getAsLong();
                final ByteBuffer bbv3 = ByteBuffer.allocate(5);
                bbv3.put((byte) 3);
                bbv3.putInt(sviV3.intValue());
                out.write(bbv3.array());
                return;
            default:
                throw new IOException("Schema Encoding appears to be of an incompatible version. The latest known Protocol is Version "
                        + LATEST_PROTOCOL_VERSION + " but the data was encoded with version " + identifier.getProtocol() + " or was not encoded with this data format");
        }


    }

    @Override
    public Map<String, String> getAttributes(final RecordSchema schema) {
        return Collections.emptyMap();
    }

    @Override
    public void validateSchema(RecordSchema schema) throws SchemaNotFoundException {
        final SchemaIdentifier identifier = schema.getIdentifier();

        if(!identifier.getSchemaVersionId().isPresent()) {
            if (!identifier.getIdentifier().isPresent()) {
                throw new SchemaNotFoundException("Cannot write Encoded Schema Reference because the Schema Identifier is not known");
            }
            if (!identifier.getVersion().isPresent()) {
                throw new SchemaNotFoundException("Cannot write Encoded Schema Reference because the Schema Version is not known");
            }
        }
    }

    @Override
    public Set<SchemaField> getRequiredSchemaFields() {
        return requiredSchemaFields;
    }

}
