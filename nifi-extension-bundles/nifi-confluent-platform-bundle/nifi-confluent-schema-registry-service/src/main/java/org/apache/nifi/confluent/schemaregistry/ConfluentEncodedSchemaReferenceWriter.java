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
package org.apache.nifi.confluent.schemaregistry;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schemaregistry.services.SchemaReferenceWriter;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;

@Tags({"confluent", "schema", "registry", "kafka", "avro"})
@CapabilityDescription("Writes Schema Identifier according to Confluent encoding as a header consisting of a byte marker and an integer represented as four bytes")
public class ConfluentEncodedSchemaReferenceWriter extends AbstractControllerService implements SchemaReferenceWriter {
    private static final Set<SchemaField> REQUIRED_SCHEMA_FIELDS = EnumSet.of(SchemaField.SCHEMA_IDENTIFIER, SchemaField.SCHEMA_VERSION);

    private static final int HEADER_CAPACITY = 5;

    private static final byte MAGIC_BYTE = 0;

    /**
     * Write Record Schema Identifier according to Confluent wire format consisting of a magic byte followed by an integer
     *
     * @param recordSchema Record Schema to be written to the provided Output Stream
     * @param outputStream Output Stream to which the provided Record Schema should be written
     * @throws IOException Thrown on failure to write header
     */
    @Override
    public void writeHeader(final RecordSchema recordSchema, final OutputStream outputStream) throws IOException {
        final SchemaIdentifier schemaIdentifier = recordSchema.getIdentifier();
        final OptionalLong identifier = schemaIdentifier.getIdentifier();
        if (identifier.isPresent()) {
            final long id = identifier.getAsLong();
            final int schemaId = Math.toIntExact(id);

            final ByteBuffer header = ByteBuffer.allocate(HEADER_CAPACITY);
            header.put(MAGIC_BYTE);
            header.putInt(schemaId);

            outputStream.write(header.array());
        } else {
            throw new IOException("Identifier field not found in Schema Identifier");
        }
    }

    @Override
    public Map<String, String> getAttributes(final RecordSchema recordSchema) {
        return Collections.emptyMap();
    }

    @Override
    public void validateSchema(final RecordSchema recordSchema) throws SchemaNotFoundException {
        final SchemaIdentifier schemaIdentifier = recordSchema.getIdentifier();
        final OptionalLong identifier = schemaIdentifier.getIdentifier();
        if (identifier.isPresent()) {
            final OptionalInt version = schemaIdentifier.getVersion();
            if (version.isEmpty()) {
                throw new SchemaNotFoundException("Cannot write Confluent Schema Registry Reference because the Schema Version is not known");
            }
        } else {
            throw new SchemaNotFoundException("Cannot write Confluent Schema Registry Reference because the Schema Identifier is not known");
        }
    }

    @Override
    public Set<SchemaField> getRequiredSchemaFields() {
        return REQUIRED_SCHEMA_FIELDS;
    }
}
