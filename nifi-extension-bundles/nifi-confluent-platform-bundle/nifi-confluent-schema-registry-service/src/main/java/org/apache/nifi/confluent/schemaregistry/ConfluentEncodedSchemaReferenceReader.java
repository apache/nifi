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
import org.apache.nifi.schemaregistry.services.SchemaReferenceReader;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

@Tags({"confluent", "schema", "registry", "kafka", "avro"})
@CapabilityDescription("Reads Schema Identifier according to Confluent encoding as a header consisting of a byte marker and an integer represented as four bytes")
public class ConfluentEncodedSchemaReferenceReader extends AbstractControllerService implements SchemaReferenceReader {
    private static final int HEADER_CAPACITY = 5;

    private static final byte MAGIC_BYTE = 0;

    private static final Set<SchemaField> SUPPLIED_SCHEMA_FIELDS = Set.of(SchemaField.SCHEMA_IDENTIFIER, SchemaField.SCHEMA_VERSION);

    @Override
    public SchemaIdentifier getSchemaIdentifier(final Map<String, String> variables, final InputStream contentStream) throws SchemaNotFoundException {
        final byte[] header = new byte[HEADER_CAPACITY];
        try {
            StreamUtils.fillBuffer(contentStream, header);
        } catch (final IOException e) {
            throw new SchemaNotFoundException("Failed to read header in first 5 bytes from stream", e);
        }

        final ByteBuffer headerBuffer = ByteBuffer.wrap(header);
        final byte firstByte = headerBuffer.get();
        if (MAGIC_BYTE == firstByte) {
            final int schemaId = headerBuffer.getInt();
            getLogger().debug("Confluent Schema Identifier found [{}]", schemaId);
            return SchemaIdentifier.builder().schemaVersionId((long) schemaId).build();
        } else {
            throw new SchemaNotFoundException("Confluent Schema encoding not found in first byte of content header");
        }
    }

    @Override
    public Set<SchemaField> getSuppliedSchemaFields() {
        return SUPPLIED_SCHEMA_FIELDS;
    }
}
