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
package org.apache.nifi.aws.schemaregistry;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.aws.schemaregistry.serde.GlueSchemaRegistryDeserializerDataParser;
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
import java.util.UUID;

@Tags({"schema", "registry", "aws", "avro", "glue"})
@CapabilityDescription("Reads Schema Identifier according to AWS Glue Schema encoding as a header consisting of a two byte markers and a 16 byte UUID")
public class AmazonGlueEncodedSchemaReferenceReader extends AbstractControllerService implements SchemaReferenceReader {

    private final GlueSchemaRegistryDeserializerDataParser deserializerDataParser = GlueSchemaRegistryDeserializerDataParser.getInstance();

    private static final int HEADER_CAPACITY = GlueSchemaRegistryDeserializerDataParser.getSchemaRegistryHeaderLength();

    private static final Set<SchemaField> SUPPLIED_SCHEMA_FIELDS = Set.of(SchemaField.SCHEMA_NAME);

    @Override
    public SchemaIdentifier getSchemaIdentifier(final Map<String, String> variables, final InputStream contentStream) throws SchemaNotFoundException {
        final byte[] header = new byte[HEADER_CAPACITY];
        try {
            StreamUtils.fillBuffer(contentStream, header);
        } catch (final IOException e) {
            throw new SchemaNotFoundException("Failed to read header in first %d bytes from stream".formatted(HEADER_CAPACITY), e);
        }

        final ByteBuffer headerBuffer = ByteBuffer.wrap(header);
        final StringBuilder errorStringBuilder = new StringBuilder();
        if (!deserializerDataParser.isDataCompatible(headerBuffer, errorStringBuilder)) {
            throw new SchemaNotFoundException("Failed to parse Glue Schema Registry header: %s".formatted(errorStringBuilder));
        }
        final UUID schemaVersionId = deserializerDataParser.getSchemaVersionId(headerBuffer);
        return SchemaIdentifier.builder().name(new WireFormatAwsGlueSchemaId(schemaVersionId).toSchemaName()).build();
    }

    @Override
    public Set<SchemaField> getSuppliedSchemaFields() {
        return SUPPLIED_SCHEMA_FIELDS;
    }
}
