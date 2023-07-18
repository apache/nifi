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
package org.apache.nifi.aws.schemaregistry.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionRequest;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionResponse;
import software.amazon.awssdk.services.glue.model.SchemaId;
import software.amazon.awssdk.services.glue.model.SchemaVersionNumber;

import java.io.IOException;

public class GlueSchemaRegistryClient implements SchemaRegistryClient {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String NAMESPACE_FIELD_NAME = "namespace";

    private final GlueClient client;
    private final String registryName;

    public GlueSchemaRegistryClient(final GlueClient client, final String registryName) {
        this.client = client;
        this.registryName = registryName;
    }

    @Override
    public RecordSchema getSchema(final String schemaName) throws IOException, SchemaNotFoundException {
        final SchemaVersionNumber schemaVersionNumber = SchemaVersionNumber.builder()
                .latestVersion(true)
                .build();

        final GetSchemaVersionResponse schemaVersionResponse = getSchemaVersionResponse(schemaName, schemaVersionNumber);

        return createRecordSchema(schemaVersionResponse);
    }

    @Override
    public RecordSchema getSchema(final String schemaName, final long version) throws IOException, SchemaNotFoundException {
        final SchemaVersionNumber schemaVersionNumber = SchemaVersionNumber.builder()
                .versionNumber(version)
                .build();

        final GetSchemaVersionResponse schemaVersionResponse = getSchemaVersionResponse(schemaName, schemaVersionNumber);

        return createRecordSchema(schemaVersionResponse);
    }

    private GetSchemaVersionResponse getSchemaVersionResponse(final String schemaName, final SchemaVersionNumber schemaVersionNumber) {
        final SchemaId schemaId = buildSchemaId(schemaName);
        final GetSchemaVersionRequest request = buildSchemaVersionRequest(schemaVersionNumber, schemaId);
        return client.getSchemaVersion(request);
    }

    private GetSchemaVersionRequest buildSchemaVersionRequest(final SchemaVersionNumber schemaVersionNumber, final SchemaId schemaId) {
        return GetSchemaVersionRequest.builder()
                .schemaVersionNumber(schemaVersionNumber)
                .schemaId(schemaId)
                .build();
    }

    private SchemaId buildSchemaId(final String schemaName) {
        return SchemaId.builder()
                .registryName(registryName)
                .schemaName(schemaName)
                .build();
    }

    private RecordSchema createRecordSchema(final GetSchemaVersionResponse schemaVersionResponse) throws SchemaNotFoundException, JsonProcessingException {
        final JsonNode schemaNode = OBJECT_MAPPER.readTree(schemaVersionResponse.schemaDefinition());
        final String namespace = schemaNode.get(NAMESPACE_FIELD_NAME).asText();
        final int version = schemaVersionResponse.versionNumber().intValue();
        final String schemaText = schemaVersionResponse.schemaDefinition();

        try {
            final Schema avroSchema = new Schema.Parser().parse(schemaText);
            final SchemaIdentifier schemaId = SchemaIdentifier.builder()
                    .name(namespace)
                    .version(version)
                    .build();
            return AvroTypeUtil.createSchema(avroSchema, schemaText, schemaId);
        } catch (final SchemaParseException spe) {
            throw new SchemaNotFoundException("Obtained Schema with name " + namespace
                    + " from Glue Schema Registry but the Schema Text that was returned is not a valid Avro Schema");
        }
    }
}
