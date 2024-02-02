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
package org.apache.nifi.apicurio.schemaregistry.client;

import org.apache.nifi.apicurio.schemaregistry.util.SchemaUtils;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.OptionalInt;

public class ApicurioSchemaRegistryClient implements SchemaRegistryClient {
    private final SchemaRegistryApiClient apiClient;

    public ApicurioSchemaRegistryClient(SchemaRegistryApiClient apiClient) {
        this.apiClient = apiClient;
    }

    @Override
    public RecordSchema getSchema(final String schemaId, final OptionalInt version) throws IOException, SchemaNotFoundException {
        return createRecordSchemaForAttributes(
                schemaId,
                version
        );
    }

    private RecordSchema createRecordSchemaForAttributes(final String artifactId, final OptionalInt version) throws IOException, SchemaNotFoundException {
        final URI schemaUri = version.isPresent()
                ? apiClient.buildSchemaVersionUri(artifactId, version.getAsInt()) :
                apiClient.buildSchemaArtifactUri(artifactId);

        try (final InputStream schemaResultStream = apiClient.retrieveResponse(schemaUri)) {
            return SchemaUtils.createRecordSchema(schemaResultStream, artifactId, version);
        }
    }
}
