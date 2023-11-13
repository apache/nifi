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
import org.apache.nifi.apicurio.schemaregistry.util.SchemaUtils.ResultAttributes;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public class ApicurioSchemaRegistryClient implements SchemaRegistryClient {
    private final SchemaRegistryApiClient apiClient;

    public ApicurioSchemaRegistryClient(SchemaRegistryApiClient apiClient) {
        this.apiClient = apiClient;
    }

    @Override
    public RecordSchema getSchema(final String schemaName) throws IOException, SchemaNotFoundException {
        final ResultAttributes attributes = getAttributesForSchemaName(schemaName);
        final int version = getVersionAttributeFromMetadata(attributes);
        return createRecordSchemaForAttributes(attributes, version);
    }

    @Override
    public RecordSchema getSchema(final String schemaName, final int version) throws IOException, SchemaNotFoundException {
        final ResultAttributes attributes = getAttributesForSchemaName(schemaName);
        return createRecordSchemaForAttributes(attributes, version);
    }

    private ResultAttributes getAttributesForSchemaName(String schemaName) throws IOException {
        final URI searchUri = apiClient.buildSearchUri(schemaName);
        try (final InputStream searchResultStream = apiClient.retrieveResponse(searchUri)) {
            return SchemaUtils.getResultAttributes(searchResultStream);
        }
    }

    private int getVersionAttributeFromMetadata(final ResultAttributes attributes) throws IOException {
        final URI metaDataUri = apiClient.buildMetaDataUri(attributes.groupId(), attributes.artifactId());
        try (final InputStream metadataResultStream = apiClient.retrieveResponse(metaDataUri)) {
            return SchemaUtils.extractVersionAttributeFromStream(metadataResultStream);
        }
    }

    private RecordSchema createRecordSchemaForAttributes(ResultAttributes attributes, int version) throws IOException, SchemaNotFoundException {
        final URI schemaUri = apiClient.buildSchemaVersionUri(attributes.groupId(), attributes.artifactId(), version);

        try (final InputStream schemaResultStream = apiClient.retrieveResponse(schemaUri)) {
            return SchemaUtils.createRecordSchema(schemaResultStream, attributes.name(), version);
        }
    }
}
