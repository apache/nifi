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

import org.apache.nifi.apicurio.schemaregistry.util.SchemaUtils.ResultAttributes;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import static org.apache.nifi.apicurio.schemaregistry.util.SchemaUtils.createRecordSchema;
import static org.apache.nifi.apicurio.schemaregistry.util.SchemaUtils.getResultAttributes;
import static org.apache.nifi.apicurio.schemaregistry.util.SchemaUtils.getVersionAttribute;

public class ApicurioSchemaRegistryClient implements SchemaRegistryClient {
    private final SchemaRegistryApiClient apiClient;

    public ApicurioSchemaRegistryClient(SchemaRegistryApiClient apiClient) {
        this.apiClient = apiClient;
    }

    @Override
    public RecordSchema getSchema(String schemaName) throws IOException, SchemaNotFoundException {
        final URI searchUri = apiClient.buildSearchUri(schemaName);
        final InputStream searchResultStream = apiClient.retrieveResponse(searchUri);
        final ResultAttributes attributes = getResultAttributes(searchResultStream);

        final URI metaDataUri = apiClient.buildMetaDataUri(attributes.groupId(), attributes.artifactId());
        final InputStream metadataResultStream = apiClient.retrieveResponse(metaDataUri);
        final int version = getVersionAttribute(metadataResultStream);

        final URI schemaUri = apiClient.buildSchemaUri(attributes.groupId(), attributes.artifactId());
        final InputStream schemaResultStream = apiClient.retrieveResponse(schemaUri);

        return createRecordSchema(schemaResultStream, attributes.name(), version);
    }


    @Override
    public RecordSchema getSchema(final String schemaName, final int version) throws IOException, SchemaNotFoundException {
        final URI searchUri = apiClient.buildSearchUri(schemaName);
        final InputStream searchResultStream = apiClient.retrieveResponse(searchUri);
        final ResultAttributes attributes = getResultAttributes(searchResultStream);

        final URI schemaUri = apiClient.buildSchemaUri(attributes.groupId(), attributes.artifactId());
        final InputStream schemaResultStream = apiClient.retrieveResponse(schemaUri);

        return createRecordSchema(schemaResultStream, attributes.name(), version);
    }
}
