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

import org.apache.nifi.schemaregistry.services.SchemaReferenceReader;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Schema Access Strategy that delegates to the configured Schema Reference Reader
 */
class SchemaReferenceReaderSchemaAccessStrategy implements SchemaAccessStrategy {
    private final SchemaReferenceReader schemaReferenceReader;

    private final SchemaRegistry schemaRegistry;

    private final Set<SchemaField> suppliedSchemaFields;

    SchemaReferenceReaderSchemaAccessStrategy(final SchemaReferenceReader schemaReferenceReader, final SchemaRegistry schemaRegistry) {
        this.schemaReferenceReader = Objects.requireNonNull(schemaReferenceReader, "Schema Reference Reader required");
        this.schemaRegistry = Objects.requireNonNull(schemaRegistry, "Schema Registry required");

        final Set<SchemaField> configuredSchemaFields = new LinkedHashSet<>(schemaRegistry.getSuppliedSchemaFields());
        configuredSchemaFields.addAll(schemaReferenceReader.getSuppliedSchemaFields());
        this.suppliedSchemaFields = Collections.unmodifiableSet(configuredSchemaFields);
    }

    /**
     * Get Schema based on provided variables and content information
     *
     * @param variables Map of variables for Schema Identifier resolution can be null or empty
     * @param contentStream Stream of FlowFile content that may contain encoded Schema Identifier references
     * @param inputSchema Record Schema from input content or null when not provided
     * @return Record Schema
     * @throws SchemaNotFoundException Thrown on failure to resolve Schema Identifier information
     * @throws IOException Thrown on failure to read input content stream
     */
    @Override
    public RecordSchema getSchema(final Map<String, String> variables, final InputStream contentStream, final RecordSchema inputSchema) throws SchemaNotFoundException, IOException {
        final Map<String, String> schemaVariables = variables == null ? Collections.emptyMap() : variables;

        final SchemaIdentifier schemaIdentifier = schemaReferenceReader.getSchemaIdentifier(schemaVariables, contentStream);
        if (schemaIdentifier == null) {
            throw new IllegalArgumentException("Schema Identifier not supplied from Schema Reference Provider");
        }

        return schemaRegistry.retrieveSchema(schemaIdentifier);
    }

    /**
     * Get supplied Record Schema Fields containing of combination of fields from the Schema Registry and Schema Reference Provider services
     *
     * @return Record Schema Fields
     */
    @Override
    public Set<SchemaField> getSuppliedSchemaFields() {
        return suppliedSchemaFields;
    }
}
