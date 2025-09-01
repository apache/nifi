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
package org.apache.nifi.schemaregistry.services;

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;

import java.io.IOException;
import java.util.Set;

/**
 * Represents {@link ControllerService} strategy to expose internal and/or
 * integrate with external Schema Registry
 */
public interface SchemaRegistry extends ControllerService {
    /**
     * Retrieves the schema based on the provided descriptor. The descriptor must contain and schemaIdentifier or name, but not both, along
     * with a version, and an optional branch name. For implementations that do not support branching, the branch name will be ignored.
     *
     * @param schemaIdentifier a schema schemaIdentifier
     * @return the schema for the given descriptor
     * @throws IOException if unable to communicate with the backing store
     * @throws SchemaNotFoundException if unable to find the schema based on the given descriptor
     */
    RecordSchema retrieveSchema(SchemaIdentifier schemaIdentifier) throws IOException, SchemaNotFoundException;

    /**
     * @return the set of all Schema Fields that are supplied by the RecordSchema that is returned from {@link #retrieveSchema(SchemaIdentifier)}
     */
    Set<SchemaField> getSuppliedSchemaFields();

    /**
     * Indicates whether this schema registry implementation supports raw schema definition access.
     * <p>
     * This method allows clients to determine if the {@link #retrieveSchemaDefinition(SchemaIdentifier)}
     * method is supported before attempting to call it, avoiding UnsupportedOperationException.
     * </p>
     *
     * @return true if raw schema definition access is supported, false otherwise
     */
    default boolean isSchemaDefinitionAccessSupported() {
        return false;
    }

    /**
     * Retrieves the raw schema definition including its textual representation and references.
     * <p>
     * This method is used to retrieve the complete schema definition structure, including the raw schema text
     * and any schema references. Unlike {@link #retrieveSchema(SchemaIdentifier)}, which returns a parsed
     * {@link RecordSchema} ready for immediate use, this method returns a {@link SchemaDefinition} containing
     * the raw schema content that can be used for custom schema processing, compilation, or when schema
     * references need to be resolved.
     * </p>
     * <p>
     * This method is particularly useful for:
     * <ul>
     *   <li>Processing schemas that reference other schemas (e.g., Protocol Buffers with imports)</li>
     *   <li>Custom schema compilation workflows where the raw schema text is needed</li>
     *   <li>Accessing schema metadata and references for advanced schema processing</li>
     * </ul>
     * </p>
     *
     * @param schemaIdentifier the schema identifier containing id, name, version, and optionally branch information
     * @return a {@link SchemaDefinition} containing the raw schema text, type, identifier, and references
     * @throws IOException if unable to communicate with the backing store
     * @throws SchemaNotFoundException if unable to find the schema based on the given identifier
     * @throws UnsupportedOperationException if the schema registry implementation does not support raw schema retrieval
     */
    default SchemaDefinition retrieveSchemaDefinition(SchemaIdentifier schemaIdentifier) throws IOException, SchemaNotFoundException {
        throw new UnsupportedOperationException("retrieveSchemaRaw is not supported by this SchemaRegistry implementation");
    }
}
