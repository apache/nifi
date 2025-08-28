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
package org.apache.nifi.services.protobuf;

import org.apache.nifi.schemaregistry.services.SchemaDefinition;
import org.apache.nifi.serialization.record.SchemaIdentifier;

/**
 * Validates Protocol Buffer SchemaDefinition objects and schema identifiers.
 */
final class ProtobufSchemaValidator {

    private ProtobufSchemaValidator() {
    }

    /**
     * Validates that all SchemaDefinition identifiers end with .proto extension.
     * Performs recursive validation on all referenced schemas.
     *
     * @param schemaDefinition       the schema definition to validate
     * @param isRootSchemaDefinition set to true if schema definition is a root definition, false otherwise
     * @throws IllegalArgumentException if any identifier does not end with .proto extension
     */
    static void validateSchemaDefinitionIdentifiers(final SchemaDefinition schemaDefinition, final boolean isRootSchemaDefinition) {
        // do not validate schema identifier names for root schema definitions. They might be coming from sources like text fields,
        // flow file attributes and other sources that do not support naming.
        if (!isRootSchemaDefinition) {
            validateSchemaIdentifier(schemaDefinition.getIdentifier());
        }

        // Recursively validate all referenced schemas
        // schema references have to end with .proto extension.
        for (final SchemaDefinition referencedSchema : schemaDefinition.getReferences().values()) {
            validateSchemaDefinitionIdentifiers(referencedSchema, false);
        }
    }

    /**
     * Validates that a single SchemaIdentifier has a name ending with .proto extension.
     *
     * @param schemaIdentifier the schema identifier to validate
     * @throws IllegalArgumentException if the identifier name does not end with .proto extension
     */
    private static void validateSchemaIdentifier(final SchemaIdentifier schemaIdentifier) {
        schemaIdentifier.getName()
            .filter(name -> name.endsWith(".proto"))
            .orElseThrow(() -> new IllegalArgumentException("Schema identifier must have a name that ends with .proto extension. Schema identifier: " + schemaIdentifier));
    }
}