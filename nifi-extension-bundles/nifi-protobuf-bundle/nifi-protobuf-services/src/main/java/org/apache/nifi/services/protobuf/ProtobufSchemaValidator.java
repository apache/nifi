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

import java.util.Map;

/**
 * Validates the schema references of Protocol Buffer SchemaDefinition objects.
 */
final class ProtobufSchemaValidator {

    private static final String PROTO_EXTENSION = ".proto";

    private ProtobufSchemaValidator() {
    }

    /**
     * Validates that every schema reference, at any depth, is keyed by a path ending in the .proto extension.
     * <p>
     * A reference is keyed by the path used in the import statement of the referencing schema, for example
     * {@code airlines/ph/cdm/shared.proto}, and the referenced schema is written to exactly that path so the import
     * resolves. The extension is required because the compiler only discovers files named {@code *.proto}; without it
     * the schema would fail to compile later with an unresolved import that does not indicate the cause.
     * <p>
     * The identifier of a referenced schema is deliberately not validated. It carries the subject the schema is
     * registered under, which is unrelated to the import path and legitimately has no .proto suffix. Under the
     * Confluent RecordNameStrategy, for instance, a subject is a fully qualified record name.
     *
     * @param schemaDefinition the schema definition whose references should be validated
     * @throws IllegalArgumentException if any reference is keyed by a path that does not end in .proto
     */
    static void validateSchemaReferencePaths(final SchemaDefinition schemaDefinition) {
        for (final Map.Entry<String, SchemaDefinition> reference : schemaDefinition.getReferences().entrySet()) {
            validateReferencePath(reference.getKey());
            validateSchemaReferencePaths(reference.getValue());
        }
    }

    private static void validateReferencePath(final String referencePath) {
        if (referencePath == null || !referencePath.endsWith(PROTO_EXTENSION)) {
            throw new IllegalArgumentException(
                "Schema reference must be keyed by the import path of the referenced schema, ending with the .proto extension. Schema reference: " + referencePath);
        }
    }
}
