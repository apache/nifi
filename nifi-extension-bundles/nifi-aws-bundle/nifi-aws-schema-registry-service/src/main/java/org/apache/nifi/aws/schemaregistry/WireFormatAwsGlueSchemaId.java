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

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

record WireFormatAwsGlueSchemaId(UUID id) {
    private static final String GLUE_SCHEMA_REGISTRY_WIRE_FORMAT_UUID_PREFIX = "aws-glue-schema-registry-wire-format-uuid$$";

    WireFormatAwsGlueSchemaId {
        Objects.requireNonNull(id, "Schema version ID must not be null");
    }

    public String toSchemaName() {
        return GLUE_SCHEMA_REGISTRY_WIRE_FORMAT_UUID_PREFIX + id;
    }

    public static Optional<WireFormatAwsGlueSchemaId> fromSchemaName(final String name) {
        try {
            if (!isWireFormatName(name)) {
                return Optional.empty();
            }
            final UUID uuid = UUID.fromString(name.substring(GLUE_SCHEMA_REGISTRY_WIRE_FORMAT_UUID_PREFIX.length()));
            return Optional.of(new WireFormatAwsGlueSchemaId(uuid));
        } catch (final IllegalArgumentException e) {
            // If the string is not a valid UUID, return empty
            return Optional.empty();
        }
    }

    public static boolean isWireFormatName(final String schemaName) {
        return schemaName != null && schemaName.startsWith(GLUE_SCHEMA_REGISTRY_WIRE_FORMAT_UUID_PREFIX);
    }
}
