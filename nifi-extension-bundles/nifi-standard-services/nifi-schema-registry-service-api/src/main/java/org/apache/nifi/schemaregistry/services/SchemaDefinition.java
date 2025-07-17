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

import org.apache.nifi.serialization.record.SchemaIdentifier;

import java.util.Map;

/**
 * This acts as a data container for schema information retrieved from various schema registries.
 */
public interface SchemaDefinition {

    enum SchemaType {
        PROTOBUF, AVRO
    }

    /**
     * @return the type of schema (e.g., AVRO, PROTOBUF, JSON, XML, OTHER)
     */
    SchemaType getSchemaType();

    /**
     * @return the unique identifier for this schema
     */
    SchemaIdentifier getIdentifier();

    /**
     * @return the textual representation of the schema (e.g., Avro schema JSON, Protobuf definition)
     */
    String getText();

    /**
     * @return a map of schema references where the key is the name by which the schema is referenced
     *         from its parent, and the value is the referenced schema definition
     */
    default Map<String, SchemaDefinition> getReferences() {
        return Map.of();
    }
}
