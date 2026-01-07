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

package org.apache.nifi.confluent.schemaregistry.client;

import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schemaregistry.services.SchemaDefinition;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;

import java.io.IOException;

public interface SchemaRegistryClient {

    RecordSchema getSchema(String schemaName) throws IOException, SchemaNotFoundException;

    RecordSchema getSchema(String schemaName, int version) throws IOException, SchemaNotFoundException;

    RecordSchema getSchema(int schemaId) throws IOException, SchemaNotFoundException;

    /**
     * Gets a schema definition with all its references from the schema registry.
     * @param identifier The schema identifier containing id, name and version
     * @return The schema definition with all references resolved
     * @throws IOException if there is an error communicating with the schema registry
     * @throws SchemaNotFoundException if the schema cannot be found
     */
    SchemaDefinition getSchemaDefinition(SchemaIdentifier identifier) throws IOException, SchemaNotFoundException;
}
