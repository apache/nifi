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

import java.io.IOException;

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.record.RecordSchema;

/**
 * Represents {@link ControllerService} strategy to expose internal and/or
 * integrate with external Schema Registry
 */
public interface SchemaRegistry extends ControllerService {


    /**
     * Retrieves and returns the textual representation of the schema based on
     * the provided name of the schema available in Schema Registry.
     *
     * @return the text that corresponds to the latest version of the schema with the given name
     *
     * @throws IOException if unable to communicate with the backing store
     * @throws SchemaNotFoundException if unable to find the schema with the given name
     */
    String retrieveSchemaText(String schemaName) throws IOException, SchemaNotFoundException;

    /**
     * Retrieves the textual representation of the schema with the given ID and version
     *
     * @param schemaId the unique identifier for the desired schema
     * @param version the version of the desired schema
     * @return the textual representation of the schema with the given ID and version
     *
     * @throws IOException if unable to communicate with the backing store
     * @throws SchemaNotFoundException if unable to find the schema with the given id and version
     */
    String retrieveSchemaText(long schemaId, int version) throws IOException, SchemaNotFoundException;

    /**
     * Retrieves and returns the RecordSchema based on the provided name of the schema available in Schema Registry.
     *
     * @return the latest version of the schema with the given name, or <code>null</code> if no schema can be found with the given name.
     * @throws SchemaNotFoundException if unable to find the schema with the given name
     */
    RecordSchema retrieveSchema(String schemaName) throws IOException, SchemaNotFoundException;


    /**
     * Retrieves the schema with the given ID and version
     *
     * @param schemaId the unique identifier for the desired schema
     * @param version the version of the desired schema
     * @return the schema with the given ID and version or <code>null</code> if no schema
     *         can be found with the given ID and version
     *
     * @throws IOException if unable to communicate with the backing store
     * @throws SchemaNotFoundException if unable to find the schema with the given id and version
     */
    RecordSchema retrieveSchema(long schemaId, int version) throws IOException, SchemaNotFoundException;

}
