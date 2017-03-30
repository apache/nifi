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

import java.util.Map;

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.serialization.record.RecordSchema;

/**
 * Represents {@link ControllerService} strategy to expose internal and/or
 * integrate with external Schema Registry
 */
public interface SchemaRegistry extends ControllerService, AutoCloseable {

    public static final String SCHEMA_NAME_ATTR = "schema.name";


    /**
     * Retrieves and returns the textual representation of the schema based on
     * the provided name of the schema available in Schema Registry. Will throw
     * an runtime exception if schema can not be found.
     */
    String retrieveSchemaText(String schemaName);

    /**
     * Retrieves and returns the textual representation of the schema based on
     * the provided name of the schema available in Schema Registry and optional
     * additional attributes. Will throw an runtime exception if schema can not
     * be found.
     */
    String retrieveSchemaText(String schemaName, Map<String, String> attributes);


    RecordSchema retrieveSchema(String schemaName);


    RecordSchema retrieveSchema(String schemaName, Map<String, String> attributes);
}
