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
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.json.schema.JsonSchema;
import java.io.IOException;

/**
 * Represents {@link ControllerService} strategy to expose internal and/or
 * integrate with external Schema Registry
 */
public interface JsonSchemaRegistry extends ControllerService {
    /**
     * Retrieves the schema based on the provided schema name
     * @param schemaName The name of the schema
     * @return the schema for the given descriptor
     * @throws IOException if unable to communicate with the backing store
     * @throws SchemaNotFoundException if unable to find the schema based on the given descriptor
     */
    JsonSchema retrieveSchema(String schemaName) throws IOException, SchemaNotFoundException;
}
