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

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Set;

/**
 * Service interface responsible for writing Schema Identifier reference information to attributes or encoded content information
 */
public interface SchemaReferenceWriter extends ControllerService {
    /**
     * Writer Record Schema to the provided OutputStream as header information when required
     *
     * @param recordSchema Record Schema to be written to the provided Output Stream
     * @param outputStream Output Stream to which the provided Record Schema should be written
     * @throws IOException Thrown in failure to write Record Schema to the Output Stream
     */
    void writeHeader(RecordSchema recordSchema, OutputStream outputStream) throws IOException;

    /**
     * Get Attributes containing Record Schema content or references based on service implementation requirements
     *
     * @return Attributes containing Record Schema information or empty when not populated
     */
    Map<String, String> getAttributes(RecordSchema recordSchema);

    /**
     * Validate provided Record Schema for required information or throw SchemaNotFoundException indicating missing fields
     *
     * @param recordSchema Record Schema to be validated
     * @throws SchemaNotFoundException Thrown when the provided Record Schema is missing required information
     */
    void validateSchema(RecordSchema recordSchema) throws SchemaNotFoundException;

    /**
     * Get required Record Schema Fields for writing schema information
     *
     * @return Required Record Schema Fields
     */
    Set<SchemaField> getRequiredSchemaFields();
}
