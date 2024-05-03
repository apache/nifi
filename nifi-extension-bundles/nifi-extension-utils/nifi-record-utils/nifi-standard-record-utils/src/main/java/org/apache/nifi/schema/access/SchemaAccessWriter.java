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

package org.apache.nifi.schema.access;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.serialization.record.RecordSchema;

public interface SchemaAccessWriter {

    /**
     * Writes the given Record Schema to the given OutputStream as header information, if appropriate,
     * or returns without writing anything if the implementation does not need to write information to
     * the contents of the FlowFile
     *
     * @param schema the schema to write
     * @param out the OutputStream to write to
     * @throws IOException if unable to write to the given stream
     */
    void writeHeader(RecordSchema schema, OutputStream out) throws IOException;

    /**
     * Returns a Map of String to String that represent the attributes that should be added to the FlowFile, or
     * an empty map if no attributes should be added.
     *
     * @return a Map of attributes to add to the FlowFile.
     */
    Map<String, String> getAttributes(RecordSchema schema);

    /**
     * Ensures that the given schema can be written by this SchemaAccessWriter or throws SchemaNotFoundException if
     * the schema does not contain sufficient information to be written
     *
     * @param schema the schema to validate
     * @throws SchemaNotFoundException if the schema does not contain sufficient information to be written
     */
    void validateSchema(RecordSchema schema) throws SchemaNotFoundException;

    /**
     * Specifies the set of SchemaField's that are required in order to use this Schema Access Writer
     *
     * @return the set of SchemaField's that are required in order to use this Schema Access Writer
     */
    Set<SchemaField> getRequiredSchemaFields();
}
