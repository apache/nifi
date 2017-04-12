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

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

public interface SchemaAccessStrategy {
    /**
     * Returns the schema for the given FlowFile using the supplied stream of content and configuration
     *
     * @param flowFile flowfile
     * @param contentStream content of flowfile
     * @return the RecordSchema for the FlowFile
     */
    RecordSchema getSchema(FlowFile flowFile, InputStream contentStream) throws SchemaNotFoundException, IOException;

    /**
     * @return the set of all Schema Fields that are supplied by the RecordSchema that is returned from {@link #getSchema(FlowFile, InputStream)}.
     */
    Set<SchemaField> getSuppliedSchemaFields();
}
