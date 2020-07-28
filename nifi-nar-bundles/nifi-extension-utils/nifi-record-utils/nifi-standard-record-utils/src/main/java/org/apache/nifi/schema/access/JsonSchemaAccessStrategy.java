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

import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.util.Map;

public interface JsonSchemaAccessStrategy extends SchemaAccessStrategy {
    /**
     * Get a schema using a Map object instead of an input stream. This is meant to be used with JSON toolkits.
     *
     * @param variables Variables which is used to resolve Record Schema via Expression Language.
     *                  This can be null or empty.
     * @param content   JSON content in a Map object form.
     * @param readSchema  The schema that was read from the input content, or <code>null</code> if there was none.
     * @return The RecordSchema if found.
     */
    RecordSchema getSchema(Map<String, String> variables, Map<String, Object> content, RecordSchema readSchema) throws SchemaNotFoundException, IOException;
}
