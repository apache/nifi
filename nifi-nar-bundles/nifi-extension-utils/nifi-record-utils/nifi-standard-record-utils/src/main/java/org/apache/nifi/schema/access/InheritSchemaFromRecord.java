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
import java.io.InputStream;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.serialization.record.RecordSchema;

public class InheritSchemaFromRecord implements SchemaAccessStrategy {

    @Override
    public RecordSchema getSchema(final Map<String, String> variables, final InputStream contentStream, final RecordSchema readSchema) throws SchemaNotFoundException, IOException {
        if (readSchema == null) {
            throw new SchemaNotFoundException("Cannot inherit Schema from Record because no schema was found");
        }

        return readSchema;
    }

    @Override
    public Set<SchemaField> getSuppliedSchemaFields() {
        return EnumSet.allOf(SchemaField.class);
    }

}
