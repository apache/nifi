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

package org.apache.nifi.serialization.record;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.util.Tuple;

public class MockSchemaRegistry extends AbstractControllerService implements SchemaRegistry {
    private final ConcurrentMap<String, RecordSchema> schemaNameMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<Tuple<Long, Integer>, RecordSchema> schemaIdVersionMap = new ConcurrentHashMap<>();

    public void addSchema(final String name, final RecordSchema schema) {
        schemaNameMap.put(name, schema);
    }

    @Override
    public String retrieveSchemaText(final String schemaName) throws IOException, SchemaNotFoundException {
        final RecordSchema schema = schemaNameMap.get(schemaName);
        if (schema == null) {
            return null;
        }

        final Optional<String> text = schema.getSchemaText();
        return text.orElse(null);
    }

    @Override
    public String retrieveSchemaText(final long schemaId, final int version) throws IOException, SchemaNotFoundException {
        final Tuple<Long, Integer> tuple = new Tuple<>(schemaId, version);
        final RecordSchema schema = schemaIdVersionMap.get(tuple);
        if (schema == null) {
            return null;
        }

        final Optional<String> text = schema.getSchemaText();
        return text.orElse(null);
    }

    @Override
    public RecordSchema retrieveSchema(final String schemaName) throws IOException, SchemaNotFoundException {
        return schemaNameMap.get(schemaName);
    }

    @Override
    public RecordSchema retrieveSchema(final long schemaId, final int version) throws IOException, SchemaNotFoundException {
        final Tuple<Long, Integer> tuple = new Tuple<>(schemaId, version);
        final RecordSchema schema = schemaIdVersionMap.get(tuple);
        return schema;
    }

    @Override
    public Set<SchemaField> getSuppliedSchemaFields() {
        return EnumSet.allOf(SchemaField.class);
    }
}
