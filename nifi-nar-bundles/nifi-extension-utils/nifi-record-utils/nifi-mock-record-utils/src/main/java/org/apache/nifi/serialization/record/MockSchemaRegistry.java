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

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.util.Tuple;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MockSchemaRegistry extends AbstractControllerService implements SchemaRegistry {
    private final ConcurrentMap<String, RecordSchema> schemaNameMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<Tuple<Long, Integer>, RecordSchema> schemaIdVersionMap = new ConcurrentHashMap<>();

    public void addSchema(final String name, final RecordSchema schema) {
        schemaNameMap.put(name, schema);
    }

    private RecordSchema retrieveSchemaByName(final SchemaIdentifier schemaIdentifier) throws IOException, SchemaNotFoundException {
        final Optional<String> schemaName = schemaIdentifier.getName();
        if (!schemaName.isPresent()) {
            throw new org.apache.nifi.schema.access.SchemaNotFoundException("Cannot retrieve schema because Schema Name is not present");
        }

        return schemaNameMap.get(schemaName.get());
    }

    private RecordSchema retrieveSchemaByIdAndVersion(final SchemaIdentifier schemaIdentifier) throws IOException, SchemaNotFoundException {
        final OptionalLong schemaId = schemaIdentifier.getIdentifier();
        if (!schemaId.isPresent()) {
            throw new org.apache.nifi.schema.access.SchemaNotFoundException("Cannot retrieve schema because Schema Id is not present");
        }

        final OptionalInt version = schemaIdentifier.getVersion();
        if (!version.isPresent()) {
            throw new org.apache.nifi.schema.access.SchemaNotFoundException("Cannot retrieve schema because Schema Version is not present");
        }

        final Tuple<Long, Integer> tuple = new Tuple<>(schemaId.getAsLong(), version.getAsInt());
        final RecordSchema schema = schemaIdVersionMap.get(tuple);
        return schema;
    }

    @Override
    public RecordSchema retrieveSchema(final SchemaIdentifier schemaIdentifier) throws IOException, SchemaNotFoundException {
        if (schemaIdentifier.getName().isPresent()) {
            return retrieveSchemaByName(schemaIdentifier);
        } else {
            return retrieveSchemaByIdAndVersion(schemaIdentifier);
        }
    }

    @Override
    public Set<SchemaField> getSuppliedSchemaFields() {
        return EnumSet.allOf(SchemaField.class);
    }
}
