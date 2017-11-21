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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;

@Tags({"schema", "registry", "avro", "json", "csv"})
@CapabilityDescription("Provides a service for registering and accessing schemas. You can register a schema "
    + "as a dynamic property where 'name' represents the schema name and 'value' represents the textual "
    + "representation of the actual schema following the syntax and semantics of Avro's Schema format.")
public class AvroSchemaRegistry extends AbstractControllerService implements SchemaRegistry {
    private static final Set<SchemaField> schemaFields = EnumSet.of(SchemaField.SCHEMA_NAME, SchemaField.SCHEMA_TEXT, SchemaField.SCHEMA_TEXT_FORMAT);
    private final Map<String, String> schemaNameToSchemaMap;
    private final ConcurrentMap<String, RecordSchema> recordSchemas = new ConcurrentHashMap<>();

    public AvroSchemaRegistry() {
        this.schemaNameToSchemaMap = new HashMap<>();
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (newValue == null) {
            recordSchemas.remove(descriptor.getName());
        } else {
            try {
                final Schema avroSchema = new Schema.Parser().parse(newValue);
                final SchemaIdentifier schemaId = SchemaIdentifier.ofName(descriptor.getName());
                final RecordSchema recordSchema = AvroTypeUtil.createSchema(avroSchema, newValue, schemaId);
                recordSchemas.put(descriptor.getName(), recordSchema);
            } catch (final Exception e) {
                // not a problem - the service won't be valid and the validation message will indicate what is wrong.
            }
        }
    }

    @Override
    public String retrieveSchemaText(final String schemaName) throws SchemaNotFoundException {
        final String schemaText = schemaNameToSchemaMap.get(schemaName);
        if (schemaText == null) {
            throw new SchemaNotFoundException("Unable to find schema with name '" + schemaName + "'");
        }

        return schemaText;
    }

    @Override
    public RecordSchema retrieveSchema(final String schemaName) throws SchemaNotFoundException {
        final RecordSchema recordSchema = recordSchemas.get(schemaName);
        if (recordSchema == null) {
            throw new SchemaNotFoundException("Unable to find schema with name '" + schemaName + "'");
        }
        return recordSchema;
    }

    @Override
    public RecordSchema retrieveSchema(long schemaId, int version) throws IOException, SchemaNotFoundException {
        throw new SchemaNotFoundException("This Schema Registry does not support schema lookup by identifier and version - only by name.");
    }

    @Override
    public String retrieveSchemaText(long schemaId, int version) throws IOException, SchemaNotFoundException {
        throw new SchemaNotFoundException("This Schema Registry does not support schema lookup by identifier and version - only by name.");
    }

    @OnDisabled
    public void close() throws Exception {
        schemaNameToSchemaMap.clear();
    }


    @OnEnabled
    public void enable(final ConfigurationContext configurationContext) throws InitializationException {
        this.schemaNameToSchemaMap.putAll(configurationContext.getProperties().entrySet().stream()
            .filter(propEntry -> propEntry.getKey().isDynamic())
            .collect(Collectors.toMap(propEntry -> propEntry.getKey().getName(), propEntry -> propEntry.getValue())));
    }


    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
            .name(propertyDescriptorName)
            .required(false)
            .addValidator(new AvroSchemaValidator())
            .dynamic(true)
            .expressionLanguageSupported(true)
            .build();
    }


    @Override
    public Set<SchemaField> getSuppliedSchemaFields() {
        return schemaFields;
    }
}
