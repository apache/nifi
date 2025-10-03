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

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.json.schema.record.JsonSchemaToRecordSchemaConverter;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;

import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Tags({"schema", "registry", "json", "record"})
@CapabilityDescription("Provides a simple controller service for managing JSON Schemas and exposing them as NiFi RecordSchemas. "
        + "Each dynamic property name represents a schema name and the property value is expected to be a JSON Schema definition. "
        + "The service converts JSON Schemas to NiFi RecordSchemas, preserving validation rules defined in the JSON Schema specification.")
@DynamicProperty(name = "Schema name", value = "JSON Schema",
        description = "Adds a named JSON Schema that will be converted to a NiFi RecordSchema",
        expressionLanguageScope = ExpressionLanguageScope.NONE)
public class JsonSchemaRegistryService extends AbstractControllerService implements SchemaRegistry {
    private static final String JSON_SCHEMA_FORMAT = "json-schema";

    private static final Set<SchemaField> SUPPLIED_FIELDS = EnumSet.of(
            SchemaField.SCHEMA_NAME,
            SchemaField.SCHEMA_TEXT,
            SchemaField.SCHEMA_TEXT_FORMAT
    );

    private final JsonSchemaToRecordSchemaConverter converter = new JsonSchemaToRecordSchemaConverter();
    private final ConcurrentMap<String, RecordSchema> recordSchemas = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, SchemaDefinition> schemaDefinitions = new ConcurrentHashMap<>();

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (!descriptor.isDynamic()) {
            return;
        }

        final String schemaName = descriptor.getName();
        if (newValue == null || newValue.isBlank()) {
            recordSchemas.remove(schemaName);
            schemaDefinitions.remove(schemaName);
            return;
        }

        try {
            final RecordSchema recordSchema = buildRecordSchema(schemaName, newValue);
            recordSchemas.put(schemaName, recordSchema);

            final SchemaIdentifier identifier = SchemaIdentifier.builder().name(schemaName).build();
            final SchemaDefinition definition = new StandardSchemaDefinition(identifier, newValue, SchemaDefinition.SchemaType.JSON);
            schemaDefinitions.put(schemaName, definition);
        } catch (final Exception e) {
            recordSchemas.remove(schemaName);
            schemaDefinitions.remove(schemaName);
            getLogger().debug("Failed to parse JSON Schema for schema name '{}': {}", schemaName, e.getMessage(), e);
        }
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final Set<ValidationResult> results = new HashSet<>();
        boolean foundDynamicProperty = false;

        for (final Map.Entry<PropertyDescriptor, String> entry : validationContext.getProperties().entrySet()) {
            final PropertyDescriptor descriptor = entry.getKey();
            if (!descriptor.isDynamic()) {
                continue;
            }

            foundDynamicProperty = true;
            final String schemaName = descriptor.getName();
            final String schemaText = entry.getValue();

            if (schemaText == null || schemaText.isBlank()) {
                results.add(new ValidationResult.Builder()
                        .subject(schemaName)
                        .valid(false)
                        .explanation("Schema text must be provided")
                        .build());
                continue;
            }

            try {
                buildRecordSchema(schemaName, schemaText);
            } catch (final Exception e) {
                results.add(new ValidationResult.Builder()
                        .subject(schemaName)
                        .input(schemaText)
                        .valid(false)
                        .explanation("Not a valid JSON Schema: " + e.getMessage())
                        .build());
            }
        }

        if (!foundDynamicProperty) {
            results.add(new ValidationResult.Builder()
                    .subject("JSON Schemas")
                    .valid(false)
                    .explanation("At least one JSON Schema must be defined as a dynamic property")
                    .build());
        }

        return results;
    }

    @Override
    public RecordSchema retrieveSchema(final SchemaIdentifier schemaIdentifier) throws IOException, SchemaNotFoundException {
        final Optional<String> schemaNameOptional = schemaIdentifier.getName();
        if (schemaNameOptional.isEmpty()) {
            throw new SchemaNotFoundException("This Schema Registry only supports retrieving schemas by name");
        }

        final String schemaName = schemaNameOptional.get();
        final RecordSchema recordSchema = recordSchemas.get(schemaName);
        if (recordSchema == null) {
            throw new SchemaNotFoundException("Unable to find schema with name '" + schemaName + "'");
        }

        return recordSchema;
    }

    @Override
    public Set<SchemaField> getSuppliedSchemaFields() {
        return SUPPLIED_FIELDS;
    }

    @Override
    public boolean isSchemaDefinitionAccessSupported() {
        return true;
    }

    @Override
    public SchemaDefinition retrieveSchemaDefinition(final SchemaIdentifier schemaIdentifier) throws IOException, SchemaNotFoundException {
        final Optional<String> schemaNameOptional = schemaIdentifier.getName();
        if (schemaNameOptional.isEmpty()) {
            throw new SchemaNotFoundException("This Schema Registry only supports retrieving schemas by name");
        }

        final SchemaDefinition definition = schemaDefinitions.get(schemaNameOptional.get());
        if (definition == null) {
            throw new SchemaNotFoundException("Unable to find schema definition for name '" + schemaNameOptional.get() + "'");
        }

        return definition;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return List.of();
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.NONE)
                .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
                .required(false)
                .build();
    }

    private RecordSchema buildRecordSchema(final String schemaName, final String schemaText) {
        final RecordSchema converted = converter.convert(schemaText);
        final String schemaFormat = converted.getSchemaFormat().orElse(JSON_SCHEMA_FORMAT);
        if (converted instanceof final SimpleRecordSchema simpleRecordSchema) {
            final SimpleRecordSchema schemaWithId = new SimpleRecordSchema(simpleRecordSchema.getFields(), schemaText, schemaFormat,
                    SchemaIdentifier.builder().name(schemaName).build());
            simpleRecordSchema.getSchemaName().ifPresent(schemaWithId::setSchemaName);
            simpleRecordSchema.getSchemaNamespace().ifPresent(schemaWithId::setSchemaNamespace);
            schemaWithId.setRecordValidators(simpleRecordSchema.getRecordValidators());
            return schemaWithId;
        }

        final SimpleRecordSchema schemaWithId = new SimpleRecordSchema(converted.getFields(), schemaText, schemaFormat,
                SchemaIdentifier.builder().name(schemaName).build());
        converted.getSchemaName().ifPresent(schemaWithId::setSchemaName);
        converted.getSchemaNamespace().ifPresent(schemaWithId::setSchemaNamespace);
        schemaWithId.setRecordValidators(converted.getRecordValidators());
        return schemaWithId;
    }
}
