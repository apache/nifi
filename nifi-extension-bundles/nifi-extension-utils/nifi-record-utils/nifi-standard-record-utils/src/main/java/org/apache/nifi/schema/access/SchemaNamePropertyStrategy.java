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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;

import java.io.InputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SchemaNamePropertyStrategy implements SchemaAccessStrategy {
    private final Set<SchemaField> schemaFields;

    private final SchemaRegistry schemaRegistry;
    private final PropertyValue schemaNamePropertyValue;
    private final PropertyValue schemaBranchNamePropertyValue;
    private final PropertyValue schemaVersionPropertyValue;

    public SchemaNamePropertyStrategy(final SchemaRegistry schemaRegistry,
                                      final PropertyValue schemaNamePropertyValue,
                                      final PropertyValue schemaBranchNamePropertyValue,
                                      final PropertyValue schemaVersionPropertyValue) {
        this.schemaRegistry = schemaRegistry;
        this.schemaNamePropertyValue = schemaNamePropertyValue;
        this.schemaBranchNamePropertyValue = schemaBranchNamePropertyValue;
        this.schemaVersionPropertyValue = schemaVersionPropertyValue;

        schemaFields = new HashSet<>();
        schemaFields.add(SchemaField.SCHEMA_NAME);
        schemaFields.addAll(schemaRegistry == null ? Collections.emptySet() : schemaRegistry.getSuppliedSchemaFields());
    }

    @Override
    public RecordSchema getSchema(final Map<String, String> variables, final InputStream contentStream, final RecordSchema readSchema) throws SchemaNotFoundException {
        final String schemaName = schemaNamePropertyValue.evaluateAttributeExpressions(variables).getValue();
        if (schemaName.trim().isEmpty()) {
            throw new SchemaNotFoundException(String.format("%s did not provide appropriate Schema Name", schemaNamePropertyValue));
        }

        try {
            final String schemaBranchName = schemaBranchNamePropertyValue.evaluateAttributeExpressions(variables).getValue();
            final String schemaVersion = schemaVersionPropertyValue.evaluateAttributeExpressions(variables).getValue();

            final SchemaIdentifier.Builder identifierBuilder = SchemaIdentifier.builder();
            identifierBuilder.name(schemaName);

            if (!StringUtils.isBlank(schemaBranchName)) {
                identifierBuilder.branch(schemaBranchName);
            }

            if (!StringUtils.isBlank(schemaVersion)) {
                try {
                    identifierBuilder.version(Integer.valueOf(schemaVersion));
                } catch (NumberFormatException nfe) {
                    throw new SchemaNotFoundException("Could not retrieve schema with name '" + schemaName
                            + "' because a non-numeric version was supplied '" + schemaVersion + "'", nfe);
                }
            }

            final RecordSchema recordSchema = schemaRegistry.retrieveSchema(identifierBuilder.build());
            if (recordSchema == null) {
                throw new SchemaNotFoundException("Could not find a schema with name '" + schemaName + "' in the configured Schema Registry");
            }

            return recordSchema;
        } catch (final SchemaNotFoundException snf) {
            throw snf;
        } catch (final Exception e) {
            throw new SchemaNotFoundException("Could not retrieve schema with name '" + schemaName + "' from the configured Schema Registry", e);
        }
    }

    @Override
    public Set<SchemaField> getSuppliedSchemaFields() {
        return schemaFields;
    }
}
