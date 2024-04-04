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

import org.apache.avro.Schema;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.serialization.record.RecordSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

public class AvroSchemaTextStrategy implements SchemaAccessStrategy {
    private static final Set<SchemaField> schemaFields = EnumSet.of(SchemaField.SCHEMA_TEXT, SchemaField.SCHEMA_TEXT_FORMAT);

    private static final Logger logger = LoggerFactory.getLogger(AvroSchemaTextStrategy.class);
    private final PropertyValue schemaTextPropertyValue;

    public AvroSchemaTextStrategy(final PropertyValue schemaTextPropertyValue) {
        this.schemaTextPropertyValue = schemaTextPropertyValue;
    }

    @Override
    public RecordSchema getSchema(Map<String, String> variables, InputStream contentStream, RecordSchema readSchema) throws SchemaNotFoundException {
        final String schemaText;
        schemaText = schemaTextPropertyValue.evaluateAttributeExpressions(variables).getValue();
        if (schemaText == null || schemaText.trim().isEmpty()) {
            throw new SchemaNotFoundException("FlowFile did not contain appropriate attributes to determine Schema Text");
        }

        logger.debug("For {} found schema text {}", variables, schemaText);

        try {
            final Schema avroSchema = new Schema.Parser().parse(schemaText);
            return AvroTypeUtil.createSchema(avroSchema);
        } catch (final Exception e) {
            throw new SchemaNotFoundException("Failed to create schema from the Schema Text after evaluating FlowFile Attributes", e);
        }
    }

    @Override
    public Set<SchemaField> getSuppliedSchemaFields() {
        return schemaFields;
    }
}
