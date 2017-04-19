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
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;

@Tags({"schema", "registry", "avro", "json", "csv"})
@CapabilityDescription("Provides a service for registering and accessing schemas. You can register a schema "
    + "as a dynamic property where 'name' represents the schema name and 'value' represents the textual "
    + "representation of the actual schema following the syntax and semantics of Avro's Schema format.")
public class AvroSchemaRegistry extends AbstractControllerService implements SchemaRegistry {
    private static final Set<SchemaField> schemaFields = EnumSet.of(SchemaField.SCHEMA_NAME, SchemaField.SCHEMA_TEXT, SchemaField.SCHEMA_TEXT_FORMAT);
    private final Map<String, String> schemaNameToSchemaMap;

    private static final String LOGICAL_TYPE_DATE = "date";
    private static final String LOGICAL_TYPE_TIME_MILLIS = "time-millis";
    private static final String LOGICAL_TYPE_TIME_MICROS = "time-micros";
    private static final String LOGICAL_TYPE_TIMESTAMP_MILLIS = "timestamp-millis";
    private static final String LOGICAL_TYPE_TIMESTAMP_MICROS = "timestamp-micros";

    public AvroSchemaRegistry() {
        this.schemaNameToSchemaMap = new HashMap<>();
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
        final String schemaText = retrieveSchemaText(schemaName);
        final Schema schema = new Schema.Parser().parse(schemaText);
        return createRecordSchema(schema, schemaText, schemaName);
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


    /**
     * Converts an Avro Schema to a RecordSchema
     *
     * @param avroSchema the Avro Schema to convert
     * @param text the textual representation of the schema
     * @param schemaName the name of the schema
     * @return the Corresponding Record Schema
     */
    private RecordSchema createRecordSchema(final Schema avroSchema, final String text, final String schemaName) {
        final List<RecordField> recordFields = new ArrayList<>(avroSchema.getFields().size());
        for (final Field field : avroSchema.getFields()) {
            final String fieldName = field.name();
            final DataType dataType = determineDataType(field.schema());

            recordFields.add(new RecordField(fieldName, dataType, field.defaultVal(), field.aliases()));
        }

        final RecordSchema recordSchema = new SimpleRecordSchema(recordFields, text, "avro", SchemaIdentifier.ofName(schemaName));
        return recordSchema;
    }

    /**
     * Returns a DataType for the given Avro Schema
     *
     * @param avroSchema the Avro Schema to convert
     * @return a Data Type that corresponds to the given Avro Schema
     */
    private DataType determineDataType(final Schema avroSchema) {
        final Type avroType = avroSchema.getType();

        final LogicalType logicalType = avroSchema.getLogicalType();
        if (logicalType != null) {
            final String logicalTypeName = logicalType.getName();
            switch (logicalTypeName) {
                case LOGICAL_TYPE_DATE:
                    return RecordFieldType.DATE.getDataType();
                case LOGICAL_TYPE_TIME_MILLIS:
                case LOGICAL_TYPE_TIME_MICROS:
                    return RecordFieldType.TIME.getDataType();
                case LOGICAL_TYPE_TIMESTAMP_MILLIS:
                case LOGICAL_TYPE_TIMESTAMP_MICROS:
                    return RecordFieldType.TIMESTAMP.getDataType();
            }
        }

        switch (avroType) {
            case ARRAY:
                return RecordFieldType.ARRAY.getArrayDataType(determineDataType(avroSchema.getElementType()));
            case BYTES:
            case FIXED:
                return RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType());
            case BOOLEAN:
                return RecordFieldType.BOOLEAN.getDataType();
            case DOUBLE:
                return RecordFieldType.DOUBLE.getDataType();
            case ENUM:
            case STRING:
                return RecordFieldType.STRING.getDataType();
            case FLOAT:
                return RecordFieldType.FLOAT.getDataType();
            case INT:
                return RecordFieldType.INT.getDataType();
            case LONG:
                return RecordFieldType.LONG.getDataType();
            case RECORD: {
                final List<Field> avroFields = avroSchema.getFields();
                final List<RecordField> recordFields = new ArrayList<>(avroFields.size());

                for (final Field field : avroFields) {
                    final String fieldName = field.name();
                    final Schema fieldSchema = field.schema();
                    final DataType fieldType = determineDataType(fieldSchema);

                    recordFields.add(new RecordField(fieldName, fieldType, field.defaultVal(), field.aliases()));
                }

                final RecordSchema recordSchema = new SimpleRecordSchema(recordFields, avroSchema.toString(), "avro", SchemaIdentifier.EMPTY);
                return RecordFieldType.RECORD.getRecordDataType(recordSchema);
            }
            case NULL:
                return RecordFieldType.STRING.getDataType();
            case MAP:
                final Schema valueSchema = avroSchema.getValueType();
                final DataType valueType = determineDataType(valueSchema);
                return RecordFieldType.MAP.getMapDataType(valueType);
            case UNION: {
                final List<Schema> nonNullSubSchemas = avroSchema.getTypes().stream()
                    .filter(s -> s.getType() != Type.NULL)
                    .collect(Collectors.toList());

                if (nonNullSubSchemas.size() == 1) {
                    return determineDataType(nonNullSubSchemas.get(0));
                }

                final List<DataType> possibleChildTypes = new ArrayList<>(nonNullSubSchemas.size());
                for (final Schema subSchema : nonNullSubSchemas) {
                    final DataType childDataType = determineDataType(subSchema);
                    possibleChildTypes.add(childDataType);
                }

                return RecordFieldType.CHOICE.getChoiceDataType(possibleChildTypes);
            }
        }

        return null;
    }

    @Override
    public Set<SchemaField> getSuppliedSchemaFields() {
        return schemaFields;
    }
}
