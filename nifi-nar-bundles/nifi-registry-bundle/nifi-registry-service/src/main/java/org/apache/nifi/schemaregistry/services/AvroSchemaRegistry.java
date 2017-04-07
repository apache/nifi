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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

@Tags({"schema", "registry", "avro", "json", "csv"})
@CapabilityDescription("Provides a service for registering and accessing schemas. You can register a schema "
    + "as a dynamic property where 'name' represents the schema name and 'value' represents the textual "
    + "representation of the actual schema following the syntax and semantics of Avro's Schema format.")
public class AvroSchemaRegistry extends AbstractControllerService implements SchemaRegistry {

    private final Map<String, String> schemaNameToSchemaMap;

    private static final String LOGICAL_TYPE_DATE = "date";
    private static final String LOGICAL_TYPE_TIME_MILLIS = "time-millis";
    private static final String LOGICAL_TYPE_TIME_MICROS = "time-micros";
    private static final String LOGICAL_TYPE_TIMESTAMP_MILLIS = "timestamp-millis";
    private static final String LOGICAL_TYPE_TIMESTAMP_MICROS = "timestamp-micros";


    public AvroSchemaRegistry() {
        this.schemaNameToSchemaMap = new HashMap<>();
    }

    @OnEnabled
    public void enable(ConfigurationContext configuratiponContext) throws InitializationException {
        this.schemaNameToSchemaMap.putAll(configuratiponContext.getProperties().entrySet().stream()
            .filter(propEntry -> propEntry.getKey().isDynamic())
            .collect(Collectors.toMap(propEntry -> propEntry.getKey().getName(), propEntry -> propEntry.getValue())));
    }

    @Override
    public String retrieveSchemaText(String schemaName) {
        if (!this.schemaNameToSchemaMap.containsKey(schemaName)) {
            throw new IllegalArgumentException("Failed to find schema; Name: '" + schemaName + ".");
        } else {
            return this.schemaNameToSchemaMap.get(schemaName);
        }
    }

    @Override
    public String retrieveSchemaText(String schemaName, Map<String, String> attributes) {
        throw new UnsupportedOperationException("This version of schema registry does not "
            + "support this operation, since schemas are only identofied by name.");
    }

    @Override
    @OnDisabled
    public void close() throws Exception {
        this.schemaNameToSchemaMap.clear();
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
    public RecordSchema retrieveSchema(String schemaName) {
        final String schemaText = this.retrieveSchemaText(schemaName);
        final Schema schema = new Schema.Parser().parse(schemaText);
        return createRecordSchema(schema);
    }

    /**
     * Converts an Avro Schema to a RecordSchema
     *
     * @param avroSchema the Avro Schema to convert
     * @return the Corresponding Record Schema
     */
    private RecordSchema createRecordSchema(final Schema avroSchema) {
        final List<RecordField> recordFields = new ArrayList<>(avroSchema.getFields().size());
        for (final Field field : avroSchema.getFields()) {
            final String fieldName = field.name();
            final DataType dataType = determineDataType(field.schema());
            recordFields.add(new RecordField(fieldName, dataType));
        }

        final RecordSchema recordSchema = new SimpleRecordSchema(recordFields);
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
                    recordFields.add(new RecordField(fieldName, fieldType));
                }

                final RecordSchema recordSchema = new SimpleRecordSchema(recordFields);
                return RecordFieldType.RECORD.getRecordDataType(recordSchema);
            }
            case NULL:
            case MAP:
                return RecordFieldType.RECORD.getDataType();
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

    /*
     * For this implementation 'attributes' argument is ignored since the underlying storage mechanisms
     * is based strictly on key/value pairs. In other implementation additional attributes may play a role (e.g., version id,)
     */
    @Override
    public RecordSchema retrieveSchema(String schemaName, Map<String, String> attributes) {
        return this.retrieveSchema(schemaName);
    }
}
