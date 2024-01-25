/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.processors.iceberg.converter;

import org.apache.commons.lang.Validate;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.schema.SchemaWithPartnerVisitor;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.iceberg.UnmatchedColumnBehavior;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * This class is responsible for schema traversal and data conversion between NiFi and Iceberg internal record structure.
 */
public class IcebergRecordConverter {

    private final DataConverter<Record, GenericRecord> converter;
    public final UnmatchedColumnBehavior unmatchedColumnBehavior;
    public ComponentLog logger;

    public GenericRecord convert(Record record) {
        return converter.convert(record);
    }


    @SuppressWarnings("unchecked")
    public IcebergRecordConverter(Schema schema, RecordSchema recordSchema, FileFormat fileFormat, UnmatchedColumnBehavior unmatchedColumnBehavior, ComponentLog logger) {
        this.converter = (DataConverter<Record, GenericRecord>) IcebergSchemaVisitor.visit(schema, new RecordDataType(recordSchema), fileFormat, unmatchedColumnBehavior, logger);
        this.unmatchedColumnBehavior = unmatchedColumnBehavior;
        this.logger = logger;
    }

    private static class IcebergSchemaVisitor extends SchemaWithPartnerVisitor<DataType, DataConverter<?, ?>> {

        public static DataConverter<?, ?> visit(Schema schema, RecordDataType recordDataType, FileFormat fileFormat, UnmatchedColumnBehavior unmatchedColumnBehavior, ComponentLog logger) {
            return visit(schema, new RecordTypeWithFieldNameMapper(schema, recordDataType), new IcebergSchemaVisitor(),
                    new IcebergPartnerAccessors(schema, fileFormat, unmatchedColumnBehavior, logger));
        }

        @Override
        public DataConverter<?, ?> schema(Schema schema, DataType dataType, DataConverter<?, ?> converter) {
            return converter;
        }

        @Override
        public DataConverter<?, ?> field(Types.NestedField field, DataType dataType, DataConverter<?, ?> converter) {
            // set Iceberg schema field name (targetFieldName) in the data converter
            converter.setTargetFieldName(field.name());
            return converter;
        }

        @Override
        public DataConverter<?, ?> primitive(Type.PrimitiveType type, DataType dataType) {
            if (type.typeId() != null) {
                switch (type.typeId()) {
                    case BOOLEAN:
                    case INTEGER:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                    case DATE:
                    case STRING:
                        return new GenericDataConverters.PrimitiveTypeConverter(type, dataType);
                    case TIME:
                        return new GenericDataConverters.TimeConverter(dataType.getFormat());
                    case TIMESTAMP:
                        final Types.TimestampType timestampType = (Types.TimestampType) type;
                        if (timestampType.shouldAdjustToUTC()) {
                            return new GenericDataConverters.TimestampWithTimezoneConverter(dataType);
                        }
                        return new GenericDataConverters.TimestampConverter(dataType);
                    case UUID:
                        final UUIDDataType uuidType = (UUIDDataType) dataType;
                        if (uuidType.getFileFormat() == FileFormat.PARQUET) {
                            return new GenericDataConverters.UUIDtoByteArrayConverter();
                        }
                        return new GenericDataConverters.PrimitiveTypeConverter(type, dataType);
                    case FIXED:
                        final Types.FixedType fixedType = (Types.FixedType) type;
                        return new GenericDataConverters.FixedConverter(fixedType.length());
                    case BINARY:
                        return new GenericDataConverters.BinaryConverter();
                    case DECIMAL:
                        final Types.DecimalType decimalType = (Types.DecimalType) type;
                        return new GenericDataConverters.BigDecimalConverter(decimalType.precision(), decimalType.scale());
                    default:
                        throw new UnsupportedOperationException("Unsupported type: " + type.typeId());
                }
            }
            throw new UnsupportedOperationException("Missing type id from PrimitiveType " + type);
        }

        @Override
        public DataConverter<?, ?> struct(Types.StructType type, DataType dataType, List<DataConverter<?, ?>> converters) {
            Validate.notNull(type, "Can not create reader for null type");
            final RecordTypeWithFieldNameMapper recordType = (RecordTypeWithFieldNameMapper) dataType;
            final RecordSchema recordSchema = recordType.getChildSchema();

            // set NiFi schema field names (sourceFieldName) in the data converters
            for (DataConverter<?, ?> converter : converters) {
                final Optional<String> mappedFieldName = recordType.getNameMapping(converter.getTargetFieldName());
                if (mappedFieldName.isPresent()) {
                    final Optional<RecordField> recordField = recordSchema.getField(mappedFieldName.get());
                    converter.setSourceFieldName(recordField.get().getFieldName());
                }
            }

            return new GenericDataConverters.RecordConverter(converters, recordSchema, type);
        }

        @Override
        public DataConverter<?, ?> list(Types.ListType listTypeInfo, DataType dataType, DataConverter<?, ?> converter) {
            return new GenericDataConverters.ArrayConverter<>(converter, ((ArrayDataType) dataType).getElementType());
        }

        @Override
        public DataConverter<?, ?> map(Types.MapType mapType, DataType dataType, DataConverter<?, ?> keyConverter, DataConverter<?, ?> valueConverter) {
            return new GenericDataConverters.MapConverter<>(keyConverter, RecordFieldType.STRING.getDataType(), valueConverter, ((MapDataType) dataType).getValueType());
        }
    }

    public static class IcebergPartnerAccessors implements SchemaWithPartnerVisitor.PartnerAccessors<DataType> {
        private final Schema schema;
        private final FileFormat fileFormat;
        private final UnmatchedColumnBehavior unmatchedColumnBehavior;
        private final ComponentLog logger;

        IcebergPartnerAccessors(Schema schema, FileFormat fileFormat, UnmatchedColumnBehavior unmatchedColumnBehavior, ComponentLog logger) {
            this.schema = schema;
            this.fileFormat = fileFormat;
            this.unmatchedColumnBehavior = unmatchedColumnBehavior;
            this.logger = logger;
        }

        @Override
        public DataType fieldPartner(DataType dataType, int fieldId, String name) {
            Validate.isTrue(dataType instanceof RecordTypeWithFieldNameMapper, String.format("Invalid record: %s is not a record", dataType));
            final RecordTypeWithFieldNameMapper recordType = (RecordTypeWithFieldNameMapper) dataType;

            final Optional<String> mappedFieldName = recordType.getNameMapping(name);
            if (UnmatchedColumnBehavior.FAIL_UNMATCHED_COLUMN.equals(unmatchedColumnBehavior)) {
                Validate.isTrue(mappedFieldName.isPresent(), String.format("Cannot find field with name '%s' in the record schema", name));
            }
            if (mappedFieldName.isEmpty()) {
                if (UnmatchedColumnBehavior.WARNING_UNMATCHED_COLUMN.equals(unmatchedColumnBehavior)) {
                    if (logger != null) {
                        logger.warn("Cannot find field with name '" + name + "' in the record schema, using the target schema for datatype and a null value");
                    }
                }
                // If the field is missing, use the expected type from the schema (converted to a DataType)
                final Types.NestedField schemaField = schema.findField(fieldId);
                final Type schemaFieldType = schemaField.type();
                if (schemaField.isRequired()) {
                    // Iceberg requires a non-null value for required fields
                    throw new IllegalArgumentException("Iceberg requires a non-null value for required fields, field: "
                            + schemaField.name() + ", type: " + schemaFieldType);
                }
                return GenericDataConverters.convertSchemaTypeToDataType(schemaFieldType);
            }
            final Optional<RecordField> recordField = recordType.getChildSchema().getField(mappedFieldName.get());
            final DataType fieldType = recordField.get().getDataType();

            // If the actual record contains a nested record then we need to create a RecordTypeWithFieldNameMapper wrapper object for it.
            if (fieldType instanceof RecordDataType) {
                return new RecordTypeWithFieldNameMapper(new Schema(schema.findField(fieldId).type().asStructType().fields()), (RecordDataType) fieldType);
            }

            // If the field is an Array, and it contains Records then add the record's iceberg schema for creating RecordTypeWithFieldNameMapper
            if (fieldType instanceof ArrayDataType && ((ArrayDataType) fieldType).getElementType() instanceof RecordDataType) {
                return new ArrayTypeWithIcebergSchema(
                        new Schema(schema.findField(fieldId).type().asListType().elementType().asStructType().fields()),
                        ((ArrayDataType) fieldType).getElementType()
                );
            }

            // If the field is a Map, and it's value field contains Records then add the record's iceberg schema for creating RecordTypeWithFieldNameMapper
            if (fieldType instanceof MapDataType && ((MapDataType) fieldType).getValueType() instanceof RecordDataType) {
                return new MapTypeWithIcebergSchema(
                        new Schema(schema.findField(fieldId).type().asMapType().valueType().asStructType().fields()),
                        ((MapDataType) fieldType).getValueType()
                );
            }

            // If the source field or target field is of type UUID, create a UUIDDataType from it
            if (fieldType.getFieldType().equals(RecordFieldType.UUID) || schema.findField(fieldId).type().typeId() == Type.TypeID.UUID) {
                return new UUIDDataType(fieldType, fileFormat);
            }

            return fieldType;
        }

        @Override
        public DataType mapKeyPartner(DataType dataType) {
            return RecordFieldType.STRING.getDataType();
        }

        @Override
        public DataType mapValuePartner(DataType dataType) {
            Validate.isTrue(dataType instanceof MapDataType, String.format("Invalid map: %s is not a map", dataType));
            final MapDataType mapType = (MapDataType) dataType;
            if (mapType instanceof MapTypeWithIcebergSchema) {
                MapTypeWithIcebergSchema typeWithSchema = (MapTypeWithIcebergSchema) mapType;
                return new RecordTypeWithFieldNameMapper(typeWithSchema.getValueSchema(), (RecordDataType) typeWithSchema.getValueType());
            }
            return mapType.getValueType();
        }

        @Override
        public DataType listElementPartner(DataType dataType) {
            Validate.isTrue(dataType instanceof ArrayDataType, String.format("Invalid array: %s is not an array", dataType));
            final ArrayDataType arrayType = (ArrayDataType) dataType;
            if (arrayType instanceof ArrayTypeWithIcebergSchema) {
                ArrayTypeWithIcebergSchema typeWithSchema = (ArrayTypeWithIcebergSchema) arrayType;
                return new RecordTypeWithFieldNameMapper(typeWithSchema.getElementSchema(), (RecordDataType) typeWithSchema.getElementType());
            }
            return arrayType.getElementType();
        }
    }

    /**
     * Parquet writer expects the UUID value in different format, so it needs to be converted differently: <a href="https://github.com/apache/iceberg/issues/1881">#1881</a>
     */
    private static class UUIDDataType extends DataType {

        private final FileFormat fileFormat;

        UUIDDataType(DataType dataType, FileFormat fileFormat) {
            super(dataType.getFieldType(), dataType.getFormat());
            this.fileFormat = fileFormat;
        }

        public FileFormat getFileFormat() {
            return fileFormat;
        }
    }

    /**
     * Since the {@link RecordSchema} stores the field name and value pairs in a HashMap it makes the retrieval case-sensitive, so we create a name mapper for case-insensitive handling.
     */
    private static class RecordTypeWithFieldNameMapper extends RecordDataType {

        private final Map<String, String> fieldNameMap;

        RecordTypeWithFieldNameMapper(Schema schema, RecordDataType recordType) {
            super(recordType.getChildSchema());

            // create a lowercase map for the NiFi record schema fields
            final Map<String, String> lowerCaseMap = recordType.getChildSchema().getFieldNames().stream()
                    .collect(Collectors.toMap(String::toLowerCase, s -> s));

            // map the Iceberg record schema fields to the NiFi record schema fields
            this.fieldNameMap = new HashMap<>();
            schema.columns().forEach((s) -> this.fieldNameMap.put(s.name(), lowerCaseMap.get(s.name().toLowerCase())));
        }

        Optional<String> getNameMapping(String name) {
            return Optional.ofNullable(fieldNameMap.get(name));
        }
    }

    /**
     * Data type for Arrays which contains Records. The class stores the iceberg schema for the element type.
     */
    private static class ArrayTypeWithIcebergSchema extends ArrayDataType {

        private final Schema elementSchema;

        public ArrayTypeWithIcebergSchema(Schema elementSchema, DataType elementType) {
            super(elementType);
            this.elementSchema = elementSchema;
        }

        public Schema getElementSchema() {
            return elementSchema;
        }
    }

    /**
     * Data type for Maps which contains Records in the entries value. The class stores the iceberg schema for the value type.
     */
    private static class MapTypeWithIcebergSchema extends MapDataType {

        private final Schema valueSchema;

        public MapTypeWithIcebergSchema(Schema valueSchema, DataType valueType) {
            super(valueType);
            this.valueSchema = valueSchema;
        }

        public Schema getValueSchema() {
            return valueSchema;
        }
    }

}
