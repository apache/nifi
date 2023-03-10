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

    public GenericRecord convert(Record record) {
        return converter.convert(record);
    }

    @SuppressWarnings("unchecked")
    public IcebergRecordConverter(Schema schema, RecordSchema recordSchema, FileFormat fileFormat) {
        this.converter = (DataConverter<Record, GenericRecord>) IcebergSchemaVisitor.visit(schema, new RecordDataType(recordSchema), fileFormat);
    }

    private static class IcebergSchemaVisitor extends SchemaWithPartnerVisitor<DataType, DataConverter<?, ?>> {

        public static DataConverter<?, ?> visit(Schema schema, RecordDataType recordDataType, FileFormat fileFormat) {
            return visit(schema, new RecordTypeWithFieldNameMapper(schema, recordDataType), new IcebergSchemaVisitor(), new IcebergPartnerAccessors(schema, fileFormat));
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
                        return new GenericDataConverters.SameTypeConverter();
                    case TIME:
                        return new GenericDataConverters.TimeConverter();
                    case TIMESTAMP:
                        final Types.TimestampType timestampType = (Types.TimestampType) type;
                        if (timestampType.shouldAdjustToUTC()) {
                            return new GenericDataConverters.TimestampWithTimezoneConverter();
                        }
                        return new GenericDataConverters.TimestampConverter();
                    case UUID:
                        final UUIDDataType uuidType = (UUIDDataType) dataType;
                        if (uuidType.getFileFormat() == FileFormat.PARQUET) {
                            return new GenericDataConverters.UUIDtoByteArrayConverter();
                        }
                        return new GenericDataConverters.SameTypeConverter();
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
                final Optional<RecordField> recordField = recordSchema.getField(mappedFieldName.get());
                converter.setSourceFieldName(recordField.get().getFieldName());
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

        IcebergPartnerAccessors(Schema schema, FileFormat fileFormat) {
            this.schema = schema;
            this.fileFormat = fileFormat;
        }

        @Override
        public DataType fieldPartner(DataType dataType, int fieldId, String name) {
            Validate.isTrue(dataType instanceof RecordTypeWithFieldNameMapper, String.format("Invalid record: %s is not a record", dataType));
            final RecordTypeWithFieldNameMapper recordType = (RecordTypeWithFieldNameMapper) dataType;

            final Optional<String> mappedFieldName = recordType.getNameMapping(name);
            Validate.isTrue(mappedFieldName.isPresent(), String.format("Cannot find field with name '%s' in the record schema", name));

            final Optional<RecordField> recordField = recordType.getChildSchema().getField(mappedFieldName.get());
            final RecordField field = recordField.get();

            // If the actual record contains a nested record then we need to create a RecordTypeWithFieldNameMapper wrapper object for it.
            if (field.getDataType() instanceof RecordDataType) {
                return new RecordTypeWithFieldNameMapper(new Schema(schema.findField(fieldId).type().asStructType().fields()), (RecordDataType) field.getDataType());
            }

            if (field.getDataType().getFieldType().equals(RecordFieldType.UUID)) {
                return new UUIDDataType(field.getDataType(), fileFormat);
            }

            return field.getDataType();
        }

        @Override
        public DataType mapKeyPartner(DataType dataType) {
            return RecordFieldType.STRING.getDataType();
        }

        @Override
        public DataType mapValuePartner(DataType dataType) {
            Validate.isTrue(dataType instanceof MapDataType, String.format("Invalid map: %s is not a map", dataType));
            final MapDataType mapType = (MapDataType) dataType;
            return mapType.getValueType();
        }

        @Override
        public DataType listElementPartner(DataType dataType) {
            Validate.isTrue(dataType instanceof ArrayDataType, String.format("Invalid array: %s is not an array", dataType));
            final ArrayDataType arrayType = (ArrayDataType) dataType;
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

}
