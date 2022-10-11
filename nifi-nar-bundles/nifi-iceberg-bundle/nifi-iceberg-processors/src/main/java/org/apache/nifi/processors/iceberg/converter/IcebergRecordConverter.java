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

import java.util.List;
import java.util.Optional;

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
            return visit(schema, recordDataType, new IcebergSchemaVisitor(), new IcebergPartnerAccessors(fileFormat));
        }

        @Override
        public DataConverter<?, ?> schema(Schema schema, DataType dataType, DataConverter<?, ?> converter) {
            return converter;
        }

        @Override
        public DataConverter<?, ?> field(Types.NestedField field, DataType dataType, DataConverter<?, ?> converter) {
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
                        return GenericDataConverters.SameTypeConverter.INSTANCE;
                    case TIME:
                        return GenericDataConverters.TimeConverter.INSTANCE;
                    case TIMESTAMP:
                        final Types.TimestampType timestampType = (Types.TimestampType) type;
                        if (timestampType.shouldAdjustToUTC()) {
                            return GenericDataConverters.TimestampWithTimezoneConverter.INSTANCE;
                        }
                        return GenericDataConverters.TimestampConverter.INSTANCE;
                    case UUID:
                        final UUIDDataType uuidType = (UUIDDataType) dataType;
                        if (uuidType.getFileFormat() == FileFormat.PARQUET) {
                            return GenericDataConverters.UUIDtoByteArrayConverter.INSTANCE;
                        }
                        return GenericDataConverters.SameTypeConverter.INSTANCE;
                    case FIXED:
                        final Types.FixedType fixedType = (Types.FixedType) type;
                        return new GenericDataConverters.FixedConverter(fixedType.length());
                    case BINARY:
                        return GenericDataConverters.BinaryConverter.INSTANCE;
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
            final List<RecordField> recordFields = ((RecordDataType) dataType).getChildSchema().getFields();
            return new GenericDataConverters.RecordConverter(converters, recordFields, type);
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
        private final FileFormat fileFormat;

        IcebergPartnerAccessors(FileFormat fileFormat) {
            this.fileFormat = fileFormat;
        }

        @Override
        public DataType fieldPartner(DataType dataType, int fieldId, String name) {
            Validate.isTrue(dataType instanceof RecordDataType, String.format("Invalid record: %s is not a record", dataType));
            final RecordDataType recordType = (RecordDataType) dataType;
            final Optional<RecordField> recordField = recordType.getChildSchema().getField(name);

            Validate.isTrue(recordField.isPresent(), String.format("Cannot find record field with name %s", name));
            final RecordField field = recordField.get();

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
}
