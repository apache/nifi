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
package org.apache.nifi.processors.iceberg.appender.parquet;

import com.google.common.collect.Lists;
import org.apache.iceberg.parquet.ParquetValueWriter;
import org.apache.iceberg.parquet.ParquetValueWriters;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.DecimalDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.util.List;

import static org.apache.nifi.processors.iceberg.appender.parquet.IcebergParquetValueWriters.byteArrays;
import static org.apache.nifi.processors.iceberg.appender.parquet.IcebergParquetValueWriters.dates;
import static org.apache.nifi.processors.iceberg.appender.parquet.IcebergParquetValueWriters.decimalAsFixed;
import static org.apache.nifi.processors.iceberg.appender.parquet.IcebergParquetValueWriters.decimalAsInteger;
import static org.apache.nifi.processors.iceberg.appender.parquet.IcebergParquetValueWriters.decimalAsLong;
import static org.apache.nifi.processors.iceberg.appender.parquet.IcebergParquetValueWriters.ints;
import static org.apache.nifi.processors.iceberg.appender.parquet.IcebergParquetValueWriters.timeMicros;
import static org.apache.nifi.processors.iceberg.appender.parquet.IcebergParquetValueWriters.timestampMicros;
import static org.apache.nifi.processors.iceberg.appender.parquet.IcebergParquetValueWriters.uuids;

/**
 * This class acts as an adaptor from an ParquetFileAppender to a FileAppender&lt;Record&gt;.
 */
public class IcebergParquetWriter {

    private IcebergParquetWriter() {
    }

    @SuppressWarnings("unchecked")
    public static <T> ParquetValueWriter<T> buildWriter(RecordSchema recordSchema, MessageType type) {
        return (ParquetValueWriter<T>) ParquetWithNiFiSchemaVisitor.visit(new RecordDataType(recordSchema), type, new WriteBuilder(type));
    }

    private static class WriteBuilder extends ParquetWithNiFiSchemaVisitor<ParquetValueWriter<?>> {

        private final MessageType type;

        WriteBuilder(MessageType type) {
            this.type = type;
        }

        @Override
        public ParquetValueWriter<?> message(RecordDataType recordType, MessageType message, List<ParquetValueWriter<?>> fieldWriters) {
            return struct(recordType, message.asGroupType(), fieldWriters);
        }

        @Override
        public ParquetValueWriter<?> struct(RecordDataType recordType, GroupType struct, List<ParquetValueWriter<?>> fieldWriters) {
            List<Type> fields = struct.getFields();
            List<RecordField> recordFields = recordType.getChildSchema().getFields();
            List<ParquetValueWriter<?>> writers = Lists.newArrayListWithExpectedSize(fieldWriters.size());
            for (int i = 0; i < fields.size(); i += 1) {
                writers.add(newOption(struct.getType(i), fieldWriters.get(i)));
            }

            return new IcebergParquetValueWriters.RecordWriter(writers, recordFields);
        }

        @Override
        public ParquetValueWriter<?> list(ArrayDataType listType, GroupType array, ParquetValueWriter<?> elementWriter) {
            GroupType repeated = array.getFields().get(0).asGroupType();
            String[] repeatedPath = currentPath();

            int repeatedD = type.getMaxDefinitionLevel(repeatedPath);
            int repeatedR = type.getMaxRepetitionLevel(repeatedPath);

            return new IcebergParquetValueWriters.ArrayDataWriter<>(repeatedD, repeatedR, newOption(repeated.getType(0), elementWriter), listType.getElementType());
        }

        @Override
        public ParquetValueWriter<?> map(MapDataType mapType, GroupType map, ParquetValueWriter<?> keyWriter, ParquetValueWriter<?> valueWriter) {
            GroupType repeatedKeyValue = map.getFields().get(0).asGroupType();
            String[] repeatedPath = currentPath();

            int repeatedD = type.getMaxDefinitionLevel(repeatedPath);
            int repeatedR = type.getMaxRepetitionLevel(repeatedPath);

            return new IcebergParquetValueWriters.MapDataWriter<>(repeatedD, repeatedR, newOption(repeatedKeyValue.getType(0), keyWriter),
                    newOption(repeatedKeyValue.getType(1), valueWriter), RecordFieldType.STRING.getDataType(), mapType.getValueType());
        }

        private ParquetValueWriter<?> newOption(Type fieldType, ParquetValueWriter<?> writer) {
            int maxD = type.getMaxDefinitionLevel(path(fieldType.getName()));
            return ParquetValueWriters.option(fieldType, maxD, writer);
        }

        @Override
        public ParquetValueWriter<?> primitive(DataType dataType, PrimitiveType primitive) {
            ColumnDescriptor desc = type.getColumnDescription(currentPath());

            if (primitive.getOriginalType() != null) {
                switch (primitive.getOriginalType()) {
                    case ENUM:
                    case JSON:
                    case UTF8:
                        return ParquetValueWriters.strings(desc);
                    case DATE:
                        return dates(desc);
                    case INT_8:
                    case INT_16:
                    case INT_32:
                        return ints(dataType, desc);
                    case INT_64:
                        return ParquetValueWriters.longs(desc);
                    case TIME_MICROS:
                        return timeMicros(desc);
                    case TIMESTAMP_MICROS:
                        return timestampMicros(desc);
                    case DECIMAL:
                        DecimalDataType decimalType = (DecimalDataType) dataType;
                        switch (primitive.getPrimitiveTypeName()) {
                            case INT32:
                                return decimalAsInteger(desc, decimalType.getPrecision(), decimalType.getScale());
                            case INT64:
                                return decimalAsLong(desc, decimalType.getPrecision(), decimalType.getScale());
                            case BINARY:
                            case FIXED_LEN_BYTE_ARRAY:
                                return decimalAsFixed(desc, decimalType.getPrecision(), decimalType.getScale());
                            default:
                                throw new UnsupportedOperationException("Unsupported base type for decimal: " + primitive.getPrimitiveTypeName());
                        }
                    case BSON:
                        return byteArrays(desc);
                    default:
                        throw new UnsupportedOperationException("Unsupported logical type: " + primitive.getOriginalType());
                }
            }

            switch (primitive.getPrimitiveTypeName()) {
                case FIXED_LEN_BYTE_ARRAY:
                case BINARY:
                    if (dataType.getFieldType().equals(RecordFieldType.UUID)) {
                        return uuids(desc);
                    }
                    return byteArrays(desc);
                case BOOLEAN:
                    return ParquetValueWriters.booleans(desc);
                case INT32:
                    return ints(dataType, desc);
                case INT64:
                    return ParquetValueWriters.longs(desc);
                case FLOAT:
                    return ParquetValueWriters.floats(desc);
                case DOUBLE:
                    return ParquetValueWriters.doubles(desc);
                default:
                    throw new UnsupportedOperationException("Unsupported type: " + primitive);
            }
        }
    }

}
