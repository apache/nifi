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
package org.apache.nifi.processors.iceberg.appender.avro;

import com.google.common.base.Preconditions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.io.Encoder;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.avro.MetricsAwareDatumWriter;
import org.apache.iceberg.avro.ValueWriter;
import org.apache.iceberg.avro.ValueWriters;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.RecordDataType;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * This class acts as an adaptor from an AvroFileAppender to a FileAppender&lt;Record&gt;.
 */
public class IcebergAvroWriter implements MetricsAwareDatumWriter<Record> {

    private final RecordSchema recordSchema;
    private ValueWriter<Record> writer = null;

    public IcebergAvroWriter(RecordSchema recordSchema) {
        this.recordSchema = recordSchema;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void setSchema(Schema schema) {
        this.writer = (ValueWriter<Record>) AvroWithNifiSchemaVisitor.visit(new RecordDataType(recordSchema), schema, new WriteBuilder());
    }

    @Override
    public void write(Record datum, Encoder out) throws IOException {
        writer.write(datum, out);
    }

    @Override
    public Stream<FieldMetrics> metrics() {
        return writer.metrics();
    }

    private static class WriteBuilder extends AvroWithNifiSchemaVisitor<ValueWriter<?>> {

        @Override
        public ValueWriter<?> record(DataType structType, Schema record, List<String> names, List<ValueWriter<?>> fields) {
            Preconditions.checkArgument(structType instanceof RecordDataType, "Invalid struct: %s is not a struct", structType);
            return IcebergAvroValueWriters.row(fields,
                    IntStream.range(0, names.size())
                            .mapToObj(i -> ((RecordDataType) structType).getChildSchema().getField(i))
                            .collect(Collectors.toList()));
        }

        @Override
        public ValueWriter<?> union(DataType dataType, Schema union, List<ValueWriter<?>> options) {
            Preconditions.checkArgument(options.contains(ValueWriters.nulls()), "Cannot create writer for non-option union: %s", union);
            Preconditions.checkArgument(options.size() == 2, "Cannot create writer for non-option union: %s", union);
            if (union.getTypes().get(0).getType() == Schema.Type.NULL) {
                return ValueWriters.option(0, options.get(1));
            } else {
                return ValueWriters.option(1, options.get(0));
            }
        }

        @Override
        public ValueWriter<?> array(DataType arrayType, Schema array, ValueWriter<?> elementWriter) {
            return IcebergAvroValueWriters.array(elementWriter, arrayElementType(arrayType));
        }

        @Override
        public ValueWriter<?> map(DataType arrayType, Schema map, ValueWriter<?> valueReader) {
            return IcebergAvroValueWriters.map(ValueWriters.strings(), mapKeyType(arrayType), valueReader, mapValueType(arrayType));
        }

        @Override
        public ValueWriter<?> map(DataType arrayType, Schema map, ValueWriter<?> keyWriter, ValueWriter<?> valueWriter) {
            return IcebergAvroValueWriters.arrayMap(keyWriter, mapKeyType(arrayType), valueWriter, mapValueType(arrayType));
        }

        @Override
        public ValueWriter<?> primitive(DataType dataType, Schema primitive) {
            org.apache.avro.LogicalType logicalType = primitive.getLogicalType();
            if (logicalType != null) {
                switch (logicalType.getName()) {
                    case "date":
                        return IcebergAvroValueWriters.dates();
                    case "time-micros":
                        return IcebergAvroValueWriters.timeMicros();
                    case "timestamp-micros":
                        return IcebergAvroValueWriters.timestampMicros();
                    case "decimal":
                        LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) logicalType;
                        return IcebergAvroValueWriters.decimals(decimal.getPrecision(), decimal.getScale());
                    case "uuid":
                        return ValueWriters.uuids();
                    default:
                        throw new IllegalArgumentException("Unsupported logical type: " + logicalType);
                }
            }

            switch (primitive.getType()) {
                case NULL:
                    return ValueWriters.nulls();
                case BOOLEAN:
                    return ValueWriters.booleans();
                case INT:
                    switch (dataType.getFieldType()) {
                        case BYTE:
                            return ValueWriters.tinyints();
                        case SHORT:
                            return ValueWriters.shorts();
                        default:
                            return ValueWriters.ints();
                    }
                case LONG:
                    return ValueWriters.longs();
                case FLOAT:
                    return ValueWriters.floats();
                case DOUBLE:
                    return ValueWriters.doubles();
                case STRING:
                    return ValueWriters.strings();
                case FIXED:
                    return IcebergAvroValueWriters.fixed(primitive.getFixedSize());
                case BYTES:
                    return IcebergAvroValueWriters.bytes();
                default:
                    throw new IllegalArgumentException("Unsupported type: " + primitive);
            }
        }
    }
}
