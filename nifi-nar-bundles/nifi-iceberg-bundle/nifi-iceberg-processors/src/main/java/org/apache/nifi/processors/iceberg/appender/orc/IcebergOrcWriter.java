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
package org.apache.nifi.processors.iceberg.appender.orc;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.orc.GenericOrcWriters;
import org.apache.iceberg.orc.OrcRowWriter;
import org.apache.iceberg.orc.OrcValueWriter;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

import java.util.Deque;
import java.util.List;
import java.util.stream.Stream;

/**
 * This class acts as an adaptor from an OrcFileAppender to a FileAppender&lt;Record&gt;.
 */
public class IcebergOrcWriter implements OrcRowWriter<Record> {

    private final IcebergOrcValueWriters.RowDataWriter writer;

    private IcebergOrcWriter(RecordSchema rowType, Schema iSchema) {
        this.writer = (IcebergOrcValueWriters.RowDataWriter) IcebergOrcSchemaVisitor.visit(rowType, iSchema, new WriteBuilder());
    }

    public static OrcRowWriter<Record> buildWriter(RecordSchema rowType, Schema iSchema) {
        return new IcebergOrcWriter(rowType, iSchema);
    }

    @Override
    public void write(Record row, VectorizedRowBatch output) {
        Preconditions.checkArgument(row != null, "value must not be null");
        writer.writeRow(row, output);
    }

    @Override
    public List<OrcValueWriter<?>> writers() {
        return writer.writers();
    }

    @Override
    public Stream<FieldMetrics<?>> metrics() {
        return writer.metrics();
    }

    private static class WriteBuilder extends IcebergOrcSchemaVisitor<OrcValueWriter<?>> {

        private final Deque<Integer> fieldIds = Lists.newLinkedList();

        private WriteBuilder() {
        }

        @Override
        public void beforeField(Types.NestedField field) {
            fieldIds.push(field.fieldId());
        }

        @Override
        public void afterField(Types.NestedField field) {
            fieldIds.pop();
        }

        @Override
        public OrcValueWriter<Record> record(List<OrcValueWriter<?>> results, List<RecordField> recordFields) {
            return IcebergOrcValueWriters.struct(results, recordFields);
        }

        @Override
        public OrcValueWriter<?> map(OrcValueWriter<?> key, OrcValueWriter<?> value, DataType keyType, DataType valueType) {
            return IcebergOrcValueWriters.map(key, value, keyType, valueType);
        }

        @Override
        public OrcValueWriter<?> list(OrcValueWriter<?> element, DataType elementType) {
            return IcebergOrcValueWriters.list(element, elementType);
        }

        @Override
        public OrcValueWriter<?> primitive(Type.PrimitiveType iPrimitive, DataType dataType) {
            switch (iPrimitive.typeId()) {
                case BOOLEAN:
                    return GenericOrcWriters.booleans();
                case INTEGER:
                    switch (dataType.getFieldType()) {
                        case BYTE:
                            return GenericOrcWriters.bytes();
                        case SHORT:
                            return GenericOrcWriters.shorts();
                    }
                    return GenericOrcWriters.ints();
                case LONG:
                    return GenericOrcWriters.longs();
                case FLOAT:
                    Preconditions.checkArgument(
                            fieldIds.peek() != null,
                            String.format("Cannot find field id for primitive field with type %s. This is likely because id "
                                    + "information is not properly pushed during schema visiting.", iPrimitive));
                    return GenericOrcWriters.floats(fieldIds.peek());
                case DOUBLE:
                    Preconditions.checkArgument(
                            fieldIds.peek() != null,
                            String.format("Cannot find field id for primitive field with type %s. This is likely because id "
                                    + "information is not properly pushed during schema visiting.", iPrimitive));
                    return GenericOrcWriters.doubles(fieldIds.peek());
                case DATE:
                    return IcebergOrcValueWriters.dates();
                case TIME:
                    return IcebergOrcValueWriters.times();
                case TIMESTAMP:
                    Types.TimestampType timestampType = (Types.TimestampType) iPrimitive;
                    if (timestampType.shouldAdjustToUTC()) {
                        return IcebergOrcValueWriters.timestampTzs();
                    } else {
                        return IcebergOrcValueWriters.timestamps();
                    }
                case STRING:
                    return GenericOrcWriters.strings();
                case UUID:
                    return IcebergOrcValueWriters.uuids();
                case FIXED:
                case BINARY:
                    return IcebergOrcValueWriters.byteArray();
                case DECIMAL:
                    Types.DecimalType decimalType = (Types.DecimalType) iPrimitive;
                    return IcebergOrcValueWriters.decimal(decimalType.precision(), decimalType.scale());
                default:
                    throw new IllegalArgumentException(String.format("Invalid iceberg type %s corresponding to DataType %s", iPrimitive, dataType));
            }
        }
    }
}

