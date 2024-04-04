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

package org.apache.nifi.queryrecord;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.sql.ArrayType;
import org.apache.nifi.sql.ColumnSchema;
import org.apache.nifi.sql.ColumnType;
import org.apache.nifi.sql.MapType;
import org.apache.nifi.sql.NiFiTableSchema;
import org.apache.nifi.sql.ResettableDataSource;
import org.apache.nifi.sql.RowStream;
import org.apache.nifi.sql.ScalarType;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class RecordDataSource implements ResettableDataSource {
    private final NiFiTableSchema tableSchema;
    private final ProcessSession session;
    private final FlowFile flowFile;
    private final RecordReaderFactory readerFactory;
    private final ComponentLog logger;


    public RecordDataSource(final RecordSchema recordSchema, final ProcessSession session, final FlowFile flowFile, final RecordReaderFactory recordReaderFactory, final ComponentLog logger) {
        this.tableSchema = createTableSchema(recordSchema);
        this.session = session;
        this.flowFile = flowFile;
        this.readerFactory = recordReaderFactory;
        this.logger = logger;
    }

    @Override
    public NiFiTableSchema getSchema() {
        return tableSchema;
    }

    @Override
    public RowStream reset() throws IOException {
        final InputStream in = session.read(flowFile);
        final RecordReader reader;
        try {
            reader = readerFactory.createRecordReader(flowFile, in, logger);
        } catch (final Exception e) {
            in.close();
            throw new IOException(e);
        }

        final RecordSet recordSet = reader.createRecordSet();

        return new RowStream() {
            @Override
            public void close() throws IOException {
                reader.close();
            }

            @Override
            public Object[] nextRow() throws IOException {
                final Record record = recordSet.next();
                return record == null ? null : record.getValues();
            }
        };
    }

    public static NiFiTableSchema createTableSchema(final RecordSchema recordSchema) {
        final List<ColumnSchema> columns = new ArrayList<>();

        for (final RecordField field : recordSchema.getFields()) {
            final String columnName = field.getFieldName();
            final ColumnType columnType = getColumnType(field.getDataType());
            final boolean nullable = field.isNullable();
            columns.add(new ColumnSchema(columnName, columnType, nullable));
        }

        return new NiFiTableSchema(columns);
    }

    public static ColumnType getColumnType(final DataType fieldType) {
        return switch (fieldType.getFieldType()) {
            case BOOLEAN -> ScalarType.BOOLEAN;
            case BYTE -> ScalarType.BYTE;
            case UUID -> ScalarType.UUID;
            case CHAR -> ScalarType.CHARACTER;
            case DATE -> ScalarType.DATE;
            case DOUBLE -> ScalarType.DOUBLE;
            case FLOAT -> ScalarType.FLOAT;
            case INT -> ScalarType.INTEGER;
            case SHORT -> ScalarType.SHORT;
            case TIME -> ScalarType.TIME;
            case TIMESTAMP -> ScalarType.TIMESTAMP;
            case LONG -> ScalarType.LONG;
            case STRING, ENUM -> ScalarType.STRING;
            case ARRAY -> new ArrayType(getColumnType(((ArrayDataType) fieldType).getElementType()));
            case RECORD -> new ScalarType(Record.class);
            case MAP -> {
                final MapDataType mapDataType = (MapDataType) fieldType;
                yield new MapType(ScalarType.STRING, getColumnType(mapDataType.getValueType()));
            }
            case BIGINT -> ScalarType.BIGINT;
            case DECIMAL -> ScalarType.DECIMAL;
            case CHOICE -> getChoiceColumnType(fieldType);
        };
    }

    private static ColumnType getChoiceColumnType(final DataType fieldType) {
        final ChoiceDataType choiceDataType = (ChoiceDataType) fieldType;
        DataType widestDataType = choiceDataType.getPossibleSubTypes().get(0);
        for (final DataType possibleType : choiceDataType.getPossibleSubTypes()) {
            if (possibleType == widestDataType) {
                continue;
            }
            if (possibleType.getFieldType().isWiderThan(widestDataType.getFieldType())) {
                widestDataType = possibleType;
                continue;
            }
            if (widestDataType.getFieldType().isWiderThan(possibleType.getFieldType())) {
                continue;
            }

            // Neither is wider than the other.
            widestDataType = null;
            break;
        }

        // If one of the CHOICE data types is the widest, use it.
        if (widestDataType != null) {
            return getColumnType(widestDataType);
        }

        // None of the data types is strictly the widest. Check if all data types are numeric.
        // This would happen, for instance, if the data type is a choice between float and integer.
        // If that is the case, we can use a String type for the table schema because all values will fit
        // into a String. This will still allow for casting, etc. if the query requires it.
        boolean allNumeric = true;
        for (final DataType possibleType : choiceDataType.getPossibleSubTypes()) {
            if (!isNumeric(possibleType)) {
                allNumeric = false;
                break;
            }
        }

        if (allNumeric) {
            return ScalarType.STRING;
        }

        // There is no specific type that we can use for the schema. This would happen, for instance, if our
        // CHOICE is between an integer and a Record.
        return ScalarType.OBJECT;
    }


    private static boolean isNumeric(final DataType dataType) {
        return switch (dataType.getFieldType()) {
            case BIGINT, BYTE, DECIMAL, DOUBLE, FLOAT, INT, LONG, SHORT -> true;
            default -> false;
        };
    }
}
