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

package org.apache.nifi.json;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.sql.Array;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.DataTypeUtils;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.stream.io.NonCloseableOutputStream;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;

public class WriteJsonResult implements RecordSetWriter {
    private final boolean prettyPrint;

    private final ComponentLog logger;
    private final JsonFactory factory = new JsonFactory();
    private final DateFormat dateFormat;
    private final DateFormat timeFormat;
    private final DateFormat timestampFormat;

    public WriteJsonResult(final ComponentLog logger, final boolean prettyPrint, final String dateFormat, final String timeFormat, final String timestampFormat) {
        this.prettyPrint = prettyPrint;
        this.dateFormat = new SimpleDateFormat(dateFormat);
        this.timeFormat = new SimpleDateFormat(timeFormat);
        this.timestampFormat = new SimpleDateFormat(timestampFormat);
        this.logger = logger;
    }

    @Override
    public WriteResult write(final RecordSet rs, final OutputStream rawOut) throws IOException {
        int count = 0;

        try (final JsonGenerator generator = factory.createJsonGenerator(new NonCloseableOutputStream(rawOut))) {
            if (prettyPrint) {
                generator.useDefaultPrettyPrinter();
            }

            generator.writeStartArray();

            Record record;
            while ((record = rs.next()) != null) {
                count++;
                writeRecord(record, generator, g -> g.writeStartObject(), g -> g.writeEndObject());
            }

            generator.writeEndArray();
        } catch (final SQLException e) {
            throw new IOException("Failed to serialize Result Set to stream", e);
        }

        return WriteResult.of(count, Collections.emptyMap());
    }

    @Override
    public WriteResult write(final Record record, final OutputStream rawOut) throws IOException {
        try (final JsonGenerator generator = factory.createJsonGenerator(new NonCloseableOutputStream(rawOut))) {
            if (prettyPrint) {
                generator.useDefaultPrettyPrinter();
            }

            writeRecord(record, generator, g -> g.writeStartObject(), g -> g.writeEndObject());
        } catch (final SQLException e) {
            throw new IOException("Failed to write records to stream", e);
        }

        return WriteResult.of(1, Collections.emptyMap());
    }

    private void writeRecord(final Record record, final JsonGenerator generator, final GeneratorTask startTask, final GeneratorTask endTask)
        throws JsonGenerationException, IOException, SQLException {

        try {
            final RecordSchema schema = record.getSchema();
            startTask.apply(generator);
            for (int i = 0; i < schema.getFieldCount(); i++) {
                final String fieldName = schema.getField(i).getFieldName();
                final Object value = record.getValue(fieldName);
                if (value == null) {
                    generator.writeNullField(fieldName);
                    continue;
                }

                generator.writeFieldName(fieldName);
                final DataType dataType = schema.getDataType(fieldName).get();

                writeValue(generator, value, dataType, i < schema.getFieldCount() - 1);
            }

            endTask.apply(generator);
        } catch (final Exception e) {
            logger.error("Failed to write {} with schema {} as a JSON Object due to {}", new Object[] {record, record.getSchema(), e.toString(), e});
            throw e;
        }
    }

    private String createDate(final Object value, final DateFormat format) {
        if (value == null) {
            return null;
        }

        if (value instanceof Date) {
            return format.format((Date) value);
        }
        if (value instanceof java.sql.Date) {
            return format.format(new Date(((java.sql.Date) value).getTime()));
        }
        if (value instanceof java.sql.Time) {
            return format.format(new Date(((java.sql.Time) value).getTime()));
        }
        if (value instanceof java.sql.Timestamp) {
            return format.format(new Date(((java.sql.Timestamp) value).getTime()));
        }

        return null;
    }

    private void writeValue(final JsonGenerator generator, final Object value, final DataType dataType, final boolean moreCols)
        throws JsonGenerationException, IOException, SQLException {
        if (value == null) {
            generator.writeNull();
            return;
        }

        final DataType resolvedDataType;
        if (dataType.getFieldType() == RecordFieldType.CHOICE) {
            resolvedDataType = DataTypeUtils.inferDataType(value);
        } else {
            resolvedDataType = dataType;
        }

        switch (resolvedDataType.getFieldType()) {
            case DATE:
                generator.writeString(createDate(value, dateFormat));
                break;
            case TIME:
                generator.writeString(createDate(value, timeFormat));
                break;
            case TIMESTAMP:
                generator.writeString(createDate(value, timestampFormat));
                break;
            case DOUBLE:
                generator.writeNumber(DataTypeUtils.toDouble(value, 0D));
                break;
            case FLOAT:
                generator.writeNumber(DataTypeUtils.toFloat(value, 0F));
                break;
            case LONG:
                generator.writeNumber(DataTypeUtils.toLong(value, 0L));
                break;
            case INT:
            case BYTE:
            case SHORT:
                generator.writeNumber(DataTypeUtils.toInteger(value, 0));
                break;
            case CHAR:
            case STRING:
                generator.writeString(value.toString());
                break;
            case BIGINT:
                if (value instanceof Long) {
                    generator.writeNumber(((Long) value).longValue());
                } else {
                    generator.writeNumber((BigInteger) value);
                }
                break;
            case BOOLEAN:
                final String stringValue = value.toString();
                if ("true".equalsIgnoreCase(stringValue)) {
                    generator.writeBoolean(true);
                } else if ("false".equalsIgnoreCase(stringValue)) {
                    generator.writeBoolean(false);
                } else {
                    generator.writeString(stringValue);
                }
                break;
            case RECORD: {
                final Record record = (Record) value;
                writeRecord(record, generator, gen -> gen.writeStartObject(), gen -> gen.writeEndObject());
                break;
            }
            case ARRAY:
            default:
                if ("null".equals(value.toString())) {
                    generator.writeNull();
                } else if (value instanceof Map) {
                    final Map<?, ?> map = (Map<?, ?>) value;
                    generator.writeStartObject();

                    int i = 0;
                    for (final Map.Entry<?, ?> entry : map.entrySet()) {
                        generator.writeFieldName(entry.getKey().toString());
                        final boolean moreEntries = ++i < map.size();
                        writeValue(generator, entry.getValue(), getColType(entry.getValue()), moreEntries);
                    }
                    generator.writeEndObject();
                } else if (value instanceof List) {
                    final List<?> list = (List<?>) value;
                    writeArray(list.toArray(), generator);
                } else if (value instanceof Array) {
                    final Array array = (Array) value;
                    final Object[] values = (Object[]) array.getArray();
                    writeArray(values, generator);
                } else if (value instanceof Object[]) {
                    final Object[] values = (Object[]) value;
                    writeArray(values, generator);
                } else {
                    generator.writeString(value.toString());
                }
                break;
        }
    }

    private void writeArray(final Object[] values, final JsonGenerator generator) throws JsonGenerationException, IOException, SQLException {
        generator.writeStartArray();
        for (int i = 0; i < values.length; i++) {
            final boolean moreEntries = i < values.length - 1;
            final Object element = values[i];
            writeValue(generator, element, getColType(element), moreEntries);
        }
        generator.writeEndArray();
    }

    private DataType getColType(final Object value) {
        if (value instanceof String) {
            return RecordFieldType.STRING.getDataType();
        }
        if (value instanceof Double) {
            return RecordFieldType.DOUBLE.getDataType();
        }
        if (value instanceof Float) {
            return RecordFieldType.FLOAT.getDataType();
        }
        if (value instanceof Integer) {
            return RecordFieldType.INT.getDataType();
        }
        if (value instanceof Long) {
            return RecordFieldType.LONG.getDataType();
        }
        if (value instanceof BigInteger) {
            return RecordFieldType.BIGINT.getDataType();
        }
        if (value instanceof Boolean) {
            return RecordFieldType.BOOLEAN.getDataType();
        }
        if (value instanceof Byte || value instanceof Short) {
            return RecordFieldType.INT.getDataType();
        }
        if (value instanceof Character) {
            return RecordFieldType.STRING.getDataType();
        }
        if (value instanceof java.util.Date || value instanceof java.sql.Date) {
            return RecordFieldType.DATE.getDataType();
        }
        if (value instanceof java.sql.Time) {
            return RecordFieldType.TIME.getDataType();
        }
        if (value instanceof java.sql.Timestamp) {
            return RecordFieldType.TIMESTAMP.getDataType();
        }
        if (value instanceof Object[] || value instanceof List || value instanceof Array) {
            return RecordFieldType.ARRAY.getDataType();
        }

        return RecordFieldType.RECORD.getDataType();
    }

    @Override
    public String getMimeType() {
        return "application/json";
    }

    private static interface GeneratorTask {
        void apply(JsonGenerator generator) throws JsonGenerationException, IOException;
    }
}
