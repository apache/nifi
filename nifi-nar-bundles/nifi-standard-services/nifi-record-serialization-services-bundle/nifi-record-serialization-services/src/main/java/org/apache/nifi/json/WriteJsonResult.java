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
import java.sql.SQLException;
import java.text.DateFormat;
import java.util.Map;
import java.util.Optional;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaAccessWriter;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.serialization.record.SerializedForm;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.stream.io.NonCloseableOutputStream;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;

public class WriteJsonResult implements RecordSetWriter {
    private final ComponentLog logger;
    private final boolean prettyPrint;
    private final SchemaAccessWriter schemaAccess;
    private final RecordSchema recordSchema;
    private final JsonFactory factory = new JsonFactory();
    private final DateFormat dateFormat;
    private final DateFormat timeFormat;
    private final DateFormat timestampFormat;

    public WriteJsonResult(final ComponentLog logger, final RecordSchema recordSchema, final SchemaAccessWriter schemaAccess, final boolean prettyPrint,
        final String dateFormat, final String timeFormat, final String timestampFormat) {

        this.logger = logger;
        this.recordSchema = recordSchema;
        this.prettyPrint = prettyPrint;
        this.schemaAccess = schemaAccess;

        this.dateFormat = dateFormat == null ? null : DataTypeUtils.getDateFormat(dateFormat);
        this.timeFormat = timeFormat == null ? null : DataTypeUtils.getDateFormat(timeFormat);
        this.timestampFormat = timestampFormat == null ? null : DataTypeUtils.getDateFormat(timestampFormat);
    }

    @Override
    public WriteResult write(final RecordSet rs, final OutputStream rawOut) throws IOException {
        int count = 0;

        schemaAccess.writeHeader(recordSchema, rawOut);

        try (final JsonGenerator generator = factory.createJsonGenerator(new NonCloseableOutputStream(rawOut))) {
            if (prettyPrint) {
                generator.useDefaultPrettyPrinter();
            }

            generator.writeStartArray();

            Record record;
            while ((record = rs.next()) != null) {
                count++;
                writeRecord(record, recordSchema, generator, g -> g.writeStartObject(), g -> g.writeEndObject());
            }

            generator.writeEndArray();
        } catch (final SQLException e) {
            throw new IOException("Failed to serialize Result Set to stream", e);
        }

        return WriteResult.of(count, schemaAccess.getAttributes(recordSchema));
    }

    @Override
    public WriteResult write(final Record record, final OutputStream rawOut) throws IOException {
        schemaAccess.writeHeader(recordSchema, rawOut);

        try (final JsonGenerator generator = factory.createJsonGenerator(new NonCloseableOutputStream(rawOut))) {
            if (prettyPrint) {
                generator.useDefaultPrettyPrinter();
            }

            writeRecord(record, recordSchema, generator, g -> g.writeStartObject(), g -> g.writeEndObject());
        } catch (final SQLException e) {
            throw new IOException("Failed to write records to stream", e);
        }

        return WriteResult.of(1, schemaAccess.getAttributes(recordSchema));
    }

    private void writeRecord(final Record record, final RecordSchema writeSchema, final JsonGenerator generator, final GeneratorTask startTask, final GeneratorTask endTask)
        throws JsonGenerationException, IOException, SQLException {

        final Optional<SerializedForm> serializedForm = record.getSerializedForm();
        if (serializedForm.isPresent()) {
            final SerializedForm form = serializedForm.get();
            if (form.getMimeType().equals(getMimeType()) && record.getSchema().equals(writeSchema)) {
                final Object serialized = form.getSerialized();
                if (serialized instanceof String) {
                    generator.writeRawValue((String) serialized);
                    return;
                }
            }
        }

        try {
            startTask.apply(generator);
            for (int i = 0; i < writeSchema.getFieldCount(); i++) {
                final RecordField field = writeSchema.getField(i);
                final String fieldName = field.getFieldName();
                final Object value = record.getValue(field);
                if (value == null) {
                    generator.writeNullField(fieldName);
                    continue;
                }

                generator.writeFieldName(fieldName);
                final DataType dataType = writeSchema.getDataType(fieldName).get();

                writeValue(generator, value, fieldName, dataType, i < writeSchema.getFieldCount() - 1);
            }

            endTask.apply(generator);
        } catch (final Exception e) {
            logger.error("Failed to write {} with schema {} as a JSON Object due to {}", new Object[] {record, record.getSchema(), e.toString(), e});
            throw e;
        }
    }


    @SuppressWarnings("unchecked")
    private void writeValue(final JsonGenerator generator, final Object value, final String fieldName, final DataType dataType, final boolean moreCols)
        throws JsonGenerationException, IOException, SQLException {
        if (value == null) {
            generator.writeNull();
            return;
        }

        final DataType chosenDataType = dataType.getFieldType() == RecordFieldType.CHOICE ? DataTypeUtils.chooseDataType(value, (ChoiceDataType) dataType) : dataType;
        final Object coercedValue = DataTypeUtils.convertType(value, chosenDataType, () -> dateFormat, () -> timeFormat, () -> timestampFormat, fieldName);
        if (coercedValue == null) {
            generator.writeNull();
            return;
        }

        switch (chosenDataType.getFieldType()) {
            case DATE: {
                final String stringValue = DataTypeUtils.toString(coercedValue, () -> dateFormat);
                if (DataTypeUtils.isLongTypeCompatible(stringValue)) {
                    generator.writeNumber(DataTypeUtils.toLong(coercedValue, fieldName));
                } else {
                    generator.writeString(stringValue);
                }
                break;
            }
            case TIME: {
                final String stringValue = DataTypeUtils.toString(coercedValue, () -> timeFormat);
                if (DataTypeUtils.isLongTypeCompatible(stringValue)) {
                    generator.writeNumber(DataTypeUtils.toLong(coercedValue, fieldName));
                } else {
                    generator.writeString(stringValue);
                }
                break;
            }
            case TIMESTAMP: {
                final String stringValue = DataTypeUtils.toString(coercedValue, () -> timestampFormat);
                if (DataTypeUtils.isLongTypeCompatible(stringValue)) {
                    generator.writeNumber(DataTypeUtils.toLong(coercedValue, fieldName));
                } else {
                    generator.writeString(stringValue);
                }
                break;
            }
            case DOUBLE:
                generator.writeNumber(DataTypeUtils.toDouble(coercedValue, fieldName));
                break;
            case FLOAT:
                generator.writeNumber(DataTypeUtils.toFloat(coercedValue, fieldName));
                break;
            case LONG:
                generator.writeNumber(DataTypeUtils.toLong(coercedValue, fieldName));
                break;
            case INT:
            case BYTE:
            case SHORT:
                generator.writeNumber(DataTypeUtils.toInteger(coercedValue, fieldName));
                break;
            case CHAR:
            case STRING:
                generator.writeString(coercedValue.toString());
                break;
            case BIGINT:
                if (coercedValue instanceof Long) {
                    generator.writeNumber(((Long) coercedValue).longValue());
                } else {
                    generator.writeNumber((BigInteger) coercedValue);
                }
                break;
            case BOOLEAN:
                final String stringValue = coercedValue.toString();
                if ("true".equalsIgnoreCase(stringValue)) {
                    generator.writeBoolean(true);
                } else if ("false".equalsIgnoreCase(stringValue)) {
                    generator.writeBoolean(false);
                } else {
                    generator.writeString(stringValue);
                }
                break;
            case RECORD: {
                final Record record = (Record) coercedValue;
                final RecordDataType recordDataType = (RecordDataType) chosenDataType;
                final RecordSchema childSchema = recordDataType.getChildSchema();
                writeRecord(record, childSchema, generator, gen -> gen.writeStartObject(), gen -> gen.writeEndObject());
                break;
            }
            case MAP: {
                final MapDataType mapDataType = (MapDataType) chosenDataType;
                final DataType valueDataType = mapDataType.getValueType();
                final Map<String, ?> map = (Map<String, ?>) coercedValue;
                generator.writeStartObject();
                int i = 0;
                for (final Map.Entry<String, ?> entry : map.entrySet()) {
                    final String mapKey = entry.getKey();
                    final Object mapValue = entry.getValue();
                    generator.writeFieldName(mapKey);
                    writeValue(generator, mapValue, fieldName + "." + mapKey, valueDataType, ++i < map.size());
                }
                generator.writeEndObject();
                break;
            }
            case ARRAY:
            default:
                if (coercedValue instanceof Object[]) {
                    final Object[] values = (Object[]) coercedValue;
                    final ArrayDataType arrayDataType = (ArrayDataType) dataType;
                    final DataType elementType = arrayDataType.getElementType();
                    writeArray(values, fieldName, generator, elementType);
                } else {
                    generator.writeString(coercedValue.toString());
                }
                break;
        }
    }

    private void writeArray(final Object[] values, final String fieldName, final JsonGenerator generator, final DataType elementType)
        throws JsonGenerationException, IOException, SQLException {
        generator.writeStartArray();
        for (int i = 0; i < values.length; i++) {
            final boolean moreEntries = i < values.length - 1;
            final Object element = values[i];
            writeValue(generator, element, fieldName, elementType, moreEntries);
        }
        generator.writeEndArray();
    }


    @Override
    public String getMimeType() {
        return "application/json";
    }

    private static interface GeneratorTask {
        void apply(JsonGenerator generator) throws JsonGenerationException, IOException;
    }
}
