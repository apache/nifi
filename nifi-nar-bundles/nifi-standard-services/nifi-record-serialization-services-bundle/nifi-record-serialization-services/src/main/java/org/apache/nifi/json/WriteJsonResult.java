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
import java.text.DateFormat;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaAccessWriter;
import org.apache.nifi.serialization.AbstractRecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RawRecordWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SerializedForm;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;

public class WriteJsonResult extends AbstractRecordSetWriter implements RecordSetWriter, RawRecordWriter {
    private final ComponentLog logger;
    private final SchemaAccessWriter schemaAccess;
    private final RecordSchema recordSchema;
    private final JsonFactory factory = new JsonFactory();
    private final JsonGenerator generator;
    private final NullSuppression nullSuppression;
    private final Supplier<DateFormat> LAZY_DATE_FORMAT;
    private final Supplier<DateFormat> LAZY_TIME_FORMAT;
    private final Supplier<DateFormat> LAZY_TIMESTAMP_FORMAT;

    public WriteJsonResult(final ComponentLog logger, final RecordSchema recordSchema, final SchemaAccessWriter schemaAccess, final OutputStream out, final boolean prettyPrint,
        final NullSuppression nullSuppression, final String dateFormat, final String timeFormat, final String timestampFormat) throws IOException {

        super(out);
        this.logger = logger;
        this.recordSchema = recordSchema;
        this.schemaAccess = schemaAccess;
        this.nullSuppression = nullSuppression;

        final DateFormat df = dateFormat == null ? null : DataTypeUtils.getDateFormat(dateFormat);
        final DateFormat tf = timeFormat == null ? null : DataTypeUtils.getDateFormat(timeFormat);
        final DateFormat tsf = timestampFormat == null ? null : DataTypeUtils.getDateFormat(timestampFormat);

        LAZY_DATE_FORMAT = () -> df;
        LAZY_TIME_FORMAT = () -> tf;
        LAZY_TIMESTAMP_FORMAT = () -> tsf;

        this.generator = factory.createJsonGenerator(out);
        if (prettyPrint) {
            generator.useDefaultPrettyPrinter();
        }
    }


    @Override
    protected void onBeginRecordSet() throws IOException {
        final OutputStream out = getOutputStream();
        schemaAccess.writeHeader(recordSchema, out);

        generator.writeStartArray();
    }

    @Override
    protected Map<String, String> onFinishRecordSet() throws IOException {
        generator.writeEndArray();
        return schemaAccess.getAttributes(recordSchema);
    }

    @Override
    public void close() throws IOException {
        if (generator != null) {
            generator.close();
        }

        super.close();
    }

    @Override
    public void flush() throws IOException {
        if (generator != null) {
            generator.flush();
        }
    }


    @Override
    public Map<String, String> writeRecord(final Record record) throws IOException {
        // If we are not writing an active record set, then we need to ensure that we write the
        // schema information.
        if (!isActiveRecordSet()) {
            generator.flush();
            schemaAccess.writeHeader(recordSchema, getOutputStream());
        }

        writeRecord(record, recordSchema, generator, g -> g.writeStartObject(), g -> g.writeEndObject(), true);
        return schemaAccess.getAttributes(recordSchema);
    }

    @Override
    public WriteResult writeRawRecord(final Record record) throws IOException {
        // If we are not writing an active record set, then we need to ensure that we write the
        // schema information.
        if (!isActiveRecordSet()) {
            generator.flush();
            schemaAccess.writeHeader(recordSchema, getOutputStream());
        }

        writeRecord(record, recordSchema, generator, g -> g.writeStartObject(), g -> g.writeEndObject(), false);
        return WriteResult.of(incrementRecordCount(), schemaAccess.getAttributes(recordSchema));
    }

    private void writeRecord(final Record record, final RecordSchema writeSchema, final JsonGenerator generator,
        final GeneratorTask startTask, final GeneratorTask endTask, final boolean schemaAware) throws JsonGenerationException, IOException {

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

            if (schemaAware) {
                for (final RecordField field : writeSchema.getFields()) {
                    final String fieldName = field.getFieldName();
                    final Object value = record.getValue(field);
                    if (value == null) {
                        if (nullSuppression == NullSuppression.NEVER_SUPPRESS || (nullSuppression == NullSuppression.SUPPRESS_MISSING) && isFieldPresent(field, record)) {
                            generator.writeNullField(fieldName);
                        }

                        continue;
                    }

                    generator.writeFieldName(fieldName);

                    final DataType dataType = writeSchema.getDataType(fieldName).get();
                    writeValue(generator, value, fieldName, dataType);
                }
            } else {
                for (final String fieldName : record.getRawFieldNames()) {
                    final Object value = record.getValue(fieldName);
                    if (value == null) {
                        if (nullSuppression == NullSuppression.NEVER_SUPPRESS || (nullSuppression == NullSuppression.SUPPRESS_MISSING) && record.getRawFieldNames().contains(fieldName)) {
                            generator.writeNullField(fieldName);
                        }

                        continue;
                    }

                    generator.writeFieldName(fieldName);
                    writeRawValue(generator, value, fieldName);
                }
            }



            endTask.apply(generator);
        } catch (final Exception e) {
            logger.error("Failed to write {} with schema {} as a JSON Object due to {}", new Object[] {record, record.getSchema(), e.toString(), e});
            throw e;
        }
    }

    private boolean isFieldPresent(final RecordField field, final Record record) {
        final Set<String> rawFieldNames = record.getRawFieldNames();
        if (rawFieldNames.contains(field.getFieldName())) {
            return true;
        }

        for (final String alias : field.getAliases()) {
            if (rawFieldNames.contains(alias)) {
                return true;
            }
        }

        return false;
    }

    @SuppressWarnings("unchecked")
    private void writeRawValue(final JsonGenerator generator, final Object value, final String fieldName)
        throws JsonGenerationException, IOException {

        if (value == null) {
            generator.writeNull();
            return;
        }

        if (value instanceof Record) {
            final Record record = (Record) value;
            writeRecord(record, record.getSchema(), generator, gen -> gen.writeStartObject(), gen -> gen.writeEndObject(), false);
            return;
        }

        if (value instanceof Map) {
            final Map<String, ?> map = (Map<String, ?>) value;
            generator.writeStartObject();

            for (final Map.Entry<String, ?> entry : map.entrySet()) {
                final String mapKey = entry.getKey();
                final Object mapValue = entry.getValue();
                generator.writeFieldName(mapKey);
                writeRawValue(generator, mapValue, fieldName + "." + mapKey);
            }

            generator.writeEndObject();
            return;
        }

        if (value instanceof Object[]) {
            final Object[] values = (Object[]) value;
            generator.writeStartArray();
            for (final Object element : values) {
                writeRawValue(generator, element, fieldName);
            }
            generator.writeEndArray();
            return;
        }

        generator.writeObject(value);
    }

    @SuppressWarnings("unchecked")
    private void writeValue(final JsonGenerator generator, final Object value, final String fieldName, final DataType dataType)
        throws JsonGenerationException, IOException {
        if (value == null) {
            generator.writeNull();
            return;
        }

        final DataType chosenDataType = dataType.getFieldType() == RecordFieldType.CHOICE ? DataTypeUtils.chooseDataType(value, (ChoiceDataType) dataType) : dataType;
        final Object coercedValue = DataTypeUtils.convertType(value, chosenDataType, LAZY_DATE_FORMAT, LAZY_TIME_FORMAT, LAZY_TIMESTAMP_FORMAT, fieldName);
        if (coercedValue == null) {
            generator.writeNull();
            return;
        }

        switch (chosenDataType.getFieldType()) {
            case DATE: {
                final String stringValue = DataTypeUtils.toString(coercedValue, LAZY_DATE_FORMAT);
                if (DataTypeUtils.isLongTypeCompatible(stringValue)) {
                    generator.writeNumber(DataTypeUtils.toLong(coercedValue, fieldName));
                } else {
                    generator.writeString(stringValue);
                }
                break;
            }
            case TIME: {
                final String stringValue = DataTypeUtils.toString(coercedValue, LAZY_TIME_FORMAT);
                if (DataTypeUtils.isLongTypeCompatible(stringValue)) {
                    generator.writeNumber(DataTypeUtils.toLong(coercedValue, fieldName));
                } else {
                    generator.writeString(stringValue);
                }
                break;
            }
            case TIMESTAMP: {
                final String stringValue = DataTypeUtils.toString(coercedValue, LAZY_TIMESTAMP_FORMAT);
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
                writeRecord(record, childSchema, generator, gen -> gen.writeStartObject(), gen -> gen.writeEndObject(), true);
                break;
            }
            case MAP: {
                final MapDataType mapDataType = (MapDataType) chosenDataType;
                final DataType valueDataType = mapDataType.getValueType();
                final Map<String, ?> map = (Map<String, ?>) coercedValue;
                generator.writeStartObject();

                for (final Map.Entry<String, ?> entry : map.entrySet()) {
                    final String mapKey = entry.getKey();
                    final Object mapValue = entry.getValue();
                    generator.writeFieldName(mapKey);
                    writeValue(generator, mapValue, fieldName + "." + mapKey, valueDataType);
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
        throws JsonGenerationException, IOException {
        generator.writeStartArray();
        for (int i = 0; i < values.length; i++) {
            final Object element = values[i];
            writeValue(generator, element, fieldName, elementType);
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
