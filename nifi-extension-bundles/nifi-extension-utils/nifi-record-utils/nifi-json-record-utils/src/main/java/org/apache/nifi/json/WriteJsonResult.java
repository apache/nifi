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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonGenerator.Feature;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.NullSuppression;
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
import org.apache.nifi.serialization.record.field.FieldConverter;
import org.apache.nifi.serialization.record.field.StandardFieldConverterRegistry;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

public class WriteJsonResult extends AbstractRecordSetWriter implements RecordSetWriter, RawRecordWriter {
    private static final FieldConverter<Object, String> STRING_FIELD_CONVERTER = StandardFieldConverterRegistry.getRegistry().getFieldConverter(String.class);
    private static final Pattern SCIENTIFIC_NOTATION_PATTERN = Pattern.compile("[0-9]([eE][-+]?)[0-9]");

    private final ComponentLog logger;
    private final SchemaAccessWriter schemaAccess;
    private final RecordSchema recordSchema;
    private final JsonGenerator generator;
    private final NullSuppression nullSuppression;
    private final OutputGrouping outputGrouping;
    private final String dateFormat;
    private final String timeFormat;
    private final String timestampFormat;
    private final String mimeType;
    private final boolean prettyPrint;
    private final boolean allowScientificNotation;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public WriteJsonResult(final ComponentLog logger, final RecordSchema recordSchema, final SchemaAccessWriter schemaAccess, final OutputStream out, final boolean prettyPrint,
            final NullSuppression nullSuppression, final OutputGrouping outputGrouping, final String dateFormat, final String timeFormat, final String timestampFormat) throws IOException {
        this(logger, recordSchema, schemaAccess, out, prettyPrint, nullSuppression, outputGrouping, dateFormat, timeFormat, timestampFormat, "application/json", false);
    }

    public WriteJsonResult(final ComponentLog logger, final RecordSchema recordSchema, final SchemaAccessWriter schemaAccess, final OutputStream out, final boolean prettyPrint,
        final NullSuppression nullSuppression, final OutputGrouping outputGrouping, final String dateFormat, final String timeFormat, final String timestampFormat,
        final String mimeType, final boolean allowScientificNotation) throws IOException {

        super(out);
        this.logger = logger;
        this.recordSchema = recordSchema;
        this.schemaAccess = schemaAccess;
        this.nullSuppression = nullSuppression;
        this.outputGrouping = outputGrouping;
        this.mimeType = mimeType;
        this.allowScientificNotation = allowScientificNotation;

        this.dateFormat = dateFormat;
        this.timeFormat = timeFormat;
        this.timestampFormat = timestampFormat;

        final JsonFactory factory = new JsonFactory();
        factory.setCodec(objectMapper);

        this.generator = factory.createGenerator(out);
        if (!allowScientificNotation) {
            generator.enable(Feature.WRITE_BIGDECIMAL_AS_PLAIN);
        }

        this.prettyPrint = prettyPrint;
        if (prettyPrint) {
            generator.useDefaultPrettyPrinter();
        } else if (OutputGrouping.OUTPUT_ONELINE.equals(outputGrouping)) {
            // Use a minimal pretty printer with a newline object separator, will output one JSON object per line
            generator.setPrettyPrinter(new MinimalPrettyPrinter("\n"));
        }
    }


    @Override
    protected void onBeginRecordSet() throws IOException {
        final OutputStream out = getOutputStream();
        schemaAccess.writeHeader(recordSchema, out);

        if (outputGrouping == OutputGrouping.OUTPUT_ARRAY) {
            generator.writeStartArray();
        }
    }

    @Override
    protected Map<String, String> onFinishRecordSet() throws IOException {
        if (outputGrouping == OutputGrouping.OUTPUT_ARRAY) {
            generator.writeEndArray();
        }
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

        final List<GeneratorTask> recordGenerationTasks = new LinkedList<>();
        writeRecord(record, recordSchema, recordGenerationTasks, JsonGenerator::writeStartObject, JsonGenerator::writeEndObject, true);
        for (final GeneratorTask task : recordGenerationTasks) {
            task.apply(generator);
        }
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

        final List<GeneratorTask> recordGenerationTasks = new LinkedList<>();
        writeRecord(record, recordSchema, recordGenerationTasks, JsonGenerator::writeStartObject, JsonGenerator::writeEndObject, false);
        for (final GeneratorTask task : recordGenerationTasks) {
            task.apply(generator);
        }
        final Map<String, String> attributes = schemaAccess.getAttributes(recordSchema);
        return WriteResult.of(incrementRecordCount(), attributes);
    }

    private boolean isUseSerializeForm(final Record record, final RecordSchema writeSchema) {
        final Optional<SerializedForm> serializedForm = record.getSerializedForm();
        if (serializedForm.isEmpty()) {
            return false;
        }

        final SerializedForm form = serializedForm.get();
        if (!form.getMimeType().equals(getMimeType()) || !record.getSchema().equals(writeSchema)) {
            return false;
        }

        final Object serialized = form.getSerialized();
        if (!(serialized instanceof final String serializedString)) {
            return false;
        }
        final boolean serializedPretty = serializedString.contains("\n");
        if (serializedPretty != this.prettyPrint) {
            return false;
        }

        if (!allowScientificNotation && hasScientificNotation(serializedString)) {
            return false;
        }

        return true;
    }

    private boolean hasScientificNotation(final String value) {
        return SCIENTIFIC_NOTATION_PATTERN.matcher(value).find();
    }

    private void writeRecord(final Record record, final RecordSchema writeSchema, final List<GeneratorTask> generatorTaskBacklog,
        final GeneratorTask startTask, final GeneratorTask endTask, final boolean schemaAware) throws IOException {

        if (isUseSerializeForm(record, writeSchema)) {
            final String serialized = (String) record.getSerializedForm().get().getSerialized();
            generatorTaskBacklog.add(GeneratorTaskWithValue.of(JsonGenerator::writeRawValue, serialized));
            return;
        }

        try {
            generatorTaskBacklog.add(startTask);

            if (schemaAware) {
                for (final RecordField field : writeSchema.getFields()) {
                    final String fieldName = field.getFieldName();
                    final Object value = record.getValue(field);
                    if (value == null) {
                        if (nullSuppression == NullSuppression.NEVER_SUPPRESS || (nullSuppression == NullSuppression.SUPPRESS_MISSING) && isFieldPresent(field, record)) {
                            generatorTaskBacklog.add(GeneratorTaskWithValue.of(JsonGenerator::writeNullField, fieldName));
                        }

                        continue;
                    }

                    generatorTaskBacklog.add(GeneratorTaskWithValue.of(JsonGenerator::writeFieldName, fieldName));

                    final DataType dataType = writeSchema.getDataType(fieldName).get();
                    writeValue(generatorTaskBacklog, value, fieldName, dataType);
                }
            } else {
                for (final String fieldName : record.getRawFieldNames()) {
                    final Object value = record.getValue(fieldName);
                    if (value == null) {
                        if (nullSuppression == NullSuppression.NEVER_SUPPRESS || (nullSuppression == NullSuppression.SUPPRESS_MISSING) && record.getRawFieldNames().contains(fieldName)) {
                            generatorTaskBacklog.add(GeneratorTaskWithValue.of(JsonGenerator::writeNullField, fieldName));
                        }

                        continue;
                    }

                    generatorTaskBacklog.add(GeneratorTaskWithValue.of(JsonGenerator::writeFieldName, fieldName));
                    writeRawValue(generatorTaskBacklog, value, fieldName);
                }
            }

            generatorTaskBacklog.add(endTask);
        } catch (final Exception e) {
            logger.error("Failed to write {} with reader schema {} and writer schema {} as a JSON Object", record, record.getSchema(), writeSchema, e);
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
    private void writeRawValue(final List<GeneratorTask> generatorTaskBacklog, final Object value, final String fieldName) throws IOException {

        if (value == null) {
            generatorTaskBacklog.add(JsonGenerator::writeNull);
            return;
        }

        if (value instanceof Record record) {
            writeRecord(record, record.getSchema(), generatorTaskBacklog, JsonGenerator::writeStartObject, JsonGenerator::writeEndObject, false);
            return;
        }

        if (value instanceof Map) {
            final Map<String, ?> map = (Map<String, ?>) value;
            generatorTaskBacklog.add(JsonGenerator::writeStartObject);

            for (final Map.Entry<String, ?> entry : map.entrySet()) {
                final String mapKey = entry.getKey();
                final Object mapValue = entry.getValue();
                generatorTaskBacklog.add(GeneratorTaskWithValue.of(JsonGenerator::writeFieldName, mapKey));
                writeRawValue(generatorTaskBacklog, mapValue, fieldName + "." + mapKey);
            }

            generatorTaskBacklog.add(JsonGenerator::writeEndObject);
            return;
        }

        if (value instanceof Object[] values) {
            generatorTaskBacklog.add(JsonGenerator::writeStartArray);
            for (final Object element : values) {
                writeRawValue(generatorTaskBacklog, element, fieldName);
            }
            generatorTaskBacklog.add(JsonGenerator::writeEndArray);
            return;
        }

        if (value instanceof java.sql.Time) {
            final Object formatted = STRING_FIELD_CONVERTER.convertField(value, Optional.ofNullable(timeFormat), fieldName);
            generatorTaskBacklog.add(GeneratorTaskWithValue.of(JsonGenerator::writeObject, formatted));
            return;
        }
        if (value instanceof java.sql.Date) {
            final Object formatted = STRING_FIELD_CONVERTER.convertField(value, Optional.ofNullable(dateFormat), fieldName);
            generatorTaskBacklog.add(GeneratorTaskWithValue.of(JsonGenerator::writeObject, formatted));
            return;
        }
        if (value instanceof java.util.Date) {
            final Object formatted = STRING_FIELD_CONVERTER.convertField(value, Optional.ofNullable(timestampFormat), fieldName);
            generatorTaskBacklog.add(GeneratorTaskWithValue.of(JsonGenerator::writeObject, formatted));
            return;
        }
        if (!allowScientificNotation) {
            if (value instanceof Double || value instanceof Float) {
                generatorTaskBacklog.add(GeneratorTaskWithValue.of(JsonGenerator::writeNumber, DataTypeUtils.toBigDecimal(value, fieldName)));
                return;
            }
        }

        generatorTaskBacklog.add(GeneratorTaskWithValue.of(JsonGenerator::writeObject, value));
    }

    @SuppressWarnings("unchecked")
    private void writeValue(final List<GeneratorTask> generatorTaskBacklog, final Object value, final String fieldName, final DataType dataType) throws IOException {
        if (value == null) {
            generatorTaskBacklog.add(JsonGenerator::writeNull);
            return;
        }

        final DataType chosenDataType = dataType.getFieldType() == RecordFieldType.CHOICE ? DataTypeUtils.chooseDataType(value, (ChoiceDataType) dataType) : dataType;
        if (chosenDataType == null) {
            logger.debug("Could not find a suitable field type in the CHOICE for field {} and value {}; will use null value", fieldName, value);
            generatorTaskBacklog.add(JsonGenerator::writeNull);
            return;
        }

        final Object coercedValue = DataTypeUtils.convertType(
                value, chosenDataType, Optional.ofNullable(dateFormat), Optional.ofNullable(timeFormat), Optional.ofNullable(timestampFormat), fieldName
        );
        if (coercedValue == null) {
            generatorTaskBacklog.add(JsonGenerator::writeNull);
            return;
        }

        switch (chosenDataType.getFieldType()) {
            case DATE: {
                final String stringValue = STRING_FIELD_CONVERTER.convertField(coercedValue, Optional.ofNullable(dateFormat), fieldName);
                if (DataTypeUtils.isLongTypeCompatible(stringValue)) {
                    generatorTaskBacklog.add(GeneratorTaskWithValue.of(JsonGenerator::writeNumber, DataTypeUtils.toLong(coercedValue, fieldName)));
                } else {
                    generatorTaskBacklog.add(GeneratorTaskWithValue.of(JsonGenerator::writeString, stringValue));
                }
                break;
            }
            case TIME: {
                final String stringValue = STRING_FIELD_CONVERTER.convertField(coercedValue, Optional.ofNullable(timeFormat), fieldName);
                if (DataTypeUtils.isLongTypeCompatible(stringValue)) {
                    generatorTaskBacklog.add(GeneratorTaskWithValue.of(JsonGenerator::writeNumber, DataTypeUtils.toLong(coercedValue, fieldName)));
                } else {
                    generatorTaskBacklog.add(GeneratorTaskWithValue.of(JsonGenerator::writeString, stringValue));
                }
                break;
            }
            case TIMESTAMP: {
                final String stringValue = STRING_FIELD_CONVERTER.convertField(coercedValue, Optional.ofNullable(timestampFormat), fieldName);
                if (DataTypeUtils.isLongTypeCompatible(stringValue)) {
                    generatorTaskBacklog.add(GeneratorTaskWithValue.of(JsonGenerator::writeNumber, DataTypeUtils.toLong(coercedValue, fieldName)));
                } else {
                    generatorTaskBacklog.add(GeneratorTaskWithValue.of(JsonGenerator::writeString, stringValue));
                }
                break;
            }
            case DOUBLE:
                if (allowScientificNotation) {
                    generatorTaskBacklog.add(GeneratorTaskWithValue.of(JsonGenerator::writeNumber, DataTypeUtils.toDouble(coercedValue, fieldName)));
                } else {
                    generatorTaskBacklog.add(GeneratorTaskWithValue.of(JsonGenerator::writeNumber, DataTypeUtils.toBigDecimal(coercedValue, fieldName)));
                }
                break;
            case FLOAT:
                if (allowScientificNotation) {
                    generatorTaskBacklog.add(GeneratorTaskWithValue.of(JsonGenerator::writeNumber, DataTypeUtils.toFloat(coercedValue, fieldName)));
                } else {
                    generatorTaskBacklog.add(GeneratorTaskWithValue.of(JsonGenerator::writeNumber, DataTypeUtils.toBigDecimal(coercedValue, fieldName)));
                }
                break;
            case LONG:
                generatorTaskBacklog.add(GeneratorTaskWithValue.of(JsonGenerator::writeNumber, DataTypeUtils.toLong(coercedValue, fieldName)));
                break;
            case INT:
            case BYTE:
            case SHORT:
                generatorTaskBacklog.add(GeneratorTaskWithValue.of(JsonGenerator::writeNumber, DataTypeUtils.toInteger(coercedValue, fieldName)));
                break;
            case UUID:
            case CHAR:
            case STRING:
                generatorTaskBacklog.add(GeneratorTaskWithValue.of(JsonGenerator::writeString, coercedValue.toString()));
                break;
            case DECIMAL:
                generatorTaskBacklog.add(GeneratorTaskWithValue.of(JsonGenerator::writeNumber, DataTypeUtils.toBigDecimal(coercedValue, fieldName)));
                break;
            case BIGINT:
                if (coercedValue instanceof Long) {
                    generatorTaskBacklog.add(GeneratorTaskWithValue.of(JsonGenerator::writeNumber, (Long) coercedValue));
                } else {
                    generatorTaskBacklog.add(GeneratorTaskWithValue.of(JsonGenerator::writeNumber, (BigInteger) coercedValue));
                }
                break;
            case BOOLEAN:
                final String stringValue = coercedValue.toString();
                if ("true".equalsIgnoreCase(stringValue)) {
                    generatorTaskBacklog.add(GeneratorTaskWithValue.of(JsonGenerator::writeBoolean, true));
                } else if ("false".equalsIgnoreCase(stringValue)) {
                    generatorTaskBacklog.add(GeneratorTaskWithValue.of(JsonGenerator::writeBoolean, false));
                } else {
                    generatorTaskBacklog.add(GeneratorTaskWithValue.of(JsonGenerator::writeString, stringValue));
                }
                break;
            case RECORD: {
                final Record record = (Record) coercedValue;
                final RecordDataType recordDataType = (RecordDataType) chosenDataType;
                final RecordSchema childSchema = recordDataType.getChildSchema();
                writeRecord(record, childSchema, generatorTaskBacklog, JsonGenerator::writeStartObject, JsonGenerator::writeEndObject, true);
                break;
            }
            case MAP: {
                final MapDataType mapDataType = (MapDataType) chosenDataType;
                final DataType valueDataType = mapDataType.getValueType();
                final Map<String, ?> map = (Map<String, ?>) coercedValue;
                generatorTaskBacklog.add(JsonGenerator::writeStartObject);

                for (final Map.Entry<String, ?> entry : map.entrySet()) {
                    final String mapKey = entry.getKey();
                    final Object mapValue = entry.getValue();
                    generatorTaskBacklog.add(GeneratorTaskWithValue.of(JsonGenerator::writeFieldName, mapKey));
                    writeValue(generatorTaskBacklog, mapValue, fieldName + "." + mapKey, valueDataType);
                }
                generatorTaskBacklog.add(JsonGenerator::writeEndObject);
                break;
            }
            case ARRAY:
            default:
                if (coercedValue instanceof Object[] values) {
                    final ArrayDataType arrayDataType = (ArrayDataType) chosenDataType;
                    final DataType elementType = arrayDataType.getElementType();
                    writeArray(values, fieldName, generatorTaskBacklog, elementType);
                } else {
                    generatorTaskBacklog.add(GeneratorTaskWithValue.of(JsonGenerator::writeString, coercedValue.toString()));
                }
                break;
        }
    }

    private void writeArray(final Object[] values, final String fieldName, final List<GeneratorTask> generatorTaskBacklog, final DataType elementType) throws IOException {
        generatorTaskBacklog.add(JsonGenerator::writeStartArray);
        for (final Object element : values) {
            writeValue(generatorTaskBacklog, element, fieldName, elementType);
        }
        generatorTaskBacklog.add(JsonGenerator::writeEndArray);
    }


    @Override
    public String getMimeType() {
        return this.mimeType;
    }

    private interface GeneratorTask {
        void apply(JsonGenerator generator) throws IOException;
    }

    private record GeneratorTaskWithValue<T>(IoThrowingBiConsumer<JsonGenerator, T> task, T value) implements GeneratorTask {
        static <T> GeneratorTaskWithValue<T> of(IoThrowingBiConsumer<JsonGenerator, T> task, T value) {
            return new GeneratorTaskWithValue<>(task, value);
        }

        @Override
        public void apply(JsonGenerator generator) throws IOException {
            task.accept(generator, value);
        }
    }

    private interface IoThrowingBiConsumer<T, U> {
        void accept(T t, U u) throws IOException;
    }
}
