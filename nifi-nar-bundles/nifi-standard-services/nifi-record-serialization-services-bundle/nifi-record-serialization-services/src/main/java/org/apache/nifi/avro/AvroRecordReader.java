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

package org.apache.nifi.avro;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericData.StringType;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

public class AvroRecordReader implements RecordReader {
    private final InputStream in;
    private final Schema avroSchema;
    private final DataFileStream<GenericRecord> dataFileStream;
    private RecordSchema recordSchema;

    public AvroRecordReader(final InputStream in) throws IOException, MalformedRecordException {
        this.in = in;

        dataFileStream = new DataFileStream<>(in, new GenericDatumReader<GenericRecord>());
        this.avroSchema = dataFileStream.getSchema();
        GenericData.setStringType(this.avroSchema, StringType.String);
    }

    @Override
    public void close() throws IOException {
        dataFileStream.close();
        in.close();
    }

    @Override
    public Record nextRecord() throws IOException, MalformedRecordException {
        if (!dataFileStream.hasNext()) {
            return null;
        }

        GenericRecord record = null;
        while (record == null && dataFileStream.hasNext()) {
            record = dataFileStream.next();
        }

        final RecordSchema schema = getSchema();
        final Map<String, Object> values = convertRecordToObjectArray(record, schema);
        return new MapRecord(schema, values);
    }


    private Map<String, Object> convertRecordToObjectArray(final GenericRecord record, final RecordSchema schema) {
        final Map<String, Object> values = new HashMap<>(schema.getFieldCount());

        for (final String fieldName : schema.getFieldNames()) {
            final Object value = record.get(fieldName);

            final Field avroField = record.getSchema().getField(fieldName);
            if (avroField == null) {
                values.put(fieldName, null);
                continue;
            }

            final Schema fieldSchema = avroField.schema();
            final DataType dataType = schema.getDataType(fieldName).orElse(null);
            final Object converted = convertValue(value, fieldSchema, avroField.name(), dataType);
            values.put(fieldName, converted);
        }

        return values;
    }


    @Override
    public RecordSchema getSchema() throws MalformedRecordException {
        if (recordSchema != null) {
            return recordSchema;
        }

        recordSchema = createSchema(avroSchema);
        return recordSchema;
    }

    private RecordSchema createSchema(final Schema avroSchema) {
        final List<RecordField> recordFields = new ArrayList<>(avroSchema.getFields().size());
        for (final Field field : avroSchema.getFields()) {
            final String fieldName = field.name();
            final DataType dataType = determineDataType(field.schema());
            recordFields.add(new RecordField(fieldName, dataType));
        }

        final RecordSchema recordSchema = new SimpleRecordSchema(recordFields);
        return recordSchema;
    }

    private Object convertValue(final Object value, final Schema avroSchema, final String fieldName, final DataType desiredType) {
        if (value == null) {
            return null;
        }

        switch (avroSchema.getType()) {
            case UNION:
                if (value instanceof GenericData.Record) {
                    final GenericData.Record record = (GenericData.Record) value;
                    return convertValue(value, record.getSchema(), fieldName, desiredType);
                }
                break;
            case RECORD:
                final GenericData.Record record = (GenericData.Record) value;
                final Schema recordSchema = record.getSchema();
                final List<Field> recordFields = recordSchema.getFields();
                final Map<String, Object> values = new HashMap<>(recordFields.size());
                for (final Field field : recordFields) {
                    final DataType desiredFieldType = determineDataType(field.schema());
                    final Object avroFieldValue = record.get(field.name());
                    final Object fieldValue = convertValue(avroFieldValue, field.schema(), field.name(), desiredFieldType);
                    values.put(field.name(), fieldValue);
                }
                final RecordSchema childSchema = createSchema(recordSchema);
                return new MapRecord(childSchema, values);
            case BYTES:
                final ByteBuffer bb = (ByteBuffer) value;
                return bb.array();
            case FIXED:
                final GenericFixed fixed = (GenericFixed) value;
                return fixed.bytes();
            case ENUM:
                return value.toString();
            case NULL:
                return null;
            case STRING:
                return value.toString();
            case ARRAY:
                final Array<?> array = (Array<?>) value;
                final Object[] valueArray = new Object[array.size()];
                for (int i = 0; i < array.size(); i++) {
                    final Schema elementSchema = avroSchema.getElementType();
                    valueArray[i] = convertValue(array.get(i), elementSchema, fieldName, determineDataType(elementSchema));
                }
                return valueArray;
            case MAP:
                final Map<?, ?> avroMap = (Map<?, ?>) value;
                final Map<String, Object> map = new HashMap<>(avroMap.size());
                for (final Map.Entry<?, ?> entry : avroMap.entrySet()) {
                    Object obj = entry.getValue();
                    if (obj instanceof Utf8 || obj instanceof CharSequence) {
                        obj = obj.toString();
                    }

                    map.put(entry.getKey().toString(), obj);
                }
                return map;
        }

        return value;
    }


    private DataType determineDataType(final Schema avroSchema) {
        final Type avroType = avroSchema.getType();

        switch (avroType) {
            case ARRAY:
            case BYTES:
            case FIXED:
                return RecordFieldType.ARRAY.getDataType();
            case BOOLEAN:
                return RecordFieldType.BOOLEAN.getDataType();
            case DOUBLE:
                return RecordFieldType.DOUBLE.getDataType();
            case ENUM:
            case STRING:
                return RecordFieldType.STRING.getDataType();
            case FLOAT:
                return RecordFieldType.FLOAT.getDataType();
            case INT:
                return RecordFieldType.INT.getDataType();
            case LONG:
                return RecordFieldType.LONG.getDataType();
            case RECORD: {
                final List<Field> avroFields = avroSchema.getFields();
                final List<RecordField> recordFields = new ArrayList<>(avroFields.size());

                for (final Field field : avroFields) {
                    final String fieldName = field.name();
                    final Schema fieldSchema = field.schema();
                    final DataType fieldType = determineDataType(fieldSchema);
                    recordFields.add(new RecordField(fieldName, fieldType));
                }

                final RecordSchema recordSchema = new SimpleRecordSchema(recordFields);
                return RecordFieldType.RECORD.getDataType(recordSchema);
            }
            case NULL:
            case MAP:
                return RecordFieldType.RECORD.getDataType();
            case UNION: {
                final List<Schema> nonNullSubSchemas = avroSchema.getTypes().stream()
                    .filter(s -> s.getType() != Type.NULL)
                    .collect(Collectors.toList());

                if (nonNullSubSchemas.size() == 1) {
                    return determineDataType(nonNullSubSchemas.get(0));
                }

                final List<DataType> possibleChildTypes = new ArrayList<>(nonNullSubSchemas.size());
                for (final Schema subSchema : nonNullSubSchemas) {
                    final DataType childDataType = determineDataType(subSchema);
                    possibleChildTypes.add(childDataType);
                }

                return RecordFieldType.CHOICE.getDataType(possibleChildTypes);
            }
        }

        return null;
    }

}
