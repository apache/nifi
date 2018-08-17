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

package org.apache.nifi.repository.schema;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class SchemaRecordReader {
    private final RecordSchema schema;

    public SchemaRecordReader(final RecordSchema schema) {
        this.schema = schema;
    }

    public static SchemaRecordReader fromSchema(final RecordSchema schema) {
        return new SchemaRecordReader(schema);
    }

    private static void fillBuffer(final InputStream in, final byte[] destination) throws IOException {
        int bytesRead = 0;
        int len;
        while (bytesRead < destination.length) {
            len = in.read(destination, bytesRead, destination.length - bytesRead);
            if (len < 0) {
                throw new EOFException();
            }

            bytesRead += len;
        }
    }

    public Record readRecord(final InputStream in) throws IOException {
        final int recordIndicator = in.read();
        if (recordIndicator < 0) {
            return null;
        }

        if (recordIndicator == SchemaRecordWriter.EXTERNAL_FILE_INDICATOR) {
            throw new IOException("Expected to read a Sentinel Byte of '1' indicating that the next record is inline but the Sentinel value was '" + SchemaRecordWriter.EXTERNAL_FILE_INDICATOR
                + ", indicating that data was written to an External File. This data cannot be recovered via calls to #readRecord(InputStream) but must be recovered via #readRecords(InputStream)");
        }

        if (recordIndicator != 1) {
            throw new IOException("Expected to read a Sentinel Byte of '1' but got a value of '" + recordIndicator + "' instead");
        }

        return readInlineRecord(in);
    }

    private Record readInlineRecord(final InputStream in) throws IOException {
        final List<RecordField> schemaFields = schema.getFields();
        final Map<RecordField, Object> fields = new HashMap<>(schemaFields.size());

        for (final RecordField field : schema.getFields()) {
            final Object value = readField(in, field);
            fields.put(field, value);
        }

        return new FieldMapRecord(fields, schema);
    }

    public RecordIterator readRecords(final InputStream in) throws IOException {
        final int recordIndicator = in.read();
        if (recordIndicator < 0) {
            return null;
        }

        if (recordIndicator == SchemaRecordWriter.INLINE_RECORD_INDICATOR) {
            final Record nextRecord = readInlineRecord(in);
            return new SingleRecordIterator(nextRecord);
        }

        if (recordIndicator != SchemaRecordWriter.EXTERNAL_FILE_INDICATOR) {
            throw new IOException("Expected to read a Sentinel Byte of '" + SchemaRecordWriter.INLINE_RECORD_INDICATOR + "' or '" + SchemaRecordWriter.EXTERNAL_FILE_INDICATOR
                + "' but encountered a value of '" + recordIndicator + "' instead");
        }

        final DataInputStream dis = new DataInputStream(in);
        final String externalFilename = dis.readUTF();
        final File externalFile = new File(externalFilename);
        final FileInputStream fis = new FileInputStream(externalFile);
        final InputStream bufferedIn = new BufferedInputStream(fis);

        final RecordIterator recordIterator = new RecordIterator() {
            @Override
            public Record next() throws IOException {
                return readRecord(bufferedIn);
            }

            @Override
            public boolean isNext() throws IOException {
                bufferedIn.mark(1);
                final int nextByte = bufferedIn.read();
                bufferedIn.reset();

                return (nextByte > -1);
            }

            @Override
            public void close() throws IOException {
                bufferedIn.close();
            }
        };

        return recordIterator;
    }



    private Object readField(final InputStream in, final RecordField field) throws IOException {
        switch (field.getRepetition()) {
            case ZERO_OR_MORE: {
                // If repetition is 0+ then that means we have a list and need to read how many items are in the list.
                final int iterations = readInt(in);
                if (iterations == 0) {
                    return Collections.emptyList();
                }

                final List<Object> value = new ArrayList<>(iterations);
                for (int i = 0; i < iterations; i++) {
                    value.add(readFieldValue(in, field.getFieldType(), field.getFieldName(), field.getSubFields()));
                }

                return value;
            }
            case ZERO_OR_ONE: {
                // If repetition is 0 or 1 (optional), then check if next byte is a 0, which means field is absent or 1, which means
                // field is present. Otherwise, throw an Exception.
                final int nextByte = in.read();
                if (nextByte == -1) {
                    throw new EOFException("Unexpected End-of-File when attempting to read Repetition value for field '" + field.getFieldName() + "'");
                }
                if (nextByte == 0) {
                    return null;
                }
                if (nextByte != 1) {
                    throw new IOException("Invalid Boolean value found when reading 'Repetition' of field '" + field.getFieldName() + "'. Expected 0 or 1 but got " + (nextByte & 0xFF));
                }
            }
        }

        try {
            return readFieldValue(in, field.getFieldType(), field.getFieldName(), field.getSubFields());
        } catch (final EOFException eof) {
            final EOFException exception = new EOFException("Failed to read field '" + field.getFieldName() + "'");
            exception.addSuppressed(eof);
            throw exception;
        } catch (final IOException ioe) {
            throw new IOException("Failed to read field '" + field.getFieldName() + "'", ioe);
        }
    }


    private Object readFieldValue(final InputStream in, final FieldType fieldType, final String fieldName, final List<RecordField> subFields) throws IOException {
        switch (fieldType) {
            case BOOLEAN: {
                final DataInputStream dis = new DataInputStream(in);
                return dis.readBoolean();
            }
            case INT: {
                return readInt(in);
            }
            case LONG: {
                final DataInputStream dis = new DataInputStream(in);
                return dis.readLong();
            }
            case STRING: {
                final DataInputStream dis = new DataInputStream(in);
                return dis.readUTF();
            }
            case LONG_STRING: {
                final int length = readInt(in);
                final byte[] buffer = new byte[length];
                fillBuffer(in, buffer);
                return new String(buffer, StandardCharsets.UTF_8);
            }
            case BYTE_ARRAY: {
                final int length = readInt(in);
                final byte[] buffer = new byte[length];
                fillBuffer(in, buffer);
                return buffer;
            }
            case MAP: {
                final int numEntries = readInt(in);
                final RecordField keyField = subFields.get(0);
                final RecordField valueField = subFields.get(1);

                final Map<Object, Object> entries = new HashMap<>(numEntries);
                for (int i = 0; i < numEntries; i++) {
                    final Object key = readField(in, keyField);
                    final Object value = readField(in, valueField);
                    entries.put(key, value);
                }

                return entries;
            }
            case COMPLEX: {
                final int numSubFields = subFields.size();
                final Map<RecordField, Object> subFieldValues = new HashMap<>(numSubFields);
                for (int i = 0; i < numSubFields; i++) {
                    final Object subFieldValue = readField(in, subFields.get(i));
                    subFieldValues.put(subFields.get(i), subFieldValue);
                }

                return new FieldMapRecord(subFieldValues, new RecordSchema(subFields));
            }
            case UNION: {
                final DataInputStream dis = new DataInputStream(in);
                final String childFieldType = dis.readUTF();
                final Optional<RecordField> fieldOption = subFields.stream().filter(field -> field.getFieldName().equals(childFieldType)).findFirst();
                if (!fieldOption.isPresent()) {
                    throw new IOException("Found a field of type '" + childFieldType + "' but that was not in the expected list of types");
                }

                final RecordField matchingField = fieldOption.get();
                return readField(in, matchingField);
            }
            default: {
                throw new IOException("Unrecognized Field Type " + fieldType + " for field '" + fieldName + "'");
            }
        }
    }

    private int readInt(final InputStream in) throws IOException {
        final byte[] buffer = new byte[4];
        fillBuffer(in, buffer);
        return ByteBuffer.wrap(buffer).getInt();
    }
}
