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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class SchemaRecordWriter {

    public void writeRecord(final Record record, final OutputStream out) throws IOException {
        // write sentinel value to indicate that there is a record. This allows the reader to then read one
        // byte and check if -1. If so, the reader knows there are no more records. If not, then the reader
        // knows that it should be able to continue reading.
        out.write(1);
        writeRecordFields(record, out);
    }

    private void writeRecordFields(final Record record, final OutputStream out) throws IOException {
        final DataOutputStream dos = out instanceof DataOutputStream ? (DataOutputStream) out : new DataOutputStream(out);
        for (final RecordField field : record.getSchema().getFields()) {
            final Object value = record.getFieldValue(field);

            try {
                writeFieldRepetitionAndValue(field, value, dos);
            } catch (final Exception e) {
                throw new IOException("Failed to write field '" + field.getFieldName() + "'", e);
            }
        }
    }

    private void writeFieldRepetitionAndValue(final RecordField field, final Object value, final DataOutputStream dos) throws IOException {
        switch (field.getRepetition()) {
            case EXACTLY_ONE: {
                if (value == null) {
                    throw new IllegalArgumentException("Record does not have a value for the '" + field.getFieldName() + "' but the field is required");
                }
                writeFieldValue(field, value, dos);
                break;
            }
            case ZERO_OR_MORE: {
                if (value == null) {
                    dos.writeInt(0);
                    break;
                }

                if (!(value instanceof Collection)) {
                    throw new IllegalArgumentException("Record contains a value of type '" + value.getClass() +
                        "' for the '" + field.getFieldName() + "' but expected a Collection because the Repetition for the field is " + field.getRepetition());
                }

                final Collection<?> collection = (Collection<?>) value;
                dos.writeInt(collection.size());
                for (final Object fieldValue : collection) {
                    writeFieldValue(field, fieldValue, dos);
                }
                break;
            }
            case ZERO_OR_ONE: {
                if (value == null) {
                    dos.write(0);
                    break;
                }
                dos.write(1);
                writeFieldValue(field, value, dos);
                break;
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void writeFieldValue(final RecordField field, final Object value, final DataOutputStream out) throws IOException {
        switch (field.getFieldType()) {
            case BOOLEAN:
                out.writeBoolean((boolean) value);
                break;
            case BYTE_ARRAY:
                final byte[] array = (byte[]) value;
                out.writeInt(array.length);
                out.write(array);
                break;
            case INT:
                out.writeInt((Integer) value);
                break;
            case LONG:
                out.writeLong((Long) value);
                break;
            case STRING:
                out.writeUTF((String) value);
                break;
            case LONG_STRING:
                final byte[] charArray = ((String) value).getBytes(StandardCharsets.UTF_8);
                out.writeInt(charArray.length);
                out.write(charArray);
                break;
            case MAP:
                final Map<Object, Object> map = (Map<Object, Object>) value;
                out.writeInt(map.size());
                final List<RecordField> subFields = field.getSubFields();
                final RecordField keyField = subFields.get(0);
                final RecordField valueField = subFields.get(1);

                for (final Map.Entry<Object, Object> entry : map.entrySet()) {
                    writeFieldRepetitionAndValue(keyField, entry.getKey(), out);
                    writeFieldRepetitionAndValue(valueField, entry.getValue(), out);
                }
                break;
            case UNION:
                final NamedValue namedValue = (NamedValue) value;
                out.writeUTF(namedValue.getName());
                final Record childRecord = (Record) namedValue.getValue();
                writeRecordFields(childRecord, out);
                break;
            case COMPLEX:
                final Record record = (Record) value;
                writeRecordFields(record, out);
                break;
        }
    }
}
