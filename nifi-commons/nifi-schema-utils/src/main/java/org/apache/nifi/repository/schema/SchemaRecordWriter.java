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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UTFDataFormatException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class SchemaRecordWriter {
    static final int INLINE_RECORD_INDICATOR = 1;
    static final int EXTERNAL_FILE_INDICATOR = 8;

    public static final int MAX_ALLOWED_UTF_LENGTH = 65_535;

    private static final Logger logger = LoggerFactory.getLogger(SchemaRecordWriter.class);
    private static final int CACHE_BUFFER_SIZE = 65536;
    private static final ByteArrayCache byteArrayCache = new ByteArrayCache(32, CACHE_BUFFER_SIZE);

    public void writeRecord(final Record record, final OutputStream out) throws IOException {
        // write sentinel value to indicate that there is a record. This allows the reader to then read one
        // byte and check if -1. If so, the reader knows there are no more records. If not, then the reader
        // knows that it should be able to continue reading.
        out.write(INLINE_RECORD_INDICATOR);

        final byte[] buffer = byteArrayCache.checkOut();
        try {
            writeRecordFields(record, out, buffer);
        } finally {
            byteArrayCache.checkIn(buffer);
        }
    }

    private void writeRecordFields(final Record record, final OutputStream out, final byte[] buffer) throws IOException {
        writeRecordFields(record, record.getSchema(), out, buffer);
    }

    private void writeRecordFields(final Record record, final RecordSchema schema, final OutputStream out, final byte[] buffer) throws IOException {
        final DataOutputStream dos = out instanceof DataOutputStream ? (DataOutputStream) out : new DataOutputStream(out);
        for (final RecordField field : schema.getFields()) {
            final Object value = record.getFieldValue(field);

            try {
                writeFieldRepetitionAndValue(field, value, dos, buffer);
            } catch (final Exception e) {
                throw new IOException("Failed to write field '" + field.getFieldName() + "'", e);
            }
        }
    }

    private void writeFieldRepetitionAndValue(final RecordField field, final Object value, final DataOutputStream dos, final byte[] buffer) throws IOException {
        switch (field.getRepetition()) {
            case EXACTLY_ONE: {
                if (value == null) {
                    throw new IllegalArgumentException("Record does not have a value for the '" + field.getFieldName() + "' but the field is required");
                }
                writeFieldValue(field, value, dos, buffer);
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
                    writeFieldValue(field, fieldValue, dos, buffer);
                }
                break;
            }
            case ZERO_OR_ONE: {
                if (value == null) {
                    dos.write(0);
                    break;
                }
                dos.write(1);
                writeFieldValue(field, value, dos, buffer);
                break;
            }
        }
    }

    private boolean allSingleByteInUtf8(final String value) {
        for (int i = 0; i < value.length(); i++) {
            final char ch = value.charAt(i);
            if (ch < 1 || ch > 127) {
                return false;
            }
        }

        return true;
    }

    @SuppressWarnings("unchecked")
    private void writeFieldValue(final RecordField field, final Object value, final DataOutputStream out, final byte[] buffer) throws IOException {
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
                writeUTFLimited(out, (String) value, field.getFieldName());
                break;
            case LONG_STRING:
                // In many cases, we will see a String value that consists solely of values in the range of
                // 1-127, which means that in UTF-8 they will translate into a single byte each. If all characters
                // in the string adhere to this, then we can skip calling String.getBytes() because that will allocate
                // a new byte[] every time, which results in a lot of pressure on the garbage collector.
                final String string = (String) value;
                final int length = string.length();

                if (length <= buffer.length && allSingleByteInUtf8(string)) {
                    out.writeInt(length);

                    for (int i = 0; i < length; i++) {
                        final char ch = string.charAt(i);
                        buffer[i] = (byte) ch;
                    }

                    out.write(buffer, 0, length);
                } else {
                    final byte[] charArray = ((String) value).getBytes(StandardCharsets.UTF_8);
                    out.writeInt(charArray.length);
                    out.write(charArray);
                }
                break;
            case MAP:
                final Map<Object, Object> map = (Map<Object, Object>) value;
                out.writeInt(map.size());
                final List<RecordField> subFields = field.getSubFields();
                final RecordField keyField = subFields.get(0);
                final RecordField valueField = subFields.get(1);

                for (final Map.Entry<Object, Object> entry : map.entrySet()) {
                    writeFieldRepetitionAndValue(keyField, entry.getKey(), out, buffer);
                    writeFieldRepetitionAndValue(valueField, entry.getValue(), out, buffer);
                }
                break;
            case UNION:
                final NamedValue namedValue = (NamedValue) value;
                writeUTFLimited(out, namedValue.getName(), field.getFieldName());
                final Record childRecord = (Record) namedValue.getValue();
                writeRecordFields(childRecord, out, buffer);
                break;
            case COMPLEX:
                final Record record = (Record) value;
                writeRecordFields(record, out, buffer);
                break;
        }
    }

    private void writeUTFLimited(final DataOutputStream out, final String utfString, final String fieldName) throws IOException {
        try {
            out.writeUTF(utfString);
        } catch (UTFDataFormatException e) {
            final String truncated = utfString.substring(0, getCharsInUTF8Limit(utfString, MAX_ALLOWED_UTF_LENGTH));
            logger.warn("Truncating repository record value for field '{}'!  Attempted to write {} chars that encode to a UTF8 byte length greater than "
                            + "supported maximum ({}), truncating to {} chars.",
                    (fieldName == null) ? "" : fieldName, utfString.length(), MAX_ALLOWED_UTF_LENGTH, truncated.length());
            if (logger.isDebugEnabled()) {
                logger.warn("String value was:\n{}", truncated);
            }
            out.writeUTF(truncated);
        }
    }

    static int getCharsInUTF8Limit(final String str, final int utf8Limit) {
        // Calculate how much of String fits within UTF8 byte limit based on RFC3629.
        //
        // Java String values use char[] for storage, so character values >0xFFFF that
        // map to 4 byte UTF8 representations are not considered.

        final int charsInOriginal = str.length();
        int bytesInUTF8 = 0;

        for (int i = 0; i < charsInOriginal; i++) {
            final int curr = str.charAt(i);
            if (curr < 0x0080) {
                bytesInUTF8++;
            } else if (curr < 0x0800) {
                bytesInUTF8 += 2;
            } else {
                bytesInUTF8 += 3;
            }
            if (bytesInUTF8 > utf8Limit) {
                return i;
            }
        }
        return charsInOriginal;
    }

    public void writeExternalFileReference(final DataOutputStream out, final File externalFile) throws IOException {
        out.write(EXTERNAL_FILE_INDICATOR);
        out.writeUTF(externalFile.getAbsolutePath());
    }
}
