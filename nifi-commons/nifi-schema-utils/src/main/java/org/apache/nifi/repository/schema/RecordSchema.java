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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RecordSchema {
    private static final String FIELD_NAME = "Field Name";
    private static final String FIELD_TYPE = "Field Type";
    private static final String REPETITION = "Repetition";
    private static final String SUBFIELDS = "SubFields";

    private static final String STRING_TYPE = "String";
    private static final String INT_TYPE = "Integer";
    private static final String LONG_TYPE = "Long";
    private static final String SUBFIELD_TYPE = "SubFieldList";

    private final List<RecordField> fields;

    public RecordSchema(final List<RecordField> fields) {
        this.fields = fields;
    }

    public RecordSchema(final RecordField... fields) {
        this(Arrays.asList(fields));
    }

    public List<RecordField> getFields() {
        return fields;
    }

    public RecordField getField(final String fieldName) {
        return fields.stream()
            .filter(field -> field.getFieldName().equals(fieldName))
            .findFirst()
            .orElse(null);
    }

    public void writeTo(final OutputStream out) throws IOException {
        try {
            final DataOutputStream dos = (out instanceof DataOutputStream) ? (DataOutputStream) out : new DataOutputStream(out);

            dos.writeInt(fields.size());
            for (final RecordField field : fields) {
                writeField(field, dos);
            }
        } catch (final IOException ioe) {
            throw new IOException("Unable to write Record Schema to stream", ioe);
        }
    }

    private void writeField(final RecordField field, final DataOutputStream dos) throws IOException {
        dos.writeInt(4);    // A field is made up of 4 "elements": Field Name, Field Type, Field Repetition, Sub-Fields.

        // For each of the elements, we write a String indicating the Element Name, a String indicating the Element Type, and
        // finally the Element data itself.
        dos.writeUTF(FIELD_NAME);
        dos.writeUTF(STRING_TYPE);
        dos.writeUTF(field.getFieldName());

        dos.writeUTF(FIELD_TYPE);
        dos.writeUTF(STRING_TYPE);
        dos.writeUTF(field.getFieldType().name());

        dos.writeUTF(REPETITION);
        dos.writeUTF(STRING_TYPE);
        dos.writeUTF(field.getRepetition().name());

        dos.writeUTF(SUBFIELDS);
        dos.writeUTF(SUBFIELD_TYPE);
        final List<RecordField> subFields = field.getSubFields();
        dos.writeInt(subFields.size()); // SubField is encoded as number of Sub-Fields followed by the fields themselves.
        for (final RecordField subField : subFields) {
            writeField(subField, dos);
        }
    }

    public static RecordSchema readFrom(final InputStream in) throws IOException {
        try {
            final DataInputStream dis = (in instanceof DataInputStream) ? (DataInputStream) in : new DataInputStream(in);

            final int numFields = dis.readInt();
            final List<RecordField> fields = new ArrayList<>(numFields);

            for (int i = 0; i < numFields; i++) {
                final RecordField field = readField(dis);
                fields.add(field);
            }

            return new RecordSchema(fields);
        } catch (final IOException ioe) {
            throw new IOException("Unable to read Record Schema from stream", ioe);
        }
    }

    @SuppressWarnings("unchecked")
    private static RecordField readField(final DataInputStream dis) throws IOException {
        final Map<String, Object> schemaFieldMap = new HashMap<>();
        final int numElementsToRead = dis.readInt();
        for (int i = 0; i < numElementsToRead; i++) {
            final String fieldName = dis.readUTF();
            final String typeName = dis.readUTF();
            Object fieldValue = null;

            switch (typeName) {
                case STRING_TYPE:
                    fieldValue = dis.readUTF();
                    break;
                case INT_TYPE:
                    fieldValue = dis.readInt();
                    break;
                case LONG_TYPE:
                    fieldValue = dis.readLong();
                    break;
                case SUBFIELD_TYPE: {
                    final int numFields = dis.readInt();
                    final List<RecordField> subFields = new ArrayList<>(numFields);
                    for (int j = 0; j < numFields; j++) {
                        subFields.add(readField(dis));
                    }
                    fieldValue = subFields;
                    break;
                }
                default: {
                    throw new IOException("Cannot read schema because the schema definition contains a field named '"
                        + fieldName + "' with a Field Type of '" + typeName + "', which is not a known Field Type");
                }
            }

            schemaFieldMap.put(fieldName, fieldValue);
        }

        final String fieldName = (String) schemaFieldMap.get(FIELD_NAME);
        final String fieldTypeName = (String) schemaFieldMap.get(FIELD_TYPE);
        final String repetitionName = (String) schemaFieldMap.get(REPETITION);
        List<RecordField> subFields = (List<RecordField>) schemaFieldMap.get(SUBFIELDS);
        if (subFields == null) {
            subFields = Collections.emptyList();
        }

        final Repetition repetition = Repetition.valueOf(repetitionName);
        if (FieldType.COMPLEX.name().equals(fieldTypeName)) {
            return new ComplexRecordField(fieldName, repetition, subFields);
        } else if (FieldType.UNION.name().equals(fieldTypeName)) {
            return new UnionRecordField(fieldName, repetition, subFields);
        } else if (FieldType.MAP.name().equals(fieldTypeName)) {
            if (subFields.size() != 2) {
                throw new IOException("Found a Map that did not have a 'Key' field and a 'Value' field but instead had " + subFields.size() + " fields: " + subFields);
            }

            final RecordField keyField = subFields.get(0);
            final RecordField valueField = subFields.get(1);
            return new MapRecordField(fieldName, keyField, valueField, repetition);
        }

        return new SimpleRecordField(fieldName, FieldType.valueOf(fieldTypeName), repetition);
    }

    @Override
    public String toString() {
        return "RecordSchema[" + fields + "]";
    }
}
