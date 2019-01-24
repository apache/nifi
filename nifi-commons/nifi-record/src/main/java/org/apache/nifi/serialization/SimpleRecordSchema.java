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

package org.apache.nifi.serialization;

import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class SimpleRecordSchema implements RecordSchema {
    private List<RecordField> fields = null;
    private Map<String, RecordField> fieldMap = null;
    private final boolean textAvailable;
    private final AtomicReference<String> text = new AtomicReference<>();
    private final String schemaFormat;
    private final SchemaIdentifier schemaIdentifier;
    private String schemaName;
    private String schemaNamespace;
    private volatile int hashCode;

    public SimpleRecordSchema(final List<RecordField> fields) {
        this(fields, createText(fields), null, false, SchemaIdentifier.EMPTY);
    }

    public SimpleRecordSchema(final List<RecordField> fields, final SchemaIdentifier id) {
        this(fields, createText(fields), null, false, id);
    }

    public SimpleRecordSchema(final String text, final String schemaFormat, final SchemaIdentifier id) {
        this(text, schemaFormat, true, id);
    }

    public SimpleRecordSchema(final SchemaIdentifier id) {
        this(null, null, false, id);
    }

    public SimpleRecordSchema(final List<RecordField> fields, final String text, final String schemaFormat, final SchemaIdentifier id) {
        this(fields, text, schemaFormat, true, id);
    }

    private SimpleRecordSchema(final List<RecordField> fields, final String text, final String schemaFormat, final boolean textAvailable, final SchemaIdentifier id) {
        this(text, schemaFormat, textAvailable, id);
        setFields(fields);
    }

    private SimpleRecordSchema(final String text, final String schemaFormat, final boolean textAvailable, final SchemaIdentifier id) {
        this.text.set(text);
        this.schemaFormat = schemaFormat;
        this.schemaIdentifier = id;
        this.textAvailable = textAvailable;
    }

    @Override
    public Optional<String> getSchemaText() {
        if (textAvailable) {
            return Optional.ofNullable(text.get());
        } else {
            return Optional.empty();
        }
    }


    @Override
    public Optional<String> getSchemaFormat() {
        return Optional.ofNullable(schemaFormat);
    }

    @Override
    public List<RecordField> getFields() {
        return fields;
    }

    public void setFields(final List<RecordField> fields) {
        if (this.fields != null) {
            throw new IllegalArgumentException("Fields have already been set.");
        }

        this.fields = Collections.unmodifiableList(new ArrayList<>(fields));
        this.fieldMap = new HashMap<>(fields.size() * 2);

        for (final RecordField field : fields) {
            RecordField previousValue = fieldMap.put(field.getFieldName(), field);
            if (previousValue != null) {
                throw new IllegalArgumentException("Two fields are given with the same name (or alias) of '" + field.getFieldName() + "'");
            }

            for (final String alias : field.getAliases()) {
                previousValue = fieldMap.put(alias, field);
                if (previousValue != null) {
                    throw new IllegalArgumentException("Two fields are given with the same name (or alias) of '" + field.getFieldName() + "'");
                }
            }
        }
    }

    @Override
    public int getFieldCount() {
        return fields.size();
    }

    @Override
    public RecordField getField(final int index) {
        return fields.get(index);
    }

    @Override
    public List<DataType> getDataTypes() {
        return getFields().stream().map(RecordField::getDataType)
            .collect(Collectors.toList());
    }

    @Override
    public List<String> getFieldNames() {
        return getFields().stream().map(RecordField::getFieldName)
            .collect(Collectors.toList());
    }

    @Override
    public Optional<DataType> getDataType(final String fieldName) {
        final RecordField field = fieldMap.get(fieldName);
        if (field == null) {
            return Optional.empty();
        }
        return Optional.of(field.getDataType());
    }

    @Override
    public Optional<RecordField> getField(final String fieldName) {
        return Optional.ofNullable(fieldMap.get(fieldName));
    }


    @Override
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof RecordSchema)) {
            return false;
        }

        final RecordSchema other = (RecordSchema) obj;
        return fields.equals(other.getFields());
    }

    @Override
    public int hashCode() {
        int computed = this.hashCode;
        if (computed == 0) {
            computed = this.hashCode = 143 + 3 * fields.hashCode();
        }

        return computed;
    }

    private static String createText(final List<RecordField> fields) {
        final StringBuilder sb = new StringBuilder("[");

        for (int i = 0; i < fields.size(); i++) {
            final RecordField field = fields.get(i);

            sb.append("\"");
            sb.append(field.getFieldName());
            sb.append("\" : \"");
            sb.append(field.getDataType());
            sb.append("\"");

            if (i < fields.size() - 1) {
                sb.append(", ");
            }
        }
        sb.append("]");
        return sb.toString();
    }

    @Override
    public String toString() {
        String textValue = text.get();
        if (textValue != null) {
            return textValue;
        }

        textValue = createText(fields);
        final boolean updated = text.compareAndSet(null, textValue);

        if (updated) {
            return textValue;
        } else {
            return text.get();
        }
    }

    @Override
    public SchemaIdentifier getIdentifier() {
        return schemaIdentifier;
    }

    /**
     * Set schema name.
     * @param schemaName schema name as defined in a root record.
     */
    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    @Override
    public Optional<String> getSchemaName() {
        return Optional.ofNullable(schemaName);
    }

    /**
     * Set schema namespace.
     * @param schemaNamespace schema namespace as defined in a root record.
     */
    public void setSchemaNamespace(String schemaNamespace) {
        this.schemaNamespace = schemaNamespace;
    }

    @Override
    public Optional<String> getSchemaNamespace() {
        return Optional.ofNullable(schemaNamespace);
    }
}
