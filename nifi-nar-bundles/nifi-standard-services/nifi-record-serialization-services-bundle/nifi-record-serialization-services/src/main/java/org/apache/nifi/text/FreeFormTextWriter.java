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

package org.apache.nifi.text;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.serialization.AbstractRecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;

public class FreeFormTextWriter extends AbstractRecordSetWriter implements RecordSetWriter {
    private static final byte NEW_LINE = (byte) '\n';
    private final PropertyValue propertyValue;
    private final Charset charset;

    public FreeFormTextWriter(final PropertyValue textPropertyValue, final Charset characterSet, final OutputStream out) {
        super(new BufferedOutputStream(out));
        this.propertyValue = textPropertyValue;
        this.charset = characterSet;
    }

    private List<String> getColumnNames(final RecordSchema schema) {
        final List<String> columnNames = new ArrayList<>();
        for (final RecordField field : schema.getFields()) {
            columnNames.add(field.getFieldName());
            for (final String alias : field.getAliases()) {
                columnNames.add(alias);
            }
        }

        return columnNames;
    }

    @Override
    public Map<String, String> writeRecord(final Record record) throws IOException {
        write(record, getOutputStream(), getColumnNames(record.getSchema()));
        return Collections.emptyMap();
    }

    private void write(final Record record, final OutputStream out, final List<String> columnNames) throws IOException {
        final int numCols = columnNames.size();
        final Map<String, String> values = new HashMap<>(numCols);
        for (int i = 0; i < numCols; i++) {
            final String columnName = columnNames.get(i);
            final String columnValue = record.getAsString(columnName);
            values.put(columnName, columnValue);
        }

        final String evaluated = propertyValue.evaluateAttributeExpressions(values).getValue();
        out.write(evaluated.getBytes(charset));
        out.write(NEW_LINE);
    }

    @Override
    public String getMimeType() {
        return "text/plain";
    }
}
