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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;

public class FreeFormTextWriter implements RecordSetWriter {
    private static final byte NEW_LINE = (byte) '\n';
    private final PropertyValue propertyValue;
    private final Charset charset;

    public FreeFormTextWriter(final PropertyValue textPropertyValue, final Charset characterSet) {
        propertyValue = textPropertyValue;
        charset = characterSet;
    }

    @Override
    public WriteResult write(final RecordSet recordSet, final OutputStream out) throws IOException {
        int count = 0;

        try {
            Record record;
            while ((record = recordSet.next()) != null) {
                final RecordSchema schema = record.getSchema();
                final List<String> colNames = getColumnNames(schema);

                count++;
                write(record, out, colNames);
            }
        } catch (final Exception e) {
            throw new ProcessException(e);
        }

        return WriteResult.of(count, Collections.emptyMap());
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
    public WriteResult write(final Record record, final OutputStream out) throws IOException {
        write(record, out, getColumnNames(record.getSchema()));
        return WriteResult.of(1, Collections.emptyMap());
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
