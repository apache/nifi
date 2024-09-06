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

package org.apache.nifi.cs.tests.system;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockCSVReader extends AbstractControllerService implements RecordReaderFactory {
    @Override
    public RecordReader createRecordReader(final FlowFile flowFile, final InputStream in, final ComponentLog logger)
                throws IOException {
        return createRecordReader(flowFile.getAttributes(), in, flowFile.getSize(), logger);
    }

    @Override
    public RecordReader createRecordReader(final Map<String, String> variables, final InputStream in, final long inputLength, final ComponentLog logger) throws IOException {
        final Reader inputStreamReader = new InputStreamReader(in);
        final BufferedReader reader = new BufferedReader(inputStreamReader);

        final List<String> fieldNames = new ArrayList<>();
        final String firstLine = reader.readLine();
        final String[] colNames = firstLine.split(",");
        for (final String colName : colNames) {
            fieldNames.add(colName.trim());
        }

        final List<RecordField> fields = new ArrayList<>();
        for (final String fieldName : fieldNames) {
            fields.add(new RecordField(fieldName, RecordFieldType.STRING.getDataType()));
        }
        final RecordSchema schema = new SimpleRecordSchema(fields);

        return new RecordReader() {
            @Override
            public Record nextRecord(final boolean coerceTypes, final boolean dropUnknownFields) throws IOException, MalformedRecordException {
                String nextLine;
                final Map<String, Object> values = new HashMap<>();

                while ((nextLine = reader.readLine()) != null) {
                    nextLine = nextLine.trim();
                    if (nextLine.isEmpty()) {
                        continue;
                    }

                    final String[] colValues = nextLine.split(",");
                    int i = 0;
                    for (final String colValue : colValues) {
                        final String colName = fieldNames.get(i++);
                        values.put(colName, colValue.trim());
                    }

                    return new MapRecord(schema, values);
                }

                return null;
            }

            @Override
            public RecordSchema getSchema() {
                return schema;
            }

            @Override
            public void close() throws IOException {
                reader.close();
            }
        };
    }
}
