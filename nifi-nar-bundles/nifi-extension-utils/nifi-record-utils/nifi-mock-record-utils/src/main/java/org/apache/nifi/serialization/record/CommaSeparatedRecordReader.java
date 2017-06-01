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

package org.apache.nifi.serialization.record;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;

public class CommaSeparatedRecordReader extends AbstractControllerService implements RecordReaderFactory {
    private int failAfterN;
    private int recordCount = 0;

    public CommaSeparatedRecordReader() {
        this(-1);
    }

    public CommaSeparatedRecordReader(final int failAfterN) {
        this.failAfterN = failAfterN;
    }

    public void failAfter(final int failAfterN) {
        this.failAfterN = failAfterN;
    }

    @Override
    public RecordReader createRecordReader(Map<String, String> variables, final InputStream in, final ComponentLog logger) throws IOException, SchemaNotFoundException {
        final BufferedReader reader = new BufferedReader(new InputStreamReader(in));

        final List<RecordField> fields = new ArrayList<>();

        final String headerLine = reader.readLine();
        for (final String colName : headerLine.split(",")) {
            fields.add(new RecordField(colName.trim(), RecordFieldType.STRING.getDataType()));
        }

        return new RecordReader() {

            @Override
            public void close() throws IOException {
                reader.close();
            }

            @Override
            public Record nextRecord(final boolean coerceTypes, final boolean dropUnknown) throws IOException, MalformedRecordException {
                if (failAfterN > -1 && recordCount >= failAfterN) {
                    throw new MalformedRecordException("Intentional Unit Test Exception because " + recordCount + " records have been read");
                }

                final String nextLine = reader.readLine();
                if (nextLine == null) {
                    return null;
                }

                recordCount++;

                final String[] values = nextLine.split(",");
                final Map<String, Object> valueMap = new HashMap<>();
                int i = 0;
                for (final RecordField field : fields) {
                    final String fieldName = field.getFieldName();
                    valueMap.put(fieldName, values[i++].trim());
                }

                return new MapRecord(new SimpleRecordSchema(fields), valueMap);
            }

            @Override
            public RecordSchema getSchema() {
                return new SimpleRecordSchema(fields);
            }
        };
    }
}