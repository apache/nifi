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
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MockCSVWriter extends AbstractControllerService implements RecordSetWriterFactory {
    @Override
    public RecordSchema getSchema(final Map<String, String> variables, final RecordSchema readSchema) {
        return readSchema;
    }

    @Override
    public RecordSetWriter createWriter(final ComponentLog logger, final RecordSchema schema, final OutputStream out, final Map<String, String> variables) {
        final Writer streamWriter = new OutputStreamWriter(out);
        final BufferedWriter writer = new BufferedWriter(streamWriter);

        return new RecordSetWriter() {
            int recordCount = 0;

            @Override
            public WriteResult write(final RecordSet recordSet) throws IOException {
                beginRecordSet();
                Record record;
                while ((record = recordSet.next()) != null) {
                    write(record);
                }
                return finishRecordSet();
            }

            @Override
            public void beginRecordSet() throws IOException {
                final List<String> fieldNames = schema.getFieldNames();
                int i = 0;
                for (final String fieldName : fieldNames) {
                    writer.write(fieldName);
                    if (++i < fieldNames.size()) {
                        writer.write(',');
                        writer.write(' ');
                    }
                }

                writer.write("\n");
            }

            @Override
            public WriteResult finishRecordSet() {
                return createWriteResult();
            }

            @Override
            public WriteResult write(final Record record) throws IOException {
                final Object[] values = record.getValues();
                int i = 0;
                for (final Object value : values) {
                    writer.write(value == null ? "" : value.toString());
                    if (++i < values.length) {
                        writer.write(',');
                        writer.write(' ');
                    }
                }

                writer.write("\n");
                recordCount++;

                return createWriteResult();
            }

            private WriteResult createWriteResult() {
                return new WriteResult() {
                    @Override
                    public int getRecordCount() {
                        return recordCount;
                    }

                    @Override
                    public Map<String, String> getAttributes() {
                        return Collections.emptyMap();
                    }
                };
            }

            @Override
            public String getMimeType() {
                return "text/csv";
            }

            @Override
            public void flush() throws IOException {
                writer.flush();
            }

            @Override
            public void close() throws IOException {
                writer.close();
            }
        };
    }
}
