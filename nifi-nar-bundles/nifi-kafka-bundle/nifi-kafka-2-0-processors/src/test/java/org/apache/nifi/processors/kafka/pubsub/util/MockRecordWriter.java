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

package org.apache.nifi.processors.kafka.pubsub.util;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Map;

public class MockRecordWriter extends AbstractControllerService implements RecordSetWriterFactory {
    private final String header;
    private final int failAfterN;
    private final boolean quoteValues;

    public MockRecordWriter(final String header) {
        this(header, true, -1);
    }

    public MockRecordWriter(final String header, final boolean quoteValues) {
        this(header, quoteValues, -1);
    }

    public MockRecordWriter(final String header, final boolean quoteValues, final int failAfterN) {
        this.header = header;
        this.quoteValues = quoteValues;
        this.failAfterN = failAfterN;
    }

    @Override
    public RecordSchema getSchema(Map<String, String> variables, RecordSchema readSchema) throws SchemaNotFoundException, IOException {
        return null;
    }

    @Override
    public RecordSetWriter createWriter(final ComponentLog logger, final RecordSchema schema, final OutputStream out) {
        return new RecordSetWriter() {

            @Override
            public void flush() throws IOException {
                out.flush();
            }

            @Override
            public WriteResult write(final RecordSet rs) throws IOException {
                out.write(header.getBytes());
                out.write("\n".getBytes());

                int recordCount = 0;
                final int numCols = rs.getSchema().getFieldCount();
                Record record = null;
                while ((record = rs.next()) != null) {
                    if (++recordCount > failAfterN && failAfterN > -1) {
                        throw new IOException("Unit Test intentionally throwing IOException after " + failAfterN + " records were written");
                    }

                    int i = 0;
                    for (final String fieldName : record.getSchema().getFieldNames()) {
                        final String val = record.getAsString(fieldName);
                        if (quoteValues) {
                            out.write("\"".getBytes());
                            if (val != null) {
                                out.write(val.getBytes());
                            }
                            out.write("\"".getBytes());
                        } else if (val != null) {
                            out.write(val.getBytes());
                        }

                        if (i++ < numCols - 1) {
                            out.write(",".getBytes());
                        }
                    }
                    out.write("\n".getBytes());
                }

                return WriteResult.of(recordCount, Collections.emptyMap());
            }

            @Override
            public String getMimeType() {
                return "text/plain";
            }

            @Override
            public WriteResult write(Record record) throws IOException {
                return WriteResult.of(1, Collections.emptyMap());
            }

            @Override
            public void close() throws IOException {
            }

            @Override
            public void beginRecordSet() throws IOException {
            }

            @Override
            public WriteResult finishRecordSet() throws IOException {
                return null;
            }
        };
    }
}