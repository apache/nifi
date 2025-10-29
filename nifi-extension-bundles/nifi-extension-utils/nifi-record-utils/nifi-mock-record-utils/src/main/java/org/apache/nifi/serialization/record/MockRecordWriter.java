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

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class MockRecordWriter extends AbstractControllerService implements RecordSetWriterFactory {
    static final String DEFAULT_SEPARATOR = ",";

    private final String header;
    private final int failAfterN;
    private final boolean quoteValues;
    private final boolean bufferOutput;
    private final RecordSchema writeSchema;
    private final Function<Map<String, String>, String> mapToSeparator;

    public MockRecordWriter() {
        this(null);
    }

    public MockRecordWriter(final String header) {
        this(header, true, -1, false, null);
    }

    public MockRecordWriter(final String header, final boolean quoteValues) {
        this(header, quoteValues, false);
    }

    public MockRecordWriter(final String header, final boolean quoteValues, final int failAfterN) {
        this(header, quoteValues, failAfterN, false, null);
    }

    public MockRecordWriter(final String header, final boolean quoteValues, final boolean bufferOutput) {
        this(header, quoteValues, -1, bufferOutput, null);
    }

    public MockRecordWriter(final String header,
                            final boolean quoteValues,
                            final int failAfterN,
                            final boolean bufferOutput,
                            final RecordSchema writeSchema) {
        this(header, quoteValues, failAfterN, bufferOutput, writeSchema, variables -> DEFAULT_SEPARATOR);
    }

    protected MockRecordWriter(final String header,
                               final boolean quoteValues,
                               final int failAfterN,
                               final boolean bufferOutput,
                               final RecordSchema writeSchema,
                               final Function<Map<String, String>, String> mapToSeparator) {
        this.header = header;
        this.quoteValues = quoteValues;
        this.failAfterN = failAfterN;
        this.bufferOutput = bufferOutput;
        this.writeSchema = writeSchema;
        this.mapToSeparator = mapToSeparator;
    }

    @Override
    public RecordSchema getSchema(Map<String, String> variables, RecordSchema readSchema)
            throws SchemaNotFoundException, IOException {
        return (writeSchema != null) ? writeSchema : new SimpleRecordSchema(Collections.emptyList());
    }

    @Override
    public RecordSetWriter createWriter(
            final ComponentLog logger,
            final RecordSchema schema,
            final OutputStream rawOut,
            final Map<String, String> variables) {
        final String separator = mapToSeparator.apply(variables);
        final OutputStream out = bufferOutput ? new BufferedOutputStream(rawOut) : rawOut;

        return new RecordSetWriter() {
            private int recordCount = 0;
            private boolean headerWritten = false;
            private final RecordSchema writerSchema = schema;

            @Override
            public void flush() throws IOException {
                out.flush();
            }

            @Override
            public WriteResult write(final RecordSet rs) throws IOException {
                if (header != null && !headerWritten) {
                    out.write(header.getBytes(StandardCharsets.UTF_8));
                    out.write("\n".getBytes());
                    headerWritten = true;
                }

                int recordCount = 0;
                Record record;
                while ((record = rs.next()) != null) {
                    if (++recordCount > failAfterN && failAfterN > -1) {
                        throw new IOException("Unit Test intentionally throwing IOException after " + failAfterN + " records were written");
                    }

                    final int numCols;
                    final List<String> fieldNames;
                    if (this.writerSchema != null && this.writerSchema.getFieldCount() != 0) {
                        fieldNames = this.writerSchema.getFieldNames();
                        numCols = this.writerSchema.getFieldCount();
                    } else {
                        fieldNames = record.getSchema().getFieldNames();
                        numCols = record.getSchema().getFieldCount();
                    }

                    int i = 0;
                    for (final String fieldName : fieldNames) {
                        final String val = record.getAsString(fieldName);
                        if (val != null) {
                            if (quoteValues) {
                                out.write("\"".getBytes());
                                out.write(val.getBytes(StandardCharsets.UTF_8));
                                out.write("\"".getBytes());
                            } else {
                                out.write(val.getBytes(StandardCharsets.UTF_8));
                            }
                        }

                        if (i++ < numCols - 1) {
                            out.write(separator.getBytes());
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
                if (++recordCount > failAfterN && failAfterN > -1) {
                    throw new IOException("Unit Test intentionally throwing IOException after " + failAfterN + " records were written");
                }

                if (header != null && !headerWritten) {
                    out.write(header.getBytes(StandardCharsets.UTF_8));
                    out.write("\n".getBytes());
                    headerWritten = true;
                }

                final int numCols;
                final List<String> fieldNames;
                if (this.writerSchema != null && this.writerSchema.getFieldCount() != 0) {
                    fieldNames = this.writerSchema.getFieldNames();
                    numCols = this.writerSchema.getFieldCount();
                } else {
                    fieldNames = record.getSchema().getFieldNames();
                    numCols = record.getSchema().getFieldCount();
                }
                int i = 0;
                for (final String fieldName : fieldNames) {
                    final String val = record.getAsString(fieldName);
                    if (val != null) {
                        if (quoteValues) {
                            out.write("\"".getBytes());
                            out.write(val.getBytes());
                            out.write("\"".getBytes());
                        } else {
                            out.write(val.getBytes());
                        }
                    }

                    if (i++ < numCols - 1) {
                        out.write(separator.getBytes());
                    }
                }
                out.write("\n".getBytes());

                return WriteResult.of(1, Collections.emptyMap());
            }

            @Override
            public void close() throws IOException {
                out.close();
            }

            @Override
            public void beginRecordSet() {
            }

            @Override
            public WriteResult finishRecordSet() {
                return WriteResult.of(recordCount, Collections.emptyMap());
            }
        };
    }
}