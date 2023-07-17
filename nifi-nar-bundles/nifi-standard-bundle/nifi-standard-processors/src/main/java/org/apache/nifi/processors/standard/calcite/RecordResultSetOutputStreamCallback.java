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
package org.apache.nifi.processors.standard.calcite;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.ResultSetRecordSet;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class RecordResultSetOutputStreamCallback implements OutputStreamCallback {
    private final ComponentLog logger;
    private final ResultSet rs;
    private final RecordSchema writerSchema;
    private final Integer defaultPrecision;
    private final Integer defaultScale;
    private final RecordSetWriterFactory recordSetWriterFactory;
    private final Map<String, String> originalAttributes;

    private WriteResult writeResult;
    private String mimeType;

    public RecordResultSetOutputStreamCallback(
            final ComponentLog logger, final ResultSet rs, final RecordSchema writerSchema,
            final Integer defaultPrecision, final Integer defaultScale,
            final RecordSetWriterFactory recordSetWriterFactory, final Map<String, String> originalAttributes) {
        this.logger = logger;
        this.rs = rs;
        this.writerSchema = writerSchema;
        this.defaultPrecision = defaultPrecision;
        this.defaultScale = defaultScale;
        this.recordSetWriterFactory = recordSetWriterFactory;
        this.originalAttributes = originalAttributes;
    }

    public WriteResult getWriteResult() throws ProcessException {
        return ObjectUtils.defaultIfNull(writeResult, WriteResult.EMPTY);
    }

    public String getMimeType() {
        return mimeType;
    }

    @Override
    public void process(OutputStream out) throws IOException {
        final RecordSchema writeSchema;

        try (final ResultSetRecordSet recordSet = new ResultSetRecordSet(rs, writerSchema, defaultPrecision, defaultScale)) {
            final RecordSchema resultSetSchema = recordSet.getSchema();
            writeSchema = recordSetWriterFactory.getSchema(originalAttributes, resultSetSchema);

            try (final RecordSetWriter resultSetWriter = recordSetWriterFactory.createWriter(logger, writeSchema, out, originalAttributes)) {
                writeResult = resultSetWriter.write(recordSet);
                mimeType = resultSetWriter.getMimeType();
            } catch (final Exception e) {
                throw new IOException("Writing result records failed", e);
            }
        } catch (final SQLException | SchemaNotFoundException e) {
            throw new ProcessException("Reading query result records failed", e);
        }
    }
}
