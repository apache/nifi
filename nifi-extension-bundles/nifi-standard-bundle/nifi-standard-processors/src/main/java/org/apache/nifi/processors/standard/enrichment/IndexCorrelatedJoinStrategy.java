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

package org.apache.nifi.processors.standard.enrichment;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public abstract class IndexCorrelatedJoinStrategy implements RecordJoinStrategy {
    private final ComponentLog logger;

    public IndexCorrelatedJoinStrategy(final ComponentLog logger) {
        this.logger = logger;
    }

    protected ComponentLog getLogger() {
        return logger;
    }

    @Override
    public RecordJoinResult join(final RecordJoinInput originalInput, final RecordJoinInput enrichmentInput, final Map<String, String> combinedAttributes,
                final ProcessSession session,final RecordSchema writerSchema) throws Exception {

        final FlowFile originalFlowFile = originalInput.getFlowFile();
        final FlowFile enrichmentFlowFile = enrichmentInput.getFlowFile();

        InputStream originalIn = null;
        RecordReader originalRecordReader = null;
        InputStream enrichmentIn = null;
        RecordReader enrichmentRecordReader = null;

        try {
            originalIn = session.read(originalFlowFile);
            originalRecordReader = originalInput.getRecordReaderFactory().createRecordReader(originalFlowFile, originalIn, logger);

            enrichmentIn = session.read(enrichmentFlowFile);
            enrichmentRecordReader = enrichmentInput.getRecordReaderFactory().createRecordReader(enrichmentFlowFile, enrichmentIn, logger);

            final Record firstOriginalRecord = originalRecordReader.nextRecord();
            final Record firstEnrichmentRecord = enrichmentRecordReader.nextRecord();

            final RecordSchema resultSchema = createResultSchema(firstOriginalRecord, firstEnrichmentRecord);

            final InputStream finalOriginalIn = originalIn;
            final RecordReader finalOriginalRecordReader = originalRecordReader;
            final InputStream finalEnrichmentIn = enrichmentIn;
            final RecordReader finalEnrichmentRecordReader = enrichmentRecordReader;

            final RecordSet recordSet = new RecordSet() {
                private boolean usedFirstRecords = false;

                @Override
                public RecordSchema getSchema() {
                    return resultSchema;
                }

                @Override
                public Record next() throws IOException {
                    if (!usedFirstRecords) {
                        usedFirstRecords = true;
                        final Record combined = combineRecords(firstOriginalRecord, firstEnrichmentRecord, resultSchema);
                        return combined;
                    }

                    try {
                        final Record originalRecord = finalOriginalRecordReader.nextRecord();
                        final Record enrichmentRecord = finalEnrichmentRecordReader.nextRecord();

                        if (originalRecord == null && enrichmentRecord == null) {
                            return null;
                        }

                        final Record combined = combineRecords(originalRecord, enrichmentRecord, resultSchema);
                        return combined;
                    } catch (final MalformedRecordException e) {
                        throw new IOException("Failed to read record", e);
                    }
                }
            };

            return new RecordJoinResult() {
                @Override
                public RecordSet getRecordSet() {
                    return recordSet;
                }

                @Override
                public void close() {
                    closeQuietly(finalOriginalRecordReader, finalOriginalIn, finalEnrichmentRecordReader, finalEnrichmentIn);
                }
            };
        } catch (final Throwable t) {
            closeQuietly(originalRecordReader, originalIn, enrichmentRecordReader, enrichmentIn);
            throw t;
        }
    }


    private void closeQuietly(final AutoCloseable... closeables) {
        for (final AutoCloseable closeable : closeables) {
            closeQuietly(closeable);
        }
    }

    private void closeQuietly(final AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (final Exception e) {
                logger.warn("Failed to close {}", closeable, e);
            }
        }
    }

    protected abstract Record combineRecords(Record originalRecord, Record enrichmentRecord, RecordSchema resultSchema);

    protected abstract RecordSchema createResultSchema(Record firstOriginalRecord, Record firstEnrichmentRecord);

}
