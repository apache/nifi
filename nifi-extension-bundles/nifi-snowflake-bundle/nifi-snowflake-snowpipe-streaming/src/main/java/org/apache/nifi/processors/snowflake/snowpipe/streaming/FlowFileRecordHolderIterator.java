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
package org.apache.nifi.processors.snowflake.snowpipe.streaming;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * FlowFile Record Holder Iterator responsible for handling buffering and Record conversion
 */
class FlowFileRecordHolderIterator implements Iterator<FlowFileRecordHolder>, Closeable {
    private final List<FlowFileAndReader> readers;
    private final Iterator<FlowFileAndReader> recordReaderIterator;
    private final RecordSchema recordSchema;

    private FlowFileRecordHolder nextRecord;
    private FlowFile flowFile;
    private RecordReader currentReader;

    public FlowFileRecordHolderIterator(
            final ComponentLog logger,
            final List<FlowFile> flowFiles,
            final RecordReaderFactory readerFactory,
            final ProcessSession session
    ) throws IOException {
        readers = new ArrayList<>(flowFiles.size());

        RecordSchema mergedSchema = null;

        for (final FlowFile flowFile : flowFiles) {
            final InputStream in = session.read(flowFile);
            try {
                final RecordReader reader = readerFactory.createRecordReader(flowFile, in, logger);
                readers.add(new FlowFileAndReader(flowFile, reader));

                final RecordSchema schema = reader.getSchema();
                if (mergedSchema == null) {
                    mergedSchema = schema;
                } else {
                    mergedSchema = DataTypeUtils.merge(mergedSchema, schema);
                }
            } catch (final IOException | SchemaNotFoundException | MalformedRecordException e) {
                for (final FlowFileAndReader flowFileAndReader : readers) {
                    try {
                        flowFileAndReader.reader().close();
                    } catch (final IOException closeException) {
                        e.addSuppressed(closeException);
                    }
                }

                throw new IOException("Failed to create RecordReader for FlowFile " + flowFile, e);
            }
        }

        this.recordSchema = mergedSchema;
        this.recordReaderIterator = readers.iterator();
    }

    public RecordSchema getSchema() {
        return recordSchema;
    }

    private boolean bufferNextRecord() throws IOException, SchemaNotFoundException, MalformedRecordException {
        try {
            while (true) {
                if (currentReader == null) {
                    if (!recordReaderIterator.hasNext()) {
                        return false;
                    }

                    final FlowFileAndReader flowFileAndReader = recordReaderIterator.next();
                    flowFile = flowFileAndReader.flowFile();
                    currentReader = flowFileAndReader.reader();
                }

                final Record record = currentReader.nextRecord();
                if (record == null) {
                    currentReader.close();
                    currentReader = null;
                } else {
                    nextRecord = new FlowFileRecordHolder(flowFile, record);
                    return true;
                }
            }
        } catch (final Exception e) {
            if (currentReader != null) {
                currentReader.close();
            }

            while (recordReaderIterator.hasNext()) {
                try {
                    recordReaderIterator.next().reader().close();
                } catch (final IOException closeException) {
                    e.addSuppressed(closeException);
                }
            }

            throw e;
        }
    }

    @Override
    public boolean hasNext() {
        if (nextRecord != null) {
            return true;
        }

        try {
            return bufferNextRecord();
        } catch (final IOException e) {
            throw new UncheckedIOException("Could not buffer next record", e);
        } catch (final SchemaNotFoundException | MalformedRecordException e) {
            throw new UncheckedIOException("Could not buffer next record", new IOException(e));
        }
    }

    @Override
    public FlowFileRecordHolder next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        final FlowFileRecordHolder record = nextRecord;
        nextRecord = null;
        return record;
    }

    @Override
    public void close() {
        for (final FlowFileAndReader flowFileAndReader : readers) {
            try {
                flowFileAndReader.reader().close();
            } catch (final IOException ignored) {
            }
        }
    }

    private record FlowFileAndReader(FlowFile flowFile, RecordReader reader) {
    }
}
