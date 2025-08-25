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
package org.apache.nifi.processors.aws.kinesis;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyMap;
import static org.apache.nifi.processors.aws.kinesis.ConsumeKinesisAttributes.RECORD_COUNT;
import static org.apache.nifi.processors.aws.kinesis.ConsumeKinesisAttributes.RECORD_ERROR_MESSAGE;

final class ReaderRecordProcessor {

    private final RecordReaderFactory recordReaderFactory;
    private final RecordSetWriterFactory recordWriterFactory;
    private final ComponentLog logger;

    ReaderRecordProcessor(
            final RecordReaderFactory recordReaderFactory,
            final RecordSetWriterFactory recordWriterFactory,
            final ComponentLog logger) {
        this.recordReaderFactory = recordReaderFactory;
        this.recordWriterFactory = recordWriterFactory;
        this.logger = logger;
    }

    ProcessingResult processRecords(
            final ProcessSession session,
            final String streamName,
            final String shardId,
            final List<KinesisClientRecord> records) {
        final List<FlowFile> successFlowFiles = new ArrayList<>();
        final List<FlowFile> failureFlowFiles = new ArrayList<>();

        ActiveFlowFile activeFlowFile = null;

        for (final KinesisClientRecord kinesisRecord : records) {
            final byte[] data = new byte[kinesisRecord.data().remaining()];
            kinesisRecord.data().get(data);

            try (final InputStream in = new ByteArrayInputStream(data);
                 final RecordReader reader = recordReaderFactory.createRecordReader(emptyMap(), in, data.length, logger)) {

                Record record;
                while ((record = reader.nextRecord()) != null) {
                    final RecordSchema writeSchema = recordWriterFactory.getSchema(emptyMap(), record.getSchema());

                    if (activeFlowFile == null) {
                        activeFlowFile = ActiveFlowFile.startNewFile(logger, session, recordWriterFactory, writeSchema, streamName, shardId);
                    } else if (!writeSchema.equals(activeFlowFile.schema())) {
                        // If the write schema has changed, we need to complete the current FlowFile and start a new one.
                        final FlowFile completedFlowFile = activeFlowFile.complete();
                        successFlowFiles.add(completedFlowFile);

                        activeFlowFile = ActiveFlowFile.startNewFile(logger, session, recordWriterFactory, writeSchema, streamName, shardId);
                    }

                    activeFlowFile.writeRecord(record, kinesisRecord);
                }
            } catch (final IOException | MalformedRecordException | SchemaNotFoundException e) {
                logger.error("Failed to parse record from Kinesis stream using configured Record Reader", e);
                final FlowFile failureFlowFile = createParseFailureFlowFile(session, streamName, shardId, kinesisRecord, e);
                failureFlowFiles.add(failureFlowFile);
            }
        }

        if (activeFlowFile != null) {
            final FlowFile completedFlowFile = activeFlowFile.complete();
            successFlowFiles.add(completedFlowFile);
        }

        return new ProcessingResult(successFlowFiles, failureFlowFiles);
    }

    private static FlowFile createParseFailureFlowFile(
            final ProcessSession session,
            final String streamName,
            final String shardId,
            final KinesisClientRecord record,
            final Exception e) {
        FlowFile flowFile = session.create();

        record.data().rewind();
        flowFile = session.write(flowFile, out -> Channels.newChannel(out).write(record.data()));

        flowFile = session.putAllAttributes(flowFile, ConsumeKinesisAttributes.fromKinesisRecord(streamName, shardId, record));

        final Throwable cause = e.getCause() != null ? e.getCause() : e;
        final String errorMessage = cause.getLocalizedMessage() != null ? cause.getLocalizedMessage() : cause.getClass().getCanonicalName() + " thrown";
        flowFile = session.putAttribute(flowFile, RECORD_ERROR_MESSAGE, errorMessage);

        return flowFile;
    }

    record ProcessingResult(List<FlowFile> successFlowFiles, List<FlowFile> parseFailureFlowFiles) {
    }

    private static final class ActiveFlowFile {

        private final ComponentLog logger;

        private final ProcessSession session;
        private final FlowFile flowFile;
        private final RecordSetWriter writer;
        private final RecordSchema schema;

        private final String streamName;
        private final String shardId;

        private KinesisClientRecord lastRecord;

        private ActiveFlowFile(
                final ComponentLog logger,
                final ProcessSession session,
                final FlowFile flowFile,
                final RecordSetWriter writer,
                final RecordSchema schema,
                final String streamName,
                final String shardId) {
            this.logger = logger;
            this.session = session;
            this.flowFile = flowFile;
            this.writer = writer;
            this.schema = schema;
            this.streamName = streamName;
            this.shardId = shardId;
        }

        static ActiveFlowFile startNewFile(
                final ComponentLog logger,
                final ProcessSession session,
                final RecordSetWriterFactory recordWriterFactory,
                final RecordSchema writeSchema,
                final String streamName,
                final String shardId) throws SchemaNotFoundException {
            final FlowFile flowFile = session.create();
            final OutputStream outputStream = session.write(flowFile);

            try {
                final RecordSetWriter writer = recordWriterFactory.createWriter(logger, writeSchema, outputStream, flowFile);
                writer.beginRecordSet();

                return new ActiveFlowFile(logger, session, flowFile, writer, writeSchema, streamName, shardId);

            } catch (final SchemaNotFoundException e) {
                logger.debug("Failed to find writeSchema for Kinesis stream record", e);
                try {
                    outputStream.close();
                } catch (final IOException ioe) {
                    logger.warn("Failed to close FlowFile output stream", ioe);
                    e.addSuppressed(ioe);
                }
                throw e;

            } catch (final IOException e) {
                final ProcessException processException = new ProcessException("Failed to create a writer for a FlowFile", e);

                logger.debug("Failed to create a writer for a FlowFile. Stopping Kinesis records processing", e);
                try {
                    outputStream.close();
                } catch (final IOException ioe) {
                    logger.warn("Failed to close FlowFile output stream", ioe);
                    processException.addSuppressed(ioe);
                }
                throw processException;
            }
        }

        RecordSchema schema() {
            return schema;
        }

        void writeRecord(final Record record, final KinesisClientRecord kinesisRecord) {
            try {
                writer.write(record);
            } catch (final IOException e) {
                logger.debug("Failed to write to a FlowFile. Stopping Kinesis records processing", e);
                throw new ProcessException("Failed to write a record into a FlowFile", e);
            }

            lastRecord = kinesisRecord;
        }

        FlowFile complete() {
            try {
                final WriteResult finalResult = writer.finishRecordSet();
                writer.close();

                FlowFile completedFlowFile = session.putAllAttributes(this.flowFile, ConsumeKinesisAttributes.fromKinesisRecord(streamName, shardId, lastRecord));
                completedFlowFile = session.putAllAttributes(completedFlowFile, finalResult.getAttributes());
                completedFlowFile = session.putAttribute(completedFlowFile, RECORD_COUNT, String.valueOf(finalResult.getRecordCount()));
                completedFlowFile = session.putAttribute(completedFlowFile, "mime.type", writer.getMimeType());

                return completedFlowFile;

            } catch (final IOException e) {
                final ProcessException processException = new ProcessException("Failed to complete a FlowFile", e);

                logger.debug("Failed to complete a FlowFile. Stopping Kinesis records processing", e);
                try {
                    writer.close();
                } catch (final IOException ioe) {
                    logger.warn("Failed to close writer", ioe);
                    processException.addSuppressed(ioe);
                }

                throw processException;
            }
        }
    }
}