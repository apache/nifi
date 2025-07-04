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
package org.apache.nifi.processors.aws.kinesis.stream.record;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processors.aws.kinesis.stream.ConsumeKinesisStream;
import org.apache.nifi.processors.aws.kinesis.stream.pause.RecordProcessorBlocker;
import org.apache.nifi.processors.aws.kinesis.stream.record.converter.RecordConverter;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.PushBackRecordSet;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.serialization.record.util.IllegalTypeConversionException;
import org.apache.nifi.util.StopWatch;
import org.jetbrains.annotations.NotNull;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class KinesisRecordProcessorRecord extends AbstractKinesisRecordProcessor {
    final RecordReaderFactory readerFactory;
    final RecordSetWriterFactory writerFactory;
    final Map<String, String> schemaRetrievalVariables;

    private FlowFileState flowFileState;
    private final RecordConverter recordConverter;

    private KinesisClientRecord failedKinesisRecordCausingDataLoss = null;
    private CompleteFlowFileMultipleKinesisRecordException failedKinesisRecordCausingDataLossException = null;

    public KinesisRecordProcessorRecord(final ProcessSessionFactory sessionFactory, final ComponentLog log, final String streamName,
                                        final String endpointPrefix, final String kinesisEndpoint,
                                        final long checkpointIntervalMillis, final long retryWaitMillis,
                                        final int numRetries, final DateTimeFormatter dateTimeFormatter,
                                        final RecordReaderFactory readerFactory, final RecordSetWriterFactory writerFactory,
                                        final RecordConverter recordConverter, final RecordProcessorBlocker recordProcessorBlocker) {
        super(sessionFactory, log, streamName, endpointPrefix, kinesisEndpoint, checkpointIntervalMillis, retryWaitMillis,
                numRetries, dateTimeFormatter, recordProcessorBlocker);
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;

        schemaRetrievalVariables = Collections.singletonMap(KINESIS_RECORD_SCHEMA_KEY, streamName);
        this.recordConverter = recordConverter;
    }

    @Override
    void startProcessingRecords() {
        super.startProcessingRecords();
        if (flowFileState != null) {
            getLogger().warn("FlowFile State is not null at the start of processing records, this is not expected.");
            closeSafe(flowFileState, "FlowFile State");
            flowFileState = null;
        }
    }

    @Override
    void finishProcessingRecords(final ProcessSession session, final List<FlowFile> flowFiles, final StopWatch stopWatch) {
        super.finishProcessingRecords(session, flowFiles, stopWatch);
        try {
            if (flowFileState == null) {
                return;
            }
            if (!flowFiles.contains(flowFileState.flowFile)) {
                getLogger().warn("Currently processed FlowFile is no longer available at processing end, this is not expected.", flowFiles);
                closeSafe(flowFileState, "FlowFile State");
                return;
            }
            completeFlowFile(flowFiles, session, stopWatch);
        } catch (CompleteFlowFileSingleKinesisRecordException e) {
            final boolean removeFirstFlowFileIfAvailable = true;
            final KinesisClientRecord kinesisRecord = e.kinesisClientRecord;
            final byte[] data = getData(kinesisRecord);
            outputRawRecordOnException(removeFirstFlowFileIfAvailable, flowFiles, session, data, kinesisRecord, e);
        } finally {
            flowFileState = null;
            failedKinesisRecordCausingDataLoss = null;
            failedKinesisRecordCausingDataLossException = null;
        }
    }

    @Override
    void processRecord(final List<FlowFile> flowFiles, final KinesisClientRecord kinesisRecord,
                       final ProcessSession session, final StopWatch stopWatch) {
        if (flowFileState != null && !flowFiles.contains(flowFileState.flowFile)) {
            getLogger().warn("Currently processed FlowFile is no longer available, this is not expected.", flowFiles);
            closeSafe(flowFileState, "FlowFile State");
            flowFileState = null;
        }
        if (kinesisRecord == failedKinesisRecordCausingDataLoss)  {
            // AbstractKinesisRecordProcessor does retry processing of failed records. however in case of CompleteFlowFileMultipleKinesisRecordException it is impossible to determine the state of
            // the affected FlowFile and replay all records that were in it. To prevent data loss, the exception needs to be rethrown until it is given up by the abstract processor. still there may be
            // other cases where we end up in undetermined state.
            throw failedKinesisRecordCausingDataLossException;
        }
        failedKinesisRecordCausingDataLoss = null;
        failedKinesisRecordCausingDataLossException = null;

        final byte[] data = getData(kinesisRecord);

        try (final InputStream in = new ByteArrayInputStream(data);
             final RecordReader reader = readerFactory.createRecordReader(schemaRetrievalVariables, in, data.length, getLogger())
        ) {
            Record intermediateRecord;
            final PushBackRecordSet recordSet = new PushBackRecordSet(reader.createRecordSet());
            while ((intermediateRecord = recordSet.next()) != null) {
                final Record outputRecord = recordConverter.convert(intermediateRecord, kinesisRecord, getStreamName(), getKinesisShardId());
                if (flowFileState == null) {
                    // writer schema is determined by some, usually the first record
                    flowFileState = initializeState(session, outputRecord, null);
                    flowFiles.add(flowFileState.flowFile);
                }

                if (!DataTypeUtils.isRecordTypeCompatible(flowFileState.recordSchema, outputRecord, false)) {
                    // subsequent records may have different schema. if so, try complete current FlowFile and start a new one with wider schema to continue
                    completeFlowFile(flowFiles, session, stopWatch);
                    flowFileState = initializeState(session, outputRecord, flowFileState);
                    flowFiles.add(flowFileState.flowFile);
                }

                flowFileState.write(outputRecord, kinesisRecord);
            }
        } catch (final CompleteFlowFileMultipleKinesisRecordException dataLossException) {
            failedKinesisRecordCausingDataLoss = kinesisRecord;
            failedKinesisRecordCausingDataLossException = dataLossException;
            throw dataLossException;
        } catch (final MalformedRecordException | IOException | SchemaNotFoundException | IllegalTypeConversionException | CompleteFlowFileSingleKinesisRecordException e) {
            // write raw Kinesis Record to the parse failure relationship
            getLogger().error("Failed to parse message from Kinesis Stream using configured Record Reader and Writer due to {}",
                    e.getLocalizedMessage(), e);
            // nothing was written, file can be removed
            final boolean removeCurrentStateFlowFileIfAvailable = flowFileState != null && flowFileState.isFlowFileEmpty();
            outputRawRecordOnException(removeCurrentStateFlowFileIfAvailable, flowFiles, session, data, kinesisRecord, e);
            if (removeCurrentStateFlowFileIfAvailable) {
                flowFileState = null;
            }
        }

        if (getLogger().isDebugEnabled()) {
            getLogger().debug("Sequence No: {}, Partition Key: {}, Data: {}",
                    kinesisRecord.sequenceNumber(), kinesisRecord.partitionKey(), BASE_64_ENCODER.encodeToString(data));
        }
    }

    private static byte @NotNull [] getData(final KinesisClientRecord kinesisRecord) {
        final ByteBuffer dataBuffer = kinesisRecord.data();
        final byte[] data = dataBuffer != null ? new byte[dataBuffer.remaining()] : new byte[0];
        if (dataBuffer != null) {
            dataBuffer.get(data);
        }
        return data;
    }

    /**
     * Initializes the FlowFile state for the current processing. This includes creating a new FlowFile, initializing the RecordSetWriter, and setting up the output stream.
     * In case of an exception during initialization, the FlowFile is removed from the session and the resources are closed properly.
     */
    private FlowFileState initializeState(final ProcessSession session, final Record outputRecord, final FlowFileState previousFlowFileState) throws IOException, SchemaNotFoundException {
        FlowFile flowFile = null;
        OutputStream outputStream = null;
        RecordSetWriter writer = null;
        try {
            flowFile = session.create();
            // If we have a previous schema, we need to merge it with the current schema to ensure that the writer can handle all fields from both schemas going forward.
            // There is an assumption here that all the records are expected by the downstream to have the same-ish schema. It is possible to imagine a scenario where records with different schemas
            // should be written to separate FlowFiles.
            final RecordSchema newReadSchema = Optional.ofNullable(previousFlowFileState).map(it ->
                    DataTypeUtils.merge(it.recordSchema, outputRecord.getSchema())).orElse(outputRecord.getSchema());

            final RecordSchema writeSchema = writerFactory.getSchema(schemaRetrievalVariables, newReadSchema);
            outputStream = session.write(flowFile);
            writer = writerFactory.createWriter(getLogger(), writeSchema, outputStream, flowFile);
            writer.beginRecordSet();
        } catch (final Exception e) {
            if (flowFile != null) {
                session.remove(flowFile);
            }
            closeSafe(writer, "Record Writer");
            closeSafe(outputStream, "Output Stream");
            throw e;
        }
        return new FlowFileState(flowFile, writer, outputStream, outputRecord.getSchema());
    }

    private void completeFlowFile(final List<FlowFile> flowFiles, final ProcessSession session, final StopWatch stopWatch)
            throws CompleteFlowFileSingleKinesisRecordException {
        if (flowFileState.isFlowFileEmpty()) {
            flowFiles.remove(flowFileState.flowFile);
            session.remove(flowFileState.flowFile);
            closeSafe(flowFileState, "FlowFile State");
            return;
        }
        try {
            flowFileState.writer.finishRecordSet();
            closeSafe(flowFileState, "FlowFile State");
            reportProvenance(session, flowFileState.flowFile, null, null, stopWatch);

            final Map<String, String> attributes = getDefaultAttributes(flowFileState.lastSuccessfulWriteInfo.kinesisRecord);
            attributes.put("record.count", String.valueOf(flowFileState.lastSuccessfulWriteInfo.writeResult.getRecordCount()));
            attributes.put(CoreAttributes.MIME_TYPE.key(), flowFileState.writer.getMimeType());
            attributes.putAll(flowFileState.lastSuccessfulWriteInfo.writeResult.getAttributes());
            final int flowFileIndex = flowFiles.indexOf(flowFileState.flowFile);
            flowFiles.set(flowFileIndex, session.putAllAttributes(flowFileState.flowFile, attributes));
        } catch (final IOException e) {
            flowFiles.remove(flowFileState.flowFile);
            session.remove(flowFileState.flowFile);
            closeSafe(flowFileState, "FlowFile State");
            final String message = "Failed to complete a FlowFile containing records from Stream Name: %s, Shard Id: %s, Sequence/Subsequence No range: [%s/%d, %s/%d)".formatted(
                    getStreamName(),
                    getKinesisShardId(),
                    flowFileState.firstSuccessfulWriteInfo.kinesisRecord.sequenceNumber(),
                    flowFileState.firstSuccessfulWriteInfo.kinesisRecord.subSequenceNumber(),
                    flowFileState.lastSuccessfulWriteInfo.kinesisRecord.sequenceNumber(),
                    flowFileState.lastSuccessfulWriteInfo.kinesisRecord.subSequenceNumber()
            );
            final boolean isSingleKinesisRecordInFlowFile = flowFileState.firstSuccessfulWriteInfo.kinesisRecord.equals(flowFileState.lastSuccessfulWriteInfo.kinesisRecord);
            if (isSingleKinesisRecordInFlowFile) {
                // in case of a single Kinesis Record in FlowFile, so we can route it to failure relationship
                throw new CompleteFlowFileSingleKinesisRecordException(message, e, flowFileState.firstSuccessfulWriteInfo.kinesisRecord);
            }
            // in case of multiple Kinesis Records the whole batch needs to be failed, otherwise data can be lost
            throw new CompleteFlowFileMultipleKinesisRecordException(message, e);
        }
    }

    private void outputRawRecordOnException(final boolean removeCurrentStateFlowFileIfAvailable,
                                            final List<FlowFile> flowFiles, final ProcessSession session,
                                            final byte[] data, final KinesisClientRecord kinesisRecord, final Exception e) {
        if (removeCurrentStateFlowFileIfAvailable && flowFileState != null) {
            closeSafe(flowFileState, "FlowFile State");
            session.remove(flowFileState.flowFile);
            flowFiles.remove(flowFileState.flowFile);
        }
        FlowFile failed = session.create();
        session.write(failed, o -> o.write(data));
        final Map<String, String> attributes = getDefaultAttributes(kinesisRecord);
        final Throwable c = e.getCause() != null ? e.getCause() : e;
        attributes.put("record.error.message", (c.getLocalizedMessage() != null) ? c.getLocalizedMessage() : c.getClass().getCanonicalName() + " Thrown");
        failed = session.putAllAttributes(failed, attributes);
        transferTo(ConsumeKinesisStream.REL_PARSE_FAILURE, session, 0, 0, Collections.singletonList(failed));
    }

    private Map<String, String> getDefaultAttributes(final KinesisClientRecord kinesisRecord) {
        final String partitionKey = kinesisRecord.partitionKey();
        final String sequenceNumber = kinesisRecord.sequenceNumber();
        final long subSequenceNumber = kinesisRecord.subSequenceNumber();
        final Instant approximateArrivalTimestamp = kinesisRecord.approximateArrivalTimestamp();
        return getDefaultAttributes(sequenceNumber, subSequenceNumber, partitionKey, approximateArrivalTimestamp);
    }

    void closeSafe(final Closeable closeable, final String closeableName) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (final IOException e) {
                getLogger().warn("Failed to close {} due to {}", closeableName, e.getLocalizedMessage(), e);
            }
        }
    }

    private record SuccessfulWriteInfo(KinesisClientRecord kinesisRecord, WriteResult writeResult) { }

    private class FlowFileState implements Closeable {
        private final FlowFile flowFile;
        private final RecordSetWriter writer;
        private final OutputStream outputStream;
        private final RecordSchema recordSchema;
        private SuccessfulWriteInfo firstSuccessfulWriteInfo;
        private SuccessfulWriteInfo lastSuccessfulWriteInfo;

        public FlowFileState(final FlowFile flowFile, final RecordSetWriter writer, final OutputStream outputStream, final RecordSchema recordSchema) {
            this.flowFile = flowFile;
            this.writer = writer;
            this.outputStream = outputStream;
            this.recordSchema = recordSchema;
        }

        public boolean isFlowFileEmpty() {
            return lastSuccessfulWriteInfo == null;
        }

        public void write(final Record outputRecord, final KinesisClientRecord kinesisRecord) throws IOException {
            final WriteResult writeResult = writer.write(outputRecord);
            firstSuccessfulWriteInfo = firstSuccessfulWriteInfo == null
                    ? new SuccessfulWriteInfo(kinesisRecord, writeResult)
                    : firstSuccessfulWriteInfo;
            lastSuccessfulWriteInfo = new SuccessfulWriteInfo(kinesisRecord, writeResult);
        }

        public void close() {
            closeSafe(writer, "Record Writer");
            closeSafe(outputStream, "Output Stream");
        }
    }

    /**
     * Exception thrown when there is an issue completing a FlowFile that contains a single Kinesis Record. This exception is used to case when the record can be routed to failure relationship and/or
     * retried.
     */
    private static class CompleteFlowFileSingleKinesisRecordException extends Exception {
        private final KinesisClientRecord kinesisClientRecord;

        public CompleteFlowFileSingleKinesisRecordException(final String message, final Throwable cause, final KinesisClientRecord kinesisClientRecord) {
            super(message, cause);
            this.kinesisClientRecord = kinesisClientRecord;
        }
    }

    /**
     * Exception thrown when there is an issue completing a FlowFile that contains multiple Kinesis Records. This exception is used to case when there is possibility of loosing data.
     */
    private static class CompleteFlowFileMultipleKinesisRecordException extends RuntimeException {
        public CompleteFlowFileMultipleKinesisRecordException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }
}