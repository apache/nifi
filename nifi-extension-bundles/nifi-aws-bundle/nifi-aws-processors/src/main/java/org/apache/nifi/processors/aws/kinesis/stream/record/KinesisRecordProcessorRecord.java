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
    private final RecordReaderFactory readerFactory;
    private final RecordSetWriterFactory writerFactory;
    private final Map<String, String> schemaRetrievalVariables;
    private final RecordConverter recordConverter;

    private FlowFileState currentFlowFileState;

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
        if (currentFlowFileState != null) {
            // this may happen if the previous processing has not been completed successfully, close the leftover state
            closeSafe(currentFlowFileState, "FlowFile State");
            currentFlowFileState = null;
        }
    }

    @Override
    void finishProcessingRecords(final ProcessSession session, final List<FlowFile> flowFiles, final StopWatch stopWatch) {
        super.finishProcessingRecords(session, flowFiles, stopWatch);
        try {
            if (currentFlowFileState == null) {
                return;
            }
            if (!flowFiles.contains(currentFlowFileState.flowFile)) {
                // this is unexpected, flowFiles have been altered not in this class after the start of processing
                throw new IllegalStateException("%s is not available in provided FlowFiles [%d]".formatted(currentFlowFileState.flowFile, flowFiles.size()));
            }
            completeFlowFile(flowFiles, session, stopWatch);
        } catch (final FlowFileCompletionException e) {
            if (!currentFlowFileState.containsDataFromExactlyOneKinesisRecord()) {
                throw new KinesisBatchUnrecoverableException(e.getMessage(), e);
            }
            final boolean removeCurrentStateFlowFileIfAvailable = true;
            final KinesisClientRecord kinesisRecord = currentFlowFileState.lastSuccessfulWriteInfo.kinesisRecord;
            final byte[] data = getData(kinesisRecord);
            outputRawRecordOnException(removeCurrentStateFlowFileIfAvailable, flowFiles, session, data, kinesisRecord, e);
        } finally {
            currentFlowFileState = null;
        }
    }

    @Override
    void processRecord(final List<FlowFile> flowFiles, final KinesisClientRecord kinesisRecord,
                       final ProcessSession session, final StopWatch stopWatch) {
        if (currentFlowFileState != null && !flowFiles.contains(currentFlowFileState.flowFile)) {
            // this is unexpected, flowFiles have been altered not in this class after the start of processing
            throw new IllegalStateException("%s is not available in provided FlowFiles [%d]".formatted(currentFlowFileState.flowFile, flowFiles.size()));
        }

        final byte[] data = getData(kinesisRecord);

        try (final InputStream in = new ByteArrayInputStream(data);
             final RecordReader reader = readerFactory.createRecordReader(schemaRetrievalVariables, in, data.length, getLogger())
        ) {
            Record intermediateRecord;
            final PushBackRecordSet recordSet = new PushBackRecordSet(reader.createRecordSet());
            while ((intermediateRecord = recordSet.next()) != null) {
                final Record outputRecord = recordConverter.convert(intermediateRecord, kinesisRecord, getStreamName(), getKinesisShardId());
                if (currentFlowFileState == null) {
                    // writer schema is determined by some, usually the first record
                    currentFlowFileState = initializeState(session, outputRecord, null);
                    flowFiles.add(currentFlowFileState.flowFile);
                }

                if (!DataTypeUtils.isRecordTypeCompatible(currentFlowFileState.recordSchema, outputRecord, false)) {
                    // subsequent records may have different schema. if so, try complete current FlowFile and start a new one with wider schema to continue
                    completeFlowFile(flowFiles, session, stopWatch);
                    currentFlowFileState = initializeState(session, outputRecord, currentFlowFileState);
                    flowFiles.add(currentFlowFileState.flowFile);
                }

                currentFlowFileState.write(outputRecord, kinesisRecord);
            }
        } catch (final MalformedRecordException | IOException | SchemaNotFoundException | IllegalTypeConversionException | FlowFileCompletionException e) {
            if (e instanceof FlowFileCompletionException && currentFlowFileState != null && !currentFlowFileState.containsDataFromExactlyOneKinesisRecord())  {
                // in case of multiple Kinesis Records in FlowFile, the whole batch needs to be failed, otherwise data can be lost
                throw new KinesisBatchUnrecoverableException(e.getMessage(), e);
            }
            // write raw Kinesis Record to the parse failure relationship
            getLogger().error("Failed to parse message from Kinesis Stream using configured Record Reader and Writer due to {}",
                    e.getLocalizedMessage(), e);
            final boolean removeCurrentStateFlowFileIfAvailable = currentFlowFileState != null && currentFlowFileState.isFlowFileEmpty();
            outputRawRecordOnException(removeCurrentStateFlowFileIfAvailable, flowFiles, session, data, kinesisRecord, e);
            if (removeCurrentStateFlowFileIfAvailable) {
                currentFlowFileState = null;
            }
        }

        if (getLogger().isDebugEnabled()) {
            getLogger().debug("Sequence No: {}, Partition Key: {}, Data: {}",
                    kinesisRecord.sequenceNumber(), kinesisRecord.partitionKey(), BASE_64_ENCODER.encodeToString(data));
        }
    }

    private static byte[] getData(final KinesisClientRecord kinesisRecord) {
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
            throws FlowFileCompletionException {
        if (currentFlowFileState.isFlowFileEmpty()) {
            flowFiles.remove(currentFlowFileState.flowFile);
            session.remove(currentFlowFileState.flowFile);
            closeSafe(currentFlowFileState, "FlowFile State");
            return;
        }
        try {
            currentFlowFileState.writer.finishRecordSet();
            closeSafe(currentFlowFileState, "FlowFile State");
            reportProvenance(session, currentFlowFileState.flowFile, null, null, stopWatch);

            final Map<String, String> attributes = getDefaultAttributes(currentFlowFileState.lastSuccessfulWriteInfo.kinesisRecord);
            attributes.put("record.count", String.valueOf(currentFlowFileState.lastSuccessfulWriteInfo.writeResult.getRecordCount()));
            attributes.put(CoreAttributes.MIME_TYPE.key(), currentFlowFileState.writer.getMimeType());
            attributes.putAll(currentFlowFileState.lastSuccessfulWriteInfo.writeResult.getAttributes());
            final int flowFileIndex = flowFiles.indexOf(currentFlowFileState.flowFile);
            flowFiles.set(flowFileIndex, session.putAllAttributes(currentFlowFileState.flowFile, attributes));
        } catch (final IOException e) {
            flowFiles.remove(currentFlowFileState.flowFile);
            session.remove(currentFlowFileState.flowFile);
            closeSafe(currentFlowFileState, "FlowFile State");
            final String message = "Failed to complete a FlowFile containing records from Stream Name: %s, Shard Id: %s, Sequence/Subsequence No range: [%s/%d, %s/%d)".formatted(
                    getStreamName(),
                    getKinesisShardId(),
                    currentFlowFileState.firstSuccessfulWriteInfo.kinesisRecord.sequenceNumber(),
                    currentFlowFileState.firstSuccessfulWriteInfo.kinesisRecord.subSequenceNumber(),
                    currentFlowFileState.lastSuccessfulWriteInfo.kinesisRecord.sequenceNumber(),
                    currentFlowFileState.lastSuccessfulWriteInfo.kinesisRecord.subSequenceNumber()
            );
            throw new FlowFileCompletionException(message, e);
        }
    }

    private void outputRawRecordOnException(final boolean removeCurrentStateFlowFileIfAvailable,
                                            final List<FlowFile> flowFiles, final ProcessSession session,
                                            final byte[] data, final KinesisClientRecord kinesisRecord, final Exception e) {
        if (removeCurrentStateFlowFileIfAvailable && currentFlowFileState != null) {
            closeSafe(currentFlowFileState, "FlowFile State");
            session.remove(currentFlowFileState.flowFile);
            flowFiles.remove(currentFlowFileState.flowFile);
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
                getLogger().warn("Failed to close {}", closeableName, e);
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

        @SuppressWarnings("BooleanMethodIsAlwaysInverted")
        public boolean containsDataFromExactlyOneKinesisRecord() {
            return !isFlowFileEmpty() && firstSuccessfulWriteInfo.kinesisRecord == lastSuccessfulWriteInfo.kinesisRecord;
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

    private static class FlowFileCompletionException extends Exception {
        public FlowFileCompletionException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }
}