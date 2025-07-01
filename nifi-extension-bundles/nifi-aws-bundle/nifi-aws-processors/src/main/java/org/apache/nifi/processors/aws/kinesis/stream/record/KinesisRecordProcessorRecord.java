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
import org.apache.nifi.serialization.record.util.IllegalTypeConversionException;
import org.apache.nifi.util.StopWatch;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class KinesisRecordProcessorRecord extends AbstractKinesisRecordProcessor {
    final RecordReaderFactory readerFactory;
    final RecordSetWriterFactory writerFactory;
    final Map<String, String> schemaRetrievalVariables;

    private RecordSetWriter writer;
    private OutputStream outputStream;
    private final RecordConverter recordConverter;
    private SuccessfulWriteInfo firstSuccessfulWriteInfo;
    private SuccessfulWriteInfo lastSuccessfulWriteInfo;

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
        outputStream = null;
        writer = null;
    }

    @Override
    void processRecord(final List<FlowFile> flowFiles, final KinesisClientRecord kinesisRecord, final boolean lastRecord,
                       final ProcessSession session, final StopWatch stopWatch) {
        if (flowFiles.size() > 1) {
            // historically this code has assumed that the FlowFile it operates on is the first one in the list.
            // changing this behavior would exceed the scope of the bugfix change this comment was added in.
            // leaving this comment to inform future maintainers that assumption is rather weak and should be refactored
            getLogger().warn("More than one FlowFile is being processed at once, this is not expected.", flowFiles);
        }
        final ByteBuffer dataBuffer = kinesisRecord.data();
        byte[] data = dataBuffer != null ? new byte[dataBuffer.remaining()] : new byte[0];
        if (dataBuffer != null) {
            dataBuffer.get(data);
        }

        try (final InputStream in = new ByteArrayInputStream(data);
             final RecordReader reader = readerFactory.createRecordReader(schemaRetrievalVariables, in, data.length, getLogger())
        ) {
            Record intermediateRecord;
            final PushBackRecordSet recordSet = new PushBackRecordSet(reader.createRecordSet());
            while ((intermediateRecord = recordSet.next()) != null) {
                Record outputRecord = recordConverter.convert(intermediateRecord, kinesisRecord, getStreamName(), getKinesisShardId());
                if (flowFiles.isEmpty()) {
                    final FlowFile createdFlowFile = session.create();
                    flowFiles.add(createdFlowFile);

                    // initialize the writer when the first record is read.
                    createWriter(createdFlowFile, session, outputRecord);
                }

                final WriteResult writeResult = writer.write(outputRecord);
                firstSuccessfulWriteInfo = firstSuccessfulWriteInfo == null
                        ? new SuccessfulWriteInfo(kinesisRecord, writeResult)
                        : firstSuccessfulWriteInfo;
                lastSuccessfulWriteInfo = new SuccessfulWriteInfo(kinesisRecord, writeResult);
            }
        } catch (final MalformedRecordException | IOException | SchemaNotFoundException | IllegalTypeConversionException e) {
            // write raw Kinesis Record to the parse failure relationship
            getLogger().error("Failed to parse message from Kinesis Stream using configured Record Reader and Writer due to {}",
                    e.getLocalizedMessage(), e);
            // nothing was written, file can be removed
            final boolean removeFirstFlowFileIfAvailable = firstSuccessfulWriteInfo == null;
            outputRawRecordOnException(removeFirstFlowFileIfAvailable, flowFiles, session, data, kinesisRecord, e);
        }

        // complete the FlowFile if there are no more incoming Kinesis Records and no more records in this RecordSet
        if (lastRecord && !flowFiles.isEmpty()) {
            try {
                completeFlowFile(flowFiles, session, lastSuccessfulWriteInfo.writeResult.getRecordCount(), lastSuccessfulWriteInfo.writeResult, lastSuccessfulWriteInfo.kinesisRecord, stopWatch);
            } catch (Exception e) {
                getLogger().error("Failed to complete a FlowFile, dropped records from Stream Name: {}, Shard Id: {}, Sequence/Subsequence No range: [{}/{}, {}/{}), due to {}",
                        getStreamName(),
                        getKinesisShardId(),
                        firstSuccessfulWriteInfo.kinesisRecord.sequenceNumber(),
                        lastSuccessfulWriteInfo.kinesisRecord.sequenceNumber(),
                        e.getLocalizedMessage(),
                        e);
                final boolean removeFirstFlowFileIfAvailable = true;
                outputRawRecordOnException(removeFirstFlowFileIfAvailable, flowFiles, session, data, kinesisRecord, e);
            }
            firstSuccessfulWriteInfo = null;
            lastSuccessfulWriteInfo = null;
        }

        if (getLogger().isDebugEnabled()) {
            getLogger().debug("Sequence No: {}, Partition Key: {}, Data: {}",
                    kinesisRecord.sequenceNumber(), kinesisRecord.partitionKey(), BASE_64_ENCODER.encodeToString(data));
        }
    }

    private void createWriter(final FlowFile flowFile, final ProcessSession session, final Record outputRecord)
            throws IOException, SchemaNotFoundException {

        final RecordSchema readerSchema = outputRecord.getSchema();
        final RecordSchema writeSchema = writerFactory.getSchema(schemaRetrievalVariables, readerSchema);
        outputStream = session.write(flowFile);
        writer = writerFactory.createWriter(getLogger(), writeSchema, outputStream, flowFile);
        writer.beginRecordSet();
    }

    private void completeFlowFile(final List<FlowFile> flowFiles, final ProcessSession session, final int recordCount,
                                  final WriteResult writeResult, final KinesisClientRecord lastRecord, final StopWatch stopWatch)
            throws IOException {

        try {
            writer.finishRecordSet();
        } catch (IOException e) {
            getLogger().error("Failed to finish record output due to {}", e.getLocalizedMessage(), e);
            session.remove(flowFiles.getFirst());
            flowFiles.removeFirst();
            throw e;
        } finally {
            try {
                writer.close();
                outputStream.close();
            } catch (final IOException e) {
                getLogger().warn("Failed to close Record Writer due to {}", e.getLocalizedMessage(), e);
            }
        }

        reportProvenance(session, flowFiles.getFirst(), null, null, stopWatch);

        final Map<String, String> attributes = getDefaultAttributes(lastRecord);
        attributes.put("record.count", String.valueOf(recordCount));
        attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
        attributes.putAll(writeResult.getAttributes());
        flowFiles.set(0, session.putAllAttributes(flowFiles.getFirst(), attributes));

        writer = null;
        outputStream = null;
    }

    private void outputRawRecordOnException(final boolean removeFirstFlowFileIfAvailable,
                                            final List<FlowFile> flowFiles, final ProcessSession session,
                                            final byte[] data, final KinesisClientRecord kinesisRecord, final Exception e) {
        if (removeFirstFlowFileIfAvailable && !flowFiles.isEmpty()) {
            final FlowFile flowFile = flowFiles.removeFirst();
            session.remove(flowFile);
            if (writer != null) {
                try {
                    writer.close();
                    outputStream.close();
                } catch (IOException ioe) {
                    getLogger().warn("Failed to close Record Writer due to {}", ioe.getLocalizedMessage(), ioe);
                }
            }
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
        final Instant approximateArrivalTimestamp = kinesisRecord.approximateArrivalTimestamp();
        return getDefaultAttributes(sequenceNumber, partitionKey, approximateArrivalTimestamp);
    }

    private record SuccessfulWriteInfo(KinesisClientRecord kinesisRecord, WriteResult writeResult) { }
}