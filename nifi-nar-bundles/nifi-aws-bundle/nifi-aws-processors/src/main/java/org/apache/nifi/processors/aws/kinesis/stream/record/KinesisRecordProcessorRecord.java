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

import com.amazonaws.services.kinesis.model.Record;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processors.aws.kinesis.stream.ConsumeKinesisStream;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.PushBackRecordSet;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.StopWatch;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class KinesisRecordProcessorRecord extends AbstractKinesisRecordProcessor {
    final RecordReaderFactory readerFactory;
    final RecordSetWriterFactory writerFactory;
    final Map<String, String> schemaRetrievalVariables;

    private RecordSetWriter writer;
    private OutputStream outputStream;

    public KinesisRecordProcessorRecord(final ProcessSessionFactory sessionFactory, final ComponentLog log, final String streamName,
                                        final String endpointPrefix, final String kinesisEndpoint,
                                        final long checkpointIntervalMillis, final long retryWaitMillis,
                                        final int numRetries, final DateTimeFormatter dateTimeFormatter,
                                        final RecordReaderFactory readerFactory, final RecordSetWriterFactory writerFactory) {
        super(sessionFactory, log, streamName, endpointPrefix, kinesisEndpoint, checkpointIntervalMillis, retryWaitMillis,
                numRetries, dateTimeFormatter);
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;

        schemaRetrievalVariables = Collections.singletonMap(KINESIS_RECORD_SCHEMA_KEY, streamName);
    }

    @Override
    void startProcessingRecords() {
        super.startProcessingRecords();
        outputStream = null;
        writer = null;
    }

    @Override
    void processRecord(final List<FlowFile> flowFiles, final Record kinesisRecord, final boolean lastRecord,
                       final ProcessSession session, final StopWatch stopWatch) {
        boolean firstOutputRecord = true;
        int recordCount = 0;
        final byte[] data = kinesisRecord.getData() != null ? kinesisRecord.getData().array() : new byte[0];

        FlowFile flowFile = null;
        try (final InputStream in = new ByteArrayInputStream(data);
             final RecordReader reader = readerFactory.createRecordReader(schemaRetrievalVariables, in, data.length, getLogger())
        ) {
            org.apache.nifi.serialization.record.Record outputRecord;
            final PushBackRecordSet recordSet = new PushBackRecordSet(reader.createRecordSet());
            while ((outputRecord = recordSet.next()) != null) {
                if (flowFiles.isEmpty()) {
                    flowFile = session.create();
                    flowFiles.add(flowFile);

                    // initialize the writer when the first record is read.
                    createWriter(flowFile, session, outputRecord);
                }

                final WriteResult writeResult = writer.write(outputRecord);
                recordCount += writeResult.getRecordCount();

                // complete the FlowFile if there are no more incoming Kinesis Records and no more records in this RecordSet
                if (lastRecord && !recordSet.isAnotherRecord()) {
                    completeFlowFile(flowFiles, session, recordCount, writeResult, kinesisRecord, stopWatch);
                }
                firstOutputRecord = false;
            }
        } catch (final MalformedRecordException | IOException | SchemaNotFoundException e) {
            // write raw Kinesis Record to the parse failure relationship
            getLogger().error("Failed to parse message from Kinesis Stream using configured Record Reader and Writer due to {}",
                    e.getLocalizedMessage(), e);
            outputRawRecordOnException(firstOutputRecord, flowFile, flowFiles, session, data, kinesisRecord, e);
        }

        if (getLogger().isDebugEnabled()) {
            getLogger().debug("Sequence No: {}, Partition Key: {}, Data: {}",
                    kinesisRecord.getSequenceNumber(), kinesisRecord.getPartitionKey(), BASE_64_ENCODER.encodeToString(data));
        }
    }

    private void createWriter(final FlowFile flowFile, final ProcessSession session,
                              final org.apache.nifi.serialization.record.Record outputRecord)
            throws IOException, SchemaNotFoundException {

        final RecordSchema readerSchema = outputRecord.getSchema();
        final RecordSchema writeSchema = writerFactory.getSchema(schemaRetrievalVariables, readerSchema);
        outputStream = session.write(flowFile);
        writer = writerFactory.createWriter(getLogger(), writeSchema, outputStream, flowFile);
        writer.beginRecordSet();
    }

    private void completeFlowFile(final List<FlowFile> flowFiles, final ProcessSession session, final int recordCount,
                                  final WriteResult writeResult, final Record lastRecord, final StopWatch stopWatch)
            throws IOException {

        try {
            writer.finishRecordSet();
        } catch (IOException e) {
            getLogger().error("Failed to finish record output due to {}", e.getLocalizedMessage(), e);
            session.remove(flowFiles.get(0));
            flowFiles.remove(0);
            throw e;
        } finally {
            try {
                writer.close();
                outputStream.close();
            } catch (final IOException e) {
                getLogger().warn("Failed to close Record Writer due to {}", e.getLocalizedMessage(), e);
            }
        }

        reportProvenance(session, flowFiles.get(0), null, null, stopWatch);

        final Map<String, String> attributes = getDefaultAttributes(lastRecord);
        attributes.put("record.count", String.valueOf(recordCount));
        attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
        attributes.putAll(writeResult.getAttributes());
        flowFiles.set(0, session.putAllAttributes(flowFiles.get(0), attributes));

        writer = null;
        outputStream = null;
    }

    private void outputRawRecordOnException(final boolean firstOutputRecord, final FlowFile flowFile,
                                            final List<FlowFile> flowFiles, final ProcessSession session,
                                            final byte[] data, final Record kinesisRecord, final Exception e) {
        if (firstOutputRecord && flowFile != null) {
            session.remove(flowFile);
            flowFiles.remove(0);
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

    private Map<String, String> getDefaultAttributes(final Record kinesisRecord) {
        final String partitionKey = kinesisRecord.getPartitionKey();
        final String sequenceNumber = kinesisRecord.getSequenceNumber();
        final Date approximateArrivalTimestamp = kinesisRecord.getApproximateArrivalTimestamp();
        return getDefaultAttributes(sequenceNumber, partitionKey, approximateArrivalTimestamp);
    }
}