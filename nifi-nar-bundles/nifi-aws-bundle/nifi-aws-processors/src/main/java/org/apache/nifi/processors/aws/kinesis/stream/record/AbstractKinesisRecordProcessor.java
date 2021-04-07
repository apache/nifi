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

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processors.aws.kinesis.stream.ConsumeKinesisStream;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.StringUtils;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public abstract class AbstractKinesisRecordProcessor implements IRecordProcessor {
    public static final String AWS_KINESIS_SHARD_ID = "aws.kinesis.shard.id";

    public static final String AWS_KINESIS_SEQUENCE_NUMBER = "aws.kinesis.sequence.number";

    public static final String AWS_KINESIS_PARTITION_KEY = "aws.kinesis.partition.key";

    public static final String AWS_KINESIS_APPROXIMATE_ARRIVAL_TIMESTAMP = "aws.kinesis.approximate.arrival.timestamp";

    public static final String KINESIS_RECORD_SCHEMA_KEY = "kinesis.name";

    static final Base64.Encoder BASE_64_ENCODER = Base64.getEncoder();

    private final ProcessSessionFactory sessionFactory;
    private final ComponentLog log;
    private final String streamName;
    private final String transitUriPrefix;

    private final long checkpointIntervalMillis;
    private final long retryWaitMillis;
    private final int numRetries;
    private final DateTimeFormatter dateTimeFormatter;

    private String kinesisShardId;
    private long nextCheckpointTimeInMillis;

    private boolean processingRecords = false;

    AbstractKinesisRecordProcessor(final ProcessSessionFactory sessionFactory, final ComponentLog log, final String streamName,
                                   final String endpointPrefix, final String kinesisEndpoint,
                                   final long checkpointIntervalMillis, final long retryWaitMillis,
                                   final int numRetries, final DateTimeFormatter dateTimeFormatter) {
        this.sessionFactory = sessionFactory;
        this.log = log;
        this.streamName = streamName;
        this.checkpointIntervalMillis = checkpointIntervalMillis;
        this.retryWaitMillis = retryWaitMillis;
        this.numRetries = numRetries;
        this.dateTimeFormatter = dateTimeFormatter;

        this.transitUriPrefix = StringUtils.isBlank(kinesisEndpoint) ? String.format("http://%s.amazonaws.com", endpointPrefix) : kinesisEndpoint;
    }

    @Override
    public void initialize(final InitializationInput initializationInput) {
        if (initializationInput.getPendingCheckpointSequenceNumber() != null) {
            log.warn("Initializing record processor for stream: {} / shard {}; from sequence number: {}; indicates previously uncheckpointed sequence number: {}",
                    streamName, initializationInput.getShardId(), initializationInput.getExtendedSequenceNumber(), initializationInput.getPendingCheckpointSequenceNumber());
        } else {
            log.debug("Initializing record processor for stream: {} / shard: {}; from sequence number: {}",
                    streamName, initializationInput.getShardId(), initializationInput.getExtendedSequenceNumber());
        }

        this.kinesisShardId = initializationInput.getShardId();

        // ensure we don't immediately checkpoint
        this.nextCheckpointTimeInMillis = System.currentTimeMillis() + checkpointIntervalMillis;
    }

    @Override
    public void processRecords(final ProcessRecordsInput processRecordsInput) {
        if (log.isDebugEnabled()) {
            log.debug("Processing {} records from {}; cache entry: {}; cache exit: {}; millis behind latest: {}",
                    processRecordsInput.getRecords().size(), kinesisShardId,
                    processRecordsInput.getCacheEntryTime() != null ? dateTimeFormatter.format(processRecordsInput.getCacheEntryTime().atZone(ZoneId.systemDefault())) : null,
                    processRecordsInput.getCacheExitTime() != null ? dateTimeFormatter.format(processRecordsInput.getCacheExitTime().atZone(ZoneId.systemDefault())) : null,
                    processRecordsInput.getMillisBehindLatest());
        }

        ProcessSession session = null;
        try {
            final List<Record> records = processRecordsInput.getRecords();
            if (!records.isEmpty()) {
                final List<FlowFile> flowFiles = new ArrayList<>(records.size());
                final StopWatch stopWatch = new StopWatch(true);
                session = sessionFactory.createSession();

                startProcessingRecords();
                final int recordsTransformed = processRecordsWithRetries(records, flowFiles, session, stopWatch);
                transferTo(ConsumeKinesisStream.REL_SUCCESS, session, records.size(), recordsTransformed, flowFiles);

                session.commitAsync(() -> {
                    processingRecords = false;

                    // if creating an Kinesis checkpoint fails, then the same record(s) can be retrieved again
                    checkpointOnceEveryCheckpointInterval(processRecordsInput.getCheckpointer());
                });
            }
        } catch (final Exception e) {
            log.error("Unable to fully process received Kinesis record(s) due to {}", e.getLocalizedMessage(), e);
            // FlowFiles that are already committed will not get rolled back
            if (session != null) {
                session.rollback();
            }
        }
    }

    void startProcessingRecords() {
        processingRecords = true;
    }

    private int processRecordsWithRetries(final List<Record> records, final List<FlowFile> flowFiles,
                                           final ProcessSession session, final StopWatch stopWatch) {
        int recordsTransformed = 0;
        for (int r = 0; r < records.size(); r++) {
            final Record kinesisRecord = records.get(r);
            boolean processedSuccessfully = false;
            for (int i = 0; !processedSuccessfully && i < numRetries; i++) {
                processedSuccessfully = attemptProcessRecord(flowFiles, kinesisRecord, r == records.size() - 1, session, stopWatch);
            }

            if (processedSuccessfully) {
                recordsTransformed++;
            } else {
                log.error("Couldn't process Kinesis record {}, skipping.", kinesisRecord);
            }
        }

        return recordsTransformed;
    }

    private boolean attemptProcessRecord(final List<FlowFile> flowFiles, final Record kinesisRecord, final boolean lastRecord,
                                         final ProcessSession session, final StopWatch stopWatch) {
        boolean processedSuccessfully = false;
        try {
            processRecord(flowFiles, kinesisRecord, lastRecord, session, stopWatch);
            processedSuccessfully = true;
        } catch (final Exception e) {
            log.error("Caught Exception while processing Kinesis record {}", kinesisRecord, e);

            // backoff if we encounter an exception.
            try {
                Thread.sleep(retryWaitMillis);
            } catch (InterruptedException ie) {
                log.debug("Interrupted sleep during record processing back-off", ie);
            }
        }

        return processedSuccessfully;
    }

    /**
     * Process an individual {@link Record} and serialise to {@link FlowFile}
     *
     * @param flowFiles {@link List} of {@link FlowFile}s to be output after all processing is complete
     * @param kinesisRecord the Kinesis {@link Record} to be processed
     * @param lastRecord whether this is the last {@link Record} to be processed in this batch
     * @param session {@link ProcessSession} into which {@link FlowFile}s will be transferred
     * @param stopWatch {@link StopWatch} tracking how much time has been spent processing the current batch
     *
     * @throws RuntimeException if there are any unhandled Exceptions that should be retried
     */
    abstract void processRecord(final List<FlowFile> flowFiles, final Record kinesisRecord, final boolean lastRecord,
                                final ProcessSession session, final StopWatch stopWatch);

    void reportProvenance(final ProcessSession session, final FlowFile flowFile, final String partitionKey,
                 final String sequenceNumber, final StopWatch stopWatch) {
        final String transitUri = StringUtils.isNotBlank(partitionKey) && StringUtils.isNotBlank(sequenceNumber)
                ? String.format("%s/%s/%s#%s", transitUriPrefix, kinesisShardId, partitionKey, sequenceNumber)
                : String.format("%s/%s", transitUriPrefix, kinesisShardId);

        session.getProvenanceReporter().receive(flowFile, transitUri, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
    }

    Map<String, String> getDefaultAttributes(final String sequenceNumber, final String partitionKey, final Date approximateArrivalTimestamp) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(AWS_KINESIS_SHARD_ID, kinesisShardId);
        attributes.put(AWS_KINESIS_SEQUENCE_NUMBER, sequenceNumber);
        attributes.put(AWS_KINESIS_PARTITION_KEY, partitionKey);
        if (approximateArrivalTimestamp != null) {
            attributes.put(AWS_KINESIS_APPROXIMATE_ARRIVAL_TIMESTAMP,
                    dateTimeFormatter.format(approximateArrivalTimestamp.toInstant().atZone(ZoneId.systemDefault())));
        }
        return attributes;
    }

    void transferTo(final Relationship relationship, final ProcessSession session, final int recordsProcessed,
                    final int recordsTransformed, final List<FlowFile> flowFiles) {
        session.adjustCounter("Records Processed", recordsProcessed, false);
        if (!flowFiles.isEmpty()) {
            session.adjustCounter("Records Transformed", recordsTransformed, false);
            session.transfer(flowFiles, relationship);
        }
    }

    private void checkpointOnceEveryCheckpointInterval(final IRecordProcessorCheckpointer checkpointer) {
        if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
            checkpointWithRetries(checkpointer);
            nextCheckpointTimeInMillis = System.currentTimeMillis() + checkpointIntervalMillis;
        }
    }

    @Override
    public void shutdown(final ShutdownInput shutdownInput) {
        log.debug("Shutting down Record Processor for shard: {} with reason: {}", kinesisShardId, shutdownInput.getShutdownReason());

        // be sure to finish processing any records before shutdown on TERMINATE
        if (ShutdownReason.TERMINATE == shutdownInput.getShutdownReason()) {
            for (int i = 0; processingRecords && i < numRetries; i++) {
                log.debug("Record Processor for shard {} still processing records, waiting before shutdown", kinesisShardId);
                try {
                    Thread.sleep(retryWaitMillis);
                } catch (InterruptedException ie) {
                    log.debug("Interrupted sleep while waiting for record processing to complete before shutdown (TERMINATE)", ie);
                }
            }

            if (processingRecords) {
                log.warn("Record Processor for shard {} still running, but maximum wait time elapsed, checkpoint will be attempted", kinesisShardId);
            }
        }
        checkpointWithRetries(shutdownInput.getCheckpointer());
    }

    private void checkpointWithRetries(final IRecordProcessorCheckpointer checkpointer) {
        log.debug("Checkpointing shard " + kinesisShardId);
        try {
            for (int i = 0; i < numRetries; i++) {
                if (attemptCheckpoint(checkpointer, i)) {
                    break;
                }
            }
        } catch (ShutdownException se) {
            // Ignore checkpoint if the processor instance has been shutdown (fail over).
            log.info("Caught shutdown exception, skipping checkpoint.", se);
        } catch (InvalidStateException e) {
            // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
            log.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
        }
    }

    private boolean attemptCheckpoint(final IRecordProcessorCheckpointer checkpointer, final int attempt) throws ShutdownException, InvalidStateException {
        boolean success = false;
        try {
            checkpointer.checkpoint();
            success = true;
        } catch (ThrottlingException e) {
            // Backoff and re-attempt checkpoint upon transient failures
            if (attempt >= (numRetries - 1)) {
                log.error("Checkpoint failed after {} attempts.", attempt + 1, e);
            } else {
                log.warn("Transient issue when checkpointing - attempt {} of {}", attempt + 1, numRetries, e);

                try {
                    Thread.sleep(retryWaitMillis);
                } catch (InterruptedException ie) {
                    log.debug("Interrupted sleep during checkpoint back-off", ie);
                }
            }
        }
        return success;
    }

    ComponentLog getLogger() {
        return log;
    }

    String getKinesisShardId() {
        return kinesisShardId;
    }

    void setKinesisShardId(final String kinesisShardId) {
        this.kinesisShardId = kinesisShardId;
    }

    long getNextCheckpointTimeInMillis() {
        return nextCheckpointTimeInMillis;
    }

    void setNextCheckpointTimeInMillis(final long nextCheckpointTimeInMillis) {
        this.nextCheckpointTimeInMillis = nextCheckpointTimeInMillis;
    }

    void setProcessingRecords(final boolean processingRecords) {
        this.processingRecords = processingRecords;
    }
}