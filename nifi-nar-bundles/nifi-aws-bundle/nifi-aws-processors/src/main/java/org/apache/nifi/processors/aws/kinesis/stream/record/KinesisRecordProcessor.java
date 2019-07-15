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
import org.apache.nifi.processors.aws.kinesis.stream.GetKinesisStream;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KinesisRecordProcessor implements IRecordProcessor {
    public static final String AWS_KINESIS_SHARD_ID = "aws.kinesis.shard.id";

    public static final String AWS_KINESIS_SEQUENCE_NUMBER = "aws.kinesis.sequence.number";

    public static final String AWS_KINESIS_PARTITION_KEY = "aws.kinesis.partition.key";

    public static final String AWS_KINESIS_APPROXIMATE_ARRIVAL_TIMESTAMP = "aws.kinesis.approximate.arrival.timestamp";

    private static final Base64.Encoder BASE_64_ENCODER = Base64.getEncoder();

    final ProcessSession session;
    final ComponentLog log;
    final String endpointPrefix;

    final long checkpointIntervalMillis;
    final long retryWaitMillis;
    final int numRetries;
    final DateTimeFormatter dateTimeFormatter;

    String kinesisShardId;
    long nextCheckpointTimeInMillis;

    boolean processingRecords = false;

    KinesisRecordProcessor(final ProcessSession session, final ComponentLog log, final String endpointPrefix,
                           final long checkpointIntervalMillis, final long retryWaitMillis, final int numRetries,
                           final DateTimeFormatter dateTimeFormatter) {
        this.session = session;
        this.log = log;
        this.endpointPrefix = endpointPrefix;
        this.checkpointIntervalMillis = checkpointIntervalMillis;
        this.retryWaitMillis = retryWaitMillis;
        this.numRetries = numRetries;
        this.dateTimeFormatter = dateTimeFormatter;
    }

    @Override
    public void initialize(final InitializationInput initializationInput) {
        if (initializationInput.getPendingCheckpointSequenceNumber() != null) {
            log.warn("Initializing record processor for shard {}; from sequence number: {}; indicates previously uncheckpointed sequence number: {}",
                    initializationInput.getShardId(), initializationInput.getExtendedSequenceNumber(), initializationInput.getPendingCheckpointSequenceNumber());
        } else {
            log.debug("Initializing record processor for shard: {}; from sequence number: {}",
                    initializationInput.getShardId(), initializationInput.getExtendedSequenceNumber());
        }

        this.kinesisShardId = initializationInput.getShardId();

        // ensure we don't immediately checkpoint
        this.nextCheckpointTimeInMillis = System.currentTimeMillis() + checkpointIntervalMillis;
    }

    @Override
    @SuppressWarnings({"java:S3252"}) // GetKinesisStream reference to REL_SUCCESS instead of deprecated AbstractAWSProcessor
    public void processRecords(final ProcessRecordsInput processRecordsInput) {
        log.debug("Processing {} records from {}; cache entry: {}; cache exit: {}; millis behind latest: {}",
                processRecordsInput.getRecords().size(), kinesisShardId,
                processRecordsInput.getCacheEntryTime() != null ? dateTimeFormatter.format(processRecordsInput.getCacheEntryTime().atZone(ZoneId.systemDefault())) : null,
                processRecordsInput.getCacheExitTime() != null ? dateTimeFormatter.format(processRecordsInput.getCacheExitTime().atZone(ZoneId.systemDefault())) : null,
                processRecordsInput.getMillisBehindLatest());

        processingRecords = true;
        final List<FlowFile> flowFiles = processRecordsWithRetries(processRecordsInput.getRecords());
        session.adjustCounter("Records Processed", processRecordsInput.getRecords().size(), false);
        if (!flowFiles.isEmpty()) {
            session.adjustCounter("Records Transformed", flowFiles.size(), false);
            session.transfer(flowFiles, GetKinesisStream.REL_SUCCESS);
            session.commit();
        }
        processingRecords = false;

        if (!processRecordsInput.getRecords().isEmpty()) {
            checkpointOnceEveryCheckpointInterval(processRecordsInput.getCheckpointer());
        }
    }

    private void checkpointOnceEveryCheckpointInterval(final IRecordProcessorCheckpointer checkpointer) {
        if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
            checkpointWithRetries(checkpointer);
            nextCheckpointTimeInMillis = System.currentTimeMillis() + checkpointIntervalMillis;
        }
    }

    /**
     * Process records performing retries as needed. Skip "poison pill" records.
     *
     * @param records Data records to be processed.
     */
    private List<FlowFile> processRecordsWithRetries(final List<Record> records) {
        final List<FlowFile> flowFiles = new ArrayList<>(records.size());
        for (final Record record : records) {
            boolean processedSuccessfully = false;
            for (int i = 0; !processedSuccessfully && i < numRetries; i++) {
                try {
                    flowFiles.add(processRecord(record));
                    processedSuccessfully = true;
                } catch (Exception e) {
                    log.error("Caught Exception while processing record {}", record, e);

                    // backoff if we encounter an exception.
                    try {
                        Thread.sleep(retryWaitMillis);
                    } catch (@SuppressWarnings("java:S2142") InterruptedException ie) {
                        log.debug("Interrupted sleep during record processing back-off", ie);
                    }
                }
            }

            if (!processedSuccessfully) {
                log.error("Couldn't process record {}. Skipping the record.", record);
            }
        }
        return flowFiles;
    }

    private FlowFile processRecord(final Record record) {
        final String partitionKey = record.getPartitionKey();
        final String sequenceNumber = record.getSequenceNumber();
        final Date approximateArrivalTimestamp = record.getApproximateArrivalTimestamp();
        final byte[] data = record.getData() != null ? record.getData().array() : new byte[0];

        final FlowFile flowFile = session.create();
        session.write(flowFile, out -> out.write(data));
        session.getProvenanceReporter().receive(flowFile, String.format("http://%s.amazonaws.com/%s/%s#%s", endpointPrefix, kinesisShardId,
                partitionKey, sequenceNumber));

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(AWS_KINESIS_SHARD_ID, kinesisShardId);
        attributes.put(AWS_KINESIS_SEQUENCE_NUMBER, sequenceNumber);
        attributes.put(AWS_KINESIS_PARTITION_KEY, partitionKey);
        if (approximateArrivalTimestamp != null) {
            attributes.put(AWS_KINESIS_APPROXIMATE_ARRIVAL_TIMESTAMP,
                    dateTimeFormatter.format(approximateArrivalTimestamp.toInstant().atZone(ZoneId.systemDefault())));
        }
        session.putAllAttributes(flowFile, attributes);

        if (log.isDebugEnabled()) {
            log.debug("Sequence No: {}, Partition Key: {}, Data: {}", sequenceNumber, partitionKey, BASE_64_ENCODER.encodeToString(data));
        }

        return flowFile;
    }

    @Override
    public void shutdown(final ShutdownInput shutdownInput) {
        log.debug("Shutting down record processor for shard: {} with reason: {}", kinesisShardId, shutdownInput.getShutdownReason());

        // be sure to finish processing any records before shutdown on TERMINATE
        if (ShutdownReason.TERMINATE == shutdownInput.getShutdownReason()) {
            for (int i = 0; processingRecords && i < numRetries; i++) {
                log.debug("Record Processor for shard {} still processing records, waiting before shutdown", kinesisShardId);
                try {
                    Thread.sleep(retryWaitMillis);
                } catch (@SuppressWarnings("java:S2142") InterruptedException ie) {
                    log.debug("Interrupted sleep while waiting for record processing to complete before shutdown (TERMINATE)", ie);
                }
            }
        }
        checkpointWithRetries(shutdownInput.getCheckpointer());
    }

    private void checkpointWithRetries(final IRecordProcessorCheckpointer checkpointer) {
        log.debug("Checkpointing shard " + kinesisShardId);
        try {
            for (int i = 0; i < numRetries; i++) {
                if (checkpoint(checkpointer, i)) {
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

    private boolean checkpoint(final IRecordProcessorCheckpointer checkpointer, final int attempt) throws ShutdownException, InvalidStateException {
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
                } catch (@SuppressWarnings("java:S2142") InterruptedException ie) {
                    log.debug("Interrupted sleep during checkpoint back-off", ie);
                }
            }
        }
        return success;
    }
}