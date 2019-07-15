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
package org.apache.nifi.processors.aws.kinesis.stream;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;

import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Processes records and checkpoints progress.
 */
public class RecordProcessor implements IRecordProcessor {

    private static final String AWS_KINESIS_SHARD_ID = "aws.kinesis.shard.id";

    private static final String AWS_KINESIS_SEQUENCE_NUMBER = "aws.kinesis.sequence.number";

    static final String AWS_KINESIS_PARTITION_KEY = "aws.kinesis.partition.key";

    private static final Base64.Encoder BASE_64_ENCODER = Base64.getEncoder();

    private String kinesisShardId;

    // Backoff and retry settings
    private static final long BACKOFF_TIME_IN_MILLIS = 3_000L;
    private static final int NUM_RETRIES = 10;

    private ProcessSession session;
    private ComponentLog log;
    private long nextCheckpointTimeInMillis;

    RecordProcessor(ProcessSession session, ComponentLog log) {
        this.session = session;
        this.log = log;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(String shardId) {
        log.info("Initializing record processor for shard: " + shardId);
        this.kinesisShardId = shardId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
        log.info("Processing " + records.size() + " records from " + kinesisShardId);

        processRecordsWithRetries(records);

        checkpointOnceEveryCheckpointInterval(checkpointer);
    }

    private void checkpointOnceEveryCheckpointInterval(IRecordProcessorCheckpointer checkpointer) {
        if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
            checkpoint(checkpointer);
            long checkpointIntervalMillis = 60_000L;
            nextCheckpointTimeInMillis = System.currentTimeMillis() + checkpointIntervalMillis;
        }
    }

    /**
     * Process records performing retries as needed. Skip "poison pill" records.
     *
     * @param records Data records to be processed.
     */
    private void processRecordsWithRetries(List<Record> records) {
        List<FlowFile> successfulFlowFiles = new LinkedList<>();

        for (Record record : records) {
            boolean processedSuccessfully = false;
            for (int i = 0; i < NUM_RETRIES; i++) {
                try {
                    //
                    // Logic to process record goes here.
                    //
                    FlowFile flowFile = processSingleRecord(record);
                    successfulFlowFiles.add(flowFile);

                    processedSuccessfully = true;
                    break;
                } catch (Throwable t) {
                    log.warn("Caught throwable while processing record " + record, t);
                }

                // backoff if we encounter an exception.
                try {
                    Thread.sleep(BACKOFF_TIME_IN_MILLIS);
                } catch (InterruptedException e) {
                    log.debug("Interrupted sleep", e);
                }
            }

            if (!processedSuccessfully) {
                log.error("Couldn't process record " + record + ". Skipping the record.");
            }
        }

        session.transfer(successfulFlowFiles, GetKinesisStream.REL_SUCCESS);
    }

    private FlowFile processSingleRecord(Record record) {
        FlowFile flowFile = session.create();

        String partitionKey = record.getPartitionKey();
        String sequenceNumber = record.getSequenceNumber();
        byte[] data = record.getData().array();

        session.append(flowFile, out -> out.write(data));

        Map<String, String> attributes = new HashMap<>();
        attributes.put(AWS_KINESIS_SHARD_ID, kinesisShardId);
        attributes.put(AWS_KINESIS_SEQUENCE_NUMBER, sequenceNumber);
        attributes.put(AWS_KINESIS_PARTITION_KEY, partitionKey);

        session.putAllAttributes(flowFile, attributes);

        if (log.isDebugEnabled()) {
            log.debug(String.format("Sequence No: %s, Partition Key: %s, Data: %s",
                    sequenceNumber, partitionKey, BASE_64_ENCODER.encodeToString(data)));
        }

        return flowFile;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
        log.info("Shutting down record processor for shard: " + kinesisShardId);
        // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
        if (reason == ShutdownReason.TERMINATE) {
            checkpoint(checkpointer);
        }
    }

    /**
     * Checkpoint with retries.
     *
     * @param checkpointer checkpointer
     */
    private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
        log.info("Checkpointing shard " + kinesisShardId);
        for (int i = 0; i < NUM_RETRIES; i++) {
            try {
                checkpointer.checkpoint();
                break;
            } catch (ShutdownException se) {
                // Ignore checkpoint if the processor instance has been shutdown (fail over).
                log.info("Caught shutdown exception, skipping checkpoint.", se);
                break;
            } catch (ThrottlingException e) {
                // Backoff and re-attempt checkpoint upon transient failures
                if (i >= (NUM_RETRIES - 1)) {
                    log.error("Checkpoint failed after " + (i + 1) + "attempts.", e);
                    break;
                } else {
                    log.info("Transient issue when checkpointing - attempt " + (i + 1) + " of "
                            + NUM_RETRIES, e);
                }
            } catch (InvalidStateException e) {
                // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
                log.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
                break;
            }

            try {
                Thread.sleep(BACKOFF_TIME_IN_MILLIS);
            } catch (InterruptedException e) {
                log.debug("Interrupted sleep", e);
            }
        }
    }
}