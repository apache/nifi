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

import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.util.List;
import java.util.Optional;

/**
 * RecordBuffer keeps track of all created Shard buffers, including exclusive read access via leasing.
 * It acts as the main interface between KCL callbacks and the {@link ConsumeKinesis} processor,
 * routing events to appropriate Shard buffers and ensuring thread-safe operations.
 */
interface RecordBuffer {

    /**
     * Interface for interactions from the Kinesis Client Library to the Record Buffer.
     * Reflects the methods called by {@link software.amazon.kinesis.processor.ShardRecordProcessor}.
     */
    interface ForKinesisClientLibrary {

        ShardBufferId createBuffer(String shardId);

        void addRecords(ShardBufferId bufferId, List<KinesisClientRecord> records, RecordProcessorCheckpointer checkpointer);

        /**
         * Called when a shard ends - waits until the buffer is flushed then performs the final checkpoint.
         */
        void checkpointEndedShard(ShardBufferId bufferId, RecordProcessorCheckpointer checkpointer);

        /**
         * Called when a consumer is shut down. Performs the checkpoint and returns
         * without waiting for the buffer to be flushed.
         */
        void shutdownShardConsumption(ShardBufferId bufferId, RecordProcessorCheckpointer checkpointer);

        /**
         * Called when lease is lost - immediately invalidates the buffer to prevent further operations.
         */
        void consumerLeaseLost(ShardBufferId bufferId);
    }

    /**
     * Interface for interactions from {@link ConsumeKinesis} processor to the Record Buffer.
     */
    interface ForProcessor<LEASE extends ShardBufferLease> {

        /**
         * Acquires an exclusive lease for a buffer that has data available for consumption.
         * If no data is available in the buffers, returns an empty Optional.
         * <p>
         * After acquiring a lease, the processor can consume records from the buffer.
         * After consuming the records the processor must always {@link #returnBufferLease(LEASE)}.
         */
        Optional<LEASE> acquireBufferLease();

        /**
         * Consumes records from the buffer associated with the given lease.
         * The records have to be committed or rolled back later.
         */
        List<KinesisClientRecord> consumeRecords(LEASE lease);

        void commitConsumedRecords(LEASE lease);

        void rollbackConsumedRecords(LEASE lease);

        /**
         * Returns the lease for a buffer back to the pool making it available for consumption again.
         * The method can be called multiple times with the same lease, but only the first call will actually take an effect.
         */
        void returnBufferLease(LEASE lease);
    }

    record ShardBufferId(String shardId, long bufferId) {
    }

    interface ShardBufferLease {
        String shardId();
    }
}
