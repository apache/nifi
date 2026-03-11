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

import java.time.Instant;

/**
 * A single user record extracted from a Kinesis record. For non-aggregated records,
 * the fields map directly from the Kinesis API {@code Record}. For KPL-aggregated
 * records, each sub-record within the aggregate gets its own instance with a unique
 * {@code subSequenceNumber}.
 *
 * @param shardId the shard from which this record was fetched
 * @param sequenceNumber the Kinesis sequence number of the enclosing record
 * @param subSequenceNumber zero for non-aggregated records; index within the aggregate for KPL records
 * @param partitionKey the partition key (from the enclosing record or the KPL sub-record)
 * @param data the user payload bytes
 * @param approximateArrivalTimestamp approximate time the enclosing record arrived at Kinesis
 */
record UserRecord(
        String shardId,
        String sequenceNumber,
        long subSequenceNumber,
        String partitionKey,
        byte[] data,
        Instant approximateArrivalTimestamp) {
}
