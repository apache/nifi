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

import java.math.BigInteger;

/**
 * Immutable checkpoint position within a Kinesis shard, composed of a sequence number
 * and a sub-sequence number. The sub-sequence number is non-zero only for KPL-aggregated
 * records and identifies the position within the aggregate.
 *
 * @param sequenceNumber the Kinesis record sequence number
 * @param subSequenceNumber the sub-record index within a KPL aggregate (0 for non-aggregated)
 */
record ShardCheckpoint(BigInteger sequenceNumber, long subSequenceNumber) {

    /**
     * Returns the higher of two checkpoints. Comparison is first by sequence number,
     * then by sub-sequence number within the same aggregate.
     */
    ShardCheckpoint max(final ShardCheckpoint other) {
        final int comparison = this.sequenceNumber.compareTo(other.sequenceNumber);
        if (comparison > 0) {
            return this;
        }
        if (comparison < 0) {
            return other;
        }
        return this.subSequenceNumber >= other.subSequenceNumber ? this : other;
    }
}
