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
package org.apache.nifi.kafka.service.api.consumer.share;

import org.apache.nifi.kafka.service.api.record.ByteRecord;

import java.io.Closeable;
import java.time.Duration;

/**
 * NiFi service contract for consuming records from a Kafka share group (KIP-932).
 *
 * <p>Share groups differ from classic consumer groups in important ways:</p>
 * <ul>
 *   <li>Records are not bound to per-consumer partition assignments; multiple consumers in a share
 *       group may receive records from the same partition.</li>
 *   <li>Position is tracked per record by acknowledgement, not by committed offset. Records are
 *       acknowledged with {@link Acknowledgement#ACCEPT}, {@link Acknowledgement#RELEASE}, or
 *       {@link Acknowledgement#REJECT}.</li>
 *   <li>There is no rollback by offset seek. The closest equivalent is to release records, which
 *       returns them to the share-group queue for redelivery to another (or the same) consumer.</li>
 * </ul>
 *
 * <p>This service must be closed to avoid leaking connection resources.</p>
 */
public interface KafkaShareConsumerService extends Closeable {

    /**
     * Poll the share group for records up to the given duration.
     *
     * @param maxWaitDuration Maximum amount of time to wait for records
     * @return Records delivered by the broker, possibly empty
     */
    Iterable<ByteRecord> poll(Duration maxWaitDuration);

    /**
     * Acknowledge a single record returned from the most recent {@link #poll(Duration)}.
     * Calling this method is only valid when the underlying consumer is configured for
     * {@link ShareAcknowledgementMode#EXPLICIT}.
     *
     * @param record          Record to acknowledge
     * @param acknowledgement Outcome of the record's processing
     */
    void acknowledge(ByteRecord record, Acknowledgement acknowledgement);

    /**
     * Commit pending acknowledgements to the broker.
     *
     * <p>In implicit mode this commits acknowledgement of all delivered-but-unacknowledged records
     * as {@link Acknowledgement#ACCEPT}. In explicit mode this commits the per-record
     * acknowledgements that have been recorded since the previous commit; any record delivered by
     * the most recent poll that has not been explicitly acknowledged is implicitly accepted as part
     * of the commit so the consumer can move on to the next poll.</p>
     */
    void commit();

    /**
     * Release all delivered-but-unacknowledged records and commit the release acknowledgements.
     * This is the share-group analogue of a rollback: released records become eligible for
     * redelivery to other consumers in the same share group.
     */
    void rollback();

    /**
     * @return {@code true} if this service has been closed
     */
    boolean isClosed();
}
