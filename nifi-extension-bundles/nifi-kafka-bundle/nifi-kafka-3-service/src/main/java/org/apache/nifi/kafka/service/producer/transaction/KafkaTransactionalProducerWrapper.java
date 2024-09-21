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
package org.apache.nifi.kafka.service.producer.transaction;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.nifi.kafka.service.api.producer.PublishContext;
import org.apache.nifi.kafka.service.api.record.KafkaRecord;
import org.apache.nifi.kafka.service.producer.ProducerCallback;

import java.util.Iterator;

public class KafkaTransactionalProducerWrapper extends KafkaProducerWrapper {
    private volatile boolean inTransaction = false;

    public KafkaTransactionalProducerWrapper(final Producer<byte[], byte[]> producer) {
        super(producer);
        producer.initTransactions();
    }

    @Override
    public void send(final Iterator<KafkaRecord> kafkaRecords, final PublishContext publishContext, final ProducerCallback callback) {
        if (!inTransaction) {
            producer.beginTransaction();
            inTransaction = true;
        }

        super.send(kafkaRecords, publishContext, callback);
    }

    @Override
    public void commit() {
        try {
            // Commit the transaction. If a TimeoutException is thrown, retry up to 3 times.
            // The producer will throw an Exception if we attempt to abort a transaction
            // after a commit times out.
            boolean failure = false;
            for (int i = 0; i < 3; i++) {
                try {
                    producer.commitTransaction();

                    // If we logged any warning that we timed out and will retry, we should log a notification
                    // that we were successful this time. Otherwise, don't spam the logs.
                    if (failure) {
                        logger.info("Successfully commited producer transaction after {} retries", i);
                    }
                    break;
                } catch (final TimeoutException te) {
                    failure = true;
                    if (i == 2) {
                        logger.warn("Failed to commit producer transaction after 3 attempts, each timing out. Aborting transaction.");
                        throw te;
                    }

                    logger.warn("Timed out while committing producer transaction. Retrying...");
                }
            }

            inTransaction = false;
        } catch (final Exception e) {
            logger.error("Failed to commit producer transaction", e);
            abort();
        }
    }

    @Override
    public void abort() {
        if (!inTransaction) {
            return;
        }

        inTransaction = false;
        producer.abortTransaction();
    }
}
