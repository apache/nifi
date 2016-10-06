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
package org.apache.nifi.processors.kafka.pubsub;

import java.io.Closeable;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.stream.io.util.StreamDemarcator;

/**
 * Wrapper over {@link KafkaProducer} to assist {@link PublishKafka} processor
 * with sending contents of the {@link FlowFile}s to Kafka.
 */
class KafkaPublisher implements Closeable {

    private final Producer<byte[], byte[]> kafkaProducer;

    private volatile long ackWaitTime = 30000;

    private final ComponentLog componentLog;

    private final int ackCheckSize;

    KafkaPublisher(Properties kafkaProperties, ComponentLog componentLog) {
        this(kafkaProperties, 100, componentLog);
    }

    /**
     * Creates an instance of this class as well as the instance of the
     * corresponding Kafka {@link KafkaProducer} using provided Kafka
     * configuration properties.
     *
     * @param kafkaProperties instance of {@link Properties} used to bootstrap
     * {@link KafkaProducer}
     */
    KafkaPublisher(Properties kafkaProperties, int ackCheckSize, ComponentLog componentLog) {
        this.kafkaProducer = new KafkaProducer<>(kafkaProperties);
        this.ackCheckSize = ackCheckSize;
        this.componentLog = componentLog;
    }

    /**
     * Publishes messages to Kafka topic. It uses {@link StreamDemarcator} to
     * determine how many messages to Kafka will be sent from a provided
     * {@link InputStream} (see {@link PublishingContext#getContentStream()}).
     * It supports two publishing modes:
     * <ul>
     * <li>Sending all messages constructed from
     * {@link StreamDemarcator#nextToken()} operation.</li>
     * <li>Sending only unacknowledged messages constructed from
     * {@link StreamDemarcator#nextToken()} operation.</li>
     * </ul>
     * The unacknowledged messages are determined from the value of
     * {@link PublishingContext#getLastAckedMessageIndex()}.
     * <br>
     * This method assumes content stream affinity where it is expected that the
     * content stream that represents the same Kafka message(s) will remain the
     * same across possible retries. This is required specifically for cases
     * where delimiter is used and a single content stream may represent
     * multiple Kafka messages. The
     * {@link PublishingContext#getLastAckedMessageIndex()} will provide the
     * index of the last ACKed message, so upon retry only messages with the
     * higher index are sent.
     *
     * @param publishingContext instance of {@link PublishingContext} which hold
     * context information about the message(s) to be sent.
     * @return The index of the last successful offset.
     */
    KafkaPublisherResult publish(PublishingContext publishingContext) {
        StreamDemarcator streamTokenizer = new StreamDemarcator(publishingContext.getContentStream(),
                publishingContext.getDelimiterBytes(), publishingContext.getMaxRequestSize());

        int prevLastAckedMessageIndex = publishingContext.getLastAckedMessageIndex();
        List<Future<RecordMetadata>> resultFutures = new ArrayList<>();

        byte[] messageBytes;
        int tokenCounter = 0;
        boolean continueSending = true;
        KafkaPublisherResult result = null;
        for (; continueSending && (messageBytes = streamTokenizer.nextToken()) != null; tokenCounter++) {
            if (prevLastAckedMessageIndex < tokenCounter) {
                ProducerRecord<byte[], byte[]> message = new ProducerRecord<>(publishingContext.getTopic(), publishingContext.getKeyBytes(), messageBytes);
                resultFutures.add(this.kafkaProducer.send(message));

                if (tokenCounter % this.ackCheckSize == 0) {
                    int lastAckedMessageIndex = this.processAcks(resultFutures, prevLastAckedMessageIndex);
                    resultFutures.clear();
                    if (lastAckedMessageIndex % this.ackCheckSize != 0) {
                        continueSending = false;
                        result = new KafkaPublisherResult(tokenCounter, lastAckedMessageIndex);
                    }
                    prevLastAckedMessageIndex = lastAckedMessageIndex;
                }
            }
        }

        if (result == null) {
            int lastAckedMessageIndex = this.processAcks(resultFutures, prevLastAckedMessageIndex);
            resultFutures.clear();
            result = new KafkaPublisherResult(tokenCounter, lastAckedMessageIndex);
        }
        return result;
    }

    /**
     * Sets the time this publisher will wait for the {@link Future#get()}
     * operation (the Future returned by
     * {@link KafkaProducer#send(ProducerRecord)}) to complete before timing
     * out.
     *
     * This value will also be used as a timeout when closing the underlying
     * {@link KafkaProducer}. See {@link #close()}.
     */
    void setAckWaitTime(long ackWaitTime) {
        this.ackWaitTime = ackWaitTime;
    }

    /**
     * This operation will process ACKs from Kafka in the order in which
     * {@link KafkaProducer#send(ProducerRecord)} invocation were made returning
     * the index of the last ACKed message. Within this operation processing ACK
     * simply means successful invocation of 'get()' operation on the
     * {@link Future} returned by {@link KafkaProducer#send(ProducerRecord)}
     * operation. Upon encountering any type of error while interrogating such
     * {@link Future} the ACK loop will end. Messages that were not ACKed would
     * be considered non-delivered and therefore could be resent at the later
     * time.
     *
     * @param sendFutures list of {@link Future}s representing results of
     * publishing to Kafka
     *
     * @param lastAckMessageIndex the index of the last ACKed message. It is
     * important to provide the last ACKed message especially while re-trying so
     * the proper index is maintained.
     */
    private int processAcks(List<Future<RecordMetadata>> sendFutures, int lastAckMessageIndex) {
        boolean exceptionThrown = false;
        for (int segmentCounter = 0; segmentCounter < sendFutures.size() && !exceptionThrown; segmentCounter++) {
            Future<RecordMetadata> future = sendFutures.get(segmentCounter);
            try {
                future.get(this.ackWaitTime, TimeUnit.MILLISECONDS);
                lastAckMessageIndex++;
            } catch (InterruptedException e) {
                exceptionThrown = true;
                Thread.currentThread().interrupt();
                this.warnOrError("Interrupted while waiting for acks from Kafka", null);
            } catch (ExecutionException e) {
                exceptionThrown = true;
                this.warnOrError("Failed while waiting for acks from Kafka", e);
            } catch (TimeoutException e) {
                exceptionThrown = true;
                this.warnOrError("Timed out while waiting for acks from Kafka", null);
            }
        }

        return lastAckMessageIndex;
    }

    /**
     * Will close the underlying {@link KafkaProducer} waiting if necessary for
     * the same duration as supplied {@link #setAckWaitTime(long)}
     */
    @Override
    public void close() {
        this.kafkaProducer.close(this.ackWaitTime, TimeUnit.MILLISECONDS);
    }

    /**
     *
     */
    private void warnOrError(String message, Exception e) {
        if (e == null) {
            this.componentLog.warn(message);
        } else {
            this.componentLog.error(message, e);
        }
    }

    /**
     * Encapsulates the result received from publishing messages to Kafka
     */
    static class KafkaPublisherResult {

        private final int messagesSent;
        private final int lastMessageAcked;

        KafkaPublisherResult(int messagesSent, int lastMessageAcked) {
            this.messagesSent = messagesSent;
            this.lastMessageAcked = lastMessageAcked;
        }

        public int getMessagesSent() {
            return this.messagesSent;
        }

        public int getLastMessageAcked() {
            return this.lastMessageAcked;
        }

        public boolean isAllAcked() {
            return this.lastMessageAcked > -1 && this.messagesSent - 1 == this.lastMessageAcked;
        }

        @Override
        public String toString() {
            return "Sent:" + this.messagesSent + "; Last ACK:" + this.lastMessageAcked;
        }
    }
}
