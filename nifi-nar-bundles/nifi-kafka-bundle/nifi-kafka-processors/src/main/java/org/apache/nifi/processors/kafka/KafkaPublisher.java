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
package org.apache.nifi.processors.kafka;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.producer.KeyedMessage;
import kafka.producer.Partitioner;

/**
 * Wrapper over {@link KafkaProducer} to assist {@link PutKafka} processor with
 * sending content of {@link FlowFile}s to Kafka.
 */
public class KafkaPublisher implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(KafkaPublisher.class);

    private final KafkaProducer<byte[], byte[]> producer;

    private final Partitioner partitioner;

    private final long ackWaitTime;

    private ProcessorLog processLog;

    /**
     * Creates an instance of this class as well as the instance of the
     * corresponding Kafka {@link KafkaProducer} using provided Kafka
     * configuration properties.
     */
    KafkaPublisher(Properties kafkaProperties) {
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        this.producer = new KafkaProducer<byte[], byte[]>(kafkaProperties);
        this.ackWaitTime = Long.parseLong(kafkaProperties.getProperty(ProducerConfig.TIMEOUT_CONFIG)) * 2;
        try {
            if (kafkaProperties.containsKey("partitioner.class")){
                this.partitioner = (Partitioner) Class.forName(kafkaProperties.getProperty("partitioner.class")).newInstance();
            } else {
                this.partitioner = null;
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create partitioner", e);
        }
    }

    /**
     *
     */
    void setProcessLog(ProcessorLog processLog) {
        this.processLog = processLog;
    }

    /**
     * Publishes messages to Kafka topic. It supports three publishing
     * mechanisms.
     * <ul>
     * <li>Sending the entire content stream as a single Kafka message.</li>
     * <li>Splitting the incoming content stream into chunks and sending
     * individual chunks as separate Kafka messages.</li>
     * <li>Splitting the incoming content stream into chunks and sending only
     * the chunks that have failed previously @see
     * {@link SplittableMessageContext#getFailedSegments()}.</li>
     * </ul>
     * This method assumes content stream affinity where it is expected that the
     * content stream that represents the same Kafka message(s) will remain the
     * same across possible retries. This is required specifically for cases
     * where delimiter is used and a single content stream may represent
     * multiple Kafka messages. The failed segment list will keep the index of
     * of each content stream segment that had failed to be sent to Kafka, so
     * upon retry only the failed segments are sent.
     *
     * @param messageContext
     *            instance of {@link SplittableMessageContext} which hold
     *            context information about the message to be sent
     * @param contentStream
     *            instance of open {@link InputStream} carrying the content of
     *            the message(s) to be send to Kafka
     * @param partitionKey
     *            the value of the partition key. Only relevant is user wishes
     *            to provide a custom partition key instead of relying on
     *            variety of provided {@link Partitioner}(s)
     * @return The set containing the failed segment indexes for messages that
     *         failed to be sent to Kafka.
     */
    BitSet publish(SplittableMessageContext messageContext, InputStream contentStream, Integer partitionKey) {
        List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        BitSet prevFailedSegmentIndexes = messageContext.getFailedSegments();
        int segmentCounter = 0;
        StreamScanner scanner = new StreamScanner(contentStream, messageContext.getDelimiterPattern());

        while (scanner.hasNext()) {
            byte[] content = scanner.next();
            if (content.length > 0){
                byte[] key = messageContext.getKeyBytes();
                String topicName = messageContext.getTopicName();
                if (partitionKey == null && key != null) {
                    partitionKey = this.getPartition(key, topicName);
                }
                if (prevFailedSegmentIndexes == null || prevFailedSegmentIndexes.get(segmentCounter)) {
                    ProducerRecord<byte[], byte[]> message = new ProducerRecord<byte[], byte[]>(topicName, partitionKey, key, content);
                    sendFutures.add(this.toKafka(message));
                }
                segmentCounter++;
            }
        }
        scanner.close();
        return this.processAcks(sendFutures);
    }

    /**
     *
     */
    private BitSet processAcks(List<Future<RecordMetadata>> sendFutures) {
        int segmentCounter = 0;
        BitSet failedSegments = new BitSet();
        for (Future<RecordMetadata> future : sendFutures) {
            try {
                future.get(this.ackWaitTime, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                failedSegments.set(segmentCounter);
                Thread.currentThread().interrupt();
                logger.warn("Interrupted while waiting for acks from Kafka");
                if (this.processLog != null) {
                    this.processLog.warn("Interrupted while waiting for acks from Kafka");
                }
            } catch (ExecutionException e) {
                failedSegments.set(segmentCounter);
                logger.error("Failed while waiting for acks from Kafka", e);
                if (this.processLog != null) {
                    this.processLog.error("Failed while waiting for acks from Kafka", e);
                }
            } catch (TimeoutException e) {
                failedSegments.set(segmentCounter);
                logger.warn("Timed out while waiting for acks from Kafka");
                if (this.processLog != null) {
                    this.processLog.warn("Timed out while waiting for acks from Kafka");
                }
            }
            segmentCounter++;
        }
        return failedSegments;
    }

    /**
     *
     */
    private int getPartition(Object key, String topicName) {
        int partSize = this.producer.partitionsFor(topicName).size();
        return this.partitioner.partition(key, partSize);
    }

    /**
     * Closes {@link KafkaProducer}
     */
    @Override
    public void close() throws Exception {
        this.producer.close();
    }

    /**
     * Sends the provided {@link KeyedMessage} to Kafka async returning
     * {@link Future}
     */
    private Future<RecordMetadata> toKafka(ProducerRecord<byte[], byte[]> message) {
        if (logger.isDebugEnabled()) {
            logger.debug("Publishing message to '" + message.topic() + "' topic.");
        }
        return this.producer.send(message);
    }
}
