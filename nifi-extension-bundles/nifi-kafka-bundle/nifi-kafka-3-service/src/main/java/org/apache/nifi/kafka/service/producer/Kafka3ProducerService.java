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
package org.apache.nifi.kafka.service.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.nifi.kafka.service.api.common.PartitionState;
import org.apache.nifi.kafka.service.api.common.ServiceConfiguration;
import org.apache.nifi.kafka.service.api.producer.FlowFileResult;
import org.apache.nifi.kafka.service.api.producer.KafkaProducerService;
import org.apache.nifi.kafka.service.api.producer.ProducerConfiguration;
import org.apache.nifi.kafka.service.api.producer.PublishContext;
import org.apache.nifi.kafka.service.api.producer.RecordSummary;
import org.apache.nifi.kafka.service.api.record.KafkaRecord;
import org.apache.nifi.kafka.service.producer.transaction.KafkaNonTransactionalProducerWrapper;
import org.apache.nifi.kafka.service.producer.transaction.KafkaProducerWrapper;
import org.apache.nifi.kafka.service.producer.transaction.KafkaTransactionalProducerWrapper;

import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class Kafka3ProducerService implements KafkaProducerService {

    private final Producer<byte[], byte[]> producer;
    private final List<ProducerCallback> callbacks;
    private final ServiceConfiguration serviceConfiguration;
    private final KafkaProducerWrapper wrapper;

    private volatile boolean closed = false;

    public Kafka3ProducerService(final Properties properties,
                                 final ServiceConfiguration serviceConfiguration,
                                 final ProducerConfiguration producerConfiguration) {
        final ByteArraySerializer serializer = new ByteArraySerializer();
        this.producer = new KafkaProducer<>(properties, serializer, serializer);
        this.callbacks = new ArrayList<>();

        this.serviceConfiguration = serviceConfiguration;

        this.wrapper = producerConfiguration.getTransactionsEnabled()
                ? new KafkaTransactionalProducerWrapper(producer)
                : new KafkaNonTransactionalProducerWrapper(producer);
    }

    @Override
    public void close() {
        closed = true;
        producer.close(Duration.ofSeconds(30));
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void send(final Iterator<KafkaRecord> kafkaRecords, final PublishContext publishContext) {
        final ProducerCallback callback = new ProducerCallback(publishContext.getFlowFile());
        callbacks.add(callback);

        final List<Exception> callbackExceptions = callback.getExceptions();

        final Exception publishException = publishContext.getException();
        if (publishException != null) {
            callbackExceptions.add(publishException);
        }

        if (callbackExceptions.isEmpty()) {
            try {
                wrapper.send(kafkaRecords, publishContext, callback);
            } catch (final UncheckedIOException e) {
                // We don't throw the Exception because we will later deal with this by
                // checking if there are any Exceptions.
                callbackExceptions.add(e);
            } catch (final Exception e) {
                // We re-throw the Exception in this case because it is an unexpected Exception
                callbackExceptions.add(e);
                throw e;
            }
        }
    }

    @Override
    public RecordSummary complete() {
        try {
            final boolean shouldCommit = callbacks.stream().noneMatch(ProducerCallback::isFailure);
            if (shouldCommit) {
                producer.flush();  // finish Kafka processing of in-flight data
                wrapper.commit();  // commit Kafka transaction (when transactions configured)
            } else {
                // rollback on transactions + exception
                wrapper.abort();
            }

            final RecordSummary recordSummary = new RecordSummary();  // scrape the Kafka callbacks for disposition of in-flight data
            final List<FlowFileResult> flowFileResults = recordSummary.getFlowFileResults();
            for (final ProducerCallback callback : callbacks) {
                // short-circuit the handling of the flowfile results here
                if (callback.isFailure()) {
                    flowFileResults.add(callback.toFailureResult());
                } else {
                    flowFileResults.add(callback.waitComplete(serviceConfiguration.getMaxAckWait().toMillis()));
                }
            }

            return recordSummary;
        } finally {
            callbacks.clear();
        }
    }

    @Override
    public List<PartitionState> getPartitionStates(final String topic) {
        final List<PartitionInfo> partitionInfos = producer.partitionsFor(topic);
        return partitionInfos.stream()
                .map(p -> new PartitionState(p.topic(), p.partition()))
                .collect(Collectors.toList());
    }
}
