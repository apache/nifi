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

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.exception.ProcessException;

/**
 * Base class for {@link Processor}s to publish and consume messages from Kafka
 *
 * @see PutKafka
 */
abstract class AbstractKafkaProcessor<T extends Closeable> extends AbstractSessionFactoryProcessor {


    private volatile boolean acceptTask = true;

    private final AtomicInteger taskCounter = new AtomicInteger();


    /**
     * @see KafkaPublisher
     */
    volatile T kafkaResource;

    /**
     *
     */
    @Override
    public final void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        if (this.acceptTask) { // acts as a circuit breaker to allow existing tasks to wind down so 'kafkaResource' can be reset before new tasks are accepted.
            this.taskCounter.incrementAndGet();
            final ProcessSession session = sessionFactory.createSession();
            try {
                /*
                 * We can't be doing double null check here since as a pattern
                 * it only works for lazy init but not reset, which is what we
                 * are doing here. In fact the first null check is dangerous
                 * since 'kafkaResource' can become null right after its null
                 * check passed causing subsequent NPE.
                 */
                synchronized (this) {
                    if (this.kafkaResource == null) {
                        this.kafkaResource = this.buildKafkaResource(context, session);
                    }
                }

                /*
                 * The 'processed' boolean flag does not imply any failure or success. It simply states that:
                 * - ConsumeKafka - some messages were received form Kafka and 1_ FlowFile were generated
                 * - PublishKafka - some messages were sent to Kafka based on existence of the input FlowFile
                 */
                boolean processed = this.rendezvousWithKafka(context, session);
                session.commit();
                if (processed) {
                    this.postCommit(context);
                } else {
                    context.yield();
                }
            } catch (Throwable e) {
                this.acceptTask = false;
                session.rollback(true);
                this.getLogger().error("{} failed to process due to {}; rolling back session", new Object[] { this, e });
            } finally {
                synchronized (this) {
                    if (this.taskCounter.decrementAndGet() == 0 && !this.acceptTask) {
                        this.close();
                        this.acceptTask = true;
                    }
                }
            }
        } else {
            context.yield();
        }
    }

    /**
     * Will call {@link Closeable#close()} on the target resource after which
     * the target resource will be set to null. Should only be called when there
     * are no more threads being executed on this processor or when it has been
     * verified that only a single thread remains.
     *
     * @see KafkaPublisher
     */
    @OnStopped
    public void close() {
        if (this.taskCounter.get() == 0) {
            try {
                if (this.kafkaResource != null) {
                    try {
                        this.kafkaResource.close();
                    } catch (Exception e) {
                        this.getLogger().warn("Failed while closing " + this.kafkaResource, e);
                    }
                }
            } finally {
                this.kafkaResource = null;
            }
        }
    }

    /**
     * This operation will be executed after {@link ProcessSession#commit()} has
     * been called.
     */
    protected void postCommit(ProcessContext context) {

    }

    /**
     * This operation is called from
     * {@link #onTrigger(ProcessContext, ProcessSessionFactory)} method and
     * contains main processing logic for this Processor.
     */
    protected abstract boolean rendezvousWithKafka(ProcessContext context, ProcessSession session);

    /**
     * Builds target resource for interacting with Kafka. The target resource
     * could be one of {@link KafkaPublisher} or {@link KafkaConsumer}
     */
    protected abstract T buildKafkaResource(ProcessContext context, ProcessSession session) throws ProcessException;
}
