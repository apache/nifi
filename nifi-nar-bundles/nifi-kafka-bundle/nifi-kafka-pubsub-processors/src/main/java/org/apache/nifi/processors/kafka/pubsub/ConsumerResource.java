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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.nifi.logging.ComponentLog;

import java.io.Closeable;
import java.io.IOException;

/**
 * A wrapper for a Kafka Consumer obtained from a ConsumerPool. Client's should call poison() to indicate that this
 * consumer should no longer be used by other clients, and should always call close(). Calling close() will pass
 * this consumer back to the pool and the pool will determine the appropriate handling based on whether the consumer
 * has been poisoned and whether the pool is already full.
 */
public class ConsumerResource implements Closeable {

    private final ComponentLog logger;
    private final Consumer<byte[],byte[]> consumer;
    private final ConsumerPool consumerPool;
    private boolean poisoned = false;

    /**
     * @param consumer the Kafka Consumer
     * @param consumerPool the ConsumerPool this ConsumerResource was obtained from
     * @param logger the logger to report any errors/warnings
     */
    public ConsumerResource(Consumer<byte[], byte[]> consumer, ConsumerPool consumerPool, ComponentLog logger) {
        this.logger = logger;
        this.consumer = consumer;
        this.consumerPool = consumerPool;
    }

    /**
     * @return the Kafka Consumer for this
     */
    public Consumer<byte[],byte[]> getConsumer() {
        return consumer;
    }

    /**
     * Sets the poison flag for this consumer to true.
     */
    public void poison() {
        poisoned = true;
    }

    /**
     * @return true if this consumer has been poisoned, false otherwise
     */
    public boolean isPoisoned() {
        return poisoned;
    }

    @Override
    public void close() throws IOException {
        consumerPool.returnConsumerResource(this);
    }

}
