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
package org.apache.nifi.amqp.processors;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.nifi.logging.ComponentLog;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

/**
 * Base class for implementing publishing and consuming AMQP workers.
 *
 * @see AMQPPublisher
 * @see AMQPConsumer
 */
abstract class AMQPWorker implements AutoCloseable {

    protected final ComponentLog processorLog;

    private final Channel channel;
    protected volatile boolean closed = false;

    /**
     * Creates an instance of this worker initializing it with AMQP
     * {@link Connection} and creating a target {@link Channel} used by
     * sub-classes to interact with AMQP-based messaging system.
     *
     * @param connection instance of {@link Connection}
     */
    public AMQPWorker(final Connection connection, ComponentLog processorLog) {
        this.processorLog = processorLog;

        validateConnection(connection);

        try {
            this.channel = connection.createChannel();
        } catch (IOException e) {
            processorLog.error("Failed to create Channel for {}", connection, e);
            throw new IllegalStateException(e);
        }
    }

    protected Channel getChannel() {
        return channel;
    }

    @Override
    public void close() throws TimeoutException, IOException {
        if (closed) {
            return;
        }

        if (channel.isOpen()) {
            if (processorLog.isDebugEnabled()) {
                processorLog.debug("Closing AMQP channel for {}", this.channel.getConnection());
            }

            this.channel.close();
        }
        closed = true;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + ":" + this.channel.getConnection().toString();
    }

    /**
     * Validates that a String property has value (not null nor empty)
     *
     * @param propertyName the name of the property
     * @param value the value of the property
     */
    void validateStringProperty(String propertyName, String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("'" + propertyName + "' must not be null or empty");
        }
    }

    /**
     * Validates that {@link Connection} is not null and open.
     *
     * @param connection instance of {@link Connection}
     */
    private void validateConnection(Connection connection) {
        if (connection == null) {
            throw new IllegalArgumentException("'connection' must not be null!");
        }
        if (!connection.isOpen()) {
            throw new IllegalStateException("'connection' must be open!");
        }
    }
}
