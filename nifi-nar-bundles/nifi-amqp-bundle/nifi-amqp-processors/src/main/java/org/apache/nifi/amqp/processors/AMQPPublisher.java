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

import org.apache.nifi.logging.ComponentLog;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ReturnListener;

/**
 * Generic publisher of messages to AMQP-based messaging system. It is based on
 * RabbitMQ client API (https://www.rabbitmq.com/api-guide.html)
 */
final class AMQPPublisher extends AMQPWorker {

    private final ComponentLog processLog;
    private final String connectionString;

    /**
     * Creates an instance of this publisher
     *
     * @param connection instance of AMQP {@link Connection}
     */
    AMQPPublisher(Connection connection, ComponentLog processLog) {
        super(connection);
        this.processLog = processLog;
        getChannel().addReturnListener(new UndeliverableMessageLogger());
        this.connectionString = connection.toString();
    }

    /**
     * Publishes message with provided AMQP properties (see
     * {@link BasicProperties}) to a pre-defined AMQP Exchange.
     *
     * @param bytes bytes representing a message.
     * @param properties instance of {@link BasicProperties}
     * @param exchange the name of AMQP exchange to which messages will be published.
     *            If not provided 'default' exchange will be used.
     * @param routingKey (required) the name of the routingKey to be used by AMQP-based
     *            system to route messages to its final destination (queue).
     */
    void publish(byte[] bytes, BasicProperties properties, String routingKey, String exchange) {
        this.validateStringProperty("routingKey", routingKey);
        exchange = exchange == null ? "" : exchange.trim();
        if (exchange.length() == 0) {
            processLog.info("The 'exchangeName' is not specified. Messages will be sent to default exchange");
        }
        processLog.info("Successfully connected AMQPPublisher to " + this.connectionString + " and '" + exchange
                + "' exchange with '" + routingKey + "' as a routing key.");

        final Channel channel = getChannel();
        if (channel.isOpen()) {
            try {
                channel.basicPublish(exchange, routingKey, true, properties, bytes);
            } catch (Exception e) {
                throw new IllegalStateException("Failed to publish to Exchange '" + exchange + "' with Routing Key '" + routingKey + "'.", e);
            }
        } else {
            throw new IllegalStateException("This instance of AMQPPublisher is invalid since its publishingChannel is closed");
        }
    }

    @Override
    public String toString() {
        return this.connectionString;
    }

    /**
     * Listener to listen and WARN-log undeliverable messages which are returned
     * back to the sender. Since in the current implementation messages are sent
     * with 'mandatory' bit set, such messages must have final destination
     * otherwise they are silently dropped which could cause a confusion
     * especially during early stages of flow development. This implies that
     * bindings between exchange -> routingKey -> queue must exist and are
     * typically done by AMQP administrator. This logger simply helps to monitor
     * for such conditions by logging such messages as warning. In the future
     * this can be extended to provide other type of functionality (e.g., fail
     * processor etc.)
     */
    private final class UndeliverableMessageLogger implements ReturnListener {
        @Override
        public void handleReturn(int replyCode, String replyText, String exchangeName, String routingKey, BasicProperties properties, byte[] message) throws IOException {
            String logMessage = "Message destined for '" + exchangeName + "' exchange with '" + routingKey
                    + "' as routing key came back with replyCode=" + replyCode + " and replyText=" + replyText + ".";
            processLog.warn(logMessage);
            AMQPPublisher.this.processLog.warn(logMessage);
        }
    }
}
