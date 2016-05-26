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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ReturnListener;

/**
 * Generic publisher of messages to AMQP-based messaging system. It is based on
 * RabbitMQ client API (https://www.rabbitmq.com/api-guide.html)
 */
final class AMQPPublisher extends AMQPWorker {

    private final static Logger logger = LoggerFactory.getLogger(AMQPPublisher.class);

    private final String exchangeName;

    private final String routingKey;

    private final ComponentLog processLog;

    /**
     * Creates an instance of this publisher
     *
     * @param connection
     *            instance of AMQP {@link Connection}
     * @param exchangeName
     *            the name of AMQP exchange to which messages will be published.
     *            If not provided 'default' exchange will be used.
     * @param routingKey
     *            (required) the name of the routingKey to be used by AMQP-based
     *            system to route messages to its final destination (queue).
     */
    AMQPPublisher(Connection connection, String exchangeName, String routingKey, ComponentLog processLog) {
        super(connection);
        this.processLog = processLog;
        this.validateStringProperty("routingKey", routingKey);
        this.exchangeName = exchangeName == null ? "" : exchangeName.trim();
        if (this.exchangeName.length() == 0) {
            logger.info("The 'exchangeName' is not specified. Messages will be sent to default exchange");
        }

        this.routingKey = routingKey;
        this.channel.addReturnListener(new UndeliverableMessageLogger());
        logger.info("Successfully connected AMQPPublisher to " + connection.toString() + " and '" + this.exchangeName
                + "' exchange with '" + routingKey + "' as a routing key.");
    }

    /**
     * Publishes message without any AMQP properties (see
     * {@link BasicProperties}) to a pre-defined AMQP Exchange.
     *
     * @param bytes
     *            bytes representing a message.
     */
    void publish(byte[] bytes) {
        this.publish(bytes, null);
    }

    /**
     * Publishes message with provided AMQP properties (see
     * {@link BasicProperties}) to a pre-defined AMQP Exchange.
     *
     * @param bytes
     *            bytes representing a message.
     * @param properties
     *            instance of {@link BasicProperties}
     */
    void publish(byte[] bytes, BasicProperties properties) {
        if (this.channel.isOpen()) {
            try {
                this.channel.basicPublish(this.exchangeName, this.routingKey, true, properties, bytes);
            } catch (Exception e) {
                throw new IllegalStateException("Failed to publish to '" +
                        this.exchangeName + "' with '" + this.routingKey + "'.", e);
            }
        } else {
            throw new IllegalStateException("This instance of AMQPPublisher is invalid since "
                    + "its publishigChannel is closed");
        }
    }

    /**
     *
     */
    @Override
    public String toString() {
        return super.toString() + ", EXCHANGE:" + this.exchangeName + ", ROUTING_KEY:" + this.routingKey;
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
        public void handleReturn(int replyCode, String replyText, String exchangeName, String routingKey, BasicProperties properties, byte[] message)
                throws IOException {
            String logMessage = "Message destined for '" + exchangeName + "' exchange with '" + routingKey
                    + "' as routing key came back with replyCode=" + replyCode + " and replyText=" + replyText + ".";
            logger.warn(logMessage);
            AMQPPublisher.this.processLog.warn(logMessage);
        }
    }
}
