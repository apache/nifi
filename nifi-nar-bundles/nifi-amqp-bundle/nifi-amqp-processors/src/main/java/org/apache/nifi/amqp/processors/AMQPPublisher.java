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
import java.net.SocketException;

import com.rabbitmq.client.AlreadyClosedException;
import org.apache.nifi.logging.ComponentLog;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ReturnListener;

/**
 * Generic publisher of messages to AMQP-based messaging system. It is based on
 * RabbitMQ client API (https://www.rabbitmq.com/api-guide.html)
 */
final class AMQPPublisher extends AMQPWorker {

    private final String connectionString;

    /**
     * Creates an instance of this publisher
     *
     * @param connection instance of AMQP {@link Connection}
     */
    AMQPPublisher(Connection connection, ComponentLog processorLog) {
        super(connection, processorLog);
        getChannel().addReturnListener(new UndeliverableMessageLogger());
        this.connectionString = connection.toString();

        processorLog.info("Successfully connected AMQPPublisher to " + this.connectionString);
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

        if (processorLog.isDebugEnabled()) {
            if (exchange.length() == 0) {
                processorLog.debug("The 'exchangeName' is not specified. Messages will be sent to default exchange");
            }
            processorLog.debug("Successfully connected AMQPPublisher to " + this.connectionString + " and '" + exchange
                    + "' exchange with '" + routingKey + "' as a routing key.");
        }

        try {
            getChannel().basicPublish(exchange, routingKey, true, properties, bytes);
        } catch (AlreadyClosedException | SocketException e) {
            throw new AMQPRollbackException("Failed to publish message because the AMQP connection is lost or has been closed", e);
        } catch (Exception e) {
            throw new AMQPException("Failed to publish message to Exchange '" + exchange + "' with Routing Key '" + routingKey + "'.", e);
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
            processorLog.warn(logMessage);
        }
    }
}
