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

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ReturnListener;
import com.rabbitmq.client.ShutdownSignalException;
import org.apache.nifi.logging.ComponentLog;

import java.io.IOException;
import java.net.SocketException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Generic publisher of messages to AMQP-based messaging system. It is based on
 * RabbitMQ client API (<a href="https://www.rabbitmq.com/api-guide.html">Java Client API Guide</a>)
 */
final class AMQPPublisher extends AMQPWorker {

    private final String connectionString;
    private final boolean useConfirms;

    /**
     * Stores the broker's return reason when a message is published with mandatory=true
     * but the broker cannot route it to any queue. Written by the AMQP I/O thread via
     * {@link UndeliverableMessageLogger} and read by the publishing thread after
     * {@link com.rabbitmq.client.Channel#waitForConfirms} synchronizes the two.
     * Only populated when {@link #useConfirms} is true.
     */
    private final AtomicReference<String> undeliverableReturnReason = new AtomicReference<>(null);

    /**
     * Creates an instance of this publisher
     *
     * @param connection instance of AMQP {@link Connection}
     * @param useConfirms when true, enables RabbitMQ Publisher Confirms so that
     *                    {@link #publish} waits for a broker ack/nack and reliably
     *                    detects undeliverable messages; when false, the original
     *                    fire-and-forget behaviour is used for maximum throughput
     */
    AMQPPublisher(Connection connection, ComponentLog processorLog, boolean useConfirms) {
        super(connection, processorLog);
        this.useConfirms = useConfirms;
        getChannel().addReturnListener(new UndeliverableMessageLogger());
        this.connectionString = connection.toString();

        if (useConfirms) {
            try {
                getChannel().confirmSelect();
            } catch (final IOException e) {
                throw new AMQPException("Failed to enable Publisher Confirms on AMQP channel", e);
            }
        }

        processorLog.info("Successfully connected AMQPPublisher to {}", this.connectionString);
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
            if (exchange.isEmpty()) {
                processorLog.debug("The 'exchangeName' is not specified. Messages will be sent to default exchange");
            }
            processorLog.debug("Successfully connected AMQPPublisher to {} and '{}' exchange with '{}' as a routing key.", this.connectionString, exchange, routingKey);
        }

        // Reset any stale return reason from a previous publish before sending.
        undeliverableReturnReason.set(null);

        try {
            getChannel().basicPublish(exchange, routingKey, true, properties, bytes);
        } catch (AlreadyClosedException | SocketException e) {
            throw new AMQPRollbackException("Failed to publish message because the AMQP connection is lost or has been closed", e);
        } catch (Exception e) {
            throw new AMQPException("Failed to publish message to Exchange '" + exchange + "' with Routing Key '" + routingKey + "'.", e);
        }

        if (useConfirms) {
            // Wait for the broker's publish confirm (ack/nack). Because the broker sends a basic.return
            // frame BEFORE the corresponding confirm frame for mandatory messages it cannot route,
            // UndeliverableMessageLogger.handleReturn() is guaranteed to have run by the time
            // waitForConfirms() returns. This makes undeliverable-message detection reliable.
            try {
                if (!getChannel().waitForConfirms(5_000L)) {
                    throw new AMQPException("Broker negatively acknowledged (NACK) message published to Exchange '"
                            + exchange + "' with Routing Key '" + routingKey + "'");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AMQPException("Interrupted while waiting for publish confirmation from broker", e);
            } catch (TimeoutException e) {
                throw new AMQPException("Timed out waiting for publish confirmation from broker for Exchange '"
                        + exchange + "' with Routing Key '" + routingKey + "'", e);
            } catch (ShutdownSignalException e) {
                throw new AMQPException("Broker closed channel while waiting for publish confirmation — "
                        + "Exchange '" + exchange + "' may not exist: " + e.getMessage(), e);
            }

            final String returnReason = undeliverableReturnReason.get();
            if (returnReason != null) {
                throw new AMQPException(returnReason);
            }
        }
    }

    @Override
    public String toString() {
        return this.connectionString;
    }

    /**
     * Listens for messages returned by the broker when they cannot be routed to any queue
     * (mandatory=true publish with no matching binding).
     *
     * In {@link PublishAMQP.DeliveryGuarantee#AT_MOST_ONCE} mode (the default), this listener
     * only logs a warning — matching the original behaviour.
     *
     * In {@link PublishAMQP.DeliveryGuarantee#AT_LEAST_ONCE} mode, the return reason is also
     * stored in {@link #undeliverableReturnReason} so that {@link #publish} can detect it after
     * {@code waitForConfirms()} synchronizes the two threads and throw an {@link AMQPException}
     * to trigger REL_FAILURE routing.
     */
    private final class UndeliverableMessageLogger implements ReturnListener {
        @Override
        public void handleReturn(int replyCode, String replyText, String exchangeName, String routingKey, BasicProperties properties, byte[] message) throws IOException {
            final String reason = "Message returned as undeliverable by broker: exchange='" + exchangeName
                    + "' routingKey='" + routingKey + "' replyCode=" + replyCode + " replyText='" + replyText + "'";
            if (useConfirms) {
                undeliverableReturnReason.set(reason);
            }
            processorLog.warn(reason);
        }
    }
}
