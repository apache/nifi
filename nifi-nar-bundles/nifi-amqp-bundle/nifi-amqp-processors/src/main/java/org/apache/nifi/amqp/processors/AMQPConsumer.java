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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;

/**
 * Generic consumer of messages from AMQP-based messaging system. It is based on
 * RabbitMQ client API (https://www.rabbitmq.com/api-guide.html)
 */
final class AMQPConsumer extends AMQPWorker {

    private final String queueName;
    private final BlockingQueue<GetResponse> responseQueue;
    private final boolean autoAcknowledge;
    private final Consumer consumer;

    AMQPConsumer(final Connection connection, final String queueName, final boolean autoAcknowledge, ComponentLog processorLog) throws IOException {
        super(connection, processorLog);
        this.validateStringProperty("queueName", queueName);
        this.queueName = queueName;
        this.autoAcknowledge = autoAcknowledge;
        this.responseQueue = new LinkedBlockingQueue<>(10);

        processorLog.info("Successfully connected AMQPConsumer to " + connection.toString() + " and '" + queueName + "' queue");

        final Channel channel = getChannel();
        consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(final String consumerTag, final Envelope envelope, final BasicProperties properties, final byte[] body) throws IOException {
                if (closed) {
                    // simply discard the messages, all unacknowledged messages will be redelivered by the broker when the consumer connects again
                    processorLog.info("Consumer is closed, discarding message (delivery tag: {}).", new Object[]{envelope.getDeliveryTag()});
                    return;
                }

                try {
                    responseQueue.put(new GetResponse(envelope, properties, body, Integer.MAX_VALUE));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            @Override
            public void handleCancel(String consumerTag) throws IOException {
                processorLog.error("Consumer has been cancelled by the broker, eg. due to deleted queue.");
                try {
                    close();
                } catch (Exception e) {
                    processorLog.error("Failed to close consumer.", e);
                }
            }
        };

        channel.basicConsume(queueName, autoAcknowledge, consumer);
    }

    // Visible for unit tests
    int getResponseQueueSize() {
        return responseQueue.size();
    }

    /**
     * Consumes message from the queue identified by 'queueName' on each
     * invocation via {@link Channel#basicGet(String, boolean)} operation
     * returning instance of {@link GetResponse} In the event there are no
     * messages in the queue it will return null. In the event queue is not
     * defined in the system exception is raised by the target API. It will be
     * logged and propagated as {@link ProcessException}
     *
     * @return instance of {@link GetResponse}
     */
    public GetResponse consume() {
        return responseQueue.poll();
    }

    public void acknowledge(final GetResponse response) {
        if (autoAcknowledge) {
            return;
        }

        try {
            getChannel().basicAck(response.getEnvelope().getDeliveryTag(), true);
        } catch (Exception e) {
            throw new AMQPException("Failed to acknowledge message", e);
        }
    }

    @Override
    public void close() throws TimeoutException, IOException {
        try {
            super.close();
        } finally {
            try {
                GetResponse response;
                while ((response = responseQueue.poll()) != null) {
                    // simply discard the messages, all unacknowledged messages will be redelivered by the broker when the consumer connects again
                    processorLog.info("Consumer is closed, discarding message (delivery tag: {}).", new Object[]{response.getEnvelope().getDeliveryTag()});
                }
            } catch (Exception e) {
                processorLog.error("Failed to drain response queue.");
            }
        }
    }

    @Override
    public String toString() {
        return super.toString() + ", QUEUE:" + this.queueName;
    }
}
