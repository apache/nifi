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

import org.apache.nifi.processor.exception.ProcessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private final static Logger logger = LoggerFactory.getLogger(AMQPConsumer.class);
    private final String queueName;
    private final BlockingQueue<GetResponse> responseQueue;
    private final boolean autoAcknowledge;
    private final Consumer consumer;

    private volatile boolean closed = false;


    AMQPConsumer(final Connection connection, final String queueName, final boolean autoAcknowledge) throws IOException {
        super(connection);
        this.validateStringProperty("queueName", queueName);
        this.queueName = queueName;
        this.autoAcknowledge = autoAcknowledge;
        this.responseQueue = new LinkedBlockingQueue<>(10);

        logger.info("Successfully connected AMQPConsumer to " + connection.toString() + " and '" + queueName + "' queue");

        final Channel channel = getChannel();
        consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(final String consumerTag, final Envelope envelope, final BasicProperties properties, final byte[] body) throws IOException {
                if (!autoAcknowledge && closed) {
                    channel.basicReject(envelope.getDeliveryTag(), true);
                    return;
                }

                try {
                    responseQueue.put(new GetResponse(envelope, properties, body, Integer.MAX_VALUE));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };

        channel.basicConsume(queueName, autoAcknowledge, consumer);
    }

    // Visible for unit tests
    protected Consumer getConsumer() {
        return consumer;
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

    public void acknowledge(final GetResponse response) throws IOException {
        if (autoAcknowledge) {
            return;
        }

        getChannel().basicAck(response.getEnvelope().getDeliveryTag(), true);
    }

    @Override
    public void close() throws TimeoutException, IOException {
        closed = true;

        GetResponse lastMessage = null;
        GetResponse response;
        while ((response = responseQueue.poll()) != null) {
            lastMessage = response;
        }

        if (lastMessage != null) {
            getChannel().basicNack(lastMessage.getEnvelope().getDeliveryTag(), true, true);
        }
    }

    @Override
    public String toString() {
        return super.toString() + ", QUEUE:" + this.queueName;
    }
}
