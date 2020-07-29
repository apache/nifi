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
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP.Basic.RecoverOk;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AMQP.Exchange.BindOk;
import com.rabbitmq.client.AMQP.Exchange.DeclareOk;
import com.rabbitmq.client.AMQP.Exchange.DeleteOk;
import com.rabbitmq.client.AMQP.Exchange.UnbindOk;
import com.rabbitmq.client.AMQP.Queue.PurgeOk;
import com.rabbitmq.client.AMQP.Tx.CommitOk;
import com.rabbitmq.client.AMQP.Tx.RollbackOk;
import com.rabbitmq.client.AMQP.Tx.SelectOk;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Command;
import com.rabbitmq.client.ConfirmCallback;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.ConsumerShutdownSignalCallback;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.Method;
import com.rabbitmq.client.ReturnCallback;
import com.rabbitmq.client.ReturnListener;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * Implementation of {@link Channel} to be used during testing
 */
class TestChannel implements Channel {

    private final ExecutorService executorService;
    private final Map<String, List<Consumer>> consumerMap = new HashMap<>();

    private final Map<String, BlockingQueue<GetResponse>> enqueuedMessages;
    private final Map<String, List<String>> routingKeyToQueueMappings;
    private final Map<String, String> exchangeToRoutingKeyMappings;
    private final List<ReturnListener> returnListeners;
    private boolean open;
    private boolean corrupted;
    private Connection connection;
    private long deliveryTag = 0L;
    private final BitSet acknowledgments = new BitSet();
    private final BitSet nacks = new BitSet();

    public TestChannel(Map<String, String> exchangeToRoutingKeyMappings,
            Map<String, List<String>> routingKeyToQueueMappings) {
        this.enqueuedMessages = new HashMap<>();
        this.routingKeyToQueueMappings = routingKeyToQueueMappings;
        if (this.routingKeyToQueueMappings != null) {
            for (List<String> queues : routingKeyToQueueMappings.values()) {
                for (String queue : queues) {
                    this.enqueuedMessages.put(queue, new ArrayBlockingQueue<GetResponse>(100));
                }
            }
        }
        this.exchangeToRoutingKeyMappings = exchangeToRoutingKeyMappings;
        this.executorService = Executors.newCachedThreadPool();
        this.returnListeners = new ArrayList<>();
        this.open = true;
    }

    void corruptChannel() {
        this.corrupted = true;
    }

    void setConnection(Connection connection) {
        this.connection = connection;
    }

    @Override
    public void addShutdownListener(ShutdownListener listener) {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");

    }

    @Override
    public void removeShutdownListener(ShutdownListener listener) {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");

    }

    @Override
    public ShutdownSignalException getCloseReason() {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public void notifyListeners() {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");

    }

    @Override
    public boolean isOpen() {
        return this.open;
    }

    @Override
    public int getChannelNumber() {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public Connection getConnection() {
        return this.connection;
    }

    @Override
    public void close() throws IOException, TimeoutException {
        this.open = false;
    }

    @Override
    public void close(int closeCode, String closeMessage) throws IOException, TimeoutException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");

    }

    @Override
    public void abort() throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");

    }

    @Override
    public void abort(int closeCode, String closeMessage) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");

    }

    @Override
    public void addReturnListener(ReturnListener listener) {
        this.returnListeners.add(listener);
    }

    @Override
    public boolean removeReturnListener(ReturnListener listener) {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public void clearReturnListeners() {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");

    }

    @Override
    public void addConfirmListener(ConfirmListener listener) {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");

    }

    @Override
    public boolean removeConfirmListener(ConfirmListener listener) {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public void clearConfirmListeners() {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");

    }

    @Override
    public Consumer getDefaultConsumer() {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public void setDefaultConsumer(Consumer consumer) {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");

    }

    @Override
    public void basicQos(int prefetchSize, int prefetchCount, boolean global) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");

    }

    @Override
    public void basicQos(int prefetchCount, boolean global) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");

    }

    @Override
    public void basicQos(int prefetchCount) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");

    }

    @Override
    public void basicPublish(String exchange, String routingKey, BasicProperties props, byte[] body)
            throws IOException {
        this.basicPublish(exchange, routingKey, true, props, body);
    }

    @Override
    public void basicPublish(final String exchange, final String routingKey, boolean mandatory,
            final BasicProperties props, final byte[] body) throws IOException {
        if (this.corrupted) {
            throw new IOException("Channel is corrupted");
        }

        if (exchange.equals("")){ // default exchange; routingKey corresponds to a queue.
            BlockingQueue<GetResponse> messages = this.getMessageQueue(routingKey);
            final Envelope envelope = new Envelope(deliveryTag++, false, exchange, routingKey);

            GetResponse response = new GetResponse(envelope, props, body, messages.size());
            messages.offer(response);
        } else {
            String rKey = this.exchangeToRoutingKeyMappings.get(exchange);

            if (rKey.equals(routingKey)) {
                List<String> queueNames = this.routingKeyToQueueMappings.get(routingKey);
                if (queueNames == null || queueNames.isEmpty()) {
                    this.discard(exchange, routingKey, mandatory, props, body);
                } else {
                    for (String queueName : queueNames) {
                        BlockingQueue<GetResponse> messages = this.getMessageQueue(queueName);
                        final Envelope envelope = new Envelope(deliveryTag++, false, exchange, routingKey);
                        GetResponse response = new GetResponse(envelope, props, body, messages.size());
                        messages.offer(response);

                        final List<Consumer> consumers = consumerMap.get(queueName);
                        if (consumers != null) {
                            for (final Consumer consumer : consumers) {
                                consumer.handleDelivery("consumerTag", envelope, props, body);
                            }
                        }
                    }
                }
            } else {
                this.discard(exchange, routingKey, mandatory, props, body);
            }

        }
    }

    private void discard(final String exchange, final String routingKey, boolean mandatory, final BasicProperties props,
            final byte[] body) {
        // NO ROUTE. Invoke return listener async
        for (final ReturnListener listener : returnListeners) {
            this.executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        listener.handleReturn(-9, "Rejecting", exchange, routingKey, props, body);
                    } catch (Exception e) {
                        throw new IllegalStateException("Failed to send return message", e);
                    }
                }
            });
        }
    }

    private BlockingQueue<GetResponse> getMessageQueue(String name) {
        BlockingQueue<GetResponse> messages = this.enqueuedMessages.get(name);
        if (messages == null) {
            messages = new ArrayBlockingQueue<>(100);
            this.enqueuedMessages.put(name, messages);
        }
        return messages;
    }

    @Override
    public void basicPublish(String exchange, String routingKey, boolean mandatory, boolean immediate,
            BasicProperties props, byte[] body) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public DeclareOk exchangeDeclare(String exchange, String type) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public DeclareOk exchangeDeclare(String exchange, String type, boolean durable) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public DeclareOk exchangeDeclare(String exchange, String type, boolean durable, boolean autoDelete,
            Map<String, Object> arguments) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public DeclareOk exchangeDeclare(String exchange, String type, boolean durable, boolean autoDelete,
            boolean internal, Map<String, Object> arguments) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public void exchangeDeclareNoWait(String exchange, String type, boolean durable, boolean autoDelete,
            boolean internal, Map<String, Object> arguments) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");

    }

    @Override
    public DeclareOk exchangeDeclarePassive(String name) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public DeleteOk exchangeDelete(String exchange, boolean ifUnused) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public void exchangeDeleteNoWait(String exchange, boolean ifUnused) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");

    }

    @Override
    public DeleteOk exchangeDelete(String exchange) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public BindOk exchangeBind(String destination, String source, String routingKey) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public BindOk exchangeBind(String destination, String source, String routingKey, Map<String, Object> arguments)
            throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public void exchangeBindNoWait(String destination, String source, String routingKey, Map<String, Object> arguments)
            throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");

    }

    @Override
    public UnbindOk exchangeUnbind(String destination, String source, String routingKey) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public UnbindOk exchangeUnbind(String destination, String source, String routingKey, Map<String, Object> arguments)
            throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public void exchangeUnbindNoWait(String destination, String source, String routingKey,
            Map<String, Object> arguments) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");

    }

    @Override
    public com.rabbitmq.client.AMQP.Queue.DeclareOk queueDeclare() throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public com.rabbitmq.client.AMQP.Queue.DeclareOk queueDeclare(String queue, boolean durable, boolean exclusive,
            boolean autoDelete, Map<String, Object> arguments) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public void queueDeclareNoWait(String queue, boolean durable, boolean exclusive, boolean autoDelete,
            Map<String, Object> arguments) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");

    }

    @Override
    public com.rabbitmq.client.AMQP.Queue.DeclareOk queueDeclarePassive(String queue) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public com.rabbitmq.client.AMQP.Queue.DeleteOk queueDelete(String queue) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public com.rabbitmq.client.AMQP.Queue.DeleteOk queueDelete(String queue, boolean ifUnused, boolean ifEmpty)
            throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public void queueDeleteNoWait(String queue, boolean ifUnused, boolean ifEmpty) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");

    }

    @Override
    public com.rabbitmq.client.AMQP.Queue.BindOk queueBind(String queue, String exchange, String routingKey)
            throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public com.rabbitmq.client.AMQP.Queue.BindOk queueBind(String queue, String exchange, String routingKey,
            Map<String, Object> arguments) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public void queueBindNoWait(String queue, String exchange, String routingKey, Map<String, Object> arguments)
            throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");

    }

    @Override
    public com.rabbitmq.client.AMQP.Queue.UnbindOk queueUnbind(String queue, String exchange, String routingKey)
            throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public com.rabbitmq.client.AMQP.Queue.UnbindOk queueUnbind(String queue, String exchange, String routingKey,
            Map<String, Object> arguments) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public PurgeOk queuePurge(String queue) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public GetResponse basicGet(String queue, boolean autoAck) throws IOException {
        BlockingQueue<GetResponse> messages = this.enqueuedMessages.get(queue);
        if (messages == null) {
            throw new IOException("Queue is not defined");
        } else {
            return messages.poll();
        }
    }

    @Override
    public void basicAck(long deliveryTag, boolean multiple) throws IOException {
        acknowledgments.set((int) deliveryTag);
    }

    public boolean isAck(final int deliveryTag) {
        return acknowledgments.get(deliveryTag);
    }

    @Override
    public void basicNack(long deliveryTag, boolean multiple, boolean requeue) throws IOException {
        nacks.set((int) deliveryTag);
    }

    public boolean isNack(final int deliveryTag) {
        return nacks.get(deliveryTag);
    }

    @Override
    public void basicReject(long deliveryTag, boolean requeue) throws IOException {
        nacks.set((int) deliveryTag);
    }

    @Override
    public String basicConsume(String queue, Consumer callback) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, Consumer callback) throws IOException {
        final BlockingQueue<GetResponse> messageQueue = enqueuedMessages.get(queue);
        if (messageQueue == null) {
            throw new IOException("Queue is not defined");
        }

        consumerMap.computeIfAbsent(queue, q -> new ArrayList<>()).add(callback);

        final String consumerTag = UUID.randomUUID().toString();

        GetResponse message;
        while ((message = messageQueue.poll()) != null) {
            callback.handleDelivery(consumerTag, message.getEnvelope(), message.getProps(), message.getBody());
        }

        return consumerTag;
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, Map<String, Object> arguments, Consumer callback)
            throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, Consumer callback)
            throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal, boolean exclusive,
            Map<String, Object> arguments, Consumer callback) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public void basicCancel(String consumerTag) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");

    }

    @Override
    public RecoverOk basicRecover() throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public RecoverOk basicRecover(boolean requeue) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public SelectOk txSelect() throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public CommitOk txCommit() throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public RollbackOk txRollback() throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public com.rabbitmq.client.AMQP.Confirm.SelectOk confirmSelect() throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public long getNextPublishSeqNo() {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public boolean waitForConfirms() throws InterruptedException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public boolean waitForConfirms(long timeout) throws InterruptedException, TimeoutException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public void waitForConfirmsOrDie() throws IOException, InterruptedException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");

    }

    @Override
    public void waitForConfirmsOrDie(long timeout) throws IOException, InterruptedException, TimeoutException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");

    }

    @Override
    public void asyncRpc(Method method) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");

    }

    @Override
    public Command rpc(Method method) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public long messageCount(String queue) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public long consumerCount(String queue) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public ReturnListener addReturnListener(ReturnCallback returnCallback) {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public ConfirmListener addConfirmListener(ConfirmCallback ackCallback, ConfirmCallback nackCallback) {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type, boolean durable) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type, boolean durable, boolean autoDelete, Map<String, Object> arguments) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type, boolean durable, boolean autoDelete, boolean internal, Map<String, Object> arguments) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public void exchangeDeclareNoWait(String exchange, BuiltinExchangeType type, boolean durable, boolean autoDelete, boolean internal, Map<String, Object> arguments) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public String basicConsume(String queue, DeliverCallback deliverCallback, CancelCallback cancelCallback) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public String basicConsume(String queue, DeliverCallback deliverCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public String basicConsume(String queue, DeliverCallback deliverCallback, CancelCallback cancelCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, DeliverCallback deliverCallback, CancelCallback cancelCallback) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, DeliverCallback deliverCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, DeliverCallback deliverCallback, CancelCallback cancelCallback, ConsumerShutdownSignalCallback shutdownSignalCallback)
        throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, Map<String, Object> arguments, DeliverCallback deliverCallback, CancelCallback cancelCallback) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, Map<String, Object> arguments, DeliverCallback deliverCallback, ConsumerShutdownSignalCallback shutdownSignalCallback)
        throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, Map<String, Object> arguments, DeliverCallback deliverCallback, CancelCallback cancelCallback,
        ConsumerShutdownSignalCallback shutdownSignalCallback) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, DeliverCallback deliverCallback, CancelCallback cancelCallback) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, DeliverCallback deliverCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, DeliverCallback deliverCallback, CancelCallback cancelCallback, ConsumerShutdownSignalCallback shutdownSignalCallback)
        throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal, boolean exclusive, Map<String, Object> arguments, DeliverCallback deliverCallback,
        CancelCallback cancelCallback) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal, boolean exclusive, Map<String, Object> arguments, DeliverCallback deliverCallback,
        ConsumerShutdownSignalCallback shutdownSignalCallback) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal, boolean exclusive, Map<String, Object> arguments, DeliverCallback deliverCallback,
        CancelCallback cancelCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

    @Override
    public CompletableFuture<Command> asyncCompletableRpc(Method method) throws IOException {
        throw new UnsupportedOperationException("This method is not currently supported as it is not used by current API in testing");
    }

}
