/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.gettcp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Implementation of receiving network client.
 */
class ReceivingClient extends AbstractSocketHandler {

    private final ScheduledExecutorService connectionScheduler;

    private volatile int reconnectAttempts;

    private volatile long delayMillisBeforeReconnect;

    private volatile MessageHandler messageHandler;

    private volatile InetSocketAddress connectedAddress;

    public ReceivingClient(InetSocketAddress address, ScheduledExecutorService connectionScheduler, int readingBufferSize, byte endOfMessageByte) {
        super(address, readingBufferSize, endOfMessageByte);
        this.connectionScheduler = connectionScheduler;
    }

    public void setReconnectAttempts(int reconnectAttempts) {
        this.reconnectAttempts = reconnectAttempts;
    }

    public void setDelayMillisBeforeReconnect(long delayMillisBeforeReconnect) {
        this.delayMillisBeforeReconnect = delayMillisBeforeReconnect;
    }

    public void setMessageHandler(MessageHandler messageHandler) {
        this.messageHandler = messageHandler;
    }

    /**
     *
     */
    @Override
    InetSocketAddress connect() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger attempt = new AtomicInteger();
        AtomicReference<Exception> connectionError = new AtomicReference<Exception>();
        this.connectionScheduler.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    rootChannel = doConnect(address);
                    latch.countDown();
                    connectedAddress = address;
                } catch (Exception e) {
                    if (logger.isInfoEnabled()) {
                        logger.info("Failed to connect to primary endpoint '" + address + "'.");
                    }
                    if (attempt.incrementAndGet() <= reconnectAttempts) {
                        if (logger.isInfoEnabled()) {
                            logger.info("Will attempt to reconnect to '" + address + "'.");
                        }
                        connectionScheduler.schedule(this, delayMillisBeforeReconnect, TimeUnit.MILLISECONDS);
                    } else {
                        connectionError.set(e);
                        logger.error("Failed to connect to secondary endpoint.");
                        latch.countDown();
                    }
                }
            }
        });

        try {
            boolean finishedTask = latch.await(this.reconnectAttempts * delayMillisBeforeReconnect + 2000, TimeUnit.MILLISECONDS);
            if (finishedTask){
                if (connectionError.get() != null) {
                    throw connectionError.get();
                }
            } else {
                logger.error("Exceeded wait time to connect. Possible deadlock, please report!. Interrupting."); // should never happen!
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Current thread is interrupted");
        }
        return this.connectedAddress;
    }

    private SocketChannel doConnect(InetSocketAddress addressToConnect) throws IOException {
        SocketChannel channel = SocketChannel.open();
        if (channel.connect(addressToConnect)) {
            channel.configureBlocking(false);
            channel.register(this.selector, SelectionKey.OP_READ);
        } else {
            throw new IllegalStateException("Failed to connect to Server at: " + addressToConnect);
        }
        return channel;
    }

    /**
     *
     */
    @Override
    void processData(SelectionKey selectionKey, ByteBuffer messageBuffer) throws IOException {
        byte[] message = new byte[messageBuffer.limit()];
        logger.debug("Received message(size=" + message.length + ")");
        messageBuffer.get(message);
        byte lastByteValue = message[message.length - 1];
        boolean partialMessage = false;
        if (lastByteValue != this.endOfMessageByte) {
            partialMessage = true;
            selectionKey.attach(1);
        } else {
            Integer wasLastPartial = (Integer) selectionKey.attachment();
            if (wasLastPartial != null) {
                if (wasLastPartial.intValue() == 1) {
                    partialMessage = true;
                    selectionKey.attach(0);
                }
            }
        }
        if (this.messageHandler != null) {
            this.messageHandler.handle(this.connectedAddress, message, partialMessage);
        }
    }
}