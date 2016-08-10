/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.nifi.processors.email;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.subethamail.smtp.MessageContext;
import org.subethamail.smtp.MessageHandler;
import org.subethamail.smtp.MessageHandlerFactory;
import org.subethamail.smtp.RejectException;
import org.subethamail.smtp.TooMuchDataException;
import org.subethamail.smtp.server.SMTPServer;

/**
 * A simple consumer that provides a bridge between 'push' message distribution
 * provided by {@link SMTPServer} and NiFi polling scheduler mechanism. It
 * implements both {@link MessageHandler} and {@link MessageHandlerFactory}
 * allowing it to interact directly with {@link SMTPServer}.
 */
class SmtpConsumer implements MessageHandler, MessageHandlerFactory {

    private final static int CONSUMER_STOPPED = -1;

    private final static int INTERRUPTED = -2;

    private final static int ERROR = -9;

    private final static int NO_MESSAGE = -8;

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final BlockingQueue<InputStream> messageDataQueue = new ArrayBlockingQueue<>(1);

    private final BlockingQueue<Integer> consumedMessageSizeQueue = new ArrayBlockingQueue<>(1);

    private final AtomicBoolean running = new AtomicBoolean(true);

    private volatile MessageContext messageContext;

    private volatile String from;

    private volatile String recipient;


    /**
     *
     */
    String getFrom() {
        return this.from;
    }

    /**
     *
     */
    String getRecipient() {
        return this.recipient;
    }

    /**
     *
     */
    MessageContext getMessageContext() {
        return this.messageContext;
    }

    /**
     * This operation will simply attempt to put a poison message to the
     * 'consumedMessageSizeQueue' to ensure that in the event this consumer is
     * stopped before the message is consumed (see
     * {@link #consumeUsing(Function)}), the server thread that is blocking in
     * {@link #data(InputStream)} operation can unblock.
     */
    // NOTE: the 'synchronize' is only here for API correctness, to ensure that
    // stop() and consumeUsing(..) can never be invoked at the same time.
    // However within NiFi it can never happen.
    synchronized void stop() {
        this.running.compareAndSet(true, false);
        this.consumedMessageSizeQueue.offer(CONSUMER_STOPPED);
    }

    /**
     * This operation is invoked by the consumer. Implementation of this
     * operation creates a synchronous connection with the message producer (see
     * {@link #data(InputStream)}) via a pair of queues which guarantees that
     * message is fully consumed and disposed by the consumer (provided as
     * {@link Function}) before the server closes the data stream.
     */
    // NOTE: the 'synchronize' is only here for API correctness, to ensure that
    // stop() and consumeUsing(..) can never be invoked at the same time.
    // However within NiFi it can never happen.
    synchronized void consumeUsing(Function<InputStream, Integer> resultConsumer) {
        int messageSize = 0;
        try {
            InputStream message = this.messageDataQueue.poll(1000, TimeUnit.MILLISECONDS);
            if (message != null) {
                messageSize = resultConsumer.apply(message);
            } else {
                messageSize = NO_MESSAGE;
            }
        } catch (InterruptedException e) {
            this.logger.warn("Current thread is interrupted", e);
            messageSize = INTERRUPTED;
            Thread.currentThread().interrupt();
        } finally {
            if (messageSize == 0) {
                messageSize = ERROR;
            }
            if (messageSize != NO_MESSAGE) {
                this.consumedMessageSizeQueue.offer(messageSize);
            }
        }
    }

    /**
     * This operation is invoked by the server thread and contains message data
     * as {@link InputStream}. Implementation of this operation creates a
     * synchronous connection with consumer (see {@link #onMessage(Function)})
     * via a pair of queues which guarantees that message is fully consumed and
     * disposed by the consumer by the time this operation exits.
     */
    @Override
    public void data(InputStream data) throws RejectException, TooMuchDataException, IOException {
        if (this.running.get()) {
            try {
                this.messageDataQueue.offer(data, Integer.MAX_VALUE, TimeUnit.SECONDS);
                long messageSize = this.consumedMessageSizeQueue.poll(Integer.MAX_VALUE, TimeUnit.SECONDS);
                String exceptionMessage = null;
                if (messageSize == CONSUMER_STOPPED) {
                    exceptionMessage = "NIFI Consumer was stopped before message was successfully consumed";
                } else if (messageSize == INTERRUPTED) {
                    exceptionMessage = "Consuming thread was interrupted";
                } else if (messageSize == ERROR) {
                    exceptionMessage = "Consuming thread failed while processing 'data' SMTP event.";
                }
                if (exceptionMessage != null) {
                    throw new IllegalStateException(exceptionMessage);
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Received message of size: " + messageSize);
                    }
                }
            } catch (InterruptedException e) {
                this.logger.warn("Current thread is interrupted", e);
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Current thread is interrupted", e);
            }
        } else {
            throw new IllegalStateException("NIFI Consumer was stopped before message was successfully consumed");
        }
    }

    /**
     *
     */
    @Override
    public void from(String from) throws RejectException {
        this.from = from;
    }

    /**
     *
     */
    @Override
    public void recipient(String recipient) throws RejectException {
        this.recipient = recipient;
    }

    /**
     *
     */
    @Override
    public void done() {
        // noop
    }

    /**
     *
     */
    @Override
    public MessageHandler create(MessageContext ctx) {
        this.messageContext = ctx;
        return this;
    }
}
