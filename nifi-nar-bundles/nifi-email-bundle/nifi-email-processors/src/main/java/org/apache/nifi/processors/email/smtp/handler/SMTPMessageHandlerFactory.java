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

package org.apache.nifi.processors.email.smtp.handler;

import java.io.IOException;
import java.io.InputStream;
import java.security.cert.X509Certificate;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.stream.io.ByteArrayOutputStream;
import org.apache.nifi.util.StopWatch;
import org.subethamail.smtp.DropConnectionException;
import org.subethamail.smtp.MessageContext;
import org.subethamail.smtp.MessageHandler;
import org.subethamail.smtp.MessageHandlerFactory;
import org.subethamail.smtp.RejectException;
import org.subethamail.smtp.TooMuchDataException;
import org.subethamail.smtp.server.SMTPServer;

import org.apache.nifi.processors.email.smtp.event.SmtpEvent;

public class SMTPMessageHandlerFactory implements MessageHandlerFactory {
    final LinkedBlockingQueue<SmtpEvent> incomingMessages;
    final ComponentLog logger;


    public SMTPMessageHandlerFactory(LinkedBlockingQueue<SmtpEvent> incomingMessages, ComponentLog logger) {
        this.incomingMessages = incomingMessages;
        this.logger = logger;

    }

    @Override
    public MessageHandler create(MessageContext messageContext) {
        return new Handler(messageContext, incomingMessages, logger);
    }

    class Handler implements MessageHandler {
        final MessageContext messageContext;
        String from;
        String recipient;
        ByteArrayOutputStream messageData;

        private CountDownLatch latch;

        public Handler(MessageContext messageContext, LinkedBlockingQueue<SmtpEvent> incomingMessages, ComponentLog logger){
            this.messageContext = messageContext;
            this.latch =  new CountDownLatch(1);
        }

        @Override
        public void from(String from) throws RejectException {
            // TODO: possibly whitelist senders?
            this.from = from;
        }

        @Override
        public void recipient(String recipient) throws RejectException {
            // TODO: possibly whitelist receivers?
            this.recipient = recipient;
        }

        @Override
        public void data(InputStream inputStream) throws RejectException, TooMuchDataException, IOException {
            // Start counting the timer...
            StopWatch watch = new StopWatch(true);

            long elapsed;

            SMTPServer server = messageContext.getSMTPServer();

            final long serverTimeout = TimeUnit.MILLISECONDS.convert(messageContext.getSMTPServer().getConnectionTimeout(), TimeUnit.MILLISECONDS);

            this.messageData = new ByteArrayOutputStream();

            byte [] buffer = new byte[1024];

            int rd;

            while ((rd = inputStream.read(buffer, 0, buffer.length)) != -1 ) {
                messageData.write(buffer, 0, rd);
                if (messageData.getBufferLength() > server.getMaxMessageSize() ) {
                    // NOTE: Setting processed at this stage is not desirable as message object will only be created
                    // if this test (i.e. message size) passes.
                    final SMTPResultCode returnCode = SMTPResultCode.fromCode(500);
                    logger.warn(returnCode.getLogMessage());
                    throw new TooMuchDataException(returnCode.getErrorMessage());
                }
            }
            messageData.flush();

            X509Certificate[] certificates = new X509Certificate[]{};

            final String remoteIP = messageContext.getRemoteAddress().toString();
            final String helo = messageContext.getHelo();

            if (messageContext.getTlsPeerCertificates() != null ){
                certificates = (X509Certificate[]) messageContext.getTlsPeerCertificates().clone();
            }

            SmtpEvent message = new SmtpEvent(remoteIP, helo, from, recipient, certificates, messageData, latch);

            // / Try to queue the message back to the NiFi session
            try {
                elapsed = watch.getElapsed(TimeUnit.MILLISECONDS);
                incomingMessages.offer(message, serverTimeout - elapsed, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                final SMTPResultCode returnCode = SMTPResultCode.fromCode(421);
                logger.trace(returnCode.getLogMessage());

                // NOTE: Setting processed at this stage is redundant as this catch deals with the inability of
                // adding message to the processing queue. Yet, for the sake of consistency the message is
                // updated nonetheless
                message.setReturnCode(returnCode.getCode());
                message.setAcknowledged();
                throw new DropConnectionException(returnCode.getCode(), returnCode.getErrorMessage());
            }

            // Once message has been sent to the queue, it should be processed by NiFi onTrigger,
            // a flowfile created and its processed status updated before an acknowledgment is
            // given back to the SMTP client
            elapsed = watch.getElapsed(TimeUnit.MILLISECONDS);
            try {
                latch.await(serverTimeout - elapsed, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // Latch open unexpectedly. Will return error and requestonTrigger to rollback
                logger.trace("Latch opened unexpectedly and processor indicates data wasn't processed. Returned error to SMTP client as precautionary measure");
                incomingMessages.remove(message);

                // Set the final values so onTrigger can figure out what happened to message
                final SMTPResultCode returnCode = SMTPResultCode.fromCode(423);
                message.setReturnCode(returnCode.getCode());
                message.setAcknowledged();

                // Inform client
                throw new DropConnectionException(returnCode.getCode(), returnCode.getErrorMessage());
            }

            // Remove the message from the queue.
            incomingMessages.remove(message);
            // Check if message is processed and if yes, check if it was received on time and wraps it up.
            elapsed = watch.getElapsed(TimeUnit.MILLISECONDS);
            if (!message.getProcessed() ||  (elapsed >= serverTimeout)) {
                final SMTPResultCode returnCode = SMTPResultCode.fromCode(451);
                logger.trace("Did not receive the onTrigger response within the acceptable timeframes. Data duplication may have occurred.");

                // Set the final values so onTrigger can figure out what happened to message
                message.setReturnCode(returnCode.getCode());
                message.setAcknowledged();
                throw new DropConnectionException(returnCode.getCode(), returnCode.getErrorMessage());
            }

            // Set the final values so onTrigger can figure out what happened to message
            message.setReturnCode(250);
            message.setAcknowledged();
            // Exit, allowing Handler to acknowledge the message
    }

        @Override
        public void done() {
            logger.trace("Called the last method of message handler. Exiting");
        }
    }
}
