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
package org.apache.nifi.processors.splunk;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.event.transport.EventException;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.put.AbstractPutEventProcessor;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.stream.io.ByteCountingInputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.stream.io.util.NonThreadSafeCircularBuffer;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"splunk", "logs", "tcp", "udp"})
@TriggerWhenEmpty // because we have a queue of sessions that are ready to be committed
@CapabilityDescription("Sends logs to Splunk Enterprise over TCP, TCP + TLS/SSL, or UDP. If a Message " +
        "Delimiter is provided, then this processor will read messages from the incoming FlowFile based on the " +
        "delimiter, and send each message to Splunk. If a Message Delimiter is not provided then the content of " +
        "the FlowFile will be sent directly to Splunk as if it were a single message.")
public class PutSplunk extends AbstractPutEventProcessor {

    public static final char NEW_LINE_CHAR = '\n';

    @Override
    protected List<PropertyDescriptor> getAdditionalProperties() {
        return Arrays.asList(
                TIMEOUT,
                CHARSET,
                PROTOCOL,
                MESSAGE_DELIMITER,
                SSL_CONTEXT_SERVICE
        );
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final Collection<ValidationResult> results = new ArrayList<>();

        final String protocol = context.getProperty(PROTOCOL).getValue();
        final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);

        if (UDP_VALUE.getValue().equals(protocol) && sslContextService != null) {
            results.add(new ValidationResult.Builder()
                    .explanation("SSL can not be used with UDP")
                    .valid(false).subject("SSL Context").build());
        }

        return results;
    }

    @OnStopped
    public void cleanup() {
        for (final FlowFileMessageBatch batch : activeBatches) {
            batch.cancelOrComplete();
        }

        FlowFileMessageBatch batch;
        while ((batch = completeBatches.poll()) != null) {
            batch.completeSession();
        }
    }

    @Override
    protected String createTransitUri(ProcessContext context) {
        final String port = context.getProperty(PORT).evaluateAttributeExpressions().getValue();
        final String host = context.getProperty(HOSTNAME).evaluateAttributeExpressions().getValue();
        final String protocol = context.getProperty(PROTOCOL).getValue().toLowerCase();
        return new StringBuilder().append(protocol).append("://").append(host).append(":").append(port).toString();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        // first complete any batches from previous executions
        FlowFileMessageBatch batch;
        while ((batch = completeBatches.poll()) != null) {
            batch.completeSession();
        }

        // create a session and try to get a FlowFile, if none available then close any idle senders
        final ProcessSession session = sessionFactory.createSession();
        final FlowFile flowFile = session.get();

        if (flowFile == null) {
            return;
        }

        String delimiter = context.getProperty(MESSAGE_DELIMITER).evaluateAttributeExpressions(flowFile).getValue();
        if (delimiter != null) {
            delimiter = delimiter.replace("\\n", "\n").replace("\\r", "\r").replace("\\t", "\t");
        }

        // if no delimiter then treat the whole FlowFile as a single message
        try {
            if (delimiter == null) {
                processSingleMessage(context, session, flowFile);
            } else {
                processDelimitedMessages(context, session, flowFile, delimiter);
            }
        } catch (EventException e) {
            session.transfer(flowFile, REL_FAILURE);
            session.commitAsync();
            context.yield();
        }
    }

    /**
     * Send the entire FlowFile as a single message.
     */
    private void processSingleMessage(final ProcessContext context, final ProcessSession session, final FlowFile flowFile) {
        // copy the contents of the FlowFile to the ByteArrayOutputStream
        final ByteArrayOutputStream baos = new ByteArrayOutputStream((int)flowFile.getSize() + 1);
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                StreamUtils.copy(in, baos);
            }
        });

        // if TCP and we don't end in a new line then add one
        final String protocol = context.getProperty(PROTOCOL).getValue();
        byte[] buf = baos.toByteArray();
        if (protocol.equals(TCP_VALUE.getValue()) && buf[buf.length - 1] != NEW_LINE_CHAR) {
            final byte[] updatedBuf = new byte[buf.length + 1];
            System.arraycopy(buf, 0, updatedBuf, 0, buf.length);
            updatedBuf[updatedBuf.length - 1] = NEW_LINE_CHAR;
            buf = updatedBuf;
        }

        // create a message batch of one message and add to active batches
        final FlowFileMessageBatch messageBatch = new FlowFileMessageBatch(session, flowFile);
        messageBatch.setNumMessages(1);
        activeBatches.add(messageBatch);

        // attempt to send the data and add the appropriate range
        eventSender.sendEvent(buf);
        messageBatch.addSuccessfulRange(0L, flowFile.getSize());
    }

    /**
     * Read delimited messages from the FlowFile tracking which messages are sent successfully.
     */
    private void processDelimitedMessages(final ProcessContext context, final ProcessSession session, final FlowFile flowFile,
                                          final String delimiter) {

        final String protocol = context.getProperty(PROTOCOL).getValue();
        final byte[] delimiterBytes = delimiter.getBytes(StandardCharsets.UTF_8);

        // The NonThreadSafeCircularBuffer allows us to add a byte from the stream one at a time and see if it matches
        // some pattern. We can use this to search for the delimiter as we read through the stream of bytes in the FlowFile
        final NonThreadSafeCircularBuffer buffer = new NonThreadSafeCircularBuffer(delimiterBytes);

        final AtomicLong messagesSent = new AtomicLong(0L);
        final FlowFileMessageBatch messageBatch = new FlowFileMessageBatch(session, flowFile);
        activeBatches.add(messageBatch);

        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream rawIn) throws IOException {
                    byte[] data = null; // contents of a single message
                    boolean streamFinished = false;

                    int nextByte;
                    try (final InputStream bufferedIn = new BufferedInputStream(rawIn);
                         final ByteCountingInputStream in = new ByteCountingInputStream(bufferedIn)) {

                        long messageStartOffset = in.getBytesConsumed();

                        // read until we're out of data.
                        while (!streamFinished) {
                            nextByte = in.read();

                            if (nextByte > -1) {
                                baos.write(nextByte);
                            }

                            if (nextByte == -1) {
                                // we ran out of data. This message is complete.
                                data = getMessage(baos, baos.size(), protocol);
                                streamFinished = true;
                            } else if (buffer.addAndCompare((byte) nextByte)) {
                                // we matched our delimiter. This message is complete. We want all of the bytes from the
                                // underlying BAOS except for the last 'delimiterBytes.length' bytes because we don't want
                                // the delimiter itself to be sent.
                                data = getMessage(baos, baos.size() - delimiterBytes.length, protocol);
                            }

                            if (data != null) {
                                final long messageEndOffset = in.getBytesConsumed();

                                // If the message has no data, ignore it.
                                if (data.length != 0) {
                                    final long rangeStart = messageStartOffset;
                                    eventSender.sendEvent(data);
                                    messageBatch.addSuccessfulRange(rangeStart, messageEndOffset);
                                    messagesSent.incrementAndGet();
                                }

                                // reset BAOS so that we can start a new message.
                                baos.reset();
                                data = null;
                                messageStartOffset = in.getBytesConsumed();
                            }
                        }
                    }
                }
            });

            messageBatch.setNumMessages(messagesSent.get());
        } catch (final IOException ioe) {
            // Since this can be thrown only from closing the ByteArrayOutputStream(), we have already
            // completed everything that we need to do, so there's nothing really to be done here
        }
    }

    /**
     * Helper to get the bytes of a message from the ByteArrayOutputStream, factoring in whether we need a
     * a new line at the end of our message.
     *
     * @param baos the ByteArrayOutputStream to get data from
     * @param length the amount of data to copy from the baos
     * @param protocol the protocol (TCP or UDP)
     *
     * @return the bytes from 0 to length, including a new line if the protocol was TCP
     */
    private byte[] getMessage(final ByteArrayOutputStream baos, final int length, final String protocol) {
        if (baos.size() == 0) {
            return null;
        }

        final byte[] buf = baos.toByteArray();

        // if TCP and we don't already end with a new line then add one
        if (protocol.equals(TCP_VALUE.getValue()) && buf[length - 1] != NEW_LINE_CHAR) {
            byte[] message = new byte[length + 1];

            for (int i=0; i < length; i++) {
               message[i] = buf[i];
            }
            message[message.length - 1] = NEW_LINE_CHAR;
            return message;
        } else {
            return Arrays.copyOfRange(baos.toByteArray(), 0, length);
        }
    }
}
