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

package org.apache.nifi.processors.kafka.pubsub;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.stream.io.exception.TokenTooLargeException;
import org.apache.nifi.stream.io.util.StreamDemarcator;

public class PublisherLease implements Closeable {
    private final ComponentLog logger;
    private final Producer<byte[], byte[]> producer;
    private final int maxMessageSize;
    private final long maxAckWaitMillis;
    private final boolean useTransactions;
    private final Pattern attributeNameRegex;
    private final Charset headerCharacterSet;
    private volatile boolean poisoned = false;
    private final AtomicLong messagesSent = new AtomicLong(0L);

    private volatile boolean transactionsInitialized = false;
    private volatile boolean activeTransaction = false;

    private InFlightMessageTracker tracker;

    public PublisherLease(final Producer<byte[], byte[]> producer, final int maxMessageSize, final long maxAckWaitMillis, final ComponentLog logger,
        final boolean useTransactions, final Pattern attributeNameRegex, final Charset headerCharacterSet) {
        this.producer = producer;
        this.maxMessageSize = maxMessageSize;
        this.logger = logger;
        this.maxAckWaitMillis = maxAckWaitMillis;
        this.useTransactions = useTransactions;
        this.attributeNameRegex = attributeNameRegex;
        this.headerCharacterSet = headerCharacterSet;
    }

    protected void poison() {
        this.poisoned = true;
    }

    public boolean isPoisoned() {
        return poisoned;
    }

    void beginTransaction() {
        if (!useTransactions) {
            return;
        }

        if (!transactionsInitialized) {
            producer.initTransactions();
            transactionsInitialized = true;
        }

        producer.beginTransaction();
        activeTransaction = true;
    }

    void rollback() {
        if (!useTransactions || !activeTransaction) {
            return;
        }

        producer.abortTransaction();
        activeTransaction = false;
    }

    void fail(final FlowFile flowFile, final Exception cause) {
        getTracker().fail(flowFile, cause);
        rollback();
    }

    void publish(final FlowFile flowFile, final InputStream flowFileContent, final byte[] messageKey, final byte[] demarcatorBytes, final String topic) throws IOException {
        if (tracker == null) {
            tracker = new InFlightMessageTracker(logger);
        }

        try (final StreamDemarcator demarcator = new StreamDemarcator(flowFileContent, demarcatorBytes, maxMessageSize)) {
            byte[] messageContent;
            try {
                while ((messageContent = demarcator.nextToken()) != null) {
                    // We do not want to use any key if we have a demarcator because that would result in
                    // the key being the same for multiple messages
                    final byte[] keyToUse = demarcatorBytes == null ? messageKey : null;
                    publish(flowFile, keyToUse, messageContent, topic, tracker);

                    if (tracker.isFailed(flowFile)) {
                        // If we have a failure, don't try to send anything else.
                        return;
                    }
                }
            } catch (final TokenTooLargeException ttle) {
                tracker.fail(flowFile, ttle);
            }
        } catch (final Exception e) {
            tracker.fail(flowFile, e);
            poison();
            throw e;
        }
    }

    void publish(final FlowFile flowFile, final RecordSet recordSet, final RecordSetWriterFactory writerFactory, final RecordSchema schema,
        final String messageKeyField, final String topic) throws IOException {
        if (tracker == null) {
            tracker = new InFlightMessageTracker(logger);
        }

        final ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);

        Record record;
        int recordCount = 0;

        try (final RecordSetWriter writer = writerFactory.createWriter(logger, schema, baos)) {
            while ((record = recordSet.next()) != null) {
                recordCount++;
                baos.reset();

                writer.write(record);
                writer.flush();

                final byte[] messageContent = baos.toByteArray();
                final String key = messageKeyField == null ? null : record.getAsString(messageKeyField);
                final byte[] messageKey = (key == null) ? null : key.getBytes(StandardCharsets.UTF_8);

                publish(flowFile, messageKey, messageContent, topic, tracker);

                if (tracker.isFailed(flowFile)) {
                    // If we have a failure, don't try to send anything else.
                    return;
                }
            }

            if (recordCount == 0) {
                tracker.trackEmpty(flowFile);
            }
        } catch (final TokenTooLargeException ttle) {
            tracker.fail(flowFile, ttle);
        } catch (final SchemaNotFoundException snfe) {
            throw new IOException(snfe);
        } catch (final Exception e) {
            tracker.fail(flowFile, e);
            poison();
            throw e;
        }
    }

    private void addHeaders(final FlowFile flowFile, final ProducerRecord<?, ?> record) {
        if (attributeNameRegex == null) {
            return;
        }

        final Headers headers = record.headers();
        for (final Map.Entry<String, String> entry : flowFile.getAttributes().entrySet()) {
            if (attributeNameRegex.matcher(entry.getKey()).matches()) {
                headers.add(entry.getKey(), entry.getValue().getBytes(headerCharacterSet));
            }
        }
    }

    protected void publish(final FlowFile flowFile, final byte[] messageKey, final byte[] messageContent, final String topic, final InFlightMessageTracker tracker) {
        final ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, null, messageKey, messageContent);
        addHeaders(flowFile, record);

        producer.send(record, new Callback() {
            @Override
            public void onCompletion(final RecordMetadata metadata, final Exception exception) {
                if (exception == null) {
                    tracker.incrementAcknowledgedCount(flowFile);
                } else {
                    tracker.fail(flowFile, exception);
                    poison();
                }
            }
        });

        messagesSent.incrementAndGet();
        tracker.incrementSentCount(flowFile);
    }


    public PublishResult complete() {
        if (tracker == null) {
            if (messagesSent.get() == 0L) {
                return PublishResult.EMPTY;
            }

            rollback();
            throw new IllegalStateException("Cannot complete publishing to Kafka because Publisher Lease was already closed");
        }

        producer.flush();

        if (activeTransaction) {
            producer.commitTransaction();
            activeTransaction = false;
        }

        try {
            tracker.awaitCompletion(maxAckWaitMillis);
            return tracker.createPublishResult();
        } catch (final InterruptedException e) {
            logger.warn("Interrupted while waiting for an acknowledgement from Kafka; some FlowFiles may be transferred to 'failure' even though they were received by Kafka");
            Thread.currentThread().interrupt();
            return tracker.failOutstanding(e);
        } catch (final TimeoutException e) {
            logger.warn("Timed out while waiting for an acknowledgement from Kafka; some FlowFiles may be transferred to 'failure' even though they were received by Kafka");
            return tracker.failOutstanding(e);
        } finally {
            tracker = null;
        }
    }

    @Override
    public void close() {
        producer.close(maxAckWaitMillis, TimeUnit.MILLISECONDS);
        tracker = null;
    }

    public InFlightMessageTracker getTracker() {
        if (tracker == null) {
            tracker = new InFlightMessageTracker(logger);
        }

        return tracker;
    }
}
