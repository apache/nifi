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
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
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
    private volatile boolean poisoned = false;
    private final AtomicLong messagesSent = new AtomicLong(0L);

    private InFlightMessageTracker tracker;

    public PublisherLease(final Producer<byte[], byte[]> producer, final int maxMessageSize, final long maxAckWaitMillis, final ComponentLog logger) {
        this.producer = producer;
        this.maxMessageSize = maxMessageSize;
        this.logger = logger;
        this.maxAckWaitMillis = maxAckWaitMillis;
    }

    protected void poison() {
        this.poisoned = true;
    }

    public boolean isPoisoned() {
        return poisoned;
    }

    void publish(final FlowFile flowFile, final InputStream flowFileContent, final byte[] messageKey, final byte[] demarcatorBytes, final String topic) throws IOException {
        if (tracker == null) {
            tracker = new InFlightMessageTracker();
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
            tracker = new InFlightMessageTracker();
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

    protected void publish(final FlowFile flowFile, final byte[] messageKey, final byte[] messageContent, final String topic, final InFlightMessageTracker tracker) {
        final ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, null, messageKey, messageContent);
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

            throw new IllegalStateException("Cannot complete publishing to Kafka because Publisher Lease was already closed");
        }

        producer.flush();

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
            tracker = new InFlightMessageTracker();
        }

        return tracker;
    }
}
