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
package org.apache.nifi.processors.standard;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.event.transport.configuration.TransportProtocol;
import org.apache.nifi.event.transport.netty.DelimitedInputStream;
import org.apache.nifi.event.transport.netty.NettyEventSenderFactory;
import org.apache.nifi.event.transport.netty.StreamingNettyEventSenderFactory;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.put.AbstractPutEventProcessor;
import org.apache.nifi.processors.standard.property.TransmissionStrategy;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.util.StopWatch;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@CapabilityDescription("Sends serialized FlowFiles or Records over TCP to a configurable destination with optional support for TLS")
@InputRequirement(Requirement.INPUT_REQUIRED)
@SeeAlso({ListenTCP.class, PutUDP.class})
@Tags({ "remote", "egress", "put", "tcp" })
@SupportsBatching
@WritesAttributes({
        @WritesAttribute(attribute = PutTCP.RECORD_COUNT_TRANSMITTED, description = "Count of records transmitted to configured destination address")
})
public class PutTCP extends AbstractPutEventProcessor<InputStream> {

    public static final String RECORD_COUNT_TRANSMITTED = "record.count.transmitted";

    static final PropertyDescriptor TRANSMISSION_STRATEGY = new PropertyDescriptor.Builder()
            .name("Transmission Strategy")
            .displayName("Transmission Strategy")
            .description("Specifies the strategy used for reading input FlowFiles and transmitting messages to the destination socket address")
            .required(true)
            .allowableValues(TransmissionStrategy.class)
            .defaultValue(TransmissionStrategy.FLOWFILE_ORIENTED)
            .build();

    static final PropertyDescriptor DEPENDENT_CHARSET = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(CHARSET)
            .dependsOn(TRANSMISSION_STRATEGY, TransmissionStrategy.FLOWFILE_ORIENTED)
            .build();

    static final PropertyDescriptor DEPENDENT_OUTGOING_MESSAGE_DELIMITER = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(OUTGOING_MESSAGE_DELIMITER)
            .dependsOn(TRANSMISSION_STRATEGY, TransmissionStrategy.FLOWFILE_ORIENTED)
            .build();

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("Record Reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for reading Records from input FlowFiles")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .dependsOn(TRANSMISSION_STRATEGY, TransmissionStrategy.RECORD_ORIENTED)
            .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("Record Writer")
            .displayName("Record Writer")
            .description("Specifies the Controller Service to use for writing Records to the configured socket address")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .dependsOn(TRANSMISSION_STRATEGY, TransmissionStrategy.RECORD_ORIENTED)
            .build();

    private static final List<PropertyDescriptor> ADDITIONAL_PROPERTIES = List.of(
            CONNECTION_PER_FLOWFILE,
            SSL_CONTEXT_SERVICE,
            TRANSMISSION_STRATEGY,
            DEPENDENT_OUTGOING_MESSAGE_DELIMITER,
            DEPENDENT_CHARSET,
            RECORD_READER,
            RECORD_WRITER
    );

    @Override
    protected List<PropertyDescriptor> getAdditionalProperties() {
        return ADDITIONAL_PROPERTIES;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        final ProcessSession session = sessionFactory.createSession();
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final TransmissionStrategy transmissionStrategy = context.getProperty(TRANSMISSION_STRATEGY).asAllowableValue(TransmissionStrategy.class);
        final StopWatch stopWatch = new StopWatch(true);
        try {
            final int recordCount;
            if (TransmissionStrategy.RECORD_ORIENTED == transmissionStrategy) {
                recordCount = sendRecords(context, session, flowFile);

            } else {
                sendFlowFile(context, session, flowFile);
                recordCount = 0;
            }

            final FlowFile processedFlowFile = session.putAttribute(flowFile, RECORD_COUNT_TRANSMITTED, Integer.toString(recordCount));
            session.getProvenanceReporter().send(processedFlowFile, transitUri, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(processedFlowFile, REL_SUCCESS);
            session.commitAsync();
        } catch (final Exception e) {
            getLogger().error("Send Failed {}", flowFile, e);
            session.transfer(session.penalize(flowFile), REL_FAILURE);
            session.commitAsync();
            context.yield();
        }
    }

    @Override
    protected String getProtocol(final ProcessContext context) {
        return TCP_VALUE.getValue();
    }

    @Override
    protected NettyEventSenderFactory<InputStream> getNettyEventSenderFactory(final String hostname, final int port, final String protocol) {
        return new StreamingNettyEventSenderFactory(getLogger(), hostname, port, TransportProtocol.TCP);
    }

    private void sendFlowFile(final ProcessContext context, final ProcessSession session, final FlowFile flowFile) {
        session.read(flowFile, inputStream -> {
            InputStream inputStreamEvent = inputStream;

            final String delimiter = getOutgoingMessageDelimiter(context, flowFile);
            if (delimiter != null) {
                final Charset charSet = Charset.forName(context.getProperty(CHARSET).evaluateAttributeExpressions().getValue());
                inputStreamEvent = new DelimitedInputStream(inputStream, delimiter.getBytes(charSet));
            }

            eventSender.sendEvent(inputStreamEvent);
        });
    }

    private int sendRecords(final ProcessContext context, final ProcessSession session, final FlowFile flowFile) {
        final AtomicInteger recordCount = new AtomicInteger();

        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        session.read(flowFile, inputStream -> {
            try (
                    RecordReader recordReader = readerFactory.createRecordReader(flowFile, inputStream, getLogger());
                    ReusableByteArrayInputStream eventInputStream = new ReusableByteArrayInputStream();
                    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                    RecordSetWriter recordSetWriter = writerFactory.createWriter(getLogger(), recordReader.getSchema(), outputStream, flowFile)
            ) {
                Record record;
                while ((record = recordReader.nextRecord()) != null) {
                    recordSetWriter.write(record);
                    recordSetWriter.flush();

                    final byte[] buffer = outputStream.toByteArray();
                    eventInputStream.setBuffer(buffer);
                    eventSender.sendEvent(eventInputStream);
                    outputStream.reset();

                    recordCount.getAndIncrement();
                }
            } catch (final SchemaNotFoundException | MalformedRecordException e) {
                throw new IOException("Record reading failed", e);
            }
        });

        return recordCount.get();
    }

    private static class ReusableByteArrayInputStream extends ByteArrayInputStream {

        private ReusableByteArrayInputStream() {
            super(new byte[0]);
        }

        private void setBuffer(final byte[] buffer) {
            this.buf = buffer;
            this.pos = 0;
            this.count = buffer.length;
        }
    }
}
