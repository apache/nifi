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

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.listen.AbstractListenEventProcessor;
import org.apache.nifi.processor.util.listen.dispatcher.ChannelDispatcher;
import org.apache.nifi.processor.util.listen.dispatcher.DatagramChannelDispatcher;
import org.apache.nifi.processor.util.listen.event.EventFactory;
import org.apache.nifi.processor.util.listen.event.StandardEvent;
import org.apache.nifi.processor.util.listen.event.StandardEventFactory;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

@SupportsBatching
@Tags({"ingest", "udp", "listen", "source", "record"})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Listens for Datagram Packets on a given port and reads the content of each datagram using the " +
        "configured Record Reader. Each record will then be written to a flow file using the configured Record Writer. This processor " +
        "can be restricted to listening for datagrams from  a specific remote host and port by specifying the Sending Host and " +
        "Sending Host Port properties, otherwise it will listen for datagrams from all hosts and ports.")
@WritesAttributes({
        @WritesAttribute(attribute="udp.sender", description="The sending host of the messages."),
        @WritesAttribute(attribute="udp.port", description="The sending port the messages were received."),
        @WritesAttribute(attribute="record.count", description="The number of records written to the flow file."),
        @WritesAttribute(attribute="mime.type", description="The mime-type of the writer used to write the records to the flow file.")
})
public class ListenUDPRecord extends AbstractListenEventProcessor<StandardEvent> {

    public static final PropertyDescriptor SENDING_HOST = new PropertyDescriptor.Builder()
            .name("sending-host")
            .displayName("Sending Host")
            .description("IP, or name, of a remote host. Only Datagrams from the specified Sending Host Port and this host will "
                + "be accepted. Improves Performance. May be a system property or an environment variable.")
            .addValidator(new HostValidator())
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor SENDING_HOST_PORT = new PropertyDescriptor.Builder()
            .name("sending-host-port")
            .displayName("Sending Host Port")
            .description("Port being used by remote host to send Datagrams. Only Datagrams from the specified Sending Host and "
                + "this port will be accepted. Improves Performance. May be a system property or an environment variable.")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("The Record Reader to use for reading the content of incoming datagrams.")
            .identifiesControllerService(RecordReaderFactory.class)
            .expressionLanguageSupported(false)
            .required(true)
            .build();

    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("The Record Writer to use in order to serialize the data before writing to a flow file.")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .expressionLanguageSupported(false)
            .required(true)
            .build();

    public static final PropertyDescriptor POLL_TIMEOUT = new PropertyDescriptor.Builder()
            .name("poll-timeout")
            .displayName("Poll Timeout")
            .description("The amount of time to wait when polling the internal queue for more datagrams. If no datagrams are found after waiting " +
                    "for the configured timeout, then the processor will emit whatever records have been obtained up to that point.")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("50 ms")
            .required(true)
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("batch-size")
            .displayName("Batch Size")
            .description("The maximum number of datagrams to write as records to a single FlowFile. The Batch Size will only be reached when " +
                    "data is coming in more frequently than the Poll Timeout.")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(false)
            .defaultValue("1000")
            .required(true)
            .build();

    public static final Relationship REL_PARSE_FAILURE = new Relationship.Builder()
            .name("parse.failure")
            .description("If a datagram cannot be parsed using the configured Record Reader, the contents of the "
                    + "message will be routed to this Relationship as its own individual FlowFile.")
            .build();

    public static final String UDP_PORT_ATTR = "udp.port";
    public static final String UDP_SENDER_ATTR = "udp.sender";
    public static final String RECORD_COUNT_ATTR = "record.count";

    private volatile long pollTimeout;

    @Override
    protected List<PropertyDescriptor> getAdditionalProperties() {
        return Arrays.asList(
                POLL_TIMEOUT,
                BATCH_SIZE,
                RECORD_READER,
                RECORD_WRITER,
                SENDING_HOST,
                SENDING_HOST_PORT
        );
    }

    @Override
    protected List<Relationship> getAdditionalRelationships() {
        return Arrays.asList(REL_PARSE_FAILURE);
    }

    @Override
    @OnScheduled
    public void onScheduled(ProcessContext context) throws IOException {
        super.onScheduled(context);
        this.pollTimeout = context.getProperty(POLL_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS);
    }

    @Override
    protected long getLongPollTimeout() {
        return pollTimeout;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final Collection<ValidationResult> result = new ArrayList<>();

        final String sendingHost = validationContext.getProperty(SENDING_HOST).getValue();
        final String sendingPort = validationContext.getProperty(SENDING_HOST_PORT).getValue();

        if (StringUtils.isBlank(sendingHost) && StringUtils.isNotBlank(sendingPort)) {
            result.add(
                    new ValidationResult.Builder()
                            .subject(SENDING_HOST.getName())
                            .valid(false)
                            .explanation("Must specify Sending Host when specifying Sending Host Port")
                            .build());
        } else if (StringUtils.isBlank(sendingPort) && StringUtils.isNotBlank(sendingHost)) {
            result.add(
                    new ValidationResult.Builder()
                            .subject(SENDING_HOST_PORT.getName())
                            .valid(false)
                            .explanation("Must specify Sending Host Port when specifying Sending Host")
                            .build());
        }

        return result;
    }

    @Override
    protected ChannelDispatcher createDispatcher(final ProcessContext context, final BlockingQueue<StandardEvent> events)
            throws IOException {
        final String sendingHost = context.getProperty(SENDING_HOST).evaluateAttributeExpressions().getValue();
        final Integer sendingHostPort = context.getProperty(SENDING_HOST_PORT).evaluateAttributeExpressions().asInteger();
        final Integer bufferSize = context.getProperty(RECV_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
        final BlockingQueue<ByteBuffer> bufferPool = createBufferPool(context.getMaxConcurrentTasks(), bufferSize);
        final EventFactory<StandardEvent> eventFactory = new StandardEventFactory();
        return new DatagramChannelDispatcher<>(eventFactory, bufferPool, events, getLogger(), sendingHost, sendingHostPort);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final int maxBatchSize = context.getProperty(BATCH_SIZE).asInteger();
        final Map<String, FlowFileRecordWriter> flowFileRecordWriters = new HashMap<>();

        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        for (int i=0; i < maxBatchSize; i++) {
            // this processor isn't leveraging the error queue so don't bother polling to avoid the overhead
            // if the error handling is ever changed to use the error queue then this flag needs to be changed as well
            final StandardEvent event = getMessage(true, false, session);

            // break out if we don't have any messages, don't yield since we already do a long poll inside getMessage
            if (event == null) {
                break;
            }

            // attempt to read all of the records from the current datagram into a list in memory so that we can ensure the
            // entire datagram can be read as records, and if not transfer the whole thing to parse.failure
            final RecordReader reader;
            final List<Record> records = new ArrayList<>();
            try (final InputStream in = new ByteArrayInputStream(event.getData())) {
                reader = readerFactory.createRecordReader(Collections.emptyMap(), in, getLogger());

                Record record;
                while((record = reader.nextRecord()) != null) {
                    records.add(record);
                }
            } catch (final Exception e) {
                handleParseFailure(event, session, e);
                continue;
            }

            if (records.size() == 0) {
                handleParseFailure(event, session, null);
                continue;
            }

            // see if we already started a flow file and writer for the given sender
            // if an exception happens creating the flow file or writer, put the event in the error queue to try it again later
            FlowFileRecordWriter flowFileRecordWriter = flowFileRecordWriters.get(event.getSender());

            if (flowFileRecordWriter == null) {
                FlowFile flowFile = null;
                OutputStream rawOut = null;
                RecordSetWriter writer = null;
                try {
                    flowFile = session.create();
                    rawOut  = session.write(flowFile);

                    final Record firstRecord = records.get(0);
                    final RecordSchema recordSchema = firstRecord.getSchema();
                    final RecordSchema writeSchema = writerFactory.getSchema(Collections.emptyMap(), recordSchema);

                    writer = writerFactory.createWriter(getLogger(), writeSchema, rawOut);
                    writer.beginRecordSet();

                    flowFileRecordWriter = new FlowFileRecordWriter(flowFile, writer);
                    flowFileRecordWriters.put(event.getSender(), flowFileRecordWriter);
                } catch (final Exception ex) {
                    getLogger().error("Failed to properly initialize record writer. Datagram will be queued for re-processing.", ex);
                    try {
                        if (writer != null) {
                            writer.close();
                        }
                    } catch (final Exception e) {
                        getLogger().warn("Failed to close Record Writer", e);
                    }

                    if (rawOut != null) {
                        IOUtils.closeQuietly(rawOut);
                    }

                    if (flowFile != null) {
                        session.remove(flowFile);
                    }

                    context.yield();
                    break;
                }
            }

            // attempt to write each record, if any record fails then remove the flow file and break out of the loop
            final RecordSetWriter writer = flowFileRecordWriter.getRecordWriter();
            try {
                for (final Record record : records) {
                    writer.write(record);
                }
            } catch (Exception e) {
                getLogger().error("Failed to write records due to: " + e.getMessage(), e);
                IOUtils.closeQuietly(writer);
                session.remove(flowFileRecordWriter.getFlowFile());
                flowFileRecordWriters.remove(event.getSender());
                break;
            }
        }

        // attempt to finish each record set and transfer the flow file, if an error is encountered calling
        // finishRecordSet or closing the writer then remove the flow file

        for (final Map.Entry<String,FlowFileRecordWriter> entry : flowFileRecordWriters.entrySet()) {
            final String sender = entry.getKey();
            final FlowFileRecordWriter flowFileRecordWriter = entry.getValue();
            final RecordSetWriter writer = flowFileRecordWriter.getRecordWriter();

            FlowFile flowFile = flowFileRecordWriter.getFlowFile();
            try {
                final WriteResult writeResult;
                try {
                    writeResult = writer.finishRecordSet();
                } finally {
                    writer.close();
                }

                if (writeResult.getRecordCount() == 0) {
                    session.remove(flowFile);
                    continue;
                }

                final Map<String, String> attributes = new HashMap<>();
                attributes.putAll(getAttributes(sender));
                attributes.putAll(writeResult.getAttributes());
                attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                attributes.put(RECORD_COUNT_ATTR, String.valueOf(writeResult.getRecordCount()));

                flowFile = session.putAllAttributes(flowFile, attributes);
                session.transfer(flowFile, REL_SUCCESS);

                final String transitUri = getTransitUri(sender);
                session.getProvenanceReporter().receive(flowFile, transitUri);

            } catch (final Exception e) {
                getLogger().error("Unable to properly complete record set due to: " + e.getMessage(), e);
                session.remove(flowFile);
            }
        }
    }

    private void handleParseFailure(final StandardEvent event, final ProcessSession session, final Exception cause) {
        handleParseFailure(event, session, cause, "Failed to parse datagram using the configured Record Reader. "
                + "Will route message as its own FlowFile to the 'parse.failure' relationship");
    }

    private void handleParseFailure(final StandardEvent event, final ProcessSession session, final Exception cause, final String message) {
        // If we are unable to parse the data, we need to transfer it to 'parse failure' relationship
        final Map<String, String> attributes = getAttributes(event.getSender());

        FlowFile failureFlowFile = session.create();
        failureFlowFile = session.write(failureFlowFile, out -> out.write(event.getData()));
        failureFlowFile = session.putAllAttributes(failureFlowFile, attributes);

        final String transitUri = getTransitUri(event.getSender());
        session.getProvenanceReporter().receive(failureFlowFile, transitUri);

        session.transfer(failureFlowFile, REL_PARSE_FAILURE);

        if (cause == null) {
            getLogger().error(message);
        } else {
            getLogger().error(message, cause);
        }

        session.adjustCounter("Parse Failures", 1, false);
    }

    private Map<String, String> getAttributes(final String sender) {
        final Map<String, String> attributes = new HashMap<>(3);
        attributes.put(UDP_SENDER_ATTR, sender);
        attributes.put(UDP_PORT_ATTR, String.valueOf(port));
        return attributes;
    }

    private String getTransitUri(final String sender) {
        final String senderHost = sender.startsWith("/") && sender.length() > 1 ? sender.substring(1) : sender;
        final String transitUri = new StringBuilder().append("udp").append("://").append(senderHost).append(":")
                .append(port).toString();
        return transitUri;
    }

    /**
     * Holder class to pass around a flow file and the writer that is writing records to it.
     */
    private static class FlowFileRecordWriter {

        private final FlowFile flowFile;

        private final RecordSetWriter recordWriter;

        public FlowFileRecordWriter(final FlowFile flowFile, final RecordSetWriter recordWriter) {
            this.flowFile = flowFile;
            this.recordWriter = recordWriter;
        }

        public FlowFile getFlowFile() {
            return flowFile;
        }

        public RecordSetWriter getRecordWriter() {
            return recordWriter;
        }
    }

    private static class HostValidator implements Validator {

        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            try {
                InetAddress.getByName(input);
                return new ValidationResult.Builder().subject(subject).valid(true).input(input).build();
            } catch (final UnknownHostException e) {
                return new ValidationResult.Builder().subject(subject).valid(false).input(input).explanation("Unknown host: " + e).build();
            }
        }
    }

}

