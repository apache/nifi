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
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.listen.ListenerProperties;
import org.apache.nifi.record.listen.SocketChannelRecordReader;
import org.apache.nifi.record.listen.SocketChannelRecordReaderDispatcher;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.ssl.RestrictedSSLContextService;
import org.apache.nifi.ssl.SSLContextService;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketTimeoutException;
import java.nio.channels.ServerSocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.processor.util.listen.ListenerProperties.NETWORK_INTF_NAME;

@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"listen", "tcp", "record", "tls", "ssl"})
@CapabilityDescription("Listens for incoming TCP connections and reads data from each connection using a configured record " +
        "reader, and writes the records to a flow file using a configured record writer. The type of record reader selected will " +
        "determine how clients are expected to send data. For example, when using a Grok reader to read logs, a client can keep an " +
        "open connection and continuously stream data, but when using an JSON reader, the client cannot send an array of JSON " +
        "documents and then send another array on the same connection, as the reader would be in a bad state at that point. Records " +
        "will be read from the connection in blocking mode, and will timeout according to the Read Timeout specified in the processor. " +
        "If the read times out, or if any other error is encountered when reading, the connection will be closed, and any records " +
        "read up to that point will be handled according to the configured Read Error Strategy (Discard or Transfer). In cases where " +
        "clients are keeping a connection open, the concurrent tasks for the processor should be adjusted to match the Max Number of " +
        "TCP Connections allowed, so that there is a task processing each connection.")
@WritesAttributes({
        @WritesAttribute(attribute="tcp.sender", description="The host that sent the data."),
        @WritesAttribute(attribute="tcp.port", description="The port that the processor accepted the connection on."),
        @WritesAttribute(attribute="record.count", description="The number of records written to the flow file."),
        @WritesAttribute(attribute="mime.type", description="The mime-type of the writer used to write the records to the flow file.")
})
public class ListenTCPRecord extends AbstractProcessor {

    static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("port")
            .displayName("Port")
            .description("The port to listen on for communication.")
            .required(true)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    static final PropertyDescriptor READ_TIMEOUT = new PropertyDescriptor.Builder()
            .name("read-timeout")
            .displayName("Read Timeout")
            .description("The amount of time to wait before timing out when reading from a connection.")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("10 seconds")
            .required(true)
            .build();

    static final PropertyDescriptor MAX_SOCKET_BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("max-size-socket-buffer")
            .displayName("Max Size of Socket Buffer")
            .description("The maximum size of the socket buffer that should be used. This is a suggestion to the Operating System " +
                    "to indicate how big the socket buffer should be. If this value is set too low, the buffer may fill up before " +
                    "the data can be read, and incoming data will be dropped.")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("1 MB")
            .required(true)
            .build();

    static final PropertyDescriptor MAX_CONNECTIONS = new PropertyDescriptor.Builder()
            .name("max-number-tcp-connections")
            .displayName("Max Number of TCP Connections")
            .description("The maximum number of concurrent TCP connections to accept. In cases where clients are keeping a connection open, " +
                    "the concurrent tasks for the processor should be adjusted to match the Max Number of TCP Connections allowed, so that there " +
                    "is a task processing each connection.")
            .addValidator(StandardValidators.createLongValidator(1, 65535, true))
            .defaultValue("2")
            .required(true)
            .build();

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("The Record Reader to use for incoming FlowFiles")
            .identifiesControllerService(RecordReaderFactory.class)
            .expressionLanguageSupported(false)
            .required(true)
            .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("The Record Writer to use in order to serialize the data before writing to a FlowFile")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .expressionLanguageSupported(false)
            .required(true)
            .build();

    static final AllowableValue ERROR_HANDLING_DISCARD = new AllowableValue("Discard", "Discard", "Discards any records already received and closes the connection.");
    static final AllowableValue ERROR_HANDLING_TRANSFER = new AllowableValue("Transfer", "Transfer", "Transfers any records already received and closes the connection.");

    static final PropertyDescriptor READER_ERROR_HANDLING_STRATEGY = new PropertyDescriptor.Builder()
            .name("reader-error-handling-strategy")
            .displayName("Read Error Strategy")
            .description("Indicates how to deal with an error while reading the next record from a connection, when previous records have already been read from the connection.")
            .required(true)
            .allowableValues(ERROR_HANDLING_TRANSFER, ERROR_HANDLING_DISCARD)
            .defaultValue(ERROR_HANDLING_TRANSFER.getValue())
            .build();

    static final PropertyDescriptor RECORD_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("record-batch-size")
            .displayName("Record Batch Size")
            .description("The maximum number of records to write to a single FlowFile.")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(false)
            .defaultValue("1000")
            .required(true)
            .build();

    static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("ssl-context-service")
            .displayName("SSL Context Service")
            .description("The Controller Service to use in order to obtain an SSL Context. If this property is set, " +
                    "messages will be received over a secure connection.")
            .required(false)
            .identifiesControllerService(RestrictedSSLContextService.class)
            .build();

    static final PropertyDescriptor CLIENT_AUTH = new PropertyDescriptor.Builder()
            .name("client-auth")
            .displayName("Client Auth")
            .description("The client authentication policy to use for the SSL Context. Only used if an SSL Context Service is provided.")
            .required(false)
            .allowableValues(SSLContextService.ClientAuth.values())
            .defaultValue(SSLContextService.ClientAuth.REQUIRED.name())
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Messages received successfully will be sent out this relationship.")
            .build();


    static final List<PropertyDescriptor> PROPERTIES;
    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(ListenerProperties.NETWORK_INTF_NAME);
        props.add(PORT);
        props.add(MAX_SOCKET_BUFFER_SIZE);
        props.add(MAX_CONNECTIONS);
        props.add(READ_TIMEOUT);
        props.add(RECORD_READER);
        props.add(RECORD_WRITER);
        props.add(READER_ERROR_HANDLING_STRATEGY);
        props.add(RECORD_BATCH_SIZE);
        props.add(SSL_CONTEXT_SERVICE);
        props.add(CLIENT_AUTH);
        PROPERTIES = Collections.unmodifiableList(props);
    }

    static final Set<Relationship> RELATIONSHIPS;
    static {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        RELATIONSHIPS = Collections.unmodifiableSet(rels);
    }

    static final int POLL_TIMEOUT_MS = 20;

    private volatile int port;
    private volatile SocketChannelRecordReaderDispatcher dispatcher;
    private volatile BlockingQueue<SocketChannelRecordReader> socketReaders = new LinkedBlockingQueue<>();

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();

        final String clientAuth = validationContext.getProperty(CLIENT_AUTH).getValue();
        final SSLContextService sslContextService = validationContext.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);

        if (sslContextService != null && StringUtils.isBlank(clientAuth)) {
            results.add(new ValidationResult.Builder()
                    .explanation("Client Auth must be provided when using TLS/SSL")
                    .valid(false).subject("Client Auth").build());
        }

        return results;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        this.port = context.getProperty(PORT).evaluateAttributeExpressions().asInteger();

        final int readTimeout = context.getProperty(READ_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        final int maxSocketBufferSize = context.getProperty(MAX_SOCKET_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
        final int maxConnections = context.getProperty(MAX_CONNECTIONS).asInteger();
        final RecordReaderFactory recordReaderFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);

        // if the Network Interface Property wasn't provided then a null InetAddress will indicate to bind to all interfaces
        final InetAddress nicAddress;
        final String nicAddressStr = context.getProperty(NETWORK_INTF_NAME).evaluateAttributeExpressions().getValue();
        if (!StringUtils.isEmpty(nicAddressStr)) {
            NetworkInterface netIF = NetworkInterface.getByName(nicAddressStr);
            nicAddress = netIF.getInetAddresses().nextElement();
        } else {
            nicAddress = null;
        }

        SSLContext sslContext = null;
        SslContextFactory.ClientAuth clientAuth = null;
        final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        if (sslContextService != null) {
            final String clientAuthValue = context.getProperty(CLIENT_AUTH).getValue();
            sslContext = sslContextService.createSSLContext(SSLContextService.ClientAuth.valueOf(clientAuthValue));
            clientAuth = SslContextFactory.ClientAuth.valueOf(clientAuthValue);
        }

        // create a ServerSocketChannel in non-blocking mode and bind to the given address and port
        final ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.bind(new InetSocketAddress(nicAddress, port));

        this.dispatcher = new SocketChannelRecordReaderDispatcher(serverSocketChannel, sslContext, clientAuth, readTimeout,
                maxSocketBufferSize, maxConnections, recordReaderFactory, socketReaders, getLogger());

        // start a thread to run the dispatcher
        final Thread readerThread = new Thread(dispatcher);
        readerThread.setName(getClass().getName() + " [" + getIdentifier() + "]");
        readerThread.setDaemon(true);
        readerThread.start();
    }

    @OnStopped
    public void onStopped() {
        if (dispatcher != null) {
            dispatcher.close();
            dispatcher = null;
        }

        SocketChannelRecordReader socketRecordReader;
        while ((socketRecordReader = socketReaders.poll()) != null) {
            IOUtils.closeQuietly(socketRecordReader.getRecordReader());
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final SocketChannelRecordReader socketRecordReader = pollForSocketRecordReader();
        if (socketRecordReader == null) {
            return;
        }

        if (socketRecordReader.isClosed()) {
            getLogger().warn("Unable to read records from {}, socket already closed", new Object[] {getRemoteAddress(socketRecordReader)});
            IOUtils.closeQuietly(socketRecordReader); // still need to call close so the overall count is decremented
            return;
        }

        final int recordBatchSize = context.getProperty(RECORD_BATCH_SIZE).asInteger();
        final String readerErrorHandling = context.getProperty(READER_ERROR_HANDLING_STRATEGY).getValue();
        final RecordSetWriterFactory recordSetWriterFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        // synchronize to ensure there are no stale values in the underlying SocketChannel
        synchronized (socketRecordReader) {
            FlowFile flowFile = session.create();
            try {
                // lazily creating the record reader here b/c we need a flow file, eventually shouldn't have to do this
                RecordReader recordReader = socketRecordReader.getRecordReader();
                if (recordReader == null) {
                    recordReader = socketRecordReader.createRecordReader(flowFile, getLogger());
                }

                Record record;
                try {
                    record = recordReader.nextRecord();
                } catch (final Exception e) {
                    boolean timeout = false;

                    // some of the underlying record libraries wrap the real exception in RuntimeException, so check each
                    // throwable (starting with the current one) to see if its a SocketTimeoutException
                    Throwable cause = e;
                    while (cause != null) {
                        if (cause instanceof SocketTimeoutException) {
                            timeout = true;
                            break;
                        }
                        cause = cause.getCause();
                    }

                    if (timeout) {
                        getLogger().debug("Timeout reading records, will try again later", e);
                        socketReaders.offer(socketRecordReader);
                        session.remove(flowFile);
                        return;
                    } else {
                        throw e;
                    }
                }

                if (record == null) {
                    getLogger().debug("No records available from {}, closing connection", new Object[]{getRemoteAddress(socketRecordReader)});
                    IOUtils.closeQuietly(socketRecordReader);
                    session.remove(flowFile);
                    return;
                }

                String mimeType = null;
                WriteResult writeResult = null;

                final RecordSchema recordSchema = recordSetWriterFactory.getSchema(Collections.EMPTY_MAP, record.getSchema());
                try (final OutputStream out = session.write(flowFile);
                     final RecordSetWriter recordWriter = recordSetWriterFactory.createWriter(getLogger(), recordSchema, out)) {

                    // start the record set and write the first record from above
                    recordWriter.beginRecordSet();
                    writeResult = recordWriter.write(record);

                    while (record != null && writeResult.getRecordCount() < recordBatchSize) {
                        // handle a read failure according to the strategy selected...
                        // if discarding then bounce to the outer catch block which will close the connection and remove the flow file
                        // if keeping then null out the record to break out of the loop, which will transfer what we have and close the connection
                        try {
                            record = recordReader.nextRecord();
                        } catch (final SocketTimeoutException ste) {
                            getLogger().debug("Timeout reading records, will try again later", ste);
                            break;
                        } catch (final Exception e) {
                            if (ERROR_HANDLING_DISCARD.getValue().equals(readerErrorHandling)) {
                                throw e;
                            } else {
                                record = null;
                            }
                        }

                        if (record != null) {
                            writeResult = recordWriter.write(record);
                        }
                    }

                    writeResult = recordWriter.finishRecordSet();
                    recordWriter.flush();
                    mimeType = recordWriter.getMimeType();
                }

                // if we didn't write any records then we need to remove the flow file
                if (writeResult.getRecordCount() <= 0) {
                    getLogger().debug("Removing flow file, no records were written");
                    session.remove(flowFile);
                } else {
                    final String sender = getRemoteAddress(socketRecordReader);

                    final Map<String, String> attributes = new HashMap<>(writeResult.getAttributes());
                    attributes.put(CoreAttributes.MIME_TYPE.key(), mimeType);
                    attributes.put("tcp.sender", sender);
                    attributes.put("tcp.port", String.valueOf(port));
                    attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                    flowFile = session.putAllAttributes(flowFile, attributes);

                    final String senderHost = sender.startsWith("/") && sender.length() > 1 ? sender.substring(1) : sender;
                    final String transitUri = new StringBuilder().append("tcp").append("://").append(senderHost).append(":").append(port).toString();
                    session.getProvenanceReporter().receive(flowFile, transitUri);

                    session.transfer(flowFile, REL_SUCCESS);
                }

                getLogger().debug("Re-queuing connection for further processing...");
                socketReaders.offer(socketRecordReader);

            } catch (Exception e) {
                getLogger().error("Error processing records: " + e.getMessage(), e);
                IOUtils.closeQuietly(socketRecordReader);
                session.remove(flowFile);
                return;
            }
        }
    }

    private SocketChannelRecordReader pollForSocketRecordReader() {
        try {
            return socketReaders.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    private String getRemoteAddress(final SocketChannelRecordReader socketChannelRecordReader) {
        return socketChannelRecordReader.getRemoteAddress() == null ? "null" : socketChannelRecordReader.getRemoteAddress().toString();
    }

    public final int getDispatcherPort() {
        return dispatcher == null ? 0 : dispatcher.getPort();
    }

}
