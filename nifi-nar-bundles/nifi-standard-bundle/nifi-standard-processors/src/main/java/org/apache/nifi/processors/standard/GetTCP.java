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
import org.apache.nifi.annotation.behavior.SideEffectFree;

import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.AlreadyConnectedException;
import java.nio.channels.ConnectionPendingException;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.nio.channels.UnsupportedAddressTypeException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.MalformedInputException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@SupportsBatching
@SideEffectFree
@Tags({"get", "fetch", "poll", "tcp", "ingest", "source", "input"})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Connects over TCP to the provided server. When receiving data this will writes either the" +
        " full receive buffer or messages based on demarcator to the content of a FlowFile. ")
public class GetTCP extends AbstractProcessor {

    private static final Validator ENDPOINT_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String value, final ValidationContext context) {
            if (null == value || value.isEmpty()) {
                return new ValidationResult.Builder().subject(subject).input(value).valid(false).explanation(subject + " cannot be empty").build();
            }
            //The format should be <host>:<port>{,<host>:<port>}
            //first split on ,
            final String[] hostPortPairs = value.split(",");
            boolean validHostPortPairs = true;
            String reason = "";
            String offendingSubject = subject;

            if(0 == hostPortPairs.length){
                return new ValidationResult.Builder().subject(subject).input(value).valid(false).explanation(offendingSubject + " cannot be empty").build();
            }
            for (final String hostPortPair : hostPortPairs) {
                offendingSubject = hostPortPair;
                //split pair
                if (hostPortPair.isEmpty()) {
                    validHostPortPairs = false;
                    reason = "endpoint is empty";
                    break;
                }
                if (!hostPortPair.contains(":")) {
                    validHostPortPairs = false;
                    reason = "endpoint pair does not contain valid delimiter";
                    break;
                }
                final String[] parts = hostPortPair.split(":");

                if (1 == parts.length) {
                    validHostPortPairs = false;
                    reason = "could not determine the port";
                    break;
                } else {
                    try {
                        final int intVal = Integer.parseInt(parts[1]);

                        if (intVal <= 0) {
                            reason = "not a positive value";
                            validHostPortPairs = false;
                            break;
                        }
                    } catch (final NumberFormatException e) {
                        reason = "not a valid integer";
                        validHostPortPairs = false;
                        break;
                    }
                }
                //if we already have a bad pair then exit now.
                if (!validHostPortPairs) {
                    break;
                }
            }
            return new ValidationResult.Builder().subject(offendingSubject).input(value).explanation(reason).valid(validHostPortPairs).build();
        }
    };

    public static final PropertyDescriptor ENDPOINT_LIST = new PropertyDescriptor
            .Builder().name("Endpoint List")
            .description("A comma delimited list of the servers to connect to. The format should be " +
                    "<server_address>:<port>. Only one server will be connected to at a time, the others " +
                    "will be used as fail overs.")
            .required(true)
            .addValidator(ENDPOINT_VALIDATOR)
            .build();

    public static final PropertyDescriptor FAILOVER_ENDPOINT = new PropertyDescriptor
            .Builder().name("Failover Endpoint")
            .description("A failover server to connect to if one of the main ones is unreachable after the connection " +
                    "attempt count. The format should be <server_address>:<port>.")
            .required(false)
            // .defaultValue("")
            .addValidator(ENDPOINT_VALIDATOR)
            .build();


    public static final PropertyDescriptor CONNECTION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Connection Timeout")
            .description("The amount of time to wait before timing out while creating a connection")
            .required(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("5 sec")
            .build();

    public static final PropertyDescriptor CONNECTION_ATTEMPT_COUNT = new PropertyDescriptor.Builder()
            .name("Connection Attempt Count")
            .description("The number of times to try and establish a connection, before using a backup host if available." +
                    " This same attempt count would be used for a backup host as well.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("3")
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Number of Messages in Batch")
            .description("The number of messages to write to the flow file content")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("10")
            .build();


    public static final PropertyDescriptor RECEIVE_BUFFER_SIZE = new PropertyDescriptor
            .Builder().name("Receive Buffer Size")
            .description("The size of the buffer to receive data in")
            .required(false)
            .defaultValue("2048")
            .addValidator(StandardValidators.createLongValidator(1, 2048, true))
            .build();

    public static final PropertyDescriptor KEEP_ALIVE = new PropertyDescriptor
            .Builder().name("Keep Alive")
            .description("This determines if TCP keep alive is used.")
            .required(false)
            .defaultValue("true")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("The relationship that all sucessful messages from the WebSocket will be sent to")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("Failure")
            .description("The relationship that all failed messages from the WebSocket will be sent to")
            .build();

    private final static List<PropertyDescriptor> propertyDescriptors;
    private final static Set<Relationship> relationships;
    private final static Charset charset = Charset.forName(StandardCharsets.UTF_8.name());
    private final Map<String, String> dynamicAttributes = new HashMap<>();
    private volatile Set<String> dynamicPropertyNames = new HashSet<>();

    private AtomicBoolean connectedToBackup = new AtomicBoolean();


    /*
    * Will ensure that the list of property descriptors is build only once.
    * Will also create a Set of relationships
    */
    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(ENDPOINT_LIST);
        _propertyDescriptors.add(FAILOVER_ENDPOINT);
        _propertyDescriptors.add(CONNECTION_TIMEOUT);
        _propertyDescriptors.add(CONNECTION_ATTEMPT_COUNT);
        _propertyDescriptors.add(BATCH_SIZE);
        _propertyDescriptors.add(RECEIVE_BUFFER_SIZE);
        _propertyDescriptors.add(KEEP_ALIVE);


        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    private Map<SocketRecveiverThread, Future> socketToFuture = new HashMap<>();
    private ExecutorService executorService;
    private transient ComponentLog log = getLogger();
    private transient String originalServerAddressList;
    private transient String backupServer;
    private transient int batchSize;

    /**
     * Bounded queue of messages events from the socket.
     */
    protected final BlockingQueue<String> socketMessagesReceived = new ArrayBlockingQueue<>(256);

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .required(false)
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .dynamic(true)
                .expressionLanguageSupported(true)
                .build();
    }


    @OnScheduled
    public void onScheduled(final ProcessContext context) throws ProcessException {


        final int rcvBufferSize = context.getProperty(RECEIVE_BUFFER_SIZE).asInteger();
        final boolean keepAlive = context.getProperty(KEEP_ALIVE).asBoolean();
        final int connectionTimeout = context.getProperty(CONNECTION_TIMEOUT).asTimePeriod(TimeUnit.SECONDS).intValue();
        final int connectionRetryCount = context.getProperty(CONNECTION_ATTEMPT_COUNT).asInteger();
        originalServerAddressList = context.getProperty(ENDPOINT_LIST).getValue();
        final String[] serverAddresses = originalServerAddressList.split(",");
        backupServer = context.getProperty(FAILOVER_ENDPOINT).getValue();
        executorService = Executors.newFixedThreadPool(serverAddresses.length);
        batchSize = context.getProperty(BATCH_SIZE).asInteger();

        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            final PropertyDescriptor descriptor = entry.getKey();
            if (descriptor.isDynamic()) {
                dynamicAttributes.put(descriptor.getName(), entry.getValue());

            }
        }

        //Go through all of the servers that we need to connect to and establish a connection to each of them
        //this will result in all of them generating data that goes into the flow files.
        for (final String hostPortPair : serverAddresses) {
            //split pair
            final String[] hostAndPort = hostPortPair.split(":");


            SocketRecveiverThread socketRecveiverThread = null;
            try {
                socketRecveiverThread = createSocketRecveiverThread(rcvBufferSize, keepAlive, connectionTimeout, connectionRetryCount, hostAndPort);
            } catch (Exception ex) {
                // Should we re-throw this or throw a ProcessException? If there is only one host in the list  and no backup
                // then we should otherwise there will be no data and we cannot really run. For a single hosts / port
                // pair it may make sense to throw as well and then perhaps provide a property to allow the user to decide
                // if there are more than one servers in the list do we throw or just log it. For now will throw and
                // treat it as an error.
                log = getLogger();
                log.error("Caught exception trying to create thread to process data from host: {}", new Object[]{hostAndPort[0], ex});

                if (!connectedToBackup.get() && !backupServer.isEmpty()) {
                    log.error("Attempting connection to backup server: {}", new Object[]{backupServer});
                    try {
                        final String[] backupHostAndPort = backupServer.split(":");
                        socketRecveiverThread = createSocketRecveiverThread(rcvBufferSize, keepAlive, connectionTimeout, connectionRetryCount, backupHostAndPort);
                        connectedToBackup.set(true);
                    } catch (Exception backupException) {

                        log.error("Caught exception trying to connect to backup server: ", new Object[]{backupServer, ex});
                        throw new ProcessException(String.format("Caught exception trying to create thread to process " +
                                "data from backup server: %s", backupServer), backupException);
                    }
                } else {
                    final String connectionMsg = connectedToBackup.get() ? String.format("Already connected to backup %s", backupServer) :
                            "No backup server configured to connect to";
                    log.info(connectionMsg);
                    throw new ProcessException(String.format("Caught exception trying to create thread to process data from host: %s", hostAndPort[0]), ex);
                }
            }

            getLogger().info("Created thread to process data from host: {}", new Object[]{hostAndPort[0]});
            final Future socketReceiverFuture = executorService.submit(socketRecveiverThread);
            socketToFuture.put(socketRecveiverThread, socketReceiverFuture);


        }

    }

    protected SocketRecveiverThread createSocketRecveiverThread(int rcvBufferSize, boolean keepAlive, int connectionTimeout, int connectionRetryCount, String[] hostAndPort) {
        SocketRecveiverThread socketRecveiverThread;
        socketRecveiverThread = new SocketRecveiverThread(hostAndPort[0],
                Integer.parseInt(hostAndPort[1]), backupServer, keepAlive, rcvBufferSize, connectionTimeout, connectionRetryCount);
        return socketRecveiverThread;
    }

    @OnStopped
    public void tearDown() throws ProcessException {
        try {
            for (Map.Entry<SocketRecveiverThread, Future> socketAndFuture : socketToFuture.entrySet()) {
                socketAndFuture.getKey().stopProcessing();
                socketAndFuture.getValue().cancel(true);

            }
            if (null != executorService) {
                executorService.shutdown();
                if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();

                    if (!executorService.awaitTermination(5, TimeUnit.SECONDS))
                        log.error("Executor service for receiver thread did not terminate");
                }
            }
        } catch (final Exception e) {
            throw new ProcessException(e);
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        try {
            final StringBuilder messages = new StringBuilder();
            if (socketMessagesReceived.size() >= batchSize) {
                for (int i = 0; i < batchSize; i++) {
                    messages.append(socketMessagesReceived.poll(100, TimeUnit.MILLISECONDS));
                }
            } else {
                messages.append(socketMessagesReceived.poll(100, TimeUnit.MILLISECONDS));
            }
            if (0 == messages.length()) {
                return;
            }
            FlowFile flowFile = session.create();

            flowFile = session.write(flowFile, out -> out.write(messages.toString().getBytes()));
            //need to at least put the list of hosts and ports, would be nice to know the exact
            //host the message came frome -- to do that we need to do one of two things
            //
            // 1. Have a message queue per server
            // 2. Add something to the message that then needs to be parsed.
            //
            // Both of these are not ideal, for now just show the whole list and someone can still use a
            // ROA processor and see if the attribute contains a host.
            flowFile = session.putAttribute(flowFile, "ServerList", originalServerAddressList);
            //add any dynamic properties
            if (0 < dynamicAttributes.size()) {
                flowFile = session.putAllAttributes(flowFile, dynamicAttributes);
            }
            session.transfer(flowFile, REL_SUCCESS);

        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }


    protected class SocketRecveiverThread implements Runnable {

        private SocketChannel socketChannel = null;
        private boolean keepProcessing = true;
        private ComponentLog log = getLogger();
        private final String host;
        private final int port;
        private final String backupServer;
        private final boolean keepAlive;
        private final int rcvBufferSize;
        private final int connectionTimeout;
        private final int maxConnectionAttemptCount;


        SocketRecveiverThread(final String host, final int port, final String backupServer, final boolean keepAlive,
                              final int rcvBufferSize,
                              final int connectionTimeout,
                              final int maxConnectionAttemptCount) {

            this.host = host;
            this.port = port;
            this.backupServer = backupServer;
            this.rcvBufferSize = rcvBufferSize;
            this.keepAlive = keepAlive;
            this.connectionTimeout = connectionTimeout;
            this.maxConnectionAttemptCount = maxConnectionAttemptCount;
            establishConnection(host, port);

        }

        /**
         * Try to connect to the server this thread is dedicated to.
         */
        private void establishConnection(final String host, final int port) {

            int currentConnectionAttemptCount = 0;
            //need to accommodate the use case where we are just being scheduled. Change the below
            //into a do-while.
            boolean isConnected = false;


            //we should now poll to see if it is connected until CONNECTION_TIMEOUT setting
            //and if it is not connected, then log it and try and reconnect. perhaps use exponential backoff
            long startTime = Instant.now().getEpochSecond();
            while (!isConnected) {
                //if this is our original attempt, the attempt connection.

                if (0 == currentConnectionAttemptCount) {
                    //this may not really be the first, as it may occur after we have lost the connection and are
                    //trying again.
                    log.info("Attempting first connection to host: {}:{}", new Object[]{host, port});
                    currentConnectionAttemptCount++;
                    try {
                        isConnected = initiateConnection(host, port);
                        if (isConnected) {
                            log.info("Successfully connected to: {}:{}", new Object[]{host, port});
                            break;
                        }
                    } catch (Exception ex) {
                        //this may happened, swallow it and let the rest of retry logic take hold.
                    }
                }
                if ((Instant.now().getEpochSecond() - connectionTimeout) > startTime) {
                    log.info("Attempting connection to host: {} after timeout", new Object[]{host});
                    //we have exceeded the timeout, we need to try to initiate a different connection
                    //need to only try and reconnect, if we have not exceeded a retry count.
                    if(currentConnectionAttemptCount >= maxConnectionAttemptCount){
                        //need to throw as we cannot connect to any host
                        throw new ProcessException("Could not connect to any hosts, tried main and backup. Giving up.");
                    }else{
                        try {
                            currentConnectionAttemptCount++;
                            isConnected = initiateConnection(host, port);
                            if (isConnected) {
                                log.info("Successfully connected to: {}:{}", new Object[]{host, port});
                                // startTime = Instant.now().getEpochSecond();
                                break;
                            }

                        } catch (Exception ex) {
                            //if we have not exceed the retry count, just swallow this as it may happen
                            //otherwise we need to use the fail over or bail.
                            if (currentConnectionAttemptCount == maxConnectionAttemptCount) {
                                throw ex;
                            }
                        }
                    }
                } else{
                    try {
                        log.info("Waiting to connect to: {}:{}", new Object[]{host, port});
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        //Not much we can do here... log it and let things unfold.
                        log.error("Was interrupted while trying to establish connection to: {}",
                                new Object[]{socketChannel.toString(), e});

                    }
                }
            }

        }

        private boolean initiateConnection(final String host, final int port) {

            try {

                if (null != socketChannel) {
                    //this may be the case if it is a re-open use case.
                    socketChannel.close();
                }
                socketChannel = SocketChannel.open();
                InetSocketAddress inetSocketAddress = new InetSocketAddress(host, port);
                socketChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, keepAlive);
                socketChannel.setOption(StandardSocketOptions.SO_RCVBUF, rcvBufferSize);
                socketChannel.configureBlocking(true);
                return socketChannel.connect(inetSocketAddress);

            } catch (AlreadyConnectedException alreadyConnectedException) {
                //If this channel is already connected, this is a bug as we should not be trying to re-connect.
                throw new ProcessException("Trying to connect to an address we are already connected to.", alreadyConnectedException);
            } catch (ConnectionPendingException | UnresolvedAddressException |
                    UnsupportedAddressTypeException | SecurityException | IOException exception) {
                // These can occur for the following reasons:
                // ConnectionPendingException -- A connection is still pending for this host:port
                //  we should not encounter this, as we are closing the connection prior to attempting the connect.
                //  at least it should be logged.
                // ClosedChannelException (this is a subclass of IOException) -- The channel is currently closed.
                //  this should not occur, as this method should only occur to create a connection,
                //  log it and continue.
                // UnresolvedAddressException -- If the given remote address is not fully resolved,
                // UnsupportedAddressTypeException -- If the type of the given remote address is not supported.
                // SecurityException -- If a security manager has been installed and it does not permit access
                //                      to the given remote endpoint
                // IOException -- If some other I/O error occurs
                // In any of these cases if we only have one server in the list then this is a problem and we should bail.
                // Also if we have exhausted the list we also need to bail as we are now out of servers to try. If neither
                // of those cases are true, then we should move on to the next server and try and open the connection.
                log.error("Caught exception trying to connect to the host: {}. Depending on configuration may try and reconnect.",
                        new Object[]{host, exception});


                throw new ProcessException("Caught UnresolvedAddressException for the host: " +
                        host,
                        exception);

            }
        }


        void stopProcessing() {
            keepProcessing = false;
        }

        public void run() {
            log.debug("Starting to receive messages");

            int nBytes = 0;
            ByteBuffer buf = ByteBuffer.allocate(rcvBufferSize);
            try {
                while (keepProcessing) {
                    if (socketChannel.isOpen() && socketChannel.isConnected()) {
                        nBytes = socketChannel.read(buf);
                        if (-1 == nBytes) {
                            boolean socketAlive = isSocketAlive();
                            log.info("Socket is no longer alive, will try to re-connect.");
                            if (!socketAlive) {

                                try {
                                    establishConnection(this.host, this.port);
                                } catch (Exception ex) {
                                    //we could not connect to our set host and port, so try a backup if it exists.
                                    //otherwise just bail.
                                    if (!connectedToBackup.get() && !backupServer.isEmpty()) {
                                        final String[] backupParts = backupServer.split(":");
                                        //anything thrown here will bubble.
                                        establishConnection(backupParts[0], Integer.parseInt(backupParts[1]));
                                        connectedToBackup.set(true);
                                    } else {
                                        throw ex;
                                    }
                                }
                            }
                        }
                        if (0 < nBytes) {
                            log.debug("Read {} from socket", new Object[]{nBytes});
                            buf.flip();
                            CharsetDecoder decoder = charset.newDecoder();

                            try {
                                CharBuffer charBuffer = decoder.decode(buf);


                                if (log.isDebugEnabled()) {
                                    final String message = charBuffer.toString();
                                    log.debug("Received Message: {}", new Object[]{message});
                                }

                                socketMessagesReceived.offer(charBuffer.toString());
                                buf.flip();
                                buf.clear();
                            } catch (MalformedInputException ex) {
                                log.error("Caught MalformedInputException trying to decode bytes from socket.", ex);
                            }
                        }
                    }
                }
                //make sure we did not break out the loop for other reasos.
                if (!keepProcessing) {
                    //we have been asked to stop processing and shutdown
                    socketChannel.close();
                }


            } catch (IOException e) {
                if (!Thread.interrupted()) {
                    log.error("Caught exception while processing data from the socket.", new Object[]{e});
                    throw new ProcessException(e);
                }

            }


        }

        boolean isSocketAlive() {
            boolean isAlive = false;

            SocketAddress socketAddress = new InetSocketAddress(this.host, port);
            Socket socket = new Socket();

            // Timeout required - it's in milliseconds
            int timeout = connectionTimeout * 1000;

            log.debug("Trying to connect to {}:{}", new Object[]{this.host, port});
            try {
                socket.connect(socketAddress, timeout);
                socket.close();
                isAlive = true;

            } catch (SocketTimeoutException exception) {
                log.info("Timed out trying to connect to socket for {}:{}", new Object[]{this.host, port});
            } catch (IOException exception) {
                log.info(
                        "IOException - Unable to connect to {}:{}", new Object[]{this.host, port, exception});
            }
            return isAlive;
        }

    }
}
