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

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.io.nio.BufferPool;
import org.apache.nifi.io.nio.ChannelListener;
import org.apache.nifi.io.nio.consumer.StreamConsumer;
import org.apache.nifi.io.nio.consumer.StreamConsumerFactory;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.annotation.CapabilityDescription;
import org.apache.nifi.processor.annotation.OnScheduled;
import org.apache.nifi.processor.annotation.OnStopped;
import org.apache.nifi.processor.annotation.OnUnscheduled;
import org.apache.nifi.processor.annotation.Tags;
import org.apache.nifi.processor.annotation.TriggerWhenEmpty;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.util.UDPStreamConsumer;
import org.apache.nifi.util.Tuple;

import org.apache.commons.lang3.StringUtils;

/**
 * <p>
 * This processor listens for Datagram Packets on a given port and concatenates
 * the contents of those packets together generating flow files roughly as often
 * as the internal buffer fills up or until no more data is currently available.
 * </p>
 *
 * <p>
 * This processor has the following required properties:
 * <ul>
 * <li><b>Port</b> - The port to listen on for data packets. Must be known by
 * senders of Datagrams.</li>
 * <li><b>Receive Timeout</b> - The time out period when waiting to receive data
 * from the socket. Specify units. Default is 5 secs.</li>
 * <li><b>Max Buffer Size</b> - Determines the size each receive buffer may be.
 * Specify units. Default is 1 MB.</li>
 * <li><b>FlowFile Size Trigger</b> - Determines the (almost) upper bound size
 * at which a flow file would be generated. A flow file will get made even if
 * this value isn't reached if there is no more data streaming in and this value
 * may be exceeded by the size of a single packet. Specify units. Default is 1
 * MB.</li>
 * <li><b>Max size of UDP Buffer</b> - The maximum UDP buffer size that should
 * be used. This is a suggestion to the Operating System to indicate how big the
 * udp socket buffer should be. Specify units. Default is 1 MB.")</li>
 * <li><b>Receive Buffer Count</b> - Number of receiving buffers to be used to
 * accept data from the socket. Higher numbers means more ram is allocated but
 * can allow better throughput. Default is 4.</li>
 * <li><b>Channel Reader Interval</b> - Scheduling interval for each read
 * channel. Specify units. Default is 50 millisecs.</li>
 * <li><b>FlowFiles Per Session</b> - The number of flow files per session.
 * Higher number is more efficient, but will lose more data if a problem occurs
 * that causes a rollback of a session. Default is 10</li>
 * </ul>
 * </p>
 *
 * This processor has the following optional properties:
 * <ul>
 * <li><b>Sending Host</b> - IP, or name, of a remote host. Only Datagrams from
 * the specified Sending Host Port and this host will be accepted. Improves
 * Performance. May be a system property or an environment variable.</li>
 * <li><b>Sending Host Port</b> - Port being used by remote host to send
 * Datagrams. Only Datagrams from the specified Sending Host and this port will
 * be accepted. Improves Performance. May be a system property or an environment
 * variable.</li>
 * </ul>
 * </p>
 *
 * <p>
 * The following relationships are required:
 * <ul>
 * <li><b>success</b> - Where to route newly created flow files.</li>
 * </ul>
 * </p>
 *
 */
@TriggerWhenEmpty
@Tags({"ingest", "udp", "listen", "source"})
@CapabilityDescription("Listens for Datagram Packets on a given port and concatenates the contents of those packets "
        + "together generating flow files")
public class ListenUDP extends AbstractSessionFactoryProcessor {

    private static final Set<Relationship> relationships;
    private static final List<PropertyDescriptor> properties;

    // relationships.
    public static final Relationship RELATIONSHIP_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Connection which contains concatenated Datagram Packets")
            .build();

    static {
        Set<Relationship> rels = new HashSet<>();
        rels.add(RELATIONSHIP_SUCCESS);
        relationships = Collections.unmodifiableSet(rels);
    }
    // required properties.
    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("Port")
            .description("Port to listen on. Must be known by senders of Datagrams.")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor RECV_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Receive Timeout")
            .description("The time out period when waiting to receive data from the socket. Specify units.")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("5 secs")
            .required(true)
            .build();

    public static final PropertyDescriptor MAX_BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("Max Buffer Size")
            .description("Determines the size each receive buffer may be")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("1 MB")
            .required(true)
            .build();

    public static final PropertyDescriptor FLOW_FILE_SIZE_TRIGGER = new PropertyDescriptor.Builder()
            .name("FlowFile Size Trigger")
            .description("Determines the (almost) upper bound size at which a flow file would be generated.")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("1 MB")
            .required(true)
            .build();

    public static final PropertyDescriptor MAX_UDP_BUFFER = new PropertyDescriptor.Builder()
            .name("Max size of UDP Buffer")
            .description("The maximum UDP buffer size that should be used. This is a suggestion to the Operating System "
                    + "to indicate how big the udp socket buffer should be.")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("1 MB")
            .required(true)
            .build();

    public static final PropertyDescriptor RECV_BUFFER_COUNT = new PropertyDescriptor.Builder()
            .name("Receive Buffer Count")
            .description("Number of receiving buffers to be used to accept data from the socket. Higher numbers "
                    + "means more ram is allocated but can allow better throughput.")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("4")
            .required(true)
            .build();

    public static final PropertyDescriptor CHANNEL_READER_PERIOD = new PropertyDescriptor.Builder()
            .name("Channel Reader Interval")
            .description("Scheduling interval for each read channel.")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("50 ms")
            .required(true)
            .build();

    public static final PropertyDescriptor FLOW_FILES_PER_SESSION = new PropertyDescriptor.Builder()
            .name("FlowFiles Per Session")
            .description("The number of flow files per session.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("10")
            .build();

    // optional properties.
    public static final PropertyDescriptor SENDING_HOST = new PropertyDescriptor.Builder()
            .name("Sending Host")
            .description("IP, or name, of a remote host. Only Datagrams from the specified Sending Host Port and this host will "
                    + "be accepted. Improves Performance. May be a system property or an environment variable.")
            .addValidator(new HostValidator())
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor SENDING_HOST_PORT = new PropertyDescriptor.Builder()
            .name("Sending Host Port")
            .description("Port being used by remote host to send Datagrams. Only Datagrams from the specified Sending Host and "
                    + "this port will be accepted. Improves Performance. May be a system property or an environment variable.")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    private static final Set<String> interfaceSet = new HashSet<>();

    static {
        try {
            final Enumeration<NetworkInterface> interfaceEnum
                    = NetworkInterface.getNetworkInterfaces();
            while (interfaceEnum.hasMoreElements()) {
                final NetworkInterface ifc = interfaceEnum.nextElement();
                interfaceSet.add(ifc.getName());
            }
        } catch (SocketException e) {
        }
    }
    public static final PropertyDescriptor NETWORK_INTF_NAME = new PropertyDescriptor.Builder()
            .name("Local Network Interface")
            .description("The name of a local network interface to be used to restrict listening for UDP Datagrams to a specific LAN."
                    + "May be a system property or an environment variable.")
            .addValidator(new Validator() {
                @Override
                public ValidationResult validate(String subject, String input, ValidationContext context) {
                    ValidationResult result = new ValidationResult.Builder()
                    .subject("Local Network Interface")
                    .valid(true)
                    .input(input)
                    .build();
                    if (interfaceSet.contains(input.toLowerCase())) {
                        return result;
                    }

                    String message;
                    try {
                        AttributeExpression ae = context.newExpressionLanguageCompiler().compile(input);
                        String realValue = ae.evaluate();
                        if (interfaceSet.contains(realValue.toLowerCase())) {
                            return result;
                        }

                        message = realValue + " is not a valid network name. Valid names are " + interfaceSet.toString();

                    } catch (IllegalArgumentException e) {
                        message = "Not a valid AttributeExpression: " + e.getMessage();
                    }
                    result = new ValidationResult.Builder()
                    .subject("Local Network Interface")
                    .valid(false)
                    .input(input)
                    .explanation(message)
                    .build();

                    return result;
                }
            })
            .expressionLanguageSupported(true)
            .build();

    static {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(SENDING_HOST);
        props.add(SENDING_HOST_PORT);
        props.add(NETWORK_INTF_NAME);
        props.add(CHANNEL_READER_PERIOD);
        props.add(FLOW_FILE_SIZE_TRIGGER);
        props.add(MAX_BUFFER_SIZE);
        props.add(MAX_UDP_BUFFER);
        props.add(PORT);
        props.add(RECV_BUFFER_COUNT);
        props.add(FLOW_FILES_PER_SESSION);
        props.add(RECV_TIMEOUT);
        properties = Collections.unmodifiableList(props);
    }
    // defaults
    public static final int DEFAULT_LISTENING_THREADS = 2;
    // lock used to protect channelListener
    private final Lock lock = new ReentrantLock();
    private volatile ChannelListener channelListener = null;
    private final BlockingQueue<Tuple<ProcessSession, List<FlowFile>>> flowFilesPerSessionQueue = new LinkedBlockingQueue<>();
    private final List<FlowFile> newFlowFiles = new ArrayList<>();
    private final AtomicReference<UDPStreamConsumer> consumerRef = new AtomicReference<>();
    private final AtomicBoolean stopping = new AtomicBoolean(false);
    private final AtomicReference<ProcessSessionFactory> sessionFactoryRef = new AtomicReference<>();
    private final ExecutorService consumerExecutorService = Executors.newSingleThreadExecutor();
    private final AtomicReference<Future<Tuple<ProcessSession, List<FlowFile>>>> consumerFutureRef = new AtomicReference<>();
    private final AtomicBoolean resetChannelListener = new AtomicBoolean(false);
    // instance attribute for provenance receive event generation
    private volatile String sendingHost;

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    /**
     * Create the ChannelListener and a thread that causes the Consumer to
     * create flow files.
     *
     * @param context
     * @throws IOException
     */
    @OnScheduled
    public void initializeChannelListenerAndConsumerProcessing(final ProcessContext context) throws IOException {
        getChannelListener(context);
        stopping.set(false);
        Future<Tuple<ProcessSession, List<FlowFile>>> consumerFuture = consumerExecutorService
                .submit(new Callable<Tuple<ProcessSession, List<FlowFile>>>() {

                    @Override
                    public Tuple<ProcessSession, List<FlowFile>> call() {
                        final int maxFlowFilesPerSession = context.getProperty(FLOW_FILES_PER_SESSION).asInteger();
                        final long channelReaderIntervalMSecs = context.getProperty(CHANNEL_READER_PERIOD).asTimePeriod(TimeUnit.MILLISECONDS);
                        // number of waits in 5 secs, or 1
                        final int maxWaits = (int) (channelReaderIntervalMSecs <= 1000 ? 5000 / channelReaderIntervalMSecs : 1);
                        final ProcessorLog logger = getLogger();
                        int flowFileCount = maxFlowFilesPerSession;
                        ProcessSession session = null;
                        int numWaits = 0;
                        while (!stopping.get()) {
                            UDPStreamConsumer consumer = consumerRef.get();
                            if (consumer == null || sessionFactoryRef.get() == null) {
                                try {
                                    Thread.sleep(100L);
                                } catch (InterruptedException swallow) {
                                }
                            } else {
                                try {
                                    // first time through, flowFileCount is maxFlowFilesPerSession so that a session
                                    // is created and the consumer is updated with it.
                                    if (flowFileCount == maxFlowFilesPerSession || numWaits == maxWaits) {
                                        logger.debug("Have waited {} times", new Object[]{numWaits});
                                        numWaits = 0;
                                        if (session != null) {
                                            Tuple<ProcessSession, List<FlowFile>> flowFilesPerSession = new Tuple<ProcessSession, List<FlowFile>>(
                                                    session,
                                                    new ArrayList<>(newFlowFiles));
                                            newFlowFiles.clear();
                                            flowFilesPerSessionQueue.add(flowFilesPerSession);
                                        }
                                        session = sessionFactoryRef.get().createSession();
                                        consumer.setSession(session);
                                        flowFileCount = 0;
                                    }
                                    // this will throttle the processing of the received datagrams. If there are no more
                                    // buffers to read into because none have been returned to the pool via consumer.process(),
                                    // then the desired back pressure on the channel is created.
                                    if (session.getAvailableRelationships().size() > 0) {
                                        consumer.process();
                                        if (flowFileCount == newFlowFiles.size()) {
                                            // no new datagrams received, need to throttle this thread back so it does
                                            // not consume all cpu...but don't want to cause back pressure on the channel
                                            // so the sleep time is same as the reader interval
                                            // If have done this for approx. 5 secs, assume datagram sender is down. So, push
                                            // out the remaining flow files (see numWaits == maxWaits above)
                                            Thread.sleep(channelReaderIntervalMSecs);
                                            if (flowFileCount > 0) {
                                                numWaits++;
                                            }
                                        } else {
                                            flowFileCount = newFlowFiles.size();
                                        }
                                    } else {
                                        logger.debug("Creating back pressure...no available destinations");
                                        Thread.sleep(1000L);
                                    }
                                } catch (final IOException ioe) {
                                    logger.error("Unable to fully process consumer {}", new Object[]{consumer}, ioe);
                                } catch (InterruptedException e) {
                                    // don't care
                                } finally {
                                    if (consumer.isConsumerFinished()) {
                                        logger.info("Consumer {} was closed and is finished", new Object[]{consumer});
                                        consumerRef.set(null);
                                        disconnect();
                                        if (!stopping.get()) {
                                            resetChannelListener.set(true);
                                        }
                                    }
                                }
                            }
                        }
                        // when shutting down, need consumer to drain rest of cached buffers and clean up.
                        // prior to getting here, the channelListener was shutdown
                        UDPStreamConsumer consumer;
                        while ((consumer = consumerRef.get()) != null && !consumer.isConsumerFinished()) {
                            try {
                                consumer.process();
                            } catch (IOException swallow) {
                                // if this is blown...consumer.isConsumerFinished will be true
                            }
                        }
                        Tuple<ProcessSession, List<FlowFile>> flowFilesPerSession = new Tuple<ProcessSession, List<FlowFile>>(session,
                                new ArrayList<>(newFlowFiles));
                        return flowFilesPerSession;
                    }
                });
        consumerFutureRef.set(consumerFuture);
    }

    private void disconnect() {
        if (lock.tryLock()) {
            try {
                if (channelListener != null) {
                    getLogger().debug("Shutting down channel listener {}", new Object[]{channelListener});
                    channelListener.shutdown(500L, TimeUnit.MILLISECONDS);
                    channelListener = null;
                }
            } finally {
                lock.unlock();
            }
        }
    }

    private void getChannelListener(final ProcessContext context) throws IOException {
        if (lock.tryLock()) {
            try {
                ProcessorLog logger = getLogger();
                logger.debug("Instantiating a new channel listener");
                final int port = context.getProperty(PORT).asInteger();
                final int bufferCount = context.getProperty(RECV_BUFFER_COUNT).asInteger();
                final Double bufferSize = context.getProperty(MAX_BUFFER_SIZE).asDataSize(DataUnit.B);
                final Double rcvBufferSize = context.getProperty(MAX_UDP_BUFFER).asDataSize(DataUnit.B);
                sendingHost = context.getProperty(SENDING_HOST).evaluateAttributeExpressions().getValue();
                final Integer sendingHostPort = context.getProperty(SENDING_HOST_PORT).evaluateAttributeExpressions().asInteger();
                final String nicIPAddressStr = context.getProperty(NETWORK_INTF_NAME).evaluateAttributeExpressions().getValue();
                final Double flowFileSizeTrigger = context.getProperty(FLOW_FILE_SIZE_TRIGGER).asDataSize(DataUnit.B);
                final int recvTimeoutMS = context.getProperty(RECV_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
                final StreamConsumerFactory consumerFactory = new StreamConsumerFactory() {

                    @Override
                    public StreamConsumer newInstance(final String streamId) {
                        final UDPStreamConsumer consumer = new UDPStreamConsumer(streamId, newFlowFiles, flowFileSizeTrigger.intValue(), getLogger());
                        consumerRef.set(consumer);
                        return consumer;
                    }
                };
                final int readerMilliseconds = context.getProperty(CHANNEL_READER_PERIOD).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
                final BufferPool bufferPool = new BufferPool(bufferCount, bufferSize.intValue(), false, Integer.MAX_VALUE);
                channelListener = new ChannelListener(DEFAULT_LISTENING_THREADS, consumerFactory, bufferPool, recvTimeoutMS, TimeUnit.MILLISECONDS);
                // specifying a sufficiently low number for each stream to be fast enough though very efficient
                channelListener.setChannelReaderSchedulingPeriod(readerMilliseconds, TimeUnit.MILLISECONDS);
                InetAddress nicIPAddress = null;
                if (null != nicIPAddressStr) {
                    NetworkInterface netIF = NetworkInterface.getByName(nicIPAddressStr);
                    nicIPAddress = netIF.getInetAddresses().nextElement();
                }
                channelListener.addDatagramChannel(nicIPAddress, port, rcvBufferSize.intValue(), sendingHost, sendingHostPort);
                logger.info("Registered service and initialized UDP socket listener. Now listening on port " + port + "...");
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Collection<ValidationResult> result = new ArrayList<>();
        String sendingHost = validationContext.getProperty(SENDING_HOST).getValue();
        String sendingPort = validationContext.getProperty(SENDING_HOST_PORT).getValue();
        if (StringUtils.isBlank(sendingHost) && StringUtils.isNotBlank(sendingPort)) {
            result.add(new ValidationResult.Builder()
                    .subject(SENDING_HOST.getName())
                    .valid(false)
                    .explanation("Must specify Sending Host when specifying Sending Host Port")
                    .build());
        } else if (StringUtils.isBlank(sendingPort) && StringUtils.isNotBlank(sendingHost)) {
            result.add(new ValidationResult.Builder()
                    .subject(SENDING_HOST_PORT.getName())
                    .valid(false)
                    .explanation("Must specify Sending Host Port when specifying Sending Host")
                    .build());
        }
        return result;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        final ProcessorLog logger = getLogger();
        sessionFactoryRef.compareAndSet(null, sessionFactory);
        if (resetChannelListener.getAndSet(false) && !stopping.get()) {
            try {
                getChannelListener(context);
            } catch (IOException e) {
                logger.error("Tried to reset Channel Listener and failed due to:", e);
                resetChannelListener.set(true);
            }
        }

        transferFlowFiles();
    }

    private boolean transferFlowFiles() {
        final ProcessorLog logger = getLogger();
        ProcessSession session;
        Tuple<ProcessSession, List<FlowFile>> flowFilesPerSession = null;
        boolean transferred = false;
        try {
            flowFilesPerSession = flowFilesPerSessionQueue.poll(100L, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
        }
        if (flowFilesPerSession != null) {
            session = flowFilesPerSession.getKey();
            List<FlowFile> flowFiles = flowFilesPerSession.getValue();
            String sourceSystem = sendingHost == null ? "Unknown" : sendingHost;
            try {
                for (FlowFile flowFile : flowFiles) {
                    session.getProvenanceReporter().receive(flowFile, sourceSystem);
                    session.transfer(flowFile, RELATIONSHIP_SUCCESS);
                }
                logger.info("Transferred flow files {} to success", new Object[]{flowFiles});
                transferred = true;

                // need to check for erroneous flow files in input queue
                List<FlowFile> existingFlowFiles = session.get(10);
                for (FlowFile existingFlowFile : existingFlowFiles) {
                    if (existingFlowFile != null && existingFlowFile.getSize() > 0) {
                        session.transfer(existingFlowFile, RELATIONSHIP_SUCCESS);
                        logger.warn("Found flow file in input queue (shouldn't have). Transferred flow file {} to success",
                                new Object[]{existingFlowFile});
                    } else if (existingFlowFile != null) {
                        session.remove(existingFlowFile);
                        logger.warn("Found empty flow file in input queue (shouldn't have). Removed flow file {}", new Object[]{existingFlowFile});
                    }
                }
                session.commit();
            } catch (Throwable t) {
                session.rollback();
                logger.error("Failed to transfer flow files or commit session...rolled back", t);
                throw t;
            }
        }
        return transferred;
    }

    @OnUnscheduled
    public void stopping() {
        getLogger().debug("Stopping Processor");
        disconnect();
        stopping.set(true);
        Future<Tuple<ProcessSession, List<FlowFile>>> future;
        Tuple<ProcessSession, List<FlowFile>> flowFilesPerSession;
        if ((future = consumerFutureRef.getAndSet(null)) != null) {
            try {
                flowFilesPerSession = future.get();
                if (flowFilesPerSession.getValue().size() > 0) {
                    getLogger().debug("Draining remaining flow Files when stopping");
                    flowFilesPerSessionQueue.add(flowFilesPerSession);
                } else {
                    // need to close out the session that has no flow files
                    flowFilesPerSession.getKey().commit();
                }
            } catch (InterruptedException | ExecutionException e) {
                getLogger().error("Failure in cleaning up!", e);
            }
            boolean moreFiles = true;
            while (moreFiles) {
                try {
                    moreFiles = transferFlowFiles();
                } catch (Throwable t) {
                    getLogger().error("Problem transferring cached flowfiles", t);
                }
            }
        }
    }

    @OnStopped
    public void stopped() {
        sessionFactoryRef.set(null);
    }

    public static class HostValidator implements Validator {

        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            try {
                InetAddress.getByName(input);
                return new ValidationResult.Builder()
                        .subject(subject)
                        .valid(true)
                        .input(input)
                        .build();
            } catch (final UnknownHostException e) {
                return new ValidationResult.Builder()
                        .subject(subject)
                        .valid(false)
                        .input(input)
                        .explanation("Unknown host: " + e)
                        .build();
            }
        }

    }

}
