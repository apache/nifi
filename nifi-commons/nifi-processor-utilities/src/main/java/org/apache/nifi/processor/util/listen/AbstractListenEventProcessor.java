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
package org.apache.nifi.processor.util.listen;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.listen.dispatcher.ChannelDispatcher;
import org.apache.nifi.processor.util.listen.event.Event;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * An abstract processor to extend from when listening for events over a channel. This processor
 * will start a ChannelDispatcher, and optionally a ChannelResponseDispatcher, in a background
 * thread which will end up placing events on a queue to polled by the onTrigger method. Sub-classes
 * are responsible for providing the dispatcher implementations.
 *
 * @param <E> the type of events being produced
 */
public abstract class AbstractListenEventProcessor<E extends Event> extends AbstractProcessor {

    public static final PropertyDescriptor PORT = new PropertyDescriptor
            .Builder().name("Port")
            .description("The port to listen on for communication.")
            .required(true)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();
    public static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("Character Set")
            .description("Specifies the character set of the received data.")
            .required(true)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build();
    public static final PropertyDescriptor RECV_BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("Receive Buffer Size")
            .description("The size of each buffer used to receive messages. Adjust this value appropriately based on the expected size of the " +
                    "incoming messages.")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("65507 B")
            .required(true)
            .build();
    public static final PropertyDescriptor MAX_SOCKET_BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("Max Size of Socket Buffer")
            .description("The maximum size of the socket buffer that should be used. This is a suggestion to the Operating System " +
                    "to indicate how big the socket buffer should be. If this value is set too low, the buffer may fill up before " +
                    "the data can be read, and incoming data will be dropped.")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("1 MB")
            .required(true)
            .build();
    public static final PropertyDescriptor MAX_MESSAGE_QUEUE_SIZE = new PropertyDescriptor.Builder()
            .name("Max Size of Message Queue")
            .description("The maximum size of the internal queue used to buffer messages being transferred from the underlying channel to the processor. " +
                    "Setting this value higher allows more messages to be buffered in memory during surges of incoming messages, but increases the total " +
                    "memory used by the processor.")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("10000")
            .required(true)
            .build();

    // Putting these properties here so sub-classes don't have to redefine them, but they are
    // not added to the properties by default since not all processors may need them

    public static final PropertyDescriptor MAX_CONNECTIONS = new PropertyDescriptor.Builder()
            .name("Max Number of TCP Connections")
            .description("The maximum number of concurrent TCP connections to accept.")
            .addValidator(StandardValidators.createLongValidator(1, 65535, true))
            .defaultValue("2")
            .required(true)
            .build();
    public static final PropertyDescriptor MAX_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Max Batch Size")
            .description(
                    "The maximum number of messages to add to a single FlowFile. If multiple messages are available, they will be concatenated along with "
                            + "the <Message Delimiter> up to this configured maximum number of messages")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(false)
            .defaultValue("1")
            .required(true)
            .build();
    public static final PropertyDescriptor MESSAGE_DELIMITER = new PropertyDescriptor.Builder()
            .name("Message Delimiter")
            .description("Specifies the delimiter to place between messages when multiple messages are bundled together (see <Max Batch Size> property).")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("\\n")
            .required(true)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Messages received successfully will be sent out this relationship.")
            .build();

    public static final int POLL_TIMEOUT_MS = 20;

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> descriptors;

    protected volatile int port;
    protected volatile Charset charset;
    protected volatile ChannelDispatcher dispatcher;
    protected volatile BlockingQueue<E> events;
    protected volatile BlockingQueue<E> errorEvents = new LinkedBlockingQueue<>();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(PORT);
        descriptors.add(RECV_BUFFER_SIZE);
        descriptors.add(MAX_MESSAGE_QUEUE_SIZE);
        descriptors.add(MAX_SOCKET_BUFFER_SIZE);
        descriptors.add(CHARSET);
        descriptors.addAll(getAdditionalProperties());
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.addAll(getAdditionalRelationships());
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    /**
     * Override to provide additional relationships for the processor.
     *
     * @return a list of relationships
     */
    protected List<Relationship> getAdditionalRelationships() {
        return Collections.EMPTY_LIST;
    }

    /**
     * Override to provide additional properties for the processor.
     *
     * @return a list of properties
     */
    protected List<PropertyDescriptor> getAdditionalProperties() {
        return Collections.EMPTY_LIST;
    }

    @Override
    public final Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        charset = Charset.forName(context.getProperty(CHARSET).getValue());
        port = context.getProperty(PORT).asInteger();
        events = new LinkedBlockingQueue<>(context.getProperty(MAX_MESSAGE_QUEUE_SIZE).asInteger());

        final int maxChannelBufferSize = context.getProperty(MAX_SOCKET_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();

        // create the dispatcher and call open() to bind to the given port
        dispatcher = createDispatcher(context, events);
        dispatcher.open(port, maxChannelBufferSize);

        // start a thread to run the dispatcher
        final Thread readerThread = new Thread(dispatcher);
        readerThread.setName(getClass().getName() + " [" + getIdentifier() + "]");
        readerThread.setDaemon(true);
        readerThread.start();
    }

    /**
     * @param context the ProcessContext to retrieve property values from
     * @return a ChannelDispatcher to handle incoming connections
     *
     * @throws IOException if unable to listen on the requested port
     */
    protected abstract ChannelDispatcher createDispatcher(final ProcessContext context, final BlockingQueue<E> events) throws IOException;

    // used for testing to access the random port that was selected
    public final int getDispatcherPort() {
        return dispatcher == null ? 0 : dispatcher.getPort();
    }

    public int getErrorQueueSize() {
        return errorEvents.size();
    }

    public int getQueueSize() {
        return events == null ? 0 : events.size();
    }

    @OnUnscheduled
    public void onUnscheduled() {
        if (dispatcher != null) {
            dispatcher.close();
        }
    }

    /**
     * If pollErrorQueue is true, the error queue will be checked first and event will be
     * returned from the error queue if available.
     *
     * If pollErrorQueue is false, or no data is in the error queue, the regular queue is polled.
     *
     * If longPoll is true, the regular queue will be polled with a short timeout, otherwise it will
     * poll with no timeout which will return immediately.
     *
     * @param longPoll whether or not to poll the main queue with a small timeout
     * @param pollErrorQueue whether or not to poll the error queue first
     *
     * @return an event from one of the queues, or null if none are available
     */
    protected E getMessage(final boolean longPoll, final boolean pollErrorQueue, final ProcessSession session) {
        E event = null;
        if (pollErrorQueue) {
            event = errorEvents.poll();
        }

        if (event == null) {
            try {
                if (longPoll) {
                    event = events.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                } else {
                    event = events.poll();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
        }

        if (event != null) {
            session.adjustCounter("Messages Received", 1L, false);
        }

        return event;
    }

    /**
     * Batches together up to the batchSize events. Events are grouped together based on a batch key which
     * by default is the sender of the event, but can be override by sub-classes.
     *
     * This method will return when batchSize has been reached, or when no more events are available on the queue.
     *
     * @param session the current session
     * @param totalBatchSize the total number of events to process
     * @param messageDemarcatorBytes the demarcator to put between messages when writing to a FlowFile
     *
     * @return a Map from the batch key to the FlowFile and events for that batch, the size of events in all
     *              the batches will be <= batchSize
     */
    protected Map<String,FlowFileEventBatch> getBatches(final ProcessSession session, final int totalBatchSize,
                                                        final byte[] messageDemarcatorBytes) {

        final Map<String,FlowFileEventBatch> batches = new HashMap<>();
        for (int i=0; i < totalBatchSize; i++) {
            final E event = getMessage(true, true, session);
            if (event == null) {
                break;
            }

            final String batchKey = getBatchKey(event);
            FlowFileEventBatch batch = batches.get(batchKey);

            // if we don't have a batch for this key then create a new one
            if (batch == null) {
                batch = new FlowFileEventBatch(session.create(), new ArrayList<E>());
                batches.put(batchKey, batch);
            }

            // add the current event to the batch
            batch.getEvents().add(event);

            // append the event's data to the FlowFile, write the demarcator first if not on the first event
            final boolean writeDemarcator = (i > 0);
            try {
                final byte[] rawMessage = event.getData();
                FlowFile appendedFlowFile = session.append(batch.getFlowFile(), new OutputStreamCallback() {
                    @Override
                    public void process(final OutputStream out) throws IOException {
                        if (writeDemarcator) {
                            out.write(messageDemarcatorBytes);
                        }

                        out.write(rawMessage);
                    }
                });

                // update the FlowFile reference in the batch object
                batch.setFlowFile(appendedFlowFile);

            } catch (final Exception e) {
                getLogger().error("Failed to write contents of the message to FlowFile due to {}; will re-queue message and try again",
                        new Object[] {e.getMessage()}, e);
                errorEvents.offer(event);
                break;
            }
        }

        return batches;
    }

    /**
     * @param event an event that was pulled off the queue
     *
     * @return a key to use for batching events together, by default this uses the sender of the
     *              event, but sub-classes should override this to batch by something else
     */
    protected String getBatchKey(final E event) {
        return event.getSender();
    }

    /**
     * Wrapper to hold a FlowFile and the events that have been appended to it.
     */
    protected final class FlowFileEventBatch {

        private FlowFile flowFile;
        private List<E> events;

        public FlowFileEventBatch(final FlowFile flowFile, final List<E> events) {
            this.flowFile = flowFile;
            this.events = events;
        }

        public FlowFile getFlowFile() {
            return flowFile;
        }

        public List<E> getEvents() {
            return events;
        }

        public void setFlowFile(FlowFile flowFile) {
            this.flowFile = flowFile;
        }
    }

}
