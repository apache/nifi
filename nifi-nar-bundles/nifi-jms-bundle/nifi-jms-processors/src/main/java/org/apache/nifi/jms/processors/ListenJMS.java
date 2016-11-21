package org.apache.nifi.jms.processors;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.listener.DefaultMessageListenerContainer;


@TriggerSerially
@Tags({ "jms", "get", "message", "receive", "consume" })
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Listens and consumes JMS Message of type BytesMessage or TextMessage transforming their content to "
        + "a FlowFile and transitioning them to 'success' relationship. JMS attributes such as headers and properties are copied as FlowFile attributes. "
        + "This processor starts embedded JMS Broker (using Apache ActiveMQ) and MessageListener that will listen on the P2P (i.e., QUEUE) destination"
        + " named 'queue://dest-[PORT]' (e.g., 'queue://dest-61616'), so any messages that are sent by the external producer will be consumed by this processor.")
public class ListenJMS extends AbstractSessionFactoryProcessor {

    static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("port")
            .displayName("Port")
            .description("Port for the Message Broker to listen on. Message Broker will bind to 0.0.0.0 IP address of the machine this processor wil run on.")
            .defaultValue("61616")
            .required(true)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    static final PropertyDescriptor CONCURRENT_CONSUMERS = new PropertyDescriptor.Builder()
            .name("concurrent.consumers")
            .displayName("Concurrent Consumers")
            .description("Specifies the number of concurrent consumers to create. Default is 1. If specified value is > 1, then the ordering of messages will be affected.")
            .defaultValue("1")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All received messages that are successfully written as FlowFiles are routed to this relationship")
            .build();

    private final static List<PropertyDescriptor> descriptors;

    private final static Set<Relationship> relationships;

    static {
        descriptors = new ArrayList<>();
        descriptors.add(PORT);
        descriptors.add(CONCURRENT_CONSUMERS);

        relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
    }

    private volatile boolean started;

    private volatile BrokerService broker;

    private volatile CachingConnectionFactory connectionFactory;

    private volatile DefaultMessageListenerContainer messageListenerContainer;

    private volatile ProcessSessionFactory sessionFactory;

    private volatile String url;


    @Override
    public void onTrigger(ProcessContext processContext, ProcessSessionFactory sessionFactory) throws ProcessException {
        if (this.sessionFactory == null) {
            this.sessionFactory = sessionFactory;
        }

        if (!this.started) {
            this.start(processContext);
        }

        processContext.yield();
    }

    /**
     *
     */
    @Override
    public Set<Relationship> getRelationships() {
        return Collections.unmodifiableSet(relationships);
    }

    /**
     *
     */
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.unmodifiableList(descriptors);
    }

    /**
     *
     */
    @OnScheduled
    public void initialize(ProcessContext processContext) throws Exception {
        int port = processContext.getProperty(PORT).asInteger();
        this.broker = new BrokerService();
        // TODO see if there is a need to expose property to bind to a specific
        // IP address.
        this.url = "tcp://0.0.0.0:" + port;
        broker.addConnector(this.url);

        this.connectionFactory = new CachingConnectionFactory(new ActiveMQConnectionFactory(this.url));
        this.connectionFactory.afterPropertiesSet();

        this.messageListenerContainer = new DefaultMessageListenerContainer();
        this.messageListenerContainer.setConnectionFactory(connectionFactory);
        this.messageListenerContainer.setSessionAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);

        int concurrentConsumers = processContext.getProperty(CONCURRENT_CONSUMERS).asInteger();
        this.messageListenerContainer.setConcurrentConsumers(concurrentConsumers);
        this.messageListenerContainer.setDestinationName("queue://dest-" + port);
        this.messageListenerContainer.setMessageListener(new InProcessMessageListener());
        this.messageListenerContainer.afterPropertiesSet();
    }

    /**
     *
     */
    private void start(ProcessContext processContext) {
        try {
            this.broker.start();
        } catch (Exception e) {
            throw new ProcessException("Failed to start Message Broker.", e);
        }
        this.messageListenerContainer.start();
    }

    /**
     * Will stop the Message Broker so no new messages are accepted
     */
    @OnStopped
    public void stop() {
        if (this.started) {
            try {
                this.broker.stop();
            } catch (Exception e) {
                this.getLogger().error("Failed to stop Message Broker", e);
            }
            this.messageListenerContainer.stop();
            this.connectionFactory.destroy();
        }
        this.started = false;
    }

    /**
     * Implementation of the {@link MessageListener} to de-queue messages from
     * the target P2P {@link Destination}.
     */
    private class InProcessMessageListener implements MessageListener {

        /**
         * Given that we set ACK mode as CLIENT, the message will be
         * auto-acknowledged by the MessageListenerContainer upon successful
         * execution of onMessage() operation, otherwise
         * MessageListenerContainer will issue session.recover() call to allow
         * message to remain on the queue.
         */
        @Override
        public void onMessage(Message message) {
            byte[] mBody = null;
            if (message instanceof TextMessage) {
                mBody = MessageBodyToBytesConverter.toBytes((TextMessage) message);
            } else if (message instanceof BytesMessage) {
                mBody = MessageBodyToBytesConverter.toBytes((BytesMessage) message);
            } else {
                throw new IllegalStateException("Message type other then TextMessage and BytesMessage are not supported.");
            }
            final byte[] messageBody = mBody;
            ProcessSession processSession = ListenJMS.this.sessionFactory.createSession();

            FlowFile flowFile = processSession.create();
            flowFile = processSession.write(flowFile, new OutputStreamCallback() {
                @Override
                public void process(OutputStream out) throws IOException {
                    out.write(messageBody);
                }
            });
            // add attributes (headers and properties) and properties to FF
            Map<String, Object> messageHeaders = JMSUtils.extractMessageHeaders(message);
            flowFile = ListenJMS.this.updateFlowFileAttributesWithJMSAttributes(messageHeaders, flowFile, processSession);
            Map<String, String> messageProperties = JMSUtils.extractMessageProperties(message);
            flowFile = ListenJMS.this.updateFlowFileAttributesWithJMSAttributes(messageProperties, flowFile, processSession);

            processSession.transfer(flowFile, REL_SUCCESS);
            processSession.getProvenanceReporter().receive(flowFile, ListenJMS.this.url);

            processSession.commit();
            // upon reaching the above line, the MessageListenerContainer will
            // issue session.commit() given the CLIENT ACK setting
        }
    }

    /**
     * Copies JMS attributes (i.e., headers and properties) as FF attributes.
     * Given that FF attributes mandate that values are of type String, the
     * copied values of JMS attributes will be stringified via
     * String.valueOf(attribute).
     */
    private FlowFile updateFlowFileAttributesWithJMSAttributes(Map<String, ? extends Object> jmsAttributes, FlowFile flowFile, ProcessSession processSession) {
        Map<String, String> attributes = new HashMap<String, String>();
        for (Entry<String, ? extends Object> entry : jmsAttributes.entrySet()) {
            attributes.put(entry.getKey(), String.valueOf(entry.getValue()));
        }
        flowFile = processSession.putAllAttributes(flowFile, attributes);
        return flowFile;
    }
}
