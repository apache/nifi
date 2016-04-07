package org.apache.nifi.processors.kafka.pubsub;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.FormatUtils;

abstract class AbstractKafkaProcessor<T extends Closeable> extends AbstractSessionFactoryProcessor {

    private static final String SINGLE_BROKER_REGEX = ".*?\\:\\d{3,5}";

    private static final String BROKER_REGEX = SINGLE_BROKER_REGEX + "(?:,\\s*" + SINGLE_BROKER_REGEX + ")*";


    static final AllowableValue SEC_PLAINTEXT = new AllowableValue("PLAINTEXT", "PLAINTEXT", "PLAINTEXT");
    static final AllowableValue SEC_SSL = new AllowableValue("SSL", "SSL", "SSL");
    static final AllowableValue SEC_SASL_PLAINTEXT = new AllowableValue("SASL_PLAINTEXT", "SASL_PLAINTEXT", "SASL_PLAINTEXT");
    static final AllowableValue SEC_SASL_SSL = new AllowableValue("SASL_SSL", "SASL_SSL", "SASL_SSL");

    static final PropertyDescriptor BOOTSTRAP_SERVERS = new PropertyDescriptor.Builder()
            .name(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)
            .displayName("Kafka Brokers")
            .description("A comma-separated list of known Kafka Brokers in the format <host>:<port>")
            .required(true)
            .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile(BROKER_REGEX)))
            .expressionLanguageSupported(false)
            .defaultValue("localhost:9092")
            .build();
    static final PropertyDescriptor CLIENT_ID = new PropertyDescriptor.Builder()
            .name(ProducerConfig.CLIENT_ID_CONFIG)
            .displayName("Client ID")
            .description("String value uniquely identiofying this client application. Correspons to Kafka's 'client.id' property.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();
    static final PropertyDescriptor SECURITY_PROTOCOL = new PropertyDescriptor.Builder()
            .name("security.protocol")
            .displayName("Security Protocol")
            .description("Protocol used to communicate with brokers. Corresponds to Kafka's 'security.protocol' property.")
            .required(false)
            .expressionLanguageSupported(false)
            .allowableValues(SEC_PLAINTEXT, SEC_SSL, SEC_SASL_PLAINTEXT, SEC_SASL_SSL)
            .defaultValue(SEC_PLAINTEXT.getValue())
            .build();
    static final PropertyDescriptor KERBEROS_PRINCIPLE = new PropertyDescriptor.Builder()
            .name("sasl.kerberos.service.name")
            .displayName("Kerberos Service Name")
            .description("The Kerberos principal name that Kafka runs as. This can be defined either in Kafka's JAAS config or in Kafka's config. "
                    + "Corresponds to Kafka's 'security.protocol' property."
                    + "It is ignored unless one of the SASL options of the <Security Protocol> are selected.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();

    static final Builder TOPIC_BUILDER = new PropertyDescriptor.Builder()
            .name("topic")
            .displayName("Topic Name")
            .description("The name of the Kafka Topic")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR);

    static final Builder MESSAGE_DEMARCATOR_BUILDER = new PropertyDescriptor.Builder()
            .name("Message Demarcator")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR);

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are the are successfully sent to or received from Kafka are routed to this relationship")
            .build();

    static final List<PropertyDescriptor> sharedDescriptors = new ArrayList<>();

    static final Set<Relationship> sharedRelationships = new HashSet<>();

    private volatile boolean acceptTask = true;

    private final AtomicInteger taskCounter = new AtomicInteger();


    static {
        sharedDescriptors.add(BOOTSTRAP_SERVERS);
        sharedDescriptors.add(CLIENT_ID);
        sharedDescriptors.add(SECURITY_PROTOCOL);
        sharedDescriptors.add(KERBEROS_PRINCIPLE);
        sharedRelationships.add(REL_SUCCESS);
    }

    /**
     * Instance of {@link KafkaPublisher} or {@link KafkaConsumer}
     */
    volatile T kafkaResource;

    @Override
    public final void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory)
            throws ProcessException {
        final ProcessSession session = sessionFactory.createSession();
        if (this.acceptTask) {
            try {
                this.taskCounter.incrementAndGet();
                if (this.kafkaResource == null) {
                    synchronized (this) {
                        if (this.kafkaResource == null) {
                            this.kafkaResource = this.buildKafkaResource(context, session);
                        }
                    }
                }

                this.acceptTask = this.rendezvousWithKafka(context, session);
                session.commit();
            } catch (Throwable e) {
                this.acceptTask = false;
                this.getLogger().error("{} failed to process due to {}; rolling back session", new Object[] { this, e });
                session.rollback(true);
//                throw e;
            } finally {
                this.resetProcessorIfNecessary();
            }
        } else {
            context.yield();
        }
    }
    /**
     * Resets the processor to initial state if necessary. The necessary state
     * means there are no active task which could only happen if currently
     * executing tasks are given a chance to finish while no new tasks are
     * accepted (see {@link #acceptTask}
     */
    private boolean resetProcessorIfNecessary() {
        boolean reset = this.taskCounter.decrementAndGet() == 0 && !this.acceptTask;
        if (reset) {
            this.close();
            this.acceptTask = true;
        }
        return reset;
    }

    /**
     * Will call {@link Closeable#close()} on the target resource after which
     * the target resource will be set to null
     *
     * @see KafkaPublisher
     * @see KafkaConsumer
     */
    @OnStopped
    public void close() {
        if (this.kafkaResource != null) {
            try {
                this.kafkaResource.close();
            } catch (Exception e) {
                this.getLogger().warn("Failed while closing " + this.kafkaResource, e);
            }
        }
        this.kafkaResource = null;
    }

    /**
     *
     */
    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .description("Specifies the value for '" + propertyDescriptorName + "' Kafka Configuration.")
                .name(propertyDescriptorName).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).dynamic(true)
                .build();
    }

    /**
     * This operation is called from
     * {@link #onTrigger(ProcessContext, ProcessSessionFactory)} method and
     * contains main processing logic for this Processor.
     */
    protected abstract boolean rendezvousWithKafka(ProcessContext context, ProcessSession session)
            throws ProcessException;

    /**
     * Builds target resource for interacting with Kafka. The target resource
     * could be one of {@link KafkaPublisher} or {@link KafkaConsumer}
     */
    protected abstract T buildKafkaResource(ProcessContext context, ProcessSession session) throws ProcessException;

    /**
     *
     */
    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {

        List<ValidationResult> results = new ArrayList<>();

        String securityProtocol = validationContext.getProperty(SECURITY_PROTOCOL).getValue();

        /*
         * validates that if one of SASL (Kerberos) option is selected for
         * security protocol, then Kerberos principal is provided as well
         */
        if (SEC_SASL_PLAINTEXT.getValue().equals(securityProtocol) || SEC_SASL_SSL.getValue().equals(securityProtocol)){
            String kerberosPrincipal = validationContext.getProperty(KERBEROS_PRINCIPLE).getValue();
            if (kerberosPrincipal == null || kerberosPrincipal.trim().length() == 0){
                results.add(new ValidationResult.Builder().subject(KERBEROS_PRINCIPLE.getDisplayName()).valid(false)
                        .explanation("The <" + KERBEROS_PRINCIPLE.getDisplayName() + "> property must be set when <"
                                + SECURITY_PROTOCOL.getDisplayName() + "> is configured as '"
                                + SEC_SASL_PLAINTEXT.getValue() + "' or '" + SEC_SASL_SSL.getValue() + "'.")
                        .build());
            }
        }

        return results;
    }

    /**
     * Builds Kafka {@link Properties}
     */
    Properties buildKafkaProperties(ProcessContext context) {
        Properties properties = new Properties();
        for (PropertyDescriptor propertyDescriptor : context.getProperties().keySet()) {
            if (!propertyDescriptor.isExpressionLanguageSupported()) {
                String pName = propertyDescriptor.getName();
                String pValue = context.getProperty(propertyDescriptor).getValue();
                if (pValue != null) {
                    if (pName.endsWith(".ms")) { // kafka standard time notation
                        pValue = String.valueOf(FormatUtils.getTimeDuration(pValue.trim(), TimeUnit.MILLISECONDS));
                    }
                    properties.setProperty(pName, pValue);
                }
            }
        }
        return properties;
    }
}
