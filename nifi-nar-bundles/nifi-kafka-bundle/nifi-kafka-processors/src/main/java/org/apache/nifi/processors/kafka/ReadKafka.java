package org.apache.nifi.processors.kafka;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.TriggerWhenAnyDestinationAvailable;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

@SupportsBatching
@TriggerSerially
@TriggerWhenAnyDestinationAvailable
@CapabilityDescription("Fetches messages from Apache Kafka with fine grain control over the Kafka consumer configuration")
@Tags({"Kafka", "Apache", "Get", "Ingest", "Ingress", "Topic", "PubSub"})
@WritesAttributes({ @WritesAttribute(attribute = "kafka.topic", description = "The name of the Kafka Topic from which the message was received."),
		@WritesAttribute(attribute = "kafka.partition", description = "The partition of the Kafka Topic from which the message was received."),
		@WritesAttribute(attribute = "kafka.offset", description = "The offset of the message within the Kafka partition.") })
public class ReadKafka  extends AbstractSessionFactoryProcessor {

	private final AtomicReference<ProcessSessionFactory> sessionFactoryReference = new AtomicReference<>();
	private ProcessSession session;

	/**
	 * Property Descriptors
	 */
	public static final PropertyDescriptor ZOOKEEPER_CONNECTION_STRING = new PropertyDescriptor.Builder()
		.name("ZooKeeper Connection String")
		.description("The Connection String to use in order to connect to ZooKeeper. This is often a comma-separated list of <host>:<port> combinations. For example, host1:2181,host2:2181,host3:2188")
		.required(true)
		.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
		.expressionLanguageSupported(true)
		.build();
	public static final PropertyDescriptor ZOOKEEPER_COMMIT_DELAY = new PropertyDescriptor.Builder()
		.name("Zookeeper Commit Frequency")
		.description("Specifies how often to communicate with ZooKeeper to indicate which messages have been pulled. A longer time period will result in better overall performance but can result in more data duplication if a NiFi node is lost")
		.required(true)
		.addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
		.defaultValue("30 secs")
		.build();
	public static final PropertyDescriptor GROUP_ID = new PropertyDescriptor.Builder()
		.name("Consumer Group ID")
		.description("Consumer Group ID to use when communicating with Kafka")
		.required(false)
		.expressionLanguageSupported(true)
		.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
		.build();
	public static final PropertyDescriptor CONCURRENT_STREAMS = new PropertyDescriptor.Builder()
		.name("Concurrent Message Streams")
		.description("The number of concurrent threads to consume messages from Kafka. Use with Consumer Group ID.")
		.required(true)
		.addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
		.defaultValue("1")
		.build();
	public static final PropertyDescriptor TOPIC = new PropertyDescriptor.Builder()
		.name("Topic Name")
		.description("The Kafka Topic to pull messages from")
		.required(true)
		.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
		.expressionLanguageSupported(true)
		.build();
	public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
		.name("Batch Size")
		.description("The number of Kafka messages to concatenate into the contents of each output FlowFile")
		.required(true)
		.addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
		.defaultValue("1")
		.build();
	public static final PropertyDescriptor DELIMITER = new PropertyDescriptor.Builder()
		.name("Delimiter")
		.description("When batching Kafka messages, use this value as the delimiter option in the FlowFile content")
		.required(false)
		.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
		.defaultValue("\n")
		.build();

	/**
	 * Relationships
	 */
	public static final Relationship REL_SUCCESS = new Relationship.Builder()
		.name("success")
		.description("All FlowFiles that are created are routed to this relationship")
		.build();

	private ConsumerConnector consumer;

	@Override
	protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		final List<PropertyDescriptor> props = new ArrayList<>();
		props.add(ZOOKEEPER_CONNECTION_STRING);
		props.add(TOPIC);
		props.add(ZOOKEEPER_COMMIT_DELAY);
		props.add(GROUP_ID);
		props.add(CONCURRENT_STREAMS);
		props.add(BATCH_SIZE);
		props.add(DELIMITER);
		return props;
	}

	@Override
	protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
		return new PropertyDescriptor.Builder().name(propertyDescriptorName).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).dynamic(true).build();
	}

	@Override
	public Set<Relationship> getRelationships() {
		final Set<Relationship> relationships = new HashSet<>(1);
		relationships.add(REL_SUCCESS);
		return relationships;
	}

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {

		ProcessorLog log = this.getLogger();

		sessionFactoryReference.compareAndSet(null, sessionFactory);

		if (session == null) {
			// make sure the previous consumer is shutdown
			shutdownConsumer();

			log.debug("ReadKafka Session found to be null, so creating new one");
			session = sessionFactoryReference.get().createSession();
			startConsuming(context);
		}

	}

	protected void startConsuming(final ProcessContext context) {
		final ProcessorLog log = this.getLogger();

		final Properties props = new Properties();
		props.setProperty("zookeeper.connect", context.getProperty(ZOOKEEPER_CONNECTION_STRING).evaluateAttributeExpressions().getValue()); 
		props.setProperty("group.id", context.getProperty(GROUP_ID).evaluateAttributeExpressions().getValue());
		props.setProperty("auto.commit.interval.ms", String.valueOf(context.getProperty(ZOOKEEPER_COMMIT_DELAY).asTimePeriod(TimeUnit.MILLISECONDS)));

		// Defaults if not provided by the user
		props.setProperty("auto.commit.enable", "true");
		props.setProperty("auto.offset.reset", "smallest");

		final Integer batchSize = context.getProperty(BATCH_SIZE).asInteger();

		final byte[] delimiterBytes = getDelimiterBytes(context);

		/*
		 * Load any dynamic properties that the user entered as support Kafka Consumer properties
		 */
		setDynamicProperties(context, props);

		final ConsumerConfig consumerConfig = new ConsumerConfig(props);
		consumer = Consumer.createJavaConsumerConnector(consumerConfig);

		final String topic = context.getProperty(TOPIC).evaluateAttributeExpressions().getValue();

		final Map<String, Integer> topicCountMap = new HashMap<>(1);
		topicCountMap.put(topic, context.getProperty(CONCURRENT_STREAMS).asInteger());

		final Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		final List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

		log.debug("Creating {} Kafka Streams", new Object[] {streams.size()});

		ExecutorService pool = Executors.newFixedThreadPool(streams.size());
		Collection<Future<?>> futures = new ArrayList<Future<?>>(streams.size());

		for(final KafkaStream<byte[], byte[]> stream : streams) {
			Future<?> future = pool.submit( new Runnable() {
				@Override
				public void run() {
					ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
					log.debug("Stream {} waiting for next...", new Object[] {stream.clientId()});

					// initialize variables
					int messageCount = 0;
					FlowFile ff = session.create();

					Object[] clientId = new Object[] {stream.clientId()};
					log.debug("Created FlowFile from Stream {}", clientId);

					while(iterator.hasNext()) {
						log.debug("Stream {} found next!", clientId);
						MessageAndMetadata<byte[], byte[]> mm = iterator.next();
						log.debug("Got next for stream {}", clientId);

						if(batchSize == 1) {
							ff = setAttributesFromMessage(mm, ff);
						}

						final byte[] message = mm.message();

						// Check for debug enabled before logging potentially heavy string from byte[]
						if(log.isDebugEnabled()) {
							log.debug("Found message for Stream {}: {}", new Object[] {stream.clientId(), new String(message)});
						}

						final boolean firstMessage = (messageCount == 0);
						ff = addMessageToFlowFile(ff, message, firstMessage, delimiterBytes);
						messageCount++;

						// Are we ready to send the batch?
						if(messageCount >= batchSize) {
							// Transfer the file to the relationship
							session.transfer(ff, REL_SUCCESS);
							session.commit();
							log.debug("Committed FF for Stream {}", clientId);

							// re-initialize variables for the next batch
							ff = session.create();
							messageCount = 0;
						}
					}
				}

			} );
			futures.add( future );
		}

		for(Future<?> f : futures) {
			try {
				f.get(); // waits for completion of all tasks
			} catch (InterruptedException | ExecutionException e) {
				log.info("Stopped Kafka Consumer. This is expected when the processor is stopped.", e);
			}
		}

		pool.shutdown();

	}

	/**
	 * <p>Replace common special characters (e.g., new lines and tabs) that
	 * would be written-out by the user with their actual values</p>
	 * 
	 * @param context
	 * @return
	 */
	protected byte[] getDelimiterBytes(final ProcessContext context) {
		String delimiterRaw = context.getProperty(DELIMITER).getValue();
		if(delimiterRaw != null) {
			final String delimiter = context.getProperty(DELIMITER).getValue()
					.replace("\\n", "\n")
					.replace("\\r", "\r")
					.replace("\\t", "\t");
	        final byte[] delimiterBytes = delimiter.getBytes(StandardCharsets.UTF_8);
			return delimiterBytes;
		}
		else {
			return new byte[0];
		}
	}

	/**
	 * <p>Load any dynamic properties that the user entered as support Kafka Consumer properties.</p>
	 * <p>These dynamic properties are expected to be valid Kafka Consumer properties exposed to
	 * the user for ease of configuration.</p>
	 * @param context
	 * @param props
	 */
	protected void setDynamicProperties(final ProcessContext context, final Properties props) {

		for (Map.Entry<PropertyDescriptor, String> prop : context.getProperties().entrySet()) {

			PropertyDescriptor property = prop.getKey();
			if (!property.isDynamic()) {
				getLogger().debug("Skipping non-dynamic property {}", new Object[]{property});
				continue;
			}

			PropertyValue propertyValue = context.getProperty(property);
			String propertyName = property.getName();
			if (!propertyName.isEmpty() && propertyValue.isSet()) {
				String value = propertyValue.getValue();

				getLogger()
					.debug("Found and setting dynamic property {} with value {}",
						new Object[]{propertyName, value});

				props.setProperty(propertyName, value);
			}
		}
	}

	/**
	 * 
	 * @param ff
	 * @param message
	 * @param firstMessage
	 * @param delimiterBytes
	 * @return
	 */
	protected FlowFile addMessageToFlowFile(final FlowFile ff,
			final byte[] message, final boolean firstMessage,
			final byte[] delimiterBytes) {

		return session.append(ff, new OutputStreamCallback() {
			@Override
			public void process(OutputStream out) throws IOException {
				if(!firstMessage) {
					out.write(delimiterBytes);
				}
				out.write(message);
			}
		});
	}

	protected FlowFile setAttributesFromMessage(final MessageAndMetadata<byte[], byte[]> mm, final FlowFile ff) {
		Map<String, String> attributes = new HashMap<>();
		attributes.put("kafka.offset", String.valueOf(mm.offset()));
		attributes.put("kafka.partition", String.valueOf(mm.partition()));
		attributes.put("kafka.topic", mm.topic());
		return session.putAllAttributes(ff, attributes);
	}

	@OnUnscheduled
	@OnStopped
	public void shutdownConsumer() {

		// rollback any pending flowfiles without penalizing the processor
		if (session != null) {
			session.rollback();
		}
		session = null;

		if (consumer != null) {
			consumer.shutdown(); // auto commit is enabled, so shutdown will handle committing offsets
		}
	}
}
