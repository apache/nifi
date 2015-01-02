package org.apache.nifi.processors.kafka;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.annotation.CapabilityDescription;
import org.apache.nifi.processor.annotation.OnScheduled;
import org.apache.nifi.processor.annotation.OnStopped;
import org.apache.nifi.processor.annotation.OnUnscheduled;
import org.apache.nifi.processor.annotation.SupportsBatching;
import org.apache.nifi.processor.annotation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

@SupportsBatching
@CapabilityDescription("Fetches messages from Apache Kafka")
@Tags({"Kafka", "Apache", "Get", "Ingest", "Ingress", "Topic", "PubSub"})
public class GetKafka extends AbstractProcessor {
    public static final PropertyDescriptor ZOOKEEPER_CONNECTION_STRING = new PropertyDescriptor.Builder()
        .name("ZooKeeper Connection String")
        .description("The Connection String to use in order to connect to ZooKeeper. This is often a comma-separated list of <host>:<port> combinations. For example, host1:2181,host2:2181,host3:2188")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(false)
        .build();
    public static final PropertyDescriptor TOPIC = new PropertyDescriptor.Builder()
        .name("Topic Name")
        .description("The Kafka Topic to pull messages from")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(false)
        .build();
    public static final PropertyDescriptor ZOOKEEPER_COMMIT_DELAY = new PropertyDescriptor.Builder()
		.name("Zookeeper Commit Frequency")
		.description("Specifies how often to communicate with ZooKeeper to indicate which messages have been pulled. A longer time period will result in better overall performance but can result in more data duplication if a NiFi node is lost")
		.required(true)
		.addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
		.expressionLanguageSupported(false)
		.defaultValue("60 secs")
		.build();
    public static final PropertyDescriptor ZOOKEEPER_TIMEOUT = new PropertyDescriptor.Builder()
	    .name("ZooKeeper Communications Timeout")
	    .description("The amount of time to wait for a response from ZooKeeper before determining that there is a communications error")
	    .required(true)
	    .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
	    .expressionLanguageSupported(false)
	    .defaultValue("30 secs")
	    .build();
    public static final PropertyDescriptor KAFKA_TIMEOUT = new PropertyDescriptor.Builder()
	    .name("Kafka Communications Timeout")
	    .description("The amount of time to wait for a response from Kafka before determining that there is a communications error")
	    .required(true)
	    .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
	    .expressionLanguageSupported(false)
	    .defaultValue("30 secs")
	    .build();
    public static final PropertyDescriptor CLIENT_NAME = new PropertyDescriptor.Builder()
        .name("Client Name")
        .description("Client Name to use when communicating with Kafka")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(false)
        .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
	    .name("success")
	    .description("All FlowFiles that are created are routed to this relationship")
	    .build();

    
    private final BlockingQueue<ConsumerIterator<byte[], byte[]>> streamIterators = new LinkedBlockingQueue<>();
    private volatile ConsumerConnector consumer;

    final Lock interruptionLock = new ReentrantLock();
    // guarded by interruptionLock
    private final Set<Thread> interruptableThreads = new HashSet<>();
    
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    	final PropertyDescriptor clientNameWithDefault = new PropertyDescriptor.Builder()
    		.fromPropertyDescriptor(CLIENT_NAME)
    		.defaultValue("NiFi-" + getIdentifier())
    		.build();
    	
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(ZOOKEEPER_CONNECTION_STRING);
        props.add(TOPIC);
        props.add(ZOOKEEPER_COMMIT_DELAY);
        props.add(clientNameWithDefault);
        props.add(KAFKA_TIMEOUT);
        props.add(ZOOKEEPER_TIMEOUT);
        return props;
    }
    
    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>(1);
        relationships.add(REL_SUCCESS);
        return relationships;
    }
    
    @OnScheduled
    public void createConsumers(final ProcessContext context) {
    	final String topic = context.getProperty(TOPIC).getValue();
    	
    	final Map<String, Integer> topicCountMap = new HashMap<>(1);
    	topicCountMap.put(topic, context.getMaxConcurrentTasks());
    	
    	final Properties props = new Properties();
    	props.setProperty("zookeeper.connect", context.getProperty(ZOOKEEPER_CONNECTION_STRING).getValue()); 
    	props.setProperty("group.id", getIdentifier());
    	props.setProperty("auto.commit.interval.ms", String.valueOf(context.getProperty(ZOOKEEPER_COMMIT_DELAY).asTimePeriod(TimeUnit.MILLISECONDS)));
    	props.setProperty("auto.commit.enable", "true"); // just be explicit
    	props.setProperty("auto.offset.reset", "smallest");
    	
    	final ConsumerConfig consumerConfig = new ConsumerConfig(props);
    	consumer = Consumer.createJavaConsumerConnector(consumerConfig);
    	
    	final Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
    	final List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
    	
    	this.streamIterators.clear();
    	
    	for ( final KafkaStream<byte[], byte[]> stream : streams ) {
    		streamIterators.add(stream.iterator());
    	}
    }
    
    @OnStopped
    public void shutdownConsumer() {
    	if ( consumer != null ) {
    		try {
    			consumer.commitOffsets();
    		} finally {
    			consumer.shutdown();
    		}
    	}
    }
    
    @OnUnscheduled
    public void interruptIterators() {
    	// Kafka doesn't provide a non-blocking API for pulling messages. We can, however,
    	// interrupt the Threads. We do this when the Processor is stopped so that we have the
    	// ability to shutdown the Processor.
    	interruptionLock.lock();
    	try {
    		for ( final Thread t : interruptableThreads ) {
    			t.interrupt();
    		}
    		
    		interruptableThreads.clear();
    	} finally {
    		interruptionLock.unlock();
    	}
    }
    
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    	ConsumerIterator<byte[], byte[]> iterator = streamIterators.poll();
    	if ( iterator == null ) {
    		return;
    	}
    	
    	FlowFile flowFile = null;
    	try {
    		interruptionLock.lock();
    		try {
    			interruptableThreads.add(Thread.currentThread());
    		} finally {
    			interruptionLock.unlock();
    		}
    		
    		try {
	    		if (!iterator.hasNext() ) {
	    			return;
	    		}
    		} catch (final Exception e) {
    			getLogger().warn("Failed to invoke hasNext() due to ", new Object[] {e});
    			iterator = null;
    			return;
    		}
    		
    		final long start = System.nanoTime();
    		final MessageAndMetadata<byte[], byte[]> mam = iterator.next();
    		
    		if ( mam == null ) {
    			return;
    		}
    		
    		final byte[] key = mam.key();
    		
    		final Map<String, String> attributes = new HashMap<>();
    		if ( key != null ) {
    			attributes.put("kafka.key", new String(key, StandardCharsets.UTF_8));
    		}
    		attributes.put("kafka.offset", String.valueOf(mam.offset()));
    		attributes.put("kafka.partition", String.valueOf(mam.partition()));
    		attributes.put("kafka.topic", mam.topic());
    		
    		flowFile = session.create();
    		flowFile = session.write(flowFile, new OutputStreamCallback() {
				@Override
				public void process(final OutputStream out) throws IOException {
					out.write(mam.message());
				}
    		});
    		
    		flowFile = session.putAllAttributes(flowFile, attributes);
    		final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    		session.getProvenanceReporter().receive(flowFile, "kafka://" + mam.topic() + "/partitions/" + mam.partition() + "/offsets/" + mam.offset(), millis);
    		getLogger().info("Successfully received {} from Kafka in {} millis", new Object[] {flowFile, millis});
    		session.transfer(flowFile, REL_SUCCESS);
    	} catch (final Exception e) {
    		getLogger().error("Failed to receive FlowFile from Kafka due to {}", new Object[] {e});
    		if ( flowFile != null ) {
    			session.remove(flowFile);
    		}
    	} finally {
    		interruptionLock.lock();
    		try {
    			interruptableThreads.remove(Thread.currentThread());
    		} finally {
    			interruptionLock.unlock();
    		}
    		
    		if ( iterator != null ) {
    			streamIterators.offer(iterator);
    		}
    	}
    }
	
}
