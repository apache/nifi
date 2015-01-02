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
package org.apache.nifi.processors.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.annotation.CapabilityDescription;
import org.apache.nifi.processor.annotation.OnStopped;
import org.apache.nifi.processor.annotation.SupportsBatching;
import org.apache.nifi.processor.annotation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

@SupportsBatching
@Tags({"Apache", "Kafka", "Put", "Send", "Message", "PubSub"})
@CapabilityDescription("Sends the contents of a FlowFile as a message to Apache Kafka")
public class PutKafka extends AbstractProcessor {
    private static final String SINGLE_BROKER_REGEX = ".*?\\:\\d{3,5}";
    private static final String BROKER_REGEX = SINGLE_BROKER_REGEX + "(?:,\\s*" + SINGLE_BROKER_REGEX + ")*";
    
    public static final AllowableValue DELIVERY_REPLICATED = new AllowableValue("-1", "Guarantee Replicated Delivery", "FlowFile will be routed to failure unless the message is replicated to the appropriate number of Kafka Nodes according to the Topic configuration");
    public static final AllowableValue DELIVERY_ONE_NODE = new AllowableValue("1", "Guarantee Single Node Delivery", "FlowFile will be routed to success if the message is received by a single Kafka node, whether or not it is replicated. This is faster than <Guarantee Replicated Delivery> but can result in data loss if a Kafka node crashes");
    public static final AllowableValue DELIVERY_BEST_EFFORT = new AllowableValue("0", "Best Effort", "FlowFile will be routed to success after successfully writing the content to a Kafka node, without waiting for a response. This provides the best performance but may result in data loss.");
    
    public static final PropertyDescriptor SEED_BROKERS = new PropertyDescriptor.Builder()
        .name("Known Brokers")
        .description("A comma-separated list of known Kafka Brokers in the format <host>:<port>")
        .required(true)
        .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile(BROKER_REGEX)))
        .expressionLanguageSupported(false)
        .build();
    public static final PropertyDescriptor TOPIC = new PropertyDescriptor.Builder()
	    .name("Topic Name")
	    .description("The Kafka Topic of interest")
	    .required(true)
	    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
	    .expressionLanguageSupported(true)
	    .build();
    public static final PropertyDescriptor KEY = new PropertyDescriptor.Builder()
		.name("Kafka Key")
		.description("The Key to use for the Message")
		.required(false)
		.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
		.expressionLanguageSupported(true)
		.build();
    public static final PropertyDescriptor DELIVERY_GUARANTEE = new PropertyDescriptor.Builder()
		.name("Delivery Guarantee")
		.description("Specifies the requirement for guaranteeing that a message is sent to Kafka")
		.required(true)
		.expressionLanguageSupported(false)
		.allowableValues(DELIVERY_BEST_EFFORT, DELIVERY_ONE_NODE, DELIVERY_REPLICATED)
		.defaultValue(DELIVERY_BEST_EFFORT.getValue())
		.build();
    public static final PropertyDescriptor TIMEOUT = new PropertyDescriptor.Builder()
	    .name("Communications Timeout")
	    .description("The amount of time to wait for a response from Kafka before determining that there is a communications error")
	    .required(true)
	    .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
	    .expressionLanguageSupported(false)
	    .defaultValue("30 secs")
	    .build();
    public static final PropertyDescriptor MAX_FLOWFILE_SIZE = new PropertyDescriptor.Builder()
		.name("Max FlowFile Size")
		.description("Specifies the amount of data that can be buffered to send to Kafka. If the size of a FlowFile is larger than this, that FlowFile will be routed to 'reject'. This helps to prevent the system from running out of memory")
		.required(true)
		.addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
		.expressionLanguageSupported(false)
		.defaultValue("1 MB")
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
	    .description("Any FlowFile that is successfully sent to Kafka will be routed to this Relationship")
	    .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
	    .name("failure")
	    .description("Any FlowFile that cannot be sent to Kafka will be routed to this Relationship")
	    .build();
    public static final Relationship REL_REJECT = new Relationship.Builder()
	    .name("reject")
	    .description("Any FlowFile whose size exceeds the <Max FlowFile Size> property will be routed to this Relationship")
	    .build();

    private final BlockingQueue<Producer<byte[], byte[]>> producers = new LinkedBlockingQueue<>();
    
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    	final PropertyDescriptor clientName = new PropertyDescriptor.Builder()
    		.fromPropertyDescriptor(CLIENT_NAME)
    		.defaultValue("NiFi-" + getIdentifier())
    		.build();
    	
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(SEED_BROKERS);
        props.add(TOPIC);
        props.add(KEY);
        props.add(DELIVERY_GUARANTEE);
        props.add(TIMEOUT);
        props.add(MAX_FLOWFILE_SIZE);
        props.add(clientName);
        return props;
    }
    
    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>(1);
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.add(REL_REJECT);
        return relationships;
    }
    
    
    @OnStopped
    public void closeProducers() {
    	Producer<byte[], byte[]> producer;
    	
    	while ((producer = producers.poll()) != null) {
    		producer.close();
    	}
    }
    
    
    private Producer<byte[], byte[]> createProducer(final ProcessContext context) {
    	final String brokers = context.getProperty(SEED_BROKERS).getValue();

    	final Properties properties = new Properties();
        properties.setProperty("metadata.broker.list", brokers);
        properties.setProperty("request.required.acks", context.getProperty(DELIVERY_GUARANTEE).getValue());
        properties.setProperty("client.id", context.getProperty(CLIENT_NAME).getValue());
        properties.setProperty("request.timeout.ms", String.valueOf(context.getProperty(TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).longValue()));
        
        properties.setProperty("message.send.max.retries", "1");
        properties.setProperty("producer.type", "sync");
        
        final ProducerConfig config = new ProducerConfig(properties);
        return new Producer<>(config);
    }
    
    private Producer<byte[], byte[]> borrowProducer(final ProcessContext context) {
    	Producer<byte[], byte[]> producer = producers.poll();
    	return producer == null ? createProducer(context) : producer;
    }
    
    private void returnProducer(final Producer<byte[], byte[]> producer) {
    	producers.offer(producer);
    }
    
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    	FlowFile flowFile = session.get();
    	if ( flowFile == null ) {
    		return;
    	}
    	
    	final long start = System.nanoTime();
    	final long maxSize = context.getProperty(MAX_FLOWFILE_SIZE).asDataSize(DataUnit.B).longValue();
    	if ( flowFile.getSize() > maxSize ) {
    		getLogger().info("Routing {} to 'reject' because its size exceeds the configured maximum allowed size", new Object[] {flowFile});
    		session.getProvenanceReporter().route(flowFile, REL_REJECT, "FlowFile is larger than " + maxSize);
    		session.transfer(flowFile, REL_REJECT);
    		return;
    	}
    	
        final String topic = context.getProperty(TOPIC).evaluateAttributeExpressions(flowFile).getValue();
        final String key = context.getProperty(KEY).evaluateAttributeExpressions(flowFile).getValue();
        
        final byte[] value = new byte[(int) flowFile.getSize()];
        session.read(flowFile, new InputStreamCallback() {
			@Override
			public void process(final InputStream in) throws IOException {
				StreamUtils.fillBuffer(in, value);
			}
        });
        
        final Producer<byte[], byte[]> producer = borrowProducer(context);
        boolean error = false;
        try {
        	final KeyedMessage<byte[], byte[]> message;
        	if ( key == null ) {
        		message = new KeyedMessage<>(topic, value);
        	} else {
        		message = new KeyedMessage<>(topic, key.getBytes(StandardCharsets.UTF_8), value);
        	}
        	
        	producer.send(message);
        	final long nanos = System.nanoTime() - start;
        	
        	session.getProvenanceReporter().send(flowFile, "kafka://" + topic);
        	session.transfer(flowFile, REL_SUCCESS);
        	getLogger().info("Successfully sent {} to Kafka in {} millis", new Object[] {flowFile, TimeUnit.NANOSECONDS.toMillis(nanos)});
        } catch (final Exception e) {
        	getLogger().error("Failed to send {} to Kafka due to {}; routing to failure", new Object[] {flowFile, e});
        	session.transfer(flowFile, REL_FAILURE);
        	error = true;
        } finally {
        	if ( error ) {
        		producer.close();
        	} else {
        		returnProducer(producer);
        	}
        }
    }

}
