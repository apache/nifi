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
package org.apache.nifi.processors.pulsar.pubsub;

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;

import static org.apache.nifi.processors.pulsar.RecordBasedConst.RECORD_READER;
import static org.apache.nifi.processors.pulsar.RecordBasedConst.RECORD_WRITER;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;

public class ConsumePulsarRecord_1_0 extends AbstractPulsarConsumerProcessor {

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
    		.name("Maximum Async Requests")
    		.description("The number of records to combine into a single flow file.")
    		.required(false)
    		.addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
    		.defaultValue("1000")
    		.build();
    
    // TODO Add a timeout property as well, so we don't wait based on BATCH_SIZE alone
    
    public static final Relationship REL_PARSE_FAILURE = new Relationship.Builder()
            .name("parse_failure")
            .description("FlowFiles for which the content was not prasable")
            .build();
	
    private static final List<PropertyDescriptor> PROPERTIES;
    private static final Set<Relationship> RELATIONSHIPS;

    static {
        final List<PropertyDescriptor> properties = new ArrayList<>();

        PROPERTIES = Collections.unmodifiableList(properties);
        properties.add(PULSAR_CLIENT_SERVICE);
        properties.add(RECORD_READER);
        properties.add(RECORD_WRITER);
        properties.add(TOPIC);
        properties.add(SUBSCRIPTION);
        properties.add(ASYNC_ENABLED);
        properties.add(MAX_ASYNC_REQUESTS);
        properties.add(ACK_TIMEOUT);
        properties.add(PRIORITY_LEVEL);
        properties.add(RECEIVER_QUEUE_SIZE);
        properties.add(SUBSCRIPTION_TYPE);
        properties.add(BATCH_SIZE);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.add(REL_PARSE_FAILURE);
        RELATIONSHIPS = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }


    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        
    	    final AtomicLong messagesReceived = new AtomicLong(0L);
    	    
    	    final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER)
                    .asControllerService(RecordReaderFactory.class);
    	    
    	    final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER)
                    .asControllerService(RecordSetWriterFactory.class);
    	
        try {
            if (context.getProperty(ASYNC_ENABLED).isSet() && context.getProperty(ASYNC_ENABLED).asBoolean()) {
                
            } else {
            	   List<Message> messages = consume(context, session);
            	   messagesReceived.addAndGet(messages.size());
            	   processMessages(context, session, messages, readerFactory, writerFactory);
            }
        } catch (PulsarClientException e) {
            getLogger().error("Unable to consume from Pulsar Topic ", e);
            context.yield();
            throw new ProcessException(e);
        }

    }

    /**
     * Pull messages off of the topic until we have reached BATCH_SIZE or BATCH_DURATION
     * whichever occurs first.
     * 
     * @param context
     * @param session
     * @return
     * @throws PulsarClientException
     */
	private List<Message> consume(ProcessContext context, ProcessSession session) throws PulsarClientException {
        Consumer consumer = getWrappedConsumer(context).getConsumer();
        
        List<Message> messages = new ArrayList<Message>();
        
        while (messages.size() < context.getProperty(BATCH_SIZE).asInteger()) {
            messages.add(consumer.receive());
        }
        
        return messages;
    }
	
	private void processMessages(ProcessContext context, ProcessSession session, List<Message> messages, RecordReaderFactory readerFactory, RecordSetWriterFactory writerFactory) throws PulsarClientException {
	    
	    if (messages.isEmpty()) 
	    	   return;
	    
	    final BiConsumer<Message, Exception> handleParseFailure = (msg, e) -> {
	    	    FlowFile failureFlowFile = session.create();
	    	    if (msg.getData() != null) {
	            failureFlowFile = session.write(failureFlowFile, out -> out.write(msg.getData()));
	        }
	    	    session.transfer(failureFlowFile, REL_PARSE_FAILURE);
	    };
	    
	    RecordSetWriter writer = null;
	    FlowFile flowFile = null;
	    OutputStream rawOut = null;
	    
	    for (Message msg: messages)  {
	       	RecordReader reader = getRecordReader(msg, readerFactory, handleParseFailure);
		    Record firstRecord = getFirstRecord(msg, reader, handleParseFailure);
		    
		    if (firstRecord == null) {
                // If the message doesn't contain any record, just ack the message
		        consumer.getConsumer().acknowledge(msg);
                continue;
            }
		    
		    // Session / FlowFile starts here
		    if (flowFile == null) {
		    	    flowFile = session.create();
                rawOut = session.write(flowFile);
		    }
		    
		    // Create the Record Writer
		    if (writer == null) {
		        try {
				    writer = getRecordWriter(writerFactory, firstRecord.getSchema(), rawOut);
				    writer.beginRecordSet();
			    } catch (SchemaNotFoundException | IOException ex) {
				    getLogger().error("Failed to obtain Schema for FlowFile.", ex);
                    throw new ProcessException(ex);
			    }
		    }
		   
		    // Read all the records from this message, as it may contain several
            try {
	               
                for (Record record = firstRecord; record != null; record = reader.nextRecord()) {
                    writer.write(record);
         	    }
            	                     	       
            	} catch (MalformedRecordException | IOException mEx) {
            	   handleParseFailure.accept(msg, mEx);
            	}
            
            // Acknowledge the message
            consumer.getConsumer().acknowledge(msg);
		    
	    }
	    
	    // Clean-up and transfer session
	    try {
	    	    if (writer != null)
	    	    	   writer.finishRecordSet();
	    	    
	    	    if (rawOut != null)
			  rawOut.close();
		} catch (IOException e1) {
			getLogger().error("Error cleaning up", e1);
		}
  	    
	    if (flowFile != null)
  	       session.transfer(flowFile, REL_SUCCESS);
	}
	
	private Record getFirstRecord(Message msg, RecordReader reader, BiConsumer<Message, Exception> handleParseFailure) {
		
		Record firstRecord = null;
		
        try {
			firstRecord = reader.nextRecord();
		} catch (IOException | MalformedRecordException ex) {
			handleParseFailure.accept(msg, ex);
		}
        
        return firstRecord;
	}
	
	private RecordReader getRecordReader(Message msg, RecordReaderFactory readerFactory, BiConsumer<Message, Exception> handleParseFailure) {
		
		RecordReader reader = null;
		final byte[] recordBytes = msg.getData() == null ? new byte[0] : msg.getData();
		
		try (final InputStream in = new ByteArrayInputStream(recordBytes)) {
 	       reader = readerFactory.createRecordReader(Collections.emptyMap(), in, getLogger());
		} catch (MalformedRecordException | IOException | SchemaNotFoundException ex) {
 	       handleParseFailure.accept(msg, ex);
	    }
		
		return reader;
		
	}
	
	private RecordSetWriter getRecordWriter(RecordSetWriterFactory writerFactory, RecordSchema srcSchema, OutputStream out) throws SchemaNotFoundException, IOException {
		RecordSchema writeSchema = writerFactory.getSchema(Collections.emptyMap(), srcSchema);
		return writerFactory.createWriter(getLogger(), writeSchema, out);
	}
	   
}
