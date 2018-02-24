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
package org.apache.nifi.processors.pulsar;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Matchers;
import org.mockito.Mock;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class PublishPulsarProcessorTest extends AbstractPulsarProcessorTest {
	
    @Mock
	Producer mockProducer;

    @Before
    public void init() throws InitializationException {
<<<<<<< HEAD
        runner = TestRunners.newTestRunner(PublishPulsar_1_0.class);
=======
        runner = TestRunners.newTestRunner(PublishPulsar.class);
>>>>>>> Added Pulsar processors and Controller Service
        
        mockClient = mock(PulsarClient.class);
        mockProducer = mock(Producer.class);
        
        try {
        		// Use the mockProducer for all Producer interactions
			when(mockClient.createProducer(anyString())).thenReturn(mockProducer);
			
			when(mockProducer.send(Matchers.argThat(new ArgumentMatcher<byte[]>() {
				    @Override
				    public boolean matches(Object argument) {			        
				        return true;
				    }
			}))).thenReturn(mock(MessageId.class));
			
			CompletableFuture<MessageId> future = CompletableFuture.supplyAsync(() -> {
			    return mock(MessageId.class);
			});
			
			when(mockProducer.sendAsync(Matchers.argThat(new ArgumentMatcher<byte[]>() {
			    @Override
			    public boolean matches(Object argument) {			        
			        return true;
			    }
			}))).thenReturn(future);

			
		} catch (PulsarClientException e) {
			e.printStackTrace();
		}
        
        addPulsarClientService();
    }
	
	@Test
	public void invalidTopicTest() throws UnsupportedEncodingException, PulsarClientException {
		
<<<<<<< HEAD
		runner.setProperty(PublishPulsar_1_0.TOPIC, "${topic}");
		
		final String content = "some content";
        Map<String, String> attributes = new HashMap<String, String> ();
        attributes.put(PublishPulsar_1_0.TOPIC.getName(), "");
        
		runner.enqueue(content.getBytes("UTF-8"), attributes );
        runner.run();
        runner.assertAllFlowFilesTransferred(PublishPulsar_1_0.REL_FAILURE);
=======
		runner.setProperty(PublishPulsar.TOPIC, "${topic}");
		
		final String content = "some content";
        Map<String, String> attributes = new HashMap<String, String> ();
        attributes.put(PublishPulsar.TOPIC.getName(), "");
        
		runner.enqueue(content.getBytes("UTF-8"), attributes );
        runner.run();
        runner.assertAllFlowFilesTransferred(PublishPulsar.REL_FAILURE);
>>>>>>> Added Pulsar processors and Controller Service
        
        // Confirm that no Producer as created 
        verify(mockClient, times(0)).createProducer(anyString());
	}
	
	@Test
	public void dynamicTopicTest() throws UnsupportedEncodingException, PulsarClientException {
		
<<<<<<< HEAD
		runner.setProperty(PublishPulsar_1_0.TOPIC, "${topic}");
		
		final String content = "some content";
        Map<String, String> attributes = new HashMap<String, String> ();
        attributes.put(PublishPulsar_1_0.TOPIC.getName(), "topic-b");
        
		runner.enqueue(content.getBytes("UTF-8"), attributes );
        runner.run();
        runner.assertAllFlowFilesTransferred(PublishPulsar_1_0.REL_SUCCESS);
=======
		runner.setProperty(PublishPulsar.TOPIC, "${topic}");
		
		final String content = "some content";
        Map<String, String> attributes = new HashMap<String, String> ();
        attributes.put(PublishPulsar.TOPIC.getName(), "topic-b");
        
		runner.enqueue(content.getBytes("UTF-8"), attributes );
        runner.run();
        runner.assertAllFlowFilesTransferred(PublishPulsar.REL_SUCCESS);
>>>>>>> Added Pulsar processors and Controller Service
        
        // Verify that we sent the data to topic-b.
        verify(mockClient, times(1)).createProducer("topic-b");
	}

	@Test
    public void singleFlowFileTest() throws UnsupportedEncodingException, PulsarClientException {
		
<<<<<<< HEAD
		runner.setProperty(PublishPulsar_1_0.TOPIC, "my-topic");
=======
		runner.setProperty(PublishPulsar.TOPIC, "my-topic");
>>>>>>> Added Pulsar processors and Controller Service
	
		final String content = "some content";
        runner.enqueue(content.getBytes("UTF-8"));
        runner.run();
<<<<<<< HEAD
        runner.assertAllFlowFilesTransferred(PublishPulsar_1_0.REL_SUCCESS);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(PublishPulsar_1_0.REL_SUCCESS).get(0);
=======
        runner.assertAllFlowFilesTransferred(PublishPulsar.REL_SUCCESS);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(PublishPulsar.REL_SUCCESS).get(0);
>>>>>>> Added Pulsar processors and Controller Service
        outFile.assertContentEquals(content);
        
        // Verify that we sent the data to my-topic.
        verify(mockClient, times(1)).createProducer("my-topic");
        
        // Verify that the send method on the producer was called with the expected content
        verify(mockProducer, times(1)).send(content.getBytes());
	}
	
	@Test
    public void singleFlowFileAsyncTest() throws UnsupportedEncodingException, PulsarClientException {
		
<<<<<<< HEAD
		runner.setProperty(PublishPulsar_1_0.TOPIC, "my-topic");
		runner.setProperty(PublishPulsar_1_0.ASYNC_ENABLED, Boolean.TRUE.toString());
=======
		runner.setProperty(PublishPulsar.TOPIC, "my-topic");
		runner.setProperty(PublishPulsar.ASYNC_ENABLED, Boolean.TRUE.toString());
>>>>>>> Added Pulsar processors and Controller Service
	
		final String content = "some content";
        runner.enqueue(content.getBytes("UTF-8"));
        runner.run();
<<<<<<< HEAD
        runner.assertAllFlowFilesTransferred(PublishPulsar_1_0.REL_SUCCESS);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(PublishPulsar_1_0.REL_SUCCESS).get(0);
=======
        runner.assertAllFlowFilesTransferred(PublishPulsar.REL_SUCCESS);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(PublishPulsar.REL_SUCCESS).get(0);
>>>>>>> Added Pulsar processors and Controller Service
        outFile.assertContentEquals(content);
        
        // Verify that we sent the data to my-topic.
        verify(mockClient, times(1)).createProducer("my-topic");
        
        // Verify that the send method on the producer was called with the expected content
        verify(mockProducer, times(1)).sendAsync(content.getBytes());
	}
	
	@Test
	public void multipleFlowFilesTest() throws UnsupportedEncodingException, PulsarClientException {
		
<<<<<<< HEAD
		runner.setProperty(PublishPulsar_1_0.TOPIC, "my-topic");
=======
		runner.setProperty(PublishPulsar.TOPIC, "my-topic");
>>>>>>> Added Pulsar processors and Controller Service
		final String content = "some content";
		
		// Hack, since runner.run(20, false); doesn't work as advertised
		for (int idx = 0; idx < 20; idx++) {
			runner.enqueue(content.getBytes("UTF-8"));
			runner.run();
<<<<<<< HEAD
			runner.assertAllFlowFilesTransferred(PublishPulsar_1_0.REL_SUCCESS);
=======
			runner.assertAllFlowFilesTransferred(PublishPulsar.REL_SUCCESS);
>>>>>>> Added Pulsar processors and Controller Service
		}
        
        // Verify that the send method on the producer was called with the expected content
        verify(mockProducer, times(20)).send(content.getBytes());
	}
	
	@Test
	public void multipleFlowFilesAsyncTest() throws UnsupportedEncodingException, PulsarClientException {
		
<<<<<<< HEAD
		runner.setProperty(PublishPulsar_1_0.TOPIC, "my-async-topic");
		runner.setProperty(PublishPulsar_1_0.ASYNC_ENABLED, Boolean.TRUE.toString());
=======
		runner.setProperty(PublishPulsar.TOPIC, "my-async-topic");
		runner.setProperty(PublishPulsar.ASYNC_ENABLED, Boolean.TRUE.toString());
>>>>>>> Added Pulsar processors and Controller Service
		final String content = "some content";
		
		// Hack, since runner.run(20, false); doesn't work as advertised
		for (int idx = 0; idx < 20; idx++) {
			runner.enqueue(content.getBytes("UTF-8"));
			runner.run();
<<<<<<< HEAD
			runner.assertAllFlowFilesTransferred(PublishPulsar_1_0.REL_SUCCESS);
=======
			runner.assertAllFlowFilesTransferred(PublishPulsar.REL_SUCCESS);
>>>>>>> Added Pulsar processors and Controller Service
		}
        
        // Verify that the send method on the producer was called with the expected content
        verify(mockProducer, times(20)).sendAsync(content.getBytes());
	}
	
	@Test
	public void stressTest() throws UnsupportedEncodingException {
<<<<<<< HEAD
		runner.setProperty(PublishPulsar_1_0.TOPIC, "my-async-topic");
		runner.setProperty(PublishPulsar_1_0.ASYNC_ENABLED, Boolean.TRUE.toString());
=======
		runner.setProperty(PublishPulsar.TOPIC, "my-async-topic");
		runner.setProperty(PublishPulsar.ASYNC_ENABLED, Boolean.TRUE.toString());
>>>>>>> Added Pulsar processors and Controller Service
		final String content = "some content";
		
		for (int idx = 0; idx < 9999; idx++) {
			runner.enqueue(content.getBytes("UTF-8"));
			runner.run();
<<<<<<< HEAD
			runner.assertAllFlowFilesTransferred(PublishPulsar_1_0.REL_SUCCESS);
=======
			runner.assertAllFlowFilesTransferred(PublishPulsar.REL_SUCCESS);
>>>>>>> Added Pulsar processors and Controller Service
		}
		
	}

}
