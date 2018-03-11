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

import org.apache.nifi.processors.pulsar.pubsub.mocks.MockPulsarClientService;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockRecordParser;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockRecordWriter;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.apache.nifi.processors.pulsar.RecordBasedConst.RECORD_READER;
import static org.apache.nifi.processors.pulsar.RecordBasedConst.RECORD_WRITER;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.processors.pulsar.AbstractPulsarProcessor;

public class TestConsumePulsarRecord_1_0 {

    private TestRunner runner;

    @Mock
    protected PulsarClient mockClient;
    
    @Mock
    Consumer mockConsumer;

    @Mock
    Message mockMessage;

    @Before
    public void setup() throws InitializationException {

    	    mockClient = mock(PulsarClient.class);
        mockConsumer = mock(Consumer.class);
        mockMessage = mock(Message.class);
        
        try {
            when(mockClient.subscribe(anyString(), anyString())).thenReturn(mockConsumer);
            when(mockConsumer.receive()).thenReturn(mockMessage);

            CompletableFuture<Message> future = CompletableFuture.supplyAsync(() -> {
                return mockMessage;
            });

            when(mockConsumer.receiveAsync()).thenReturn(future);

        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
    	
        runner = TestRunners.newTestRunner(ConsumePulsarRecord_1_0.class);

        final String readerId = "record-reader";
        final MockRecordParser readerService = new MockRecordParser();
        readerService.addSchemaField("name", RecordFieldType.STRING);
        readerService.addSchemaField("age", RecordFieldType.INT);
        runner.addControllerService(readerId, readerService);
        runner.enableControllerService(readerService);

        final String writerId = "record-writer";
        final RecordSetWriterFactory writerService = new MockRecordWriter("name, age");
        runner.addControllerService(writerId, writerService);
        runner.enableControllerService(writerService);

        final MockPulsarClientService pulsarClient = new MockPulsarClientService(mockClient);
        runner.addControllerService("pulsarClient", pulsarClient);
        runner.enableControllerService(pulsarClient);

        runner.setProperty(RECORD_READER, readerId);
        runner.setProperty(RECORD_WRITER, writerId);
        runner.setProperty(AbstractPulsarProcessor.PULSAR_CLIENT_SERVICE, "pulsarClient");
    }

    @Test
    public void validatePropertiesValidation() throws Exception {

        // Initially the processor won't be properly configured
        runner.assertNotValid();

        runner.setProperty(AbstractPulsarConsumerProcessor.TOPIC, "my-topic");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION, "my-sub");
        runner.assertValid();
    }

    @Test
    public void emptyMessageTest() throws PulsarClientException {
        when(mockMessage.getData()).thenReturn("".getBytes());

        runner.setProperty(ConsumePulsarRecord_1_0.TOPIC, "foo");
        runner.setProperty(ConsumePulsarRecord_1_0.SUBSCRIPTION, "bar");
        runner.setProperty(ConsumePulsarRecord_1_0.BATCH_SIZE, 1 + "");
        runner.run();
        runner.assertAllFlowFilesTransferred(ConsumePulsarRecord_1_0.REL_SUCCESS);
        
        verify(mockConsumer, times(1)).acknowledge(mockMessage);
    }
    
    @Test
    public void malformedMessageTest() throws PulsarClientException {
    	   when(mockMessage.getData()).thenReturn("malformed message".getBytes());
    	   runner.setProperty(ConsumePulsarRecord_1_0.TOPIC, "foo");
       runner.setProperty(ConsumePulsarRecord_1_0.SUBSCRIPTION, "bar");
       runner.setProperty(ConsumePulsarRecord_1_0.BATCH_SIZE, 1 + "");
       runner.run();
       runner.assertAllFlowFilesTransferred(ConsumePulsarRecord_1_0.REL_PARSE_FAILURE);
           
       verify(mockConsumer, times(1)).acknowledge(mockMessage);
    }
    
    /*
     * Send a single message containing a single record
     */
    @Test
    public void singleSyncMessageTest() throws PulsarClientException {
        this.sendMessages("Mocked Message, 1", "foo", "bar", false, 1);
    }
    
    /*
     * Send a single message with multiple records
     */
    @Test
    public void singleSyncMessageMultiRecordsTest() throws PulsarClientException {
    		StringBuffer input = new StringBuffer(1024);
    		StringBuffer expected = new StringBuffer(1024);
    		
    		for (int idx = 0; idx < 50; idx++) {
    			input.append("Justin Thyme, " + idx).append("\n");
    			expected.append("\"Justin Thyme\",\"" + idx + "\"").append("\n");
    		}
    		
    		
    		List<MockFlowFile> results = this.sendMessages(input.toString(), "foo", "bar", false, 1);
    		
    		String flowFileContents = new String(runner.getContentAsByteArray(results.get(0)));
    		assertEquals(expected.toString(), flowFileContents);
    }

    /*
     * Send 40 single record messages, and we expect them to be processed in
     * a batch and combined into a single FlowFile
     */
    @Test
    public void multipleSyncMessagesTest() throws PulsarClientException {
    	    List<MockFlowFile> results = this.sendMessages("Mocked Message, 1", "foo", "bar", false, 1, 40);   	    
    	    assertEquals(1, results.size());    	    
    }
    
    /*
     * Send multiple messages with Multiple records each
     */
    @Test
    public void multipleSyncMultiRecordsTest() throws PulsarClientException {
    	    StringBuffer input = new StringBuffer(1024);
		StringBuffer expected = new StringBuffer(1024);
		
		for (int idx = 0; idx < 50; idx++) {
			input.append("Justin Thyme, " + idx).append("\n");
			expected.append("\"Justin Thyme\",\"" + idx + "\"").append("\n");
		}
		
		List<MockFlowFile> results = this.sendMessages(input.toString(), "foo", "bar", false, 50, 1);
		assertEquals(50, results.size());
		
		String flowFileContents = new String(runner.getContentAsByteArray(results.get(0)));
		assertEquals(expected.toString(), flowFileContents);
		
    }
    
    private List<MockFlowFile> sendMessages(String msg, String topic, String sub, boolean async, int iterations) throws PulsarClientException {
    	   return sendMessages(msg, topic, sub, async, iterations, 1);
    }

    private List<MockFlowFile> sendMessages(String msg, String topic, String sub, boolean async, int iterations, int batchSize) throws PulsarClientException {

        when(mockMessage.getData()).thenReturn(msg.getBytes());

        runner.setProperty(ConsumePulsarRecord_1_0.ASYNC_ENABLED, Boolean.toString(async));
        runner.setProperty(ConsumePulsarRecord_1_0.TOPIC, topic);
        runner.setProperty(ConsumePulsarRecord_1_0.SUBSCRIPTION, sub);
        runner.setProperty(ConsumePulsarRecord_1_0.BATCH_SIZE, batchSize + "");
        runner.run(iterations, true);

        runner.assertAllFlowFilesTransferred(ConsumePulsarRecord_1_0.REL_SUCCESS);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumePulsarRecord_1_0.REL_SUCCESS);
        assertEquals(iterations, flowFiles.size());

        if (async) {
            verify(mockConsumer, times(batchSize * iterations)).receiveAsync();
        } else {
            verify(mockConsumer, times(batchSize * iterations)).receive();
        }

        // Verify that every message was acknowledged
        if (async) {
            verify(mockConsumer, times(batchSize * iterations)).acknowledgeAsync(mockMessage);
        } else {
            verify(mockConsumer, times(batchSize * iterations)).acknowledge(mockMessage);
        }
        
        return flowFiles;
    }
}
