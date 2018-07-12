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
package org.apache.nifi.processors.pulsar.pubsub.sync;

import org.apache.nifi.processors.pulsar.pubsub.ConsumePulsarRecord;
import org.apache.nifi.processors.pulsar.pubsub.TestConsumePulsarRecord;
import org.apache.nifi.util.MockFlowFile;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

public class TestSyncConsumePulsarRecord extends TestConsumePulsarRecord {

    @Test
    public void emptyMessageTest() throws PulsarClientException {
        when(mockMessage.getData()).thenReturn("".getBytes());
        mockClientService.setMockMessage(mockMessage);

        runner.setProperty(ConsumePulsarRecord.TOPICS, DEFAULT_TOPIC);
        runner.setProperty(ConsumePulsarRecord.SUBSCRIPTION_NAME, DEFAULT_SUB);
        runner.setProperty(ConsumePulsarRecord.BATCH_SIZE, 1 + "");
        runner.run();
        runner.assertAllFlowFilesTransferred(ConsumePulsarRecord.REL_PARSE_FAILURE);

        verify(mockClientService.getMockConsumer(), times(1)).acknowledge(mockMessage);
    }

    @Test
    public void singleMalformedMessageTest() throws PulsarClientException {
       when(mockMessage.getData()).thenReturn(BAD_MSG.getBytes());
       mockClientService.setMockMessage(mockMessage);

       runner.setProperty(ConsumePulsarRecord.TOPICS, DEFAULT_TOPIC);
       runner.setProperty(ConsumePulsarRecord.SUBSCRIPTION_NAME, DEFAULT_SUB);
       runner.setProperty(ConsumePulsarRecord.BATCH_SIZE, 1 + "");
       runner.run();
       runner.assertAllFlowFilesTransferred(ConsumePulsarRecord.REL_PARSE_FAILURE);

       verify(mockClientService.getMockConsumer(), times(1)).acknowledge(mockMessage);
    }

    /*
     * Send a single message containing a single record
     */
    @Test
    public void singleMessageTest() throws PulsarClientException {
        this.sendMessages(MOCKED_MSG, false, 1);
    }

    /*
     * Send a single message with multiple records
     */
    @Test
    public void singleMessageMultiRecordsTest() throws PulsarClientException {
       StringBuffer input = new StringBuffer(1024);
       StringBuffer expected = new StringBuffer(1024);

       for (int idx = 0; idx < 50; idx++) {
           input.append("Justin Thyme, " + idx).append("\n");
           expected.append("\"Justin Thyme\",\"" + idx + "\"").append("\n");
       }

       List<MockFlowFile> results = this.sendMessages(input.toString(), false, 1);

       String flowFileContents = new String(runner.getContentAsByteArray(results.get(0)));
       assertEquals(expected.toString(), flowFileContents);

       results.get(0).assertAttributeEquals(ConsumePulsarRecord.MSG_COUNT, 50 + "");
    }

    /*
     * Send a single message with multiple records,
     * some of them good and some malformed
     */
    @Test
    public void singleMessageWithGoodAndBadRecordsTest() throws PulsarClientException {
       StringBuffer input = new StringBuffer(1024);
       StringBuffer expected = new StringBuffer(1024);

       for (int idx = 0; idx < 10; idx++) {
          if (idx % 2 == 0) {
             input.append("Justin Thyme, " + idx).append("\n");
             expected.append("\"Justin Thyme\",\"" + idx + "\"").append("\n");
          } else {
             input.append(BAD_MSG).append("\n");
          }
        }

       when(mockMessage.getData()).thenReturn(input.toString().getBytes());
       mockClientService.setMockMessage(mockMessage);

       runner.setProperty(ConsumePulsarRecord.ASYNC_ENABLED, Boolean.toString(false));
       runner.setProperty(ConsumePulsarRecord.TOPICS, DEFAULT_TOPIC);
       runner.setProperty(ConsumePulsarRecord.SUBSCRIPTION_NAME, DEFAULT_SUB);
       runner.setProperty(ConsumePulsarRecord.BATCH_SIZE, 1 + "");
       runner.setProperty(ConsumePulsarRecord.MAX_WAIT_TIME, "0 sec");
       runner.run(1, true);

       List<MockFlowFile> successFlowFiles = runner.getFlowFilesForRelationship(ConsumePulsarRecord.REL_SUCCESS);
       assertEquals(1, successFlowFiles.size());

       List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(ConsumePulsarRecord.REL_PARSE_FAILURE);
       assertEquals(1, failureFlowFiles.size());
    }

    /*
     * Send multiple messages with Multiple records each
     */
    @Test
    public void multipleMultiRecordsTest() throws PulsarClientException {
        StringBuffer input = new StringBuffer(1024);
        StringBuffer expected = new StringBuffer(1024);

        for (int idx = 0; idx < 50; idx++) {
            input.append("Justin Thyme, " + idx).append("\n");
            expected.append("\"Justin Thyme\",\"" + idx + "\"").append("\n");
        }

        List<MockFlowFile> results = this.sendMessages(input.toString(), false, 50, 1);
        assertEquals(50, results.size());

        String flowFileContents = new String(runner.getContentAsByteArray(results.get(0)));
        assertEquals(expected.toString(), flowFileContents);

        results.get(0).assertAttributeEquals(ConsumePulsarRecord.MSG_COUNT, 50 + "");
    }
}
