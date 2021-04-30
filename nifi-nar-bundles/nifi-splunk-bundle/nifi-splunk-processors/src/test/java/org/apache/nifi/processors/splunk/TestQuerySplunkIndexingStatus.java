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
package org.apache.nifi.processors.splunk;

import com.splunk.RequestMessage;
import com.splunk.ResponseMessage;
import com.splunk.Service;
import com.splunk.ServiceArgs;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RunWith(MockitoJUnitRunner.class)
public class TestQuerySplunkIndexingStatus {
    private static final String EVENT = "{\"a\"=\"b\",\"c\"=\"d\",\"e\"=\"f\"}";

    @Mock
    private Service service;

    @Mock
    private ResponseMessage response;

    private MockedQuerySplunkIndexingStatus processor;
    private TestRunner testRunner;

    private ArgumentCaptor<String> path;
    private ArgumentCaptor<RequestMessage> request;

    @Before
    public void setUp() {
        processor = new MockedQuerySplunkIndexingStatus(service);
        testRunner = TestRunners.newTestRunner(processor);
        testRunner.setProperty(SplunkAPICall.SCHEME, "http");
        testRunner.setProperty(SplunkAPICall.TOKEN, "Splunk 888c5a81-8777-49a0-a3af-f76e050ab5d9");
        testRunner.setProperty(SplunkAPICall.REQUEST_CHANNEL, "22bd7414-0d77-4c73-936d-c8f5d1b21862");

        path = ArgumentCaptor.forClass(String.class);
        request = ArgumentCaptor.forClass(RequestMessage.class);
        Mockito.when(service.send(path.capture(), request.capture())).thenReturn(response);
    }

    @After
    public void tearDown() {
        testRunner.shutdown();
    }

    @Test
    public void testRunSuccess() throws Exception {
        // given
        final Map<Integer, Boolean> acks = new HashMap<>();
        acks.put(1, true);
        acks.put(2, false);
        givenSplunkReturns(acks);

        // when
        testRunner.enqueue(givenFlowFile(1, System.currentTimeMillis()));
        testRunner.enqueue(givenFlowFile(2, System.currentTimeMillis()));
        testRunner.run();

        // then
        final List<MockFlowFile> acknowledged = testRunner.getFlowFilesForRelationship(QuerySplunkIndexingStatus.RELATIONSHIP_ACKNOWLEDGED);
        final List<MockFlowFile> undetermined = testRunner.getFlowFilesForRelationship(QuerySplunkIndexingStatus.RELATIONSHIP_UNDETERMINED);

        Assert.assertEquals(1, acknowledged.size());
        Assert.assertEquals(1, undetermined.size());
        Assert.assertFalse(acknowledged.get(0).isPenalized());
        Assert.assertTrue(undetermined.get(0).isPenalized());
    }

    @Test
    public void testMoreIncomingFlowFileThanQueryLimit() throws Exception {
        // given
        testRunner.setProperty(QuerySplunkIndexingStatus.MAX_QUERY_SIZE, "2");
        final Map<Integer, Boolean> acks = new HashMap<>();
        acks.put(1, true);
        acks.put(2, true);
        givenSplunkReturns(acks);

        // when
        testRunner.enqueue(givenFlowFile(1, System.currentTimeMillis()));
        testRunner.enqueue(givenFlowFile(2, System.currentTimeMillis()));
        testRunner.enqueue(givenFlowFile(3, System.currentTimeMillis()));
        testRunner.run();

        // then
        Assert.assertEquals("{\"acks\":[1,2]}", request.getValue().getContent());
        Assert.assertEquals(1, testRunner.getQueueSize().getObjectCount());
        testRunner.assertAllFlowFilesTransferred(QuerySplunkIndexingStatus.RELATIONSHIP_ACKNOWLEDGED, 2);
    }

    @Test
    public void testWhenFlowFileIsLackOfNecessaryAttributes() throws Exception {
        // when
        testRunner.enqueue(EVENT);
        testRunner.run();

        // then
        testRunner.assertAllFlowFilesTransferred(QuerySplunkIndexingStatus.RELATIONSHIP_FAILURE, 1);
    }

    @Test
    public void testWhenSplunkReturnsWithError() throws Exception {
        // given
        givenSplunkReturnsWithFailure();

        // when
        testRunner.enqueue(givenFlowFile(1, System.currentTimeMillis()));
        testRunner.enqueue(givenFlowFile(2, System.currentTimeMillis()));
        testRunner.enqueue(givenFlowFile(3, System.currentTimeMillis()));
        testRunner.run();

        // then
        testRunner.assertAllFlowFilesTransferred(QuerySplunkIndexingStatus.RELATIONSHIP_UNDETERMINED, 3);
    }

    private void givenSplunkReturns(final Map<Integer, Boolean> acks) throws Exception {
        final StringBuilder responseContent = new StringBuilder("{\"acks\":{")
                .append(acks.entrySet().stream().map(e -> "\"" + e.getKey() + "\": " + e.getValue()).collect(Collectors.joining(", ")))
                .append("}}");

        final InputStream inputStream = new ByteArrayInputStream(responseContent.toString().getBytes("UTF-8"));
        Mockito.when(response.getStatus()).thenReturn(200);
        Mockito.when(response.getContent()).thenReturn(inputStream);
    }

    private void givenSplunkReturnsWithFailure() {
        Mockito.when(response.getStatus()).thenReturn(403);
    }

    private MockFlowFile givenFlowFile(final int ackId, final long sentAt) throws UnsupportedEncodingException {
        final MockFlowFile result = new MockFlowFile(ackId);
        result.setData(EVENT.getBytes("UTF-8"));
        Map<String, String> attributes = new HashMap<>();
        attributes.put("splunk.acknowledgement.id", String.valueOf(ackId));
        attributes.put("splunk.responded.at", String.valueOf(sentAt));
        result.putAttributes(attributes);
        return result;
    }

    public static class MockedQuerySplunkIndexingStatus extends QuerySplunkIndexingStatus {
        final Service serviceMock;

        public MockedQuerySplunkIndexingStatus(final Service serviceMock) {
            this.serviceMock = serviceMock;
        }

        @Override
        protected Service getSplunkService(final ServiceArgs splunkServiceArguments) {
            return serviceMock;
        }
    }
}
