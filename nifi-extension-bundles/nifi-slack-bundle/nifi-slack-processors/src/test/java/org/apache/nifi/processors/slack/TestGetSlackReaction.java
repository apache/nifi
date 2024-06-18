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

package org.apache.nifi.processors.slack;

import com.slack.api.bolt.App;
import com.slack.api.methods.MethodsClient;
import com.slack.api.methods.request.reactions.ReactionsGetRequest;
import com.slack.api.methods.response.reactions.ReactionsGetResponse;
import com.slack.api.model.Reaction;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.apache.nifi.processors.slack.GetSlackReaction.ATTR_ERROR_MESSAGE;
import static org.apache.nifi.processors.slack.GetSlackReaction.ATTR_WAIT_TIME;
import static org.apache.nifi.processors.slack.GetSlackReaction.REL_FAILURE;
import static org.apache.nifi.processors.slack.GetSlackReaction.REL_NO_REACTION;
import static org.apache.nifi.processors.slack.GetSlackReaction.REL_SUCCESS;
import static org.apache.nifi.processors.slack.GetSlackReaction.REL_WAIT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
@ExtendWith(MockitoExtension.class)
public class TestGetSlackReaction {
    public static final String TEST_MESSAGE_TS = "123456.789";
    public static final String TEST_CHANNEL_ID = "cid1";
    private TestRunner testRunner;
    GetSlackReaction processor;
    @Mock
    private MethodsClient clientMock;

    @BeforeEach
    public void setup() {
        processor = new GetSlackReaction() {
            @Override
            protected MethodsClient initializeClient(final App slackApp) {
                return clientMock;
            }
        };

        testRunner = TestRunners.newTestRunner(processor);
        testRunner.setProperty(GetSlackReaction.ACCESS_TOKEN, "token");
        testRunner.setProperty(GetSlackReaction.CHANNEL_ID, TEST_CHANNEL_ID);
        testRunner.setProperty(GetSlackReaction.THREAD_TIMESTAMP, TEST_MESSAGE_TS);
    }

    @Test
    public void testImmediateReaction() throws Exception {
        when(clientMock.reactionsGet(any(ReactionsGetRequest.class))).thenReturn(fetchReactions("thumbs_up", 1));
        final MockFlowFile inputFlowFile = new MockFlowFile(0);
        testRunner.enqueue(inputFlowFile);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        final MockFlowFile ff0 = testRunner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        assertEquals("1", ff0.getAttribute("slack.reaction.thumbs_up"));
        testRunner.clearTransferState();
    }

    @Test
    public void testWaitForFurtherReactions() throws Exception {
        testRunner.setProperty(GetSlackReaction.RELEASE_IF_ONE_REACTION, "false");
        testRunner.setProperty(GetSlackReaction.WAIT_MONITOR_WINDOW, "10 min");

        when(clientMock.reactionsGet(any(ReactionsGetRequest.class))).thenReturn(fetchReactions("thumbs_up", 1));
        final MockFlowFile inputFlowFile = new MockFlowFile(0);
        testRunner.enqueue(inputFlowFile);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(REL_WAIT, 1);
        MockFlowFile waitingFF = testRunner.getFlowFilesForRelationship(REL_WAIT).get(0);
        assertEquals("5", waitingFF.getAttribute(ATTR_WAIT_TIME));
        assertEquals(1, testRunner.getPenalizedFlowFiles().size());

        testRunner.clearTransferState();
        testRunner.enqueue(waitingFF);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(REL_WAIT, 1);
        waitingFF = testRunner.getFlowFilesForRelationship(REL_WAIT).get(0);
        testRunner.clearTransferState();

        final Reaction thumbsUp = getReaction("thumbs_up", 3);
        final Reaction coolDoge = getReaction("cool_doge", 1);
        when(clientMock.reactionsGet(any(ReactionsGetRequest.class))).thenReturn(fetchReactions(Arrays.asList(thumbsUp, coolDoge)));
        testRunner.enqueue(waitingFF);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        final MockFlowFile ff0 = testRunner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        assertEquals("3", ff0.getAttribute("slack.reaction.thumbs_up"));
        assertEquals("1", ff0.getAttribute("slack.reaction.cool_doge"));
        assertEquals("10", waitingFF.getAttribute(ATTR_WAIT_TIME));
        testRunner.clearTransferState();
    }

    @Test
    public void testNoReaction() throws Exception {
        when(clientMock.reactionsGet(any(ReactionsGetRequest.class))).thenReturn(fetchReactions(emptyList()));
        final MockFlowFile inputFlowFile = new MockFlowFile(0);
        testRunner.enqueue(inputFlowFile);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(REL_WAIT, 1);
        testRunner.clearTransferState();
    }

    @Test
    public void testWaitMonitorWindowLessThanPenaltyPeriod() throws Exception {
        when(clientMock.reactionsGet(any(ReactionsGetRequest.class))).thenReturn(fetchReactions(emptyList()));
        testRunner.setProperty(GetSlackReaction.WAIT_MONITOR_WINDOW, "2 min");
        final MockFlowFile inputFlowFile = new MockFlowFile(0);
        testRunner.enqueue(inputFlowFile);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(REL_NO_REACTION, 1);
        final MockFlowFile ff0 = testRunner.getFlowFilesForRelationship(REL_NO_REACTION).get(0);
        ff0.assertAttributeEquals(GetSlackReaction.ATTR_CHANNEL_ID, TEST_CHANNEL_ID);
        ff0.assertAttributeEquals(GetSlackReaction.ATTR_MESSAGE_TIMESTAMP, TEST_MESSAGE_TS);
        testRunner.clearTransferState();
    }

    @Test
    public void testRequestRateLimited() throws Exception {
        when(clientMock.reactionsGet(any(ReactionsGetRequest.class))).thenReturn(fetchReactions("thumbs_up", 1));
        final MockFlowFile inputFlowFile = new MockFlowFile(0);
        testRunner.enqueue(inputFlowFile);
        testRunner.run(1, false, true);
        testRunner.assertAllFlowFilesTransferred(GetSlackReaction.REL_SUCCESS, 1);
        testRunner.clearTransferState();

        // Set processor to be in rate limited state, therefore it will process 0 flowfiles
        processor.getRateLimit().retryAfter(Duration.ofSeconds(30));
        testRunner.enqueue(inputFlowFile);
        testRunner.run(1, true, false);
        testRunner.assertAllFlowFilesTransferred(GetSlackReaction.REL_SUCCESS, 0);
    }

    @Test
    public void testErrorResponse() throws Exception {
        //response without message
        when(clientMock.reactionsGet(any(ReactionsGetRequest.class))).thenReturn(new ReactionsGetResponse());
        final MockFlowFile inputFlowFile = new MockFlowFile(0);
        testRunner.enqueue(inputFlowFile);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(REL_FAILURE, 1);
        final MockFlowFile ff0 = testRunner.getFlowFilesForRelationship(REL_FAILURE).get(0);
        ff0.assertAttributeExists(ATTR_ERROR_MESSAGE);
        ff0.assertAttributeEquals(GetSlackReaction.ATTR_CHANNEL_ID, TEST_CHANNEL_ID);
        ff0.assertAttributeEquals(GetSlackReaction.ATTR_MESSAGE_TIMESTAMP, TEST_MESSAGE_TS);
        testRunner.clearTransferState();
    }

    public ReactionsGetResponse fetchReactions(final List<Reaction> reactions) {
        final ReactionsGetResponse response = createReactionsGetResponse();
        response.getMessage().setReactions(reactions);
        return response;
    }

    public ReactionsGetResponse fetchReactions(final String name, final Integer count) {
        final ReactionsGetResponse response = createReactionsGetResponse();
        response.getMessage().setReactions(List.of(getReaction(name, count)));
        return response;
    }

    public Reaction getReaction(final String name, final Integer count) {
        final Reaction reaction = new Reaction();
        reaction.setName(name);
        reaction.setCount(count);
        return reaction;
    }

    public ReactionsGetResponse createReactionsGetResponse() {
        final ReactionsGetResponse.Message message = new ReactionsGetResponse.Message();
        message.setText("Dummy text");
        final ReactionsGetResponse response = new ReactionsGetResponse();
        response.setOk(true);
        response.setMessage(message);
        return response;
    }

}
