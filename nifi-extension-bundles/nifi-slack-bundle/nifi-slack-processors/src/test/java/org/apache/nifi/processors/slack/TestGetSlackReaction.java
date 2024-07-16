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

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
@ExtendWith(MockitoExtension.class)
public class TestGetSlackReaction {
    public static final String TEST_MESSAGE_TS = "1719937054.266429";
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
    }

    @Test
    public void testImmediateReaction() throws Exception {
        when(clientMock.reactionsGet(any(ReactionsGetRequest.class))).thenReturn(fetchReactions("thumbs_up", 1));
        testRunner.enqueue(getInputFlowFileWithAttributes());
        testRunner.run();
        testRunner.assertTransferCount(GetSlackReaction.REL_SUCCESS, 1);
        testRunner.assertTransferCount(GetSlackReaction.REL_ORIGINAL, 1);
        final MockFlowFile ff0 = testRunner.getFlowFilesForRelationship(GetSlackReaction.REL_SUCCESS).get(0);
        assertEquals("1", ff0.getAttribute("slack.reaction.thumbs_up"));
        ff0.assertAttributeExists(GetSlackReaction.ATTR_REACTION_POLLING_SECONDS);
        testRunner.clearTransferState();
    }

    @Test
    public void testNoReaction() throws Exception {
        when(clientMock.reactionsGet(any(ReactionsGetRequest.class))).thenReturn(fetchReactions(emptyList()));
        testRunner.enqueue(getInputFlowFileWithAttributes());
        testRunner.run();
        testRunner.assertTransferCount(GetSlackReaction.REL_NO_REACTION, 1);
        testRunner.assertTransferCount(GetSlackReaction.REL_ORIGINAL, 1);
        final MockFlowFile ff0 = testRunner.getFlowFilesForRelationship(GetSlackReaction.REL_NO_REACTION).get(0);
        ff0.assertAttributeExists(GetSlackReaction.ATTR_REACTION_POLLING_SECONDS);
        testRunner.clearTransferState();
    }

    @Test
    public void testErrorResponse() throws Exception {
        //response without message
        when(clientMock.reactionsGet(any(ReactionsGetRequest.class))).thenReturn(new ReactionsGetResponse());
        testRunner.enqueue(getInputFlowFileWithAttributes());
        testRunner.run();
        testRunner.assertTransferCount(GetSlackReaction.REL_FAILURE, 1);
        testRunner.assertTransferCount(GetSlackReaction.REL_ORIGINAL, 1);
        final MockFlowFile ff0 = testRunner.getFlowFilesForRelationship(GetSlackReaction.REL_FAILURE).get(0);
        ff0.assertAttributeExists(GetSlackReaction.ATTR_ERROR_MESSAGE);
        ff0.assertAttributeEquals(GetSlackReaction.ATTR_CHANNEL_ID, TEST_CHANNEL_ID);
        ff0.assertAttributeEquals(GetSlackReaction.ATTR_MESSAGE_TIMESTAMP, TEST_MESSAGE_TS);
        testRunner.clearTransferState();
    }

    @Test
    public void testJsonInput() throws Exception {
        testRunner.setProperty(GetSlackReaction.MESSAGE_IDENTIFIER_STRATEGY, "JSON Path");
        when(clientMock.reactionsGet(any(ReactionsGetRequest.class))).thenReturn(fetchReactions("thumbs_up", 1));
        testRunner.enqueue(getInputFlowFileWithJsonContent());
        testRunner.run();
        testRunner.assertTransferCount(GetSlackReaction.REL_SUCCESS, 2);
        testRunner.assertTransferCount(GetSlackReaction.REL_ORIGINAL, 1);
        final List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(GetSlackReaction.REL_SUCCESS);
        final MockFlowFile ff0 = successFiles.get(0);
        final MockFlowFile ff1 = successFiles.get(1);

        ff0.assertContentEquals("This is the first message.");
        ff0.assertAttributeEquals(GetSlackReaction.ATTR_CHANNEL_ID, "C1234567ABC");
        ff0.assertAttributeEquals(GetSlackReaction.ATTR_MESSAGE_TIMESTAMP, "1720095796.550329");
        ff0.assertAttributeNotExists(GetSlackReaction.ATTR_LAST_REACTION_CHECK_TIMESTAMP);
        ff0.assertAttributeExists(GetSlackReaction.ATTR_REACTION_POLLING_SECONDS);

        ff1.assertContentEquals("This is the second message.");
        ff1.assertAttributeEquals(GetSlackReaction.ATTR_CHANNEL_ID, "C1234567ABC");
        ff1.assertAttributeEquals(GetSlackReaction.ATTR_MESSAGE_TIMESTAMP, "1720095797.550500");
        ff1.assertAttributeNotExists(GetSlackReaction.ATTR_LAST_REACTION_CHECK_TIMESTAMP);
        ff1.assertAttributeExists(GetSlackReaction.ATTR_REACTION_POLLING_SECONDS);
        testRunner.clearTransferState();
    }

    @Test
    public void testWrongJsonPathPropertiesOriginalFlowFileGoesToFailure() {
        testRunner.setProperty(GetSlackReaction.MESSAGE_IDENTIFIER_STRATEGY, "JSON Path");
        testRunner.setProperty(GetSlackReaction.MESSAGE_TIMESTAMP_JSON_PATH, "$.wrongTimestampPath");
        testRunner.enqueue(getInputFlowFileWithJsonContent());
        testRunner.run();
        testRunner.assertTransferCount(GetSlackReaction.REL_FAILURE, 1);
        testRunner.assertTransferCount(GetSlackReaction.REL_ORIGINAL, 0);

        final MockFlowFile ff0 = testRunner.getFlowFilesForRelationship(GetSlackReaction.REL_FAILURE).get(0);
        //the original FF went to Failure, it contains several messages, channel id and Slack timestamp can not be set in this case
        ff0.assertAttributeNotExists(GetSlackReaction.ATTR_CHANNEL_ID);
        ff0.assertAttributeNotExists(GetSlackReaction.ATTR_MESSAGE_TIMESTAMP);
        ff0.assertAttributeEquals(GetSlackReaction.ATTR_ERROR_MESSAGE, "No results for path: $['wrongTimestampPath']");

        testRunner.clearTransferState();
    }

    @Test
    public void testWaitForFurtherReactions() throws Exception {
        testRunner.setProperty(GetSlackReaction.RELEASE_PER_REACTION, "false");
        testRunner.setProperty(GetSlackReaction.WAIT_PERIOD, "5 min");
        testRunner.setProperty(GetSlackReaction.MESSAGE_IDENTIFIER_STRATEGY, "JSON Path");
        when(clientMock.reactionsGet(any(ReactionsGetRequest.class))).thenReturn(fetchReactions("thumbs_up", 1));
        testRunner.enqueue(getInputFlowFileWithJsonContentRecentMessage());
        testRunner.run();
        testRunner.assertTransferCount(GetSlackReaction.REL_WAIT, 1);
        testRunner.assertTransferCount(GetSlackReaction.REL_ORIGINAL, 1);
        MockFlowFile waitingFF = testRunner.getFlowFilesForRelationship(GetSlackReaction.REL_WAIT).get(0);
        waitingFF.assertAttributeExists(GetSlackReaction.ATTR_LAST_REACTION_CHECK_TIMESTAMP);
        waitingFF.assertAttributeExists(GetSlackReaction.ATTR_CHANNEL_ID);
        waitingFF.assertAttributeExists(GetSlackReaction.ATTR_MESSAGE_TIMESTAMP);
        assertEquals(1, testRunner.getPenalizedFlowFiles().size());
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

    private MockFlowFile getInputFlowFileWithAttributes() {
        final MockFlowFile inputFlowFile = new MockFlowFile(3);
        Map<String, String> attributes = Map.of(GetSlackReaction.ATTR_CHANNEL_ID, TEST_CHANNEL_ID,
                GetSlackReaction.ATTR_MESSAGE_TIMESTAMP, TEST_MESSAGE_TS);
        inputFlowFile.putAttributes(attributes);
        return inputFlowFile;
    }

    private MockFlowFile getInputFlowFileWithJsonContent() {
        final MockFlowFile inputFlowFile = new MockFlowFile(5);
        inputFlowFile.setData("""
                [ {
                "type" : "message",
                "channel" : "C1234567ABC",
                "text" : "This is the first message.",
                "ts" : "1720095796.550329",
                }, {
                "type" : "message",
                "channel" : "C1234567ABC",
                 "text" : "This is the second message.",
                "ts" : "1720095797.550500",
                }]""".getBytes(UTF_8));
        return inputFlowFile;
    }

    private MockFlowFile getInputFlowFileWithJsonContentRecentMessage() {
        long timestampOfRecentMessage = Instant.now().toEpochMilli() * 1000 - TimeUnit.SECONDS.toMicros(2);
        final MockFlowFile inputFlowFile = new MockFlowFile(5);
        inputFlowFile.setData("""
                [ {
                "type" : "message",
                "channel" : "C1234567ABC",
                "text" : "This is a recent message.",
                "ts" : "placeholder",
                }]""".replace("placeholder", String.valueOf(timestampOfRecentMessage)).getBytes(UTF_8));
        return inputFlowFile;
    }
}
